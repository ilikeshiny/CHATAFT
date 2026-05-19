import stoat
import asyncio
import json
import re
import time
import os
import traceback
import aiohttp
from datetime import datetime, timezone
from typing import Optional, Dict, List

from postkeep.papyrus import (
    log_info, log_error, log_warn, log_success, log_debug,
    BridgeMessage,
    should_skip_download,
    is_user_ignored,
    get_cached_avatar, store_avatar_cache, compute_avatar_hash,
    AVATAR_REFRESH_INTERVAL,
    apply_link_replacements,
    normalize_mime_type,
)

CDN_BASE = 'https://cdn.stoatusercontent.com'
IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg')


def _ulid_timestamp(ulid_str: str) -> Optional[float]:
    try:
        ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
        upper = ulid_str[:10].upper()
        ts_ms = 0
        for ch in upper:
            ts_ms = ts_ms * 32 + ENCODING.index(ch)
        return ts_ms / 1000.0
    except Exception:
        return None


class StoatchatScribe:
    def __init__(self, core):
        self.core = core
        self.client = core.client
        self.component_name = 'stoatchat_scribe'

    # ── avatar caching ───────────────────────────────

    async def _cache_stoatchat_avatar(self, message):
        """Download and cache Stoatchat avatar bytes. Each pilgrim uploads to its own platform."""
        user_id = str(message.author_id)
        if not user_id or not self.core.avatar_db:
            return

        cached = get_cached_avatar(self.core.avatar_db, user_id)
        if cached:
            age = time.time() - (cached.get('last_checked') or 0)
            if cached.get('avatar_bytes') and age < AVATAR_REFRESH_INTERVAL:
                log_debug(f"[AVATAR] Cache HIT for Stoatchat user {user_id}: age={age:.0f}s", self.component_name)
                return

        author = message.author
        avatar_obj = getattr(author, 'avatar', None)
        avatar_url_source = None
        if avatar_obj:
            avatar_id = (
                getattr(avatar_obj, '_id', None)
                or getattr(avatar_obj, 'id', None)
                or (avatar_obj.get('_id') if isinstance(avatar_obj, dict) else None)
                or (avatar_obj.get('id') if isinstance(avatar_obj, dict) else None)
            )
            if avatar_id:
                avatar_url_source = f"{CDN_BASE}/avatars/{avatar_id}"

        if not avatar_url_source:
            log_debug(f"[AVATAR] Stoatchat user {user_id} has no avatar", self.component_name)
            return

        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(avatar_url_source) as resp:
                    if resp.status != 200:
                        log_warn(f"[AVATAR] Failed to download Stoatchat avatar for {user_id}: HTTP {resp.status}", self.component_name)
                        return
                    avatar_bytes = await resp.read()
        except Exception as e:
            log_warn(f"[AVATAR] Error downloading Stoatchat avatar for {user_id}: {e}", self.component_name)
            return

        new_hash = compute_avatar_hash(avatar_bytes)
        log_debug(f"[AVATAR] Downloaded Stoatchat avatar for {user_id} ({len(avatar_bytes)} bytes, hash={new_hash[:12]}...)", self.component_name)
        store_avatar_cache(self.core.avatar_db, user_id, 'stoatchat',
                           avatar_hash=new_hash, avatar_bytes=avatar_bytes)

    # ========== Main Message Processing ==========
    # NOTE: called by BridgeClient.on_message with the message directly (not an event wrapper)

    async def process_stoatchat_message(self, message):
        import sys
        print(f"[DIAG-SCRIBE] process_stoatchat_message called: author={getattr(message, 'author_id', '?')}, channel={getattr(message, 'channel_id', '?')}", flush=True, file=sys.stderr)
        try:
            me = self.client.me
            if me and message.author_id == me.id:
                return
        except Exception as e:
            print(f"[DIAG-SCRIBE] self.client.me check failed: {e}", flush=True, file=sys.stderr)

        channel_id = str(message.channel_id)
        bridge_name, bridge_config = self.core.get_bridge_by_stoatchat_channel(channel_id)
        if not bridge_config:
            print(f"[DIAG-SCRIBE] No bridge for channel {channel_id}", flush=True, file=sys.stderr)
            log_debug(f"No bridge found for stoatchat channel {channel_id}. Known channels: {[b.get_platform('stoatchat').channel_id for _, b in self.core.bridges.items() if b.get_platform('stoatchat')]}", self.component_name)
            return

        sc = bridge_config.get_platform('stoatchat')
        if not sc:
            return

        if sc.ignore_bots and getattr(message.author, 'bot', None):
            return

        if getattr(sc, 'ignore_self', True) and message.author_id == self.client.me.id:
            return

        try:
            if is_user_ignored('stoatchat', message.author_id):
                return
        except Exception:
            pass

        if getattr(message, 'masquerade', None) and message.author_id == self.client.me.id:
            return

        # Track native StoatChat message timestamp for prefix collapse
        self.core._last_native_sc_msg[bridge_name] = time.time()

        log_info(f"Processing Stoatchat message from {getattr(message.author, 'display_name', None) or getattr(message.author, 'name', 'Unknown')}")

        attachments = []
        seen_urls = set()

        try:
            for att in (getattr(message, 'attachments', None) or []):
                try:
                    att_id = str(att.id) if hasattr(att, 'id') else ''
                    att_filename = getattr(att, 'filename', None) or getattr(att, 'name', None) or f"file_{att_id}"
                    att_url = f"{CDN_BASE}/attachments/{att_id}/{att_filename}" if att_id else None

                    if not att_url:
                        att_url = getattr(att, 'url', None)

                    if not att_url or att_url in seen_urls:
                        continue

                    if should_skip_download(att_url):
                        continue

                    file_path = await self.core.download_media(att_url, att_filename)
                    if file_path:
                        content_type = getattr(att, 'content_type', None) or getattr(att, 'metadata', {}).get('type', '')
                        att_mime = normalize_mime_type(content_type, filename=att_filename, url=att_url)

                        attachments.append({
                            'url': att_url,
                            'filename': att_filename,
                            'type': att_mime,
                            'local_path': file_path,
                        })
                        seen_urls.add(att_url)
                        log_success(f"Downloaded: {att_filename}")
                    else:
                        log_error(f"Failed to download attachment: {att_filename} from {att_url}")
                except Exception as att_exc:
                    log_debug(f"Attachment processing failed: {att_exc}", self.component_name)
                    continue
        except Exception:
            pass

        content = getattr(message, 'content', None) or ''

        reply_to_id = None
        try:
            replies = getattr(message, 'replies', None) or []
            if replies:
                first_reply = replies[0]
                reply_id_raw = getattr(first_reply, 'id', None) or (first_reply if isinstance(first_reply, str) else None)
                if reply_id_raw:
                    reply_to_id = str(reply_id_raw)
        except Exception:
            pass

        author = message.author
        author_name = (
            getattr(author, 'display_name', None)
            or getattr(author, 'name', None)
            or 'Unknown'
        )

        ts = _ulid_timestamp(str(message.id))
        if ts is None:
            ts = time.time()

        channel = getattr(message, 'channel', None)
        channel_name = getattr(channel, 'name', None) or ''

        if not attachments and not (content or '').strip():
            content = "This format is not supported"

        content = apply_link_replacements(content)

        await self._cache_stoatchat_avatar(message)

        try:
            bridge_msg = BridgeMessage(
                bridge_name=bridge_name,
                message_id=str(message.id),
                channel_id=channel_id,
                author_name=author_name,
                author_id=str(message.author_id),
                content=content,
                attachments=attachments,
                reply_to_id=reply_to_id,
                is_forward=False,
                forward_from=None,
                timestamp=ts,
                source='stoatchat',
                channel_name=channel_name,
            )
            json_body = bridge_msg.to_json()
            log_debug(f"BridgeMessage created OK, json_len={len(json_body)}", self.component_name)
        except Exception as e:
            log_error(f"BridgeMessage creation failed: {e}", self.component_name)
            traceback.print_exc()
            return

        log_debug(f"Publishing to '{self.core.queues['scribe_stoatchat']}', msg_id={bridge_msg.message_id}, author={bridge_msg.author_name}", self.component_name)
        self.core._safe_publish(self.core.queues['scribe_stoatchat'], json_body)
        log_debug(f"_safe_publish returned OK", self.component_name)
        await asyncio.sleep(0.05)

    # ========== Edit / Delete ==========
    # NOTE: these receive the full event object (MessageUpdateEvent / MessageDeleteEvent)

    async def handle_message_edit(self, event):
        try:
            # MessageUpdateEvent has .message (the updated message object)
            updated_msg = getattr(event, 'message', None)
            if not updated_msg:
                return

            message_id = str(getattr(updated_msg, 'id', None) or getattr(event, 'message_id', ''))
            channel_id = str(getattr(updated_msg, 'channel_id', None) or getattr(event, 'channel_id', ''))

            if not message_id or not channel_id:
                return

            bridge_name, bridge_config = self.core.get_bridge_by_stoatchat_channel(channel_id)
            if not bridge_config:
                return

            # Skip edits made by the bot itself (e.g. pilgrim editing a masqueraded message)
            author_id = getattr(updated_msg, 'author_id', None)
            if author_id and author_id == self.client.me.id:
                return

            try:
                pilgrim = self.client._pilgrim
                exp = pilgrim._local_stoatchat_edits.get(message_id)
                if exp and exp > time.time():
                    return
                elif exp and exp <= time.time():
                    del pilgrim._local_stoatchat_edits[message_id]
            except Exception:
                pass

            db = self.core.bridge_dbs.get(bridge_name)
            if not db:
                return

            new_content = getattr(updated_msg, 'content', None)
            if new_content is None:
                return

            # Resolve author name for prefix reconstruction
            # Update events often only carry changed fields - author may be missing
            author_name = ''
            try:
                author_obj = getattr(updated_msg, 'author', None)
                if author_obj:
                    author_name = getattr(author_obj, 'display_name', None) or getattr(author_obj, 'name', '') or ''
            except Exception:
                pass

            if not author_name:
                try:
                    ch = self.client.get_channel(channel_id)
                    if ch:
                        full_msg = await ch.fetch_message(message_id)
                        if full_msg:
                            author_obj = getattr(full_msg, 'author', None)
                            if author_obj:
                                author_name = getattr(author_obj, 'display_name', None) or getattr(author_obj, 'name', '') or ''
                except Exception:
                    pass

            channel_name = ''
            try:
                ch = self.client.get_channel(channel_id)
                if ch:
                    channel_name = getattr(ch, 'name', '') or ''
            except Exception:
                pass

            mappings = db.get_all_mappings('stoatchat', message_id)
            if mappings:
                # Single body with all target IDs - arbiter routes once
                # per target queue, each pilgrim picks out its own
                # *_message_id. Publishing one body per mapping multiplies
                # arbiter log lines and RabbitMQ traffic by len(mappings).
                edit_msg = {
                    'type': 'edit',
                    'source': 'stoatchat',
                    'bridge_name': bridge_name,
                    'new_text': new_content,
                    'author_name': author_name,
                    'channel_name': channel_name,
                }
                for platform, platform_id in mappings.items():
                    edit_msg[f'{platform}_message_id'] = platform_id
                self.core._safe_publish(self.core.queues['scribe_stoatchat'], json.dumps(edit_msg))

        except Exception as e:
            log_error(f"Failed to handle message edit: {e}", self.component_name)
            traceback.print_exc()

    async def handle_message_delete(self, event):
        try:
            # MessageDeleteEvent has .message_id and .channel_id directly
            message_id = str(getattr(event, 'message_id', ''))
            channel_id = str(getattr(event, 'channel_id', ''))

            if not message_id or not channel_id:
                return

            bridge_name, bridge_config = self.core.get_bridge_by_stoatchat_channel(channel_id)
            if not bridge_config:
                return

            db = self.core.bridge_dbs.get(bridge_name)
            if not db:
                return

            # Stoatchat delete events don't carry actor info on the event
            # itself - best effort defensive lookup, falls back to empty.
            actor_id = (
                getattr(event, 'user_id', None)
                or getattr(event, 'author_id', None)
                or getattr(event, 'deleted_by', None)
                or ''
            )
            actor_name = ''
            if actor_id:
                try:
                    user = await self.client.fetch_user(str(actor_id))
                    actor_name = getattr(user, 'display_name', None) or getattr(user, 'username', None) or str(actor_id)
                except Exception:
                    actor_name = str(actor_id)

            mappings = db.get_all_mappings('stoatchat', message_id)
            if mappings:
                # Single body with all target IDs (see edit handler for why).
                delete_msg = {
                    'type': 'delete',
                    'source': 'stoatchat',
                    'bridge_name': bridge_name,
                    'author_name': actor_name,
                }
                for platform, platform_id in mappings.items():
                    delete_msg[f'{platform}_message_id'] = platform_id
                self.core._safe_publish(self.core.queues['scribe_stoatchat'], json.dumps(delete_msg))

        except Exception as e:
            log_error(f"Failed to handle message delete: {e}", self.component_name)

    # ========== Member Events ==========

    async def handle_member_join(self, event):
        try:
            server_id = str(getattr(event, 'server_id', ''))
            user_id = str(getattr(event, 'user_id', ''))

            user = None
            try:
                user = self.client.get_user(event.user_id)
                if not user:
                    user = await self.client.fetch_user(event.user_id)
            except Exception:
                pass

            display_name = getattr(user, 'display_name', None) or getattr(user, 'name', None) or user_id

            for bridge_name, bridge_config in self.core.bridges.items():
                sc = bridge_config.get_platform('stoatchat')
                if not sc:
                    continue

                system_msg = BridgeMessage(
                    bridge_name=bridge_name,
                    message_id=str(int(time.time() * 1000)),
                    channel_id=sc.channel_id,
                    author_name="Stoatchat System",
                    author_id=None,
                    content=f"\u27a1\ufe0f **{display_name}** joined the server.",
                    attachments=[],
                    reply_to_id=None,
                    is_forward=False,
                    forward_from=None,
                    timestamp=time.time(),
                    source='stoatchat',
                    metadata={'is_system_message': True}
                )
                self.core._safe_publish(self.core.queues['scribe_stoatchat'], system_msg.to_json())
                log_info(f"Member join event published for {display_name} on bridge {bridge_name}", self.component_name)
        except Exception as e:
            log_error(f"Failed to handle member join: {e}", self.component_name)

    async def handle_member_leave(self, event):
        try:
            server_id = str(getattr(event, 'server_id', ''))
            user_id = str(getattr(event, 'user_id', ''))

            user = None
            try:
                user = self.client.get_user(event.user_id)
            except Exception:
                pass

            display_name = getattr(user, 'display_name', None) or getattr(user, 'name', None) or user_id

            for bridge_name, bridge_config in self.core.bridges.items():
                sc = bridge_config.get_platform('stoatchat')
                if not sc:
                    continue

                system_msg = BridgeMessage(
                    bridge_name=bridge_name,
                    message_id=str(int(time.time() * 1000)),
                    channel_id=sc.channel_id,
                    author_name="Stoatchat System",
                    author_id=None,
                    content=f"\u2b05\ufe0f **{display_name}** left the server.",
                    attachments=[],
                    reply_to_id=None,
                    is_forward=False,
                    forward_from=None,
                    timestamp=time.time(),
                    source='stoatchat',
                    metadata={'is_system_message': True}
                )
                self.core._safe_publish(self.core.queues['scribe_stoatchat'], system_msg.to_json())
                log_info(f"Member leave event published for {display_name} on bridge {bridge_name}", self.component_name)
        except Exception as e:
            log_error(f"Failed to handle member leave: {e}", self.component_name)
