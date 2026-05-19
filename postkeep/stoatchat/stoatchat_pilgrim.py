import stoat
import asyncio
import io
import pika
import json
import re
import time
import os
import threading
import aiohttp
import traceback
from typing import Optional, Dict

from postkeep.papyrus import (
    log_info, log_error, log_warn, log_success, log_debug,
    BridgeMessage,
    make_direction,
    get_cached_avatar, store_avatar_cache, compute_avatar_hash,
    AVATAR_REFRESH_INTERVAL,
)

CDN_BASE = 'https://cdn.stoatusercontent.com'

MAX_ATTACHMENTS = 5


class StoatchatPilgrim:
    def __init__(self, core):
        self.core = core
        self.client = core.client
        self.component_name = 'stoatchat_pilgrim'
        self._loop = None

        self._order_buffers = {}
        self._order_locks = {}
        self._last_sent_ts = {}
        self._local_stoatchat_edits = {}

        # Prefix collapsing state (per bridge_name)
        self._last_bridged_author = {}  # {bridge_name: "source:author_name"}
        self._last_bridged_time = {}    # {bridge_name: timestamp}

    def _update_collapse_tracking(self, bridge_msg: BridgeMessage):
        """Update prefix-collapse state after a successful bridged send."""
        bname = bridge_msg.bridge_name
        author_key = f"{bridge_msg.source}:{bridge_msg.author_name}"
        self._last_bridged_author[bname] = author_key
        self._last_bridged_time[bname] = time.time()

    async def _schedule_file_cleanup(self, paths, delay=120):
        """Delay file deletion so other pilgrims can read the same files."""
        await asyncio.sleep(delay)
        for p in paths:
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

    # ── masquerade avatar resolution ─────────────────

    async def _resolve_avatar_url(self, bridge_msg: BridgeMessage) -> Optional[str]:
        uid = bridge_msg.author_id
        if not uid:
            return None

        uid = str(uid).strip()
        if not uid:
            return None

        avatar_db = getattr(self.core, 'avatar_db', None)
        if not avatar_db:
            return None

        cached = get_cached_avatar(avatar_db, uid)
        if not cached:
            return None

        stoatchat_url = cached.get('stoatchat_url')
        if stoatchat_url:
            log_debug(f"[AVATAR] Stoatchat URL found in cache for {uid}: {stoatchat_url[:60]}...", self.component_name)
            return stoatchat_url

        avatar_bytes = cached.get('avatar_bytes')
        if avatar_bytes:
            log_debug(f"[AVATAR] No stoatchat_url but have avatar_bytes for {uid}, uploading to Stoatchat channel...", self.component_name)
            uploaded_url = await self._upload_avatar_to_stoatchat_channel(avatar_bytes, uid)
            if uploaded_url:
                return uploaded_url

        return None

    async def _upload_avatar_to_stoatchat_channel(self, avatar_bytes: bytes, user_id: str) -> Optional[str]:
        upload_channel_id = getattr(self.core, 'avatar_upload_channel_id', None)
        if not upload_channel_id:
            log_warn("[AVATAR] No Stoatchat avatar upload channel configured", self.component_name)
            return None

        try:
            channel = self.client.get_channel(upload_channel_id)
            if not channel:
                try:
                    channel = await self.client.fetch_channel(upload_channel_id)
                except Exception as e:
                    log_error(f"[AVATAR] Failed to fetch Stoatchat upload channel {upload_channel_id}: {e}", self.component_name)
                    return None

            safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', str(user_id))
            filename = f"avatar_{safe_id}.png"
            file_id = await self._upload_bytes_to_autumn(avatar_bytes, filename)
            if not file_id:
                log_warn(f"[AVATAR] Autumn upload failed for user {user_id}", self.component_name)
                return None

            msg = await channel.send(content=f"Avatar for {user_id}", attachments=[file_id])
            if not msg:
                log_warn(f"[AVATAR] Channel send returned no message for user {user_id}", self.component_name)
                return None

            stoatchat_url = None
            msg_attachments = getattr(msg, 'attachments', None) or []
            for att in msg_attachments:
                att_id = getattr(att, 'id', None) or (att.get('id') if isinstance(att, dict) else None)
                att_name = getattr(att, 'filename', None) or getattr(att, 'name', None) or filename
                if att_id:
                    stoatchat_url = f"{CDN_BASE}/attachments/{att_id}/{att_name}"
                    break

            if not stoatchat_url and file_id:
                stoatchat_url = f"{CDN_BASE}/attachments/{file_id}/{filename}"

            if stoatchat_url:
                avatar_db = getattr(self.core, 'avatar_db', None)
                if avatar_db:
                    store_avatar_cache(
                        avatar_db, user_id, None,
                        stoatchat_url=stoatchat_url,
                        stoatchat_channel_id=str(upload_channel_id),
                        stoatchat_message_id=str(getattr(msg, 'id', '')),
                    )
                log_success(f"[AVATAR] Uploaded avatar to Stoatchat for {user_id}: {stoatchat_url[:60]}...", self.component_name)
                return stoatchat_url

            log_warn(f"[AVATAR] Could not extract URL from Stoatchat upload for {user_id}", self.component_name)
            return None
        except Exception as e:
            log_error(f"[AVATAR] Stoatchat channel upload failed for {user_id}: {e}", self.component_name)
            return None

    async def _upload_bytes_to_autumn(self, data: bytes, filename: str) -> Optional[str]:
        autumn_url = f'{CDN_BASE}/attachments'
        token = self.core.settings.get('STOATCHAT_TOKEN', '')
        headers = {}
        if token:
            headers['X-Bot-Token'] = token
        try:
            async with aiohttp.ClientSession() as session:
                form = aiohttp.FormData()
                form.add_field('file', io.BytesIO(data), filename=filename)
                async with session.post(autumn_url, data=form, headers=headers) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        return result.get('id')
                    else:
                        body = await resp.text()
                        log_error(f"[AVATAR] Autumn bytes upload failed: HTTP {resp.status}: {body}", self.component_name)
        except Exception as e:
            log_error(f"[AVATAR] Autumn bytes upload error: {e}", self.component_name)
        return None

    def _start_weekly_avatar_refresh(self):
        def _refresh_loop():
            while True:
                try:
                    time.sleep(AVATAR_REFRESH_INTERVAL)
                    log_info("[AVATAR-REFRESH] Starting weekly Stoatchat avatar URL validity check", self.component_name)
                    self._run_stoatchat_avatar_refresh()
                except Exception as e:
                    log_error(f"[AVATAR-REFRESH] Error in refresh loop: {e}", self.component_name)

        t = threading.Thread(target=_refresh_loop, daemon=True)
        t.start()
        log_info("[AVATAR-REFRESH] Weekly Stoatchat avatar refresh thread started", self.component_name)

    def _run_stoatchat_avatar_refresh(self):
        avatar_db = getattr(self.core, 'avatar_db', None)
        if not avatar_db:
            return
        try:
            rows = avatar_db.execute(
                "SELECT user_id, avatar_hash, stoatchat_url, avatar_bytes "
                "FROM avatar_cache WHERE stoatchat_url IS NOT NULL"
            ).fetchall()
        except Exception as e:
            log_error(f"[AVATAR-REFRESH] DB query failed: {e}", self.component_name)
            return

        loop = asyncio.new_event_loop()
        try:
            for row in rows:
                uid, old_hash, old_url, avatar_bytes = row
                try:
                    loop.run_until_complete(self._refresh_single_stoatchat_url(uid, old_url, avatar_bytes))
                except Exception as e:
                    log_warn(f"[AVATAR-REFRESH] Failed for user {uid}: {e}", self.component_name)
        finally:
            loop.close()

    async def _refresh_single_stoatchat_url(self, user_id: str, old_url: str, avatar_bytes: bytes):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.head(old_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        log_debug(f"[AVATAR-REFRESH] Stoatchat URL still valid for {user_id}", self.component_name)
                        store_avatar_cache(self.core.avatar_db, user_id, None)
                        return
        except Exception as e:
            log_debug(f"[AVATAR-REFRESH] URL check failed for {user_id}: {e}", self.component_name)

        if avatar_bytes:
            log_info(f"[AVATAR-REFRESH] Stoatchat URL broken for {user_id}, re-uploading...", self.component_name)
            new_url = await self._upload_avatar_to_stoatchat_channel(avatar_bytes, user_id)
            if new_url:
                log_success(f"[AVATAR-REFRESH] Re-uploaded Stoatchat avatar for {user_id}: {new_url[:60]}...", self.component_name)
        else:
            log_warn(f"[AVATAR-REFRESH] Stoatchat URL broken for {user_id} but no avatar_bytes to re-upload", self.component_name)

    # ── file upload to autumn ─────────────────────────

    async def _upload_to_autumn(self, file_path: str, filename: str,
                                max_retries: int = 3) -> Optional[str]:
        autumn_url = f'{CDN_BASE}/attachments'
        token = self.core.settings.get('STOATCHAT_TOKEN', '')
        headers = {}
        if token:
            headers['X-Bot-Token'] = token

        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    with open(file_path, 'rb') as f:
                        form = aiohttp.FormData()
                        form.add_field('file', f, filename=filename)

                        async with session.post(autumn_url, data=form, headers=headers) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                file_id = data.get('id')
                                if file_id:
                                    log_debug(f"Uploaded to Autumn: {filename} -> {file_id}", self.component_name)
                                    return file_id
                            elif resp.status == 429:
                                retry_after = float(resp.headers.get('Retry-After', 2 ** attempt))
                                log_warn(
                                    f"Autumn rate-limited on {filename}, "
                                    f"retry {attempt + 1}/{max_retries} after {retry_after:.1f}s",
                                    self.component_name
                                )
                                await asyncio.sleep(retry_after)
                                continue
                            else:
                                body = await resp.text()
                                log_error(f"Autumn upload failed: HTTP {resp.status}: {body}", self.component_name)
                                if resp.status >= 500:
                                    await asyncio.sleep(1.0 * (attempt + 1))
                                    continue
                                return None
            except Exception as e:
                log_error(f"Failed to upload {filename} to Autumn (attempt {attempt + 1}): {e}", self.component_name)
                if attempt < max_retries - 1:
                    await asyncio.sleep(1.0 * (attempt + 1))
        return None

    # ── sending ──────────────────────────────────────

    async def _wait_for_ready(self, timeout: float = 15.0):
        """Wait for the stoat client to be connected and ready."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if self.client.me and getattr(self.client, '_ready_fired', False):
                    return True
            except Exception:
                pass
            await asyncio.sleep(1)
        return False

    async def handle_incoming_message(self, bridge_msg: BridgeMessage):
        try:
            try:
                if not self.client.me:
                    await self._wait_for_ready(timeout=10)
            except Exception:
                pass

            bridge_config = self.core.bridges.get(bridge_msg.bridge_name)
            if not bridge_config:
                log_error(f"Bridge {bridge_msg.bridge_name} not found", self.component_name)
                return

            sc = bridge_config.get_platform('stoatchat')
            if not sc:
                log_error(f"No stoatchat platform for bridge {bridge_msg.bridge_name}", self.component_name)
                return

            channel_id = sc.channel_id
            channel = self.client.get_channel(channel_id)
            if not channel:
                try:
                    channel = await self.client.fetch_channel(channel_id)
                except Exception as e:
                    log_error(f"Failed to fetch stoatchat channel {channel_id}: {e}", self.component_name)
                    return

            use_masquerade = getattr(sc, 'use_masquerade', True)

            content = bridge_msg.content or ''
            # Strip spoiler markers - StoatChat has no spoiler support
            content = re.sub(r'\|\|(.+?)\|\|', r'\1', content)

            use_prefixes = getattr(sc, 'use_prefixes', True)
            skip_prefix = False
            if use_prefixes and not use_masquerade:
                # Check if prefix should be collapsed
                bname = bridge_msg.bridge_name
                if getattr(sc, 'collapse_prefixes', False) and not bridge_msg.attachments and not bridge_msg.is_forward:
                    author_key = f"{bridge_msg.source}:{bridge_msg.author_name}"
                    now = time.time()
                    last_author = self._last_bridged_author.get(bname)
                    last_time = self._last_bridged_time.get(bname, 0.0)
                    native_ts = self.core._last_native_sc_msg.get(bname, 0.0)
                    if (last_author == author_key
                            and (now - last_time) < 120
                            and native_ts < last_time):
                        skip_prefix = True

                if not skip_prefix:
                    source_label = (bridge_msg.source or 'Unknown').capitalize()
                    channel_suffix = ''
                    if getattr(sc, 'add_channel_name', False) and bridge_msg.channel_name:
                        channel_suffix = f", #{bridge_msg.channel_name}"
                    prefix = f"**[{source_label}] {bridge_msg.author_name}{channel_suffix}**"
                    if bridge_msg.is_forward and bridge_msg.forward_from:
                        prefix += f" (fwd: {bridge_msg.forward_from})"
                    content = f"{prefix}\n{content}" if content else prefix

            masquerade = None
            if use_masquerade:
                avatar_url = await self._resolve_avatar_url(bridge_msg)
                display_name = bridge_msg.author_name or 'Unknown'

                if use_prefixes:
                    source_label = (bridge_msg.source or '').capitalize()
                    if source_label:
                        display_name = f"[{source_label}] {display_name}"

                # Add channel name only if it fits within 32 chars
                if getattr(sc, 'add_channel_name', False) and bridge_msg.channel_name:
                    ch_suffix = f", #{bridge_msg.channel_name}"
                    if len(display_name) + len(ch_suffix) <= 32:
                        display_name += ch_suffix

                # Add forward info only if it fits; otherwise move to content
                if bridge_msg.is_forward and bridge_msg.forward_from:
                    fwd_suffix = f" ⤳ {bridge_msg.forward_from}"
                    if len(display_name) + len(fwd_suffix) <= 32:
                        display_name += fwd_suffix
                    else:
                        content_prefix = f"*forwarded from: {bridge_msg.forward_from}*\n"
                        content = f"{content_prefix}{content}" if content else content_prefix

                masquerade = stoat.MessageMasquerade(
                    name=display_name[:32],
                    avatar=avatar_url,
                )

            replies_list = None
            raw_reply = bridge_msg.reply_to_id
            if raw_reply:
                db = self.core.bridge_dbs.get(bridge_msg.bridge_name)
                if db:
                    stoatchat_reply_id = None
                    source_lower = (bridge_msg.source or '').lower()

                    lookup_source = source_lower
                    lookup_id = raw_reply

                    source_reply_platform = None
                    for attr in ('metadata', 'reply_metadata', 'extra'):
                        payload = getattr(bridge_msg, attr, None)
                        if isinstance(payload, dict) and payload.get('source_reply_platform'):
                            source_reply_platform = payload['source_reply_platform']
                            break

                    if source_reply_platform:
                        lookup_source = source_reply_platform

                    if lookup_source and lookup_id:
                        try:
                            stoatchat_reply_id = db.get_mapped_id(lookup_source, str(lookup_id), 'stoatchat')
                        except Exception as e:
                            log_debug(f"Reply lookup failed: {e}", self.component_name)

                    if stoatchat_reply_id:
                        try:
                            replies_list = [stoat.Reply(id=stoatchat_reply_id, mention=False)]
                        except Exception:
                            try:
                                replies_list = [{'id': stoatchat_reply_id, 'mention': False}]
                            except Exception:
                                pass

            attachment_ids = []
            for idx, att in enumerate(bridge_msg.attachments or []):
                file_path = att.get('local_path')
                if not file_path or not os.path.exists(file_path):
                    continue
                if idx > 0:
                    await asyncio.sleep(0.3)
                filename = att.get('filename') or os.path.basename(file_path)
                file_id = await self._upload_to_autumn(file_path, filename)
                if file_id:
                    attachment_ids.append(file_id)

            if not attachment_ids and not (content or '').strip():
                names = [att.get('filename') for att in (bridge_msg.attachments or []) if att.get('filename')]
                if names:
                    content = f"`Files: {', '.join(names)}`"
                else:
                    content = "This format is not supported"

            send_kwargs = {}
            if content and content.strip():
                send_kwargs['content'] = content
            if masquerade:
                send_kwargs['masquerade'] = masquerade
            if replies_list:
                send_kwargs['replies'] = replies_list

            chunks = [attachment_ids[i:i + MAX_ATTACHMENTS]
                      for i in range(0, len(attachment_ids), MAX_ATTACHMENTS)] if attachment_ids else [None]

            sent = None
            for idx, chunk in enumerate(chunks):
                kwargs = dict(send_kwargs) if idx == 0 else {}
                if idx > 0 and masquerade:
                    kwargs['masquerade'] = masquerade
                if chunk:
                    kwargs['attachments'] = chunk

                msg = None
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        # Re-fetch channel if retrying (may be stale after reconnect)
                        if attempt > 0:
                            log_info(f"Retry {attempt}/{max_retries} sending to Stoatchat...", self.component_name)
                            await self._wait_for_ready(timeout=10)
                            channel = self.client.get_channel(channel_id)
                            if not channel:
                                channel = await self.client.fetch_channel(channel_id)
                        msg = await channel.send(**kwargs)
                        break
                    except Exception as e:
                        error_str = str(e)
                        if 'masquerade' in error_str.lower() or 'permission' in error_str.lower():
                            log_warn(f"Masquerade failed, sending as plain bot message: {e}", self.component_name)
                            kwargs.pop('masquerade', None)
                            if idx == 0 and use_prefixes:
                                source_label = (bridge_msg.source or 'Unknown').capitalize()
                                plain_content = f"**[{source_label}] {bridge_msg.author_name}**\n{bridge_msg.content or ''}"
                                kwargs['content'] = plain_content
                            try:
                                msg = await channel.send(**kwargs)
                                break
                            except Exception:
                                if attempt < max_retries - 1:
                                    await asyncio.sleep(3)
                                    continue
                                raise
                        elif attempt < max_retries - 1:
                            log_warn(f"Send failed (attempt {attempt+1}): {e}, retrying after reconnect...", self.component_name)
                            await asyncio.sleep(3)
                        else:
                            raise

                if idx == 0:
                    sent = msg

            paths = [att.get('local_path') for att in (bridge_msg.attachments or []) if att.get('local_path')]
            if paths:
                asyncio.create_task(self._schedule_file_cleanup(paths))

            if sent and bridge_msg.bridge_name in self.core.bridge_dbs:
                db = self.core.bridge_dbs[bridge_msg.bridge_name]
                sent_id = str(sent.id)
                try:
                    direction = make_direction(bridge_msg.source, 'stoatchat') if bridge_msg.source else None
                    db.store_mapping(bridge_msg.source, bridge_msg.message_id, 'stoatchat', sent_id, direction)
                    for extra_id in (bridge_msg.metadata or {}).get('media_group_ids', []):
                        db.store_mapping(bridge_msg.source, extra_id, 'stoatchat', sent_id, direction)
                except Exception as map_exc:
                    log_debug(f"Failed to store message mapping: {map_exc}", self.component_name)

            self._update_collapse_tracking(bridge_msg)
            log_success(f"Sent {bridge_msg.source} message to Stoatchat channel {channel_id}")

        except Exception as e:
            log_error(f"Error sending to Stoatchat: {e}")
            traceback.print_exc()

    # ── edit / delete from other platforms ───────────

    async def handle_edit(self, data):
        try:
            bridge_name = data.get('bridge_name')
            bridge = self.core.bridges.get(bridge_name)
            if not bridge:
                return

            sc = bridge.get_platform('stoatchat')
            if not sc:
                return
            if not getattr(sc, 'cross_edit', True):
                log_debug(f"cross_edit disabled for stoatchat in bridge '{bridge_name}', skipping", self.component_name)
                return

            db = self.core.bridge_dbs.get(bridge_name)
            if not db:
                return

            new_text = data.get('new_text') or ''

            # Only act when explicitly addressed. Looking up via a sibling
            # target's message_id causes one edit per sibling platform.
            stoatchat_id = data.get('stoatchat_message_id')
            if not stoatchat_id:
                return

            channel_id = sc.channel_id
            channel = self.client.get_channel(channel_id)
            if not channel:
                try:
                    channel = await self.client.fetch_channel(channel_id)
                except Exception:
                    return

            edited = False
            # Record BEFORE the edit call - the websocket event can arrive
            # before msg.edit() returns, causing the scribe to echo it back.
            self._local_stoatchat_edits[str(stoatchat_id)] = time.time() + 15.0
            try:
                msg = await channel.fetch_message(stoatchat_id)
                await msg.edit(content=new_text)
                log_debug(f"Edited stoatchat message {stoatchat_id}", self.component_name)
                edited = True
            except Exception as e:
                log_warn(f"Failed to edit stoatchat message {stoatchat_id}: {e}", self.component_name)
                del self._local_stoatchat_edits[str(stoatchat_id)]

            if not edited:
                try:
                    # Extract avatar from original message, then delete and re-send
                    avatar_url = None
                    try:
                        old_msg = await channel.fetch_message(stoatchat_id)
                        old_masq = getattr(old_msg, 'masquerade', None)
                        if old_masq:
                            avatar_url = getattr(old_masq, 'avatar', None)
                        await old_msg.delete()
                    except Exception:
                        pass

                    send_kwargs = {'content': new_text}
                    use_masquerade = getattr(sc, 'use_masquerade', True)
                    if use_masquerade:
                        source = data.get('source', 'unknown')
                        author_name = data.get('author_name', '') or 'Unknown'
                        display_name = author_name
                        use_prefixes = getattr(sc, 'use_prefixes', True)
                        if use_prefixes:
                            source_label = source.capitalize()
                            if source_label:
                                display_name = f"[{source_label}] {display_name}"
                        send_kwargs['masquerade'] = stoat.MessageMasquerade(
                            name=display_name[:32],
                            avatar=avatar_url,
                        )
                    await channel.send(**send_kwargs)
                except Exception:
                    pass

        except Exception as e:
            log_error(f"Failed to handle edit for stoatchat: {e}", self.component_name)

    async def handle_delete(self, data):
        try:
            bridge_name = data.get('bridge_name')
            bridge = self.core.bridges.get(bridge_name)
            if not bridge:
                return

            sc = bridge.get_platform('stoatchat')
            if not sc:
                return
            if not getattr(sc, 'cross_delete', True):
                log_debug(f"cross_delete disabled for stoatchat in bridge '{bridge_name}', skipping", self.component_name)
                return

            db = self.core.bridge_dbs.get(bridge_name)
            if not db:
                return

            # Only act when explicitly addressed. Looking up via a sibling
            # target's message_id causes one delete per sibling platform.
            stoatchat_id = data.get('stoatchat_message_id')
            if not stoatchat_id:
                return

            channel_id = sc.channel_id
            channel = self.client.get_channel(channel_id)
            if not channel:
                try:
                    channel = await self.client.fetch_channel(channel_id)
                except Exception:
                    return

            try:
                # Revolt/stoat channels may lack get_partial_message; fetch then delete
                try:
                    msg = await channel.fetch_message(stoatchat_id)
                    await msg.delete()
                except Exception:
                    # Fallback: direct HTTP DELETE via the API
                    api_url = getattr(sc, 'stoatchat_api_url', 'https://stoat.chat/api')
                    token = self.core.settings.get('STOATCHAT_TOKEN', '')
                    async with aiohttp.ClientSession() as session:
                        async with session.delete(
                            f"{api_url}/channels/{channel_id}/messages/{stoatchat_id}",
                            headers={"x-bot-token": token},
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as resp:
                            if resp.status not in (200, 204):
                                raise Exception(f"HTTP {resp.status}")
                log_debug(f"Deleted stoatchat message {stoatchat_id}", self.component_name)
            except Exception as e:
                log_warn(f"Failed to delete stoatchat message {stoatchat_id}: {e}", self.component_name)

        except Exception as e:
            log_error(f"Failed to handle delete for stoatchat: {e}", self.component_name)

    # ── ordered send ─────────────────────────────────

    async def _ordered_send(self, bridge_msg: BridgeMessage):
        key = bridge_msg.bridge_name
        if key not in self._order_locks:
            self._order_locks[key] = asyncio.Lock()
        if key not in self._order_buffers:
            self._order_buffers[key] = []
        if key not in self._last_sent_ts:
            self._last_sent_ts[key] = 0.0

        ts = float(bridge_msg.timestamp or 0.0) or time.time()

        try:
            id_int = int(bridge_msg.message_id)
        except Exception:
            id_int = 0

        hold_sec = 0.5

        async with self._order_locks[key]:
            buf = self._order_buffers[key]
            buf.append((ts, id_int, time.time(), bridge_msg))
            buf.sort(key=lambda t: (t[0], t[1]))

            last_ts = self._last_sent_ts.get(key, 0.0)

            while buf:
                ts0, id0, arrival0, msg0 = buf[0]
                age = time.time() - arrival0

                if ts0 > last_ts and age < hold_sec:
                    remaining = hold_sec - age
                    await asyncio.sleep(max(0.01, min(remaining, hold_sec)))
                    continue

                buf.pop(0)
                try:
                    await self.handle_incoming_message(msg0)
                except Exception as e:
                    log_error(f"Ordered send failed: {e}")

                if ts0 > last_ts:
                    last_ts = ts0
                    self._last_sent_ts[key] = last_ts

    # ── rabbitmq consumer ────────────────────────────

    def _handle_inbound_message(self, ch, method, properties, body):
        try:
            if not self._loop:
                log_warn("Event loop not ready yet, requeuing message", self.component_name)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return

            raw = body.decode()
            log_debug(f"Received message on pilgrim_stoatchat, len={len(raw)}", self.component_name)

            try:
                data = json.loads(raw)
            except Exception:
                log_error("Stoatchat pilgrim received non-JSON message, dropping.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            is_control = (
                isinstance(data, dict)
                and data.get('type') in ('edit', 'delete')
            )

            if is_control:
                log_debug(f"Control message: type={data['type']}", self.component_name)
                if data['type'] == 'edit':
                    future = asyncio.run_coroutine_threadsafe(
                        self.handle_edit(data), self._loop
                    )
                    def _edit_done(fut):
                        try: fut.result()
                        except Exception as e: log_error(f"Edit task failed: {e}", self.component_name)
                    future.add_done_callback(_edit_done)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                elif data['type'] == 'delete':
                    future = asyncio.run_coroutine_threadsafe(
                        self.handle_delete(data), self._loop
                    )
                    def _del_done(fut):
                        try: fut.result()
                        except Exception as e: log_error(f"Delete task failed: {e}", self.component_name)
                    future.add_done_callback(_del_done)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

            bridge_msg = BridgeMessage.from_json(raw)
            log_debug(f"BridgeMessage parsed: bridge={bridge_msg.bridge_name}, author={bridge_msg.author_name}, msg_id={bridge_msg.message_id}", self.component_name)

            future = asyncio.run_coroutine_threadsafe(
                self._ordered_send(bridge_msg),
                self._loop
            )

            def _done_cb(fut):
                try:
                    fut.result()
                except Exception as e:
                    log_error(f"Ordered send task failed: {e}")
                    traceback.print_exc()
            future.add_done_callback(_done_cb)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            log_error(f"Error handling inbound message: {e}")
            traceback.print_exc()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def run_consumers(self):
        log_info("Pilgrim consumer thread started.", self.component_name)

        while True:
            try:
                if not self.core.rabbitmq_connection or self.core.rabbitmq_connection.is_closed:
                    log_info("Pilgrim attempting to connect to RabbitMQ...", self.component_name)
                    self.core.connect_rabbitmq()
                    if not self.core.rabbitmq_channel:
                        log_warn("Pilgrim connection failed, retrying in 5s...", self.component_name)
                        time.sleep(5)
                        continue

                log_info("Pilgrim connected. Setting up consumers...", self.component_name)

                self.core.rabbitmq_channel.basic_qos(prefetch_count=10)

                pilgrim_queue = self.core.queues['pilgrim_stoatchat']
                log_debug(f"Consuming from: {pilgrim_queue}", self.component_name)
                self.core.rabbitmq_channel.basic_consume(
                    queue=pilgrim_queue,
                    on_message_callback=self._handle_inbound_message,
                    auto_ack=False
                )

                log_success("Pilgrim consumers are set up. Starting consumption loop.", self.component_name)

                _heartbeat_counter = 0
                while self.core.rabbitmq_connection and not self.core.rabbitmq_connection.is_closed:
                    self.core.rabbitmq_connection.process_data_events(time_limit=1)
                    time.sleep(0.01)
                    _heartbeat_counter += 1
                    if _heartbeat_counter >= 30:  # ~every 30s (match stoat WS heartbeat)
                        _heartbeat_counter = 0
                        try:
                            self.core.send_status_update('ready', 'heartbeat')
                        except Exception:
                            pass

            except (pika.exceptions.StreamLostError, pika.exceptions.ConnectionClosedByBroker, pika.exceptions.AMQPConnectionError) as e:
                log_warn(f"RabbitMQ connection lost: {e}. Reconnecting in 5s...", self.component_name)
            except Exception as e:
                log_error(f"Pilgrim consumer thread error: {e}. Reconnecting in 5s...", self.component_name)

            log_info("Cleaning up before reconnect...", self.component_name)
            try:
                if self.core.rabbitmq_channel and self.core.rabbitmq_channel.is_open:
                    self.core.rabbitmq_channel.close()
                if self.core.rabbitmq_connection and self.core.rabbitmq_connection.is_open:
                    self.core.rabbitmq_connection.close()
            except Exception:
                pass
            self.core.rabbitmq_connection = None
            self.core.rabbitmq_channel = None
            time.sleep(5)
