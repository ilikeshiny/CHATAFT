import discord
from discord import Webhook
import asyncio
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
    ensure_cache_dir, escape_discord_emojis,
    make_direction,
    get_cached_avatar, store_avatar_cache, compute_avatar_hash,
)


class DiscordPilgrim:
    MAX_ATTACHMENTS = 10

    def __init__(self, core):
        self.core = core
        self.bot = core.bot
        self.component_name = 'discord_pilgrim'
        self._loop = None

        self._order_buffers = {}
        self._order_locks = {}
        self._last_sent_ts = {}

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

    async def _resolve_avatar_url(self, bridge_msg: BridgeMessage) -> Optional[str]:
        user_id = bridge_msg.author_id
        if not user_id:
            return None

        user_id = str(user_id).strip()
        if not user_id or not self.core.avatar_db:
            return None

        cached = get_cached_avatar(self.core.avatar_db, user_id)
        if not cached:
            log_debug(f"[AVATAR-RESOLVE] Cache MISS for {user_id}", self.component_name)
            return None

        discord_url = cached.get('discord_url')
        if discord_url:
            from postkeep.papyrus import AVATAR_REFRESH_INTERVAL
            age = time.time() - (cached.get('last_checked') or 0)
            if age < AVATAR_REFRESH_INTERVAL:
                log_debug(f"[AVATAR-RESOLVE] Cache HIT for {user_id}: url={discord_url[:60]}...", self.component_name)
                return discord_url

        avatar_bytes = cached.get('avatar_bytes')
        if avatar_bytes:
            log_debug(f"[AVATAR-RESOLVE] Uploading avatar bytes for {user_id} to Discord CDN...", self.component_name)
            url = await self._upload_avatar_bytes_to_discord(avatar_bytes, user_id)
            if url:
                avatar_hash = cached.get('avatar_hash') or compute_avatar_hash(avatar_bytes)
                store_avatar_cache(self.core.avatar_db, user_id, None,
                                   avatar_hash=avatar_hash, avatar_bytes=avatar_bytes,
                                   discord_url=url)
                return url

        if discord_url:
            log_debug(f"[AVATAR-RESOLVE] Using stale Discord URL for {user_id}", self.component_name)
            return discord_url

        log_debug(f"[AVATAR-RESOLVE] No avatar found for user_id={user_id}", self.component_name)
        return None

    async def _upload_avatar_bytes_to_discord(self, avatar_bytes: bytes, entity_id: str) -> Optional[str]:
        try:
            if not self.core.avatar_upload_channel_id:
                log_debug("[AVATAR] No avatar upload channel configured", self.component_name)
                return None

            channel = self.bot.get_channel(self.core.avatar_upload_channel_id)
            if not channel:
                try:
                    channel = await self.bot.fetch_channel(self.core.avatar_upload_channel_id)
                except Exception as e:
                    log_warn(f"[AVATAR] Failed to fetch avatar upload channel: {e}", self.component_name)
                    return None

            import io
            file = discord.File(io.BytesIO(avatar_bytes), filename=f"avatar_{entity_id}.png")
            message = await channel.send(f"Avatar for {entity_id}", file=file)

            if message.attachments:
                url = str(message.attachments[0].url)
                log_debug(f"[AVATAR] Uploaded avatar for {entity_id}: {url[:60]}...", self.component_name)
                return url

            return None
        except Exception as e:
            log_warn(f"[AVATAR] Failed to upload avatar for {entity_id}: {e}", self.component_name)
            return None

    # ── reply metadata helpers ──────────────────────────────

    @staticmethod
    def _scan_reply_hints(bridge_msg: BridgeMessage) -> dict:
        hints = {
            'reply_marker': False,
            'force_no_webhook': False,
            'raw_reply_to': None,
            'discord_id': None,
            'source_id': None,
        }
        for attr in ('reply_metadata', 'reply_context', 'reply_source', 'extra', 'metadata'):
            payload = getattr(bridge_msg, attr, None)
            if not isinstance(payload, dict):
                if payload:
                    hints['reply_marker'] = True
                continue
            if payload.get('reply_marker') or payload.get('has_reply'):
                hints['reply_marker'] = True
            if payload.get('force_no_webhook'):
                hints['force_no_webhook'] = True
            if not hints['raw_reply_to']:
                hints['raw_reply_to'] = payload.get('raw_reply_to') or payload.get('raw_reply_hint')
            if not hints['discord_id']:
                cand = payload.get('discord_message_id') or payload.get('discord_id') or payload.get('discord_reply_id')
                if cand and str(cand).strip().lstrip('-').isdigit():
                    hints['discord_id'] = str(int(str(cand).strip()))
            if not hints['source_id']:
                cand = (payload.get('source_reply_id')
                        or payload.get('telegram_message_id') or payload.get('telegram_id')
                        or payload.get('tg_message_id') or payload.get('telegram_reply_id'))
                if cand and str(cand).strip().lstrip('-').isdigit():
                    hints['source_id'] = str(int(str(cand).strip()))
        return hints

    @staticmethod
    def _normalize_raw_reply(raw):
        if isinstance(raw, str):
            s = raw.strip()
            return None if s.lower() in ('', '0', 'none', 'null', 'undefined') else s
        if isinstance(raw, (int, float)):
            return None if int(raw) == 0 else raw
        return raw

    # ── reply context resolution ─────────────────────────────

    def _resolve_reply_context(self, bridge_msg: BridgeMessage):
        raw = self._normalize_raw_reply(
            getattr(bridge_msg, '_raw_reply_to_id', None) or bridge_msg.reply_to_id
        )
        hints = self._scan_reply_hints(bridge_msg)

        reply_marker = hints['reply_marker']
        source_reply_id = hints['source_id']
        resolved_discord_id = hints['discord_id']
        force_no_webhook = hints['force_no_webhook']

        if raw is None and hints['raw_reply_to']:
            raw = hints['raw_reply_to']

        source_reply_platform = None
        for attr in ('metadata', 'reply_metadata', 'extra'):
            payload = getattr(bridge_msg, attr, None)
            if isinstance(payload, dict) and payload.get('source_reply_platform'):
                source_reply_platform = payload['source_reply_platform']
                break

        if isinstance(raw, str):
            reply_marker = True
            if source_reply_platform and source_reply_platform != 'discord':
                force_no_webhook = True
                cleaned = raw.strip().lstrip('+').lstrip('-')
                if cleaned and cleaned.isdigit():
                    source_reply_id = source_reply_id or cleaned
            elif not resolved_discord_id:
                digits = raw if raw.isdigit() else (re.search(r'\d{5,}', raw) or [None])[0]
                if isinstance(digits, re.Match):
                    digits = digits.group(0)
                if digits:
                    cand = digits.lstrip('0') or '0'
                    if cand != '0':
                        resolved_discord_id = cand
        elif isinstance(raw, (int, float)) and int(raw) != 0:
            reply_marker = True
            if int(raw) > 0 and not resolved_discord_id:
                resolved_discord_id = str(int(raw))
        elif raw is not None:
            reply_marker = True

        # ── db lookups ──
        db = self.core.bridge_dbs.get(bridge_msg.bridge_name)
        source = (bridge_msg.source or '').lower()

        if source_reply_id and db:
            cleaned = source_reply_id.lstrip('+').lstrip('-')
            if cleaned:
                lookup_platform = source_reply_platform or (source if source != 'discord' else None)
                if lookup_platform:
                    try:
                        mapped = db.get_mapped_id(lookup_platform, cleaned, 'discord')
                        if mapped:
                            resolved_discord_id = str(mapped)
                    except Exception as exc:
                        log_debug(f"Reply mapping lookup failed for {lookup_platform}:{source_reply_id}: {exc}", self.component_name)

        if resolved_discord_id and db:
            try:
                if not db.get_all_mappings('discord', resolved_discord_id):
                    resolved_discord_id = None
            except Exception:
                resolved_discord_id = None

        if not resolved_discord_id and db and raw is not None:
            if source and source != 'discord':
                try:
                    mapped = db.get_mapped_id(source, str(raw), 'discord')
                    if mapped:
                        resolved_discord_id = str(mapped)
                except Exception:
                    pass

        if resolved_discord_id:
            try:
                if int(resolved_discord_id) <= 0:
                    resolved_discord_id = None
            except Exception:
                resolved_discord_id = None

        # ── write results back ──
        bridge_msg.reply_to_id = resolved_discord_id
        if source_reply_id not in (None, ''):
            bridge_msg._source_reply_id = str(source_reply_id)
        if reply_marker:
            bridge_msg._reply_marker = True
            # Only force legacy mode if there's an actual Discord message to reply to.
            # When the reply target has no mapping (e.g. during backfill), there's no
            # reason to disable webhooks since we can't do a native reply anyway.
            if resolved_discord_id:
                force_no_webhook = True

        if reply_marker and not resolved_discord_id:
            log_debug(
                f"Reply marker on {bridge_msg.source}:{bridge_msg.message_id} "
                f"(bridge {bridge_msg.bridge_name}), no Discord mapping; sending plain.",
                self.component_name
            )

        if reply_marker:
            meta = {'reply_marker': True, 'force_no_webhook': force_no_webhook, 'resolver': 'discord_pilgrim'}
            if raw is not None:
                meta['raw_reply_to'] = str(raw)
            if source_reply_id not in (None, ''):
                meta['source_reply_id'] = str(source_reply_id)
            if resolved_discord_id:
                meta['discord_reply_id'] = str(resolved_discord_id)
            for attr in ('metadata', 'reply_metadata', 'extra'):
                existing = getattr(bridge_msg, attr, None)
                if isinstance(existing, dict):
                    for k, v in meta.items():
                        existing.setdefault(k, v)
                    break
            else:
                bridge_msg.metadata = meta

        return reply_marker, resolved_discord_id, force_no_webhook

    # ── webhook reply bypass ────────────────────────────

    async def _build_webhook_reply_bypass(self, bridge_msg: BridgeMessage, bridge_config, channel) -> Optional[str]:
        try:
            discord_reply_id = bridge_msg.reply_to_id
            if not discord_reply_id:
                src_reply_id = getattr(bridge_msg, '_source_reply_id', None)
                source = (bridge_msg.source or '').lower()
                if src_reply_id and source and source != 'discord':
                    db = self.core.bridge_dbs.get(bridge_msg.bridge_name)
                    if db:
                        try:
                            mapped = db.get_mapped_id(source, src_reply_id, 'discord')
                            if mapped:
                                discord_reply_id = str(mapped)
                        except Exception:
                            pass

            if not discord_reply_id:
                raw_reply = getattr(bridge_msg, '_raw_reply_to_id', None)
                source = (bridge_msg.source or '').lower()
                if raw_reply and source and source != 'discord':
                    db = self.core.bridge_dbs.get(bridge_msg.bridge_name)
                    if db:
                        try:
                            mapped = db.get_mapped_id(source, str(raw_reply), 'discord')
                            if mapped:
                                discord_reply_id = str(mapped)
                        except Exception:
                            pass

            if not discord_reply_id:
                return "↩️ *(reply to unknown message)*"

            try:
                ref_msg = await channel.fetch_message(int(discord_reply_id))
            except Exception:
                return "↩️ *(reply to deleted message)*"

            guild_id = getattr(channel, 'guild', None)
            guild_id = guild_id.id if guild_id else '@me'
            msg_link = f"https://discord.com/channels/{guild_id}/{channel.id}/{discord_reply_id}"

            author_id = None
            is_bot_or_webhook = False
            if ref_msg.author:
                author_id = ref_msg.author.id
                is_bot_or_webhook = ref_msg.author.bot or bool(getattr(ref_msg, 'webhook_id', None))

            if is_bot_or_webhook:
                mention = f"<@{self.bot.user.id}>"
            elif author_id:
                mention = f"<@{author_id}>"
            else:
                mention = "someone"

            return f"↩️ {mention} {msg_link}"
        except Exception as e:
            log_debug(f"Failed to build webhook reply bypass: {e}", self.component_name)
            return "↩️ *(reply)*"

    # ── inbound message handling ────────────────────────────

    async def deliver_to_channel(self, bridge_msg: BridgeMessage, force_no_webhook: bool = False):
        try:
            try:
                if not self.bot.is_ready():
                    await self.bot.wait_until_ready()
            except Exception:
                pass

            bridge_config = self.core.bridges.get(bridge_msg.bridge_name)
            if not bridge_config:
                log_error(f"Bridge {bridge_msg.bridge_name} not found", self.component_name)
                return

            ds = bridge_config.get_platform('discord')
            source_platform = bridge_config.get_platform(bridge_msg.source) if bridge_msg.source else None
            source_cfg = source_platform or ds

            reply_marker, _, force_no_webhook_from_resolve = self._resolve_reply_context(bridge_msg)
            reply_marker = reply_marker or bool(getattr(bridge_msg, '_reply_marker', False))
            force_no_webhook_flag = bool(force_no_webhook) or force_no_webhook_from_resolve

            channel = self.bot.get_channel(ds.channel_id_int) or await self.bot.fetch_channel(ds.channel_id_int)

            should_use_webhook = bool(ds.use_webhooks and ds.webhook_url)
            avatar_url = None

            if should_use_webhook and bridge_msg.author_id:
                avatar_url = await self._resolve_avatar_url(bridge_msg)

            if should_use_webhook and not avatar_url:
                log_debug(f"No avatar resolved for {bridge_msg.author_name}, webhook will use default avatar", self.component_name)

            bypass_reply = getattr(ds, 'webhook_bypass_reply', False)
            bypass_prefix = None

            if should_use_webhook and (bridge_msg.reply_to_id or reply_marker) and bypass_reply:
                bypass_prefix = await self._build_webhook_reply_bypass(
                    bridge_msg, bridge_config, channel
                )
                bridge_msg.reply_to_id = None
                reply_marker = False
                force_no_webhook_flag = False
                bridge_msg._raw_reply_to_id = None
                bridge_msg._reply_marker = False
                for attr in ('metadata', 'reply_metadata', 'extra'):
                    data = getattr(bridge_msg, attr, None)
                    if isinstance(data, dict):
                        data.pop('force_no_webhook', None)
                        data.pop('reply_marker', None)
                        data.pop('raw_reply_to', None)
            else:
                if should_use_webhook and bridge_msg.reply_to_id:
                    should_use_webhook = False
                if force_no_webhook_flag:
                    should_use_webhook = False
                if should_use_webhook and reply_marker:
                    should_use_webhook = False

            if should_use_webhook:
                await self.send_via_webhook(bridge_msg, bridge_config, bypass_prefix=bypass_prefix)
                return

            use_prefixes = source_cfg.use_prefixes
            topic_suffix = ''
            try:
                if hasattr(source_cfg, 'topic_id') and source_cfg.topic_id:
                    label = getattr(source_cfg, 'topic_label', '')
                    topic_suffix = f" · {label}" if label else f" · topic #{source_cfg.topic_id}"
            except Exception:
                topic_suffix = ''

            source_label = (bridge_msg.source or 'Unknown').capitalize()
            content = ""
            skip_prefix = False
            if use_prefixes:
                # Check if prefix should be collapsed
                bname = bridge_msg.bridge_name
                if getattr(source_cfg, 'collapse_prefixes', False) and not bridge_msg.attachments and not bridge_msg.is_forward:
                    author_key = f"{bridge_msg.source}:{bridge_msg.author_name}"
                    now = time.time()
                    last_author = self._last_bridged_author.get(bname)
                    last_time = self._last_bridged_time.get(bname, 0.0)
                    native_ts = self.core._last_native_dc_msg.get(bname, 0.0)
                    if (last_author == author_key
                            and (now - last_time) < 120
                            and native_ts < last_time):
                        skip_prefix = True

                if not skip_prefix:
                    channel_suffix = ''
                    if source_cfg.add_channel_name and bridge_msg.channel_name:
                        channel_suffix = f", #{bridge_msg.channel_name}"
                    channel_suffix += topic_suffix
                    content = f"```ini\n[{source_label}] {bridge_msg.author_name}{channel_suffix}"
                    if bridge_msg.is_forward:
                        content += f", forwarded from: {bridge_msg.forward_from}"
                    if source_cfg.add_filenames and bridge_msg.attachments:
                        names = [att.get('filename') for att in bridge_msg.attachments if att.get('filename')]
                        if names:
                            content += f", {', '.join(names)}"
                    content += "```\n"
            content += bridge_msg.content or ""

            reference = None
            if bridge_msg.reply_to_id:
                try:
                    ref_msg = await channel.fetch_message(int(bridge_msg.reply_to_id))
                    reference = ref_msg.to_reference(fail_if_not_exists=False)
                except Exception:
                    reference = None

            discord_files = await self._await_and_collect_files(bridge_msg, max_wait=10.0)

            if (not discord_files) and not (content and content.strip()):
                names = [att.get('filename') for att in (bridge_msg.attachments or []) if att.get('filename')]
                if names:
                    content = f"`Files: {', '.join(names)}`"
                else:
                    content = "This format is not supported"

            sent = None
            try:
                if discord_files:
                    chunks = [discord_files[i:i + self.MAX_ATTACHMENTS]
                              for i in range(0, len(discord_files), self.MAX_ATTACHMENTS)]
                    for idx, chunk in enumerate(chunks):
                        try:
                            if idx == 0:
                                sent = await channel.send(
                                    content=content if (content and content.strip()) else None,
                                    files=chunk,
                                    reference=reference,
                                    mention_author=False
                                )
                            else:
                                await channel.send(files=chunk)
                        except Exception:
                            if idx == 0:
                                sent = await channel.send(
                                    content=content if (content and content.strip()) else None,
                                    files=chunk,
                                    mention_author=False
                                )
                            else:
                                await channel.send(files=chunk)
                else:
                    try:
                        sent = await channel.send(content=content, reference=reference, mention_author=False)
                    except Exception:
                        sent = await channel.send(content=content, mention_author=False)
            finally:
                paths = [att.get('local_path') for att in (bridge_msg.attachments or []) if att.get('local_path')]
                if paths:
                    asyncio.create_task(self._schedule_file_cleanup(paths))

            if bridge_msg.bridge_name in self.core.bridge_dbs and sent:
                db = self.core.bridge_dbs[bridge_msg.bridge_name]
                source = (bridge_msg.source or '').lower()
                direction = make_direction(source, 'discord') if source else None
                try:
                    db.store_mapping(source or 'unknown', bridge_msg.message_id, 'discord', str(sent.id), direction)
                    for extra_id in (bridge_msg.metadata or {}).get('media_group_ids', []):
                        db.store_mapping(source or 'unknown', extra_id, 'discord', str(sent.id), direction)
                except Exception:
                    pass

            self._update_collapse_tracking(bridge_msg)
            log_success(f"Sent {bridge_msg.source or 'unknown'} message to Discord channel {getattr(channel, 'name', channel.id)}")
        except Exception as e:
            log_error(f"Error sending to Discord: {e}")
            traceback.print_exc()

    async def send_via_webhook(self, bridge_msg: BridgeMessage, bridge_config, bypass_prefix: Optional[str] = None):
        try:
            if not bypass_prefix:
                reply_marker, _, force_no_webhook = self._resolve_reply_context(bridge_msg)
                reply_marker = reply_marker or bool(getattr(bridge_msg, '_reply_marker', False))

                if force_no_webhook or reply_marker:
                    await self.deliver_to_channel(bridge_msg, force_no_webhook=True)
                    return

            avatar_url = await self._resolve_avatar_url(bridge_msg)

            ds = bridge_config.get_platform('discord')
            source_platform = bridge_config.get_platform(bridge_msg.source) if bridge_msg.source else None
            source_cfg = source_platform or ds

            base_username = bridge_msg.author_name or (bridge_msg.source or "Unknown").capitalize()
            if getattr(source_cfg, 'use_prefixes', False):
                source_label = (bridge_msg.source or 'Unknown').capitalize()
                base_username = f"[{source_label}] {base_username}"
            if bridge_msg.channel_name and ds.webhook_add_channel_in_name:
                base_username = f"{base_username} #{bridge_msg.channel_name}"
            if bridge_msg.is_forward and ds.webhook_forward_in_name and bridge_msg.forward_from:
                base_username = f"{base_username} - {bridge_msg.forward_from}"

            content = bridge_msg.content or ""
            if bridge_msg.is_forward and ds.webhook_show_forwards and not ds.webhook_forward_in_name:
                forward_text = f"forwarded from: {bridge_msg.forward_from}"
                content = f"```ini\n{forward_text}\n```{content}" if content else f"```ini\n{forward_text}\n```"

            if bypass_prefix:
                content = f"{bypass_prefix}\n{content}" if content else bypass_prefix

            files = await self._await_and_collect_files(bridge_msg, max_wait=10.0)

            if (not files) and not (content and content.strip()):
                await self.deliver_to_channel(bridge_msg, force_no_webhook=True)
                return

            async with aiohttp.ClientSession() as session:
                webhook = Webhook.from_url(ds.webhook_url, session=session)
                safe_avatar = avatar_url if avatar_url and not avatar_url.startswith('file://') else None
                if files:
                    chunks = [files[i:i + self.MAX_ATTACHMENTS]
                              for i in range(0, len(files), self.MAX_ATTACHMENTS)]
                    for idx, chunk in enumerate(chunks):
                        if idx == 0:
                            sent = await webhook.send(
                                content=content if (content and content.strip()) else None,
                                username=base_username,
                                avatar_url=safe_avatar,
                                files=chunk,
                                wait=True
                            )
                        else:
                            await webhook.send(
                                username=base_username,
                                avatar_url=safe_avatar,
                                files=chunk,
                                wait=True
                            )
                else:
                    sent = await webhook.send(
                        content=content,
                        username=base_username,
                        avatar_url=safe_avatar,
                        wait=True
                    )

            paths = [att.get('local_path') for att in (bridge_msg.attachments or []) if att.get('local_path')]
            if paths:
                asyncio.create_task(self._schedule_file_cleanup(paths))

            if bridge_msg.bridge_name in self.core.bridge_dbs:
                db = self.core.bridge_dbs[bridge_msg.bridge_name]
                source = (bridge_msg.source or '').lower()
                direction = make_direction(source, 'discord') if source else None
                try:
                    db.store_mapping(source or 'unknown', bridge_msg.message_id, 'discord', str(sent.id), direction)
                    for extra_id in (bridge_msg.metadata or {}).get('media_group_ids', []):
                        db.store_mapping(source or 'unknown', extra_id, 'discord', str(sent.id), direction)
                except Exception:
                    pass

            self._update_collapse_tracking(bridge_msg)
            log_success("Sent message via webhook to Discord")
        except Exception as e:
            log_error(f"Failed to send via webhook: {e}")
            await self.deliver_to_channel(bridge_msg, force_no_webhook=True)

    async def _await_and_collect_files(self, bridge_msg: BridgeMessage, max_wait: float = 12.0):
        attachments = bridge_msg.attachments or []

        is_animation = False
        meta = getattr(bridge_msg, 'metadata', None)
        if isinstance(meta, dict) and meta.get('is_animation'):
            is_animation = True

        if is_animation:
            for att in attachments:
                p = att.get('local_path')
                fn = (att.get('filename') or '').lower()
                if p and os.path.exists(p) and fn.endswith('.mp4'):
                    try:
                        from postkeep.papyrus import convert_video_to_gif
                        gif_path = await convert_video_to_gif(p)
                        if gif_path and os.path.exists(gif_path):
                            att['local_path'] = gif_path
                            base = os.path.splitext(att.get('filename', 'animation'))[0]
                            att['filename'] = base + '.gif'
                            att['type'] = 'image/gif'
                            try:
                                os.remove(p)
                            except Exception:
                                pass
                            log_debug(f"Converted animation mp4 to GIF: {gif_path}", self.component_name)
                    except Exception as e:
                        log_warn(f"Failed to convert animation mp4 to GIF, sending as mp4: {e}", self.component_name)

        def collect():
            files = []
            for att in attachments:
                p = att.get('local_path')
                fn = att.get('filename') or (os.path.basename(p) if p else None) or f"file_{int(time.time())}"
                if p and os.path.exists(p) and os.path.getsize(p) > 0:
                    try:
                        lower = fn.lower()
                        for ext in ('.gif', '.png', '.jpg', '.jpeg'):
                            dup = ext + ext
                            if lower.endswith(dup):
                                fn = fn[:-len(ext)]
                                break
                    except Exception:
                        pass
                    try:
                        files.append(discord.File(p, filename=fn, spoiler=att.get('spoiler', False)))
                    except Exception as e:
                        log_warn(f"Failed to prepare file for send: {p} ({e})", self.component_name)
            return files

        files = collect()
        if files:
            return files

        start = time.time()
        while time.time() - start < max_wait:
            files = collect()
            if files:
                return files
            all_ready = all(
                (att.get('local_path') and os.path.exists(att['local_path']) and os.path.getsize(att['local_path']) > 0)
                for att in attachments
            )
            if all_ready:
                break
            await asyncio.sleep(0.25)

        files = collect()
        if not files and attachments:
            try:
                names = [att.get('filename') or att.get('local_path') for att in attachments]
                log_warn(f"No media files became available for send (waited {max_wait:.1f}s). Missing: {', '.join(str(n) for n in names if n)}", self.component_name)
            except Exception:
                log_warn(f"No media files became available for send (waited {max_wait:.1f}s).", self.component_name)
        return files

    # ── edit / delete from other platforms ──────────────

    async def handle_edit(self, data):
        try:
            bridge_name = data.get('bridge_name')
            raw_id = data.get('discord_message_id')
            if not raw_id:
                return
            discord_message_id = int(raw_id)
            new_text_raw = data.get('new_text') or ''
            bridge = self.core.bridges.get(bridge_name)
            if not bridge:
                return

            ds = bridge.get_platform('discord')
            if not getattr(ds, 'cross_edit', True):
                log_debug(f"cross_edit disabled for discord in bridge '{bridge_name}', skipping", self.component_name)
                return

            source = data.get('source', 'unknown')
            source_cfg = bridge.get_platform(source) or ds

            try:
                if not self.bot.is_ready():
                    await self.bot.wait_until_ready()
            except Exception:
                pass

            channel = self.bot.get_channel(ds.channel_id_int) or await self.bot.fetch_channel(ds.channel_id_int)
            try:
                msg = await channel.fetch_message(discord_message_id)
            except Exception:
                try:
                    await channel.send(f"[Edited]\n{new_text_raw}", mention_author=False)
                except Exception:
                    pass
                return

            def norm(s: str) -> str:
                try:
                    s = (s or '')
                    s = re.sub(r"^```(?:diff|ini)?\s*[\s\S]*?```", "", s, flags=re.M)
                    s = s.replace('\u200b', '').replace('\u200c', '').replace('\xa0', ' ')
                    s = re.sub(r"\s+", " ", s).strip()
                    return s
                except Exception:
                    return (s or '').strip()

            original = getattr(msg, 'content', '') or ''

            preserve_prefix = (not getattr(msg, 'webhook_id', None)) and getattr(source_cfg, 'use_prefixes', True)
            prefix = ''
            if preserve_prefix:
                m = re.match(r"^```(?:ini|diff)\s*\n\+? ?\[\w+\][\s\S]*?```\s*\n?", original, flags=re.S)
                if m:
                    prefix = m.group(0)

            proposed = (prefix + new_text_raw) if prefix else new_text_raw

            if not hasattr(self.core, '_inbound_edit_cache'):
                self.core._inbound_edit_cache = {}

            now = time.time()
            try:
                cached = self.core._inbound_edit_cache.get(discord_message_id)
                if cached and cached[1] > now and cached[0] == norm(proposed):
                    return
            except Exception:
                pass

            if norm(original) == norm(proposed):
                return

            try:
                is_thread = isinstance(channel, discord.Thread)
                if getattr(msg, 'webhook_id', None) and ds.webhook_url:
                    async with aiohttp.ClientSession() as session:
                        webhook = Webhook.from_url(ds.webhook_url, session=session)
                        if is_thread:
                            await webhook.edit_message(message_id=discord_message_id, content=new_text_raw, thread=channel)
                        else:
                            await webhook.edit_message(message_id=discord_message_id, content=new_text_raw)
                else:
                    await msg.edit(content=proposed)
            except Exception:
                try:
                    await channel.send(f"[Edited]\n{new_text_raw}", reference=msg, mention_author=False)
                except Exception:
                    try:
                        await channel.send(f"[Edited]\n{new_text_raw}", mention_author=False)
                    except Exception:
                        pass

            try:
                self.core._inbound_edit_cache[discord_message_id] = (norm(proposed), now + 15.0)
            except Exception:
                pass

        except Exception as e:
            log_error(f"Failed to handle edit: {e}", self.component_name)

    async def handle_delete(self, data):
        try:
            bridge_name = data.get('bridge_name')
            raw_id = data.get('discord_message_id')
            if not raw_id:
                # Try to look up from source mapping
                source = data.get('source', '')
                src_id = None
                for key in data:
                    if key.endswith('_message_id') and key != 'discord_message_id':
                        src_id = data[key]
                        if not source:
                            source = key.replace('_message_id', '')
                        break
                if source and src_id:
                    db = self.core.bridge_dbs.get(bridge_name)
                    if db:
                        raw_id = db.get_mapped_id(source, str(src_id), 'discord')
            if not raw_id:
                return
            discord_message_id = int(raw_id)
            bridge = self.core.bridges.get(bridge_name)
            if not bridge:
                return

            ds = bridge.get_platform('discord')
            if not getattr(ds, 'cross_delete', True):
                log_debug(f"cross_delete disabled for discord in bridge '{bridge_name}', skipping", self.component_name)
                return

            try:
                if not self.bot.is_ready():
                    await self.bot.wait_until_ready()
            except Exception:
                pass

            channel = self.bot.get_channel(ds.channel_id_int) or await self.bot.fetch_channel(ds.channel_id_int)
            is_thread = isinstance(channel, discord.Thread)

            try:
                msg = await channel.fetch_message(discord_message_id)
                if getattr(msg, 'webhook_id', None) and ds.webhook_url:
                    async with aiohttp.ClientSession() as session:
                        webhook = Webhook.from_url(ds.webhook_url, session=session)
                        if is_thread:
                            await webhook.delete_message(discord_message_id, thread=channel)
                        else:
                            await webhook.delete_message(discord_message_id)
                else:
                    await msg.delete()
                log_debug(f"Deleted Discord message {discord_message_id}", self.component_name)
            except Exception as e:
                log_error(f"Failed to delete Discord message {discord_message_id}: {e}", self.component_name)
        except Exception as e:
            log_error(f"Failed to handle delete: {e}", self.component_name)

    # ── ordered send ────────────────────────────────────

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
                    log_debug(f"Calling deliver_to_channel for msg_id={msg0.message_id}", self.component_name)
                    await self.deliver_to_channel(msg0)
                    log_debug(f"deliver_to_channel returned for msg_id={msg0.message_id}", self.component_name)
                except Exception as e:
                    log_error(f"Ordered send failed: {e}")

                if ts0 > last_ts:
                    last_ts = ts0
                    self._last_sent_ts[key] = last_ts

    # ── rabbitmq consumer ───────────────────────────────

    def _handle_inbound_message(self, ch, method, properties, body):
        try:
            if not self._loop:
                log_warn("Event loop not ready yet, requeuing message", self.component_name)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return

            raw = body.decode()
            log_debug(f"Received message on pilgrim_discord, len={len(raw)}", self.component_name)

            try:
                data = json.loads(raw)
            except Exception:
                log_error("Discord pilgrim received non-JSON message, dropping.")
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
                    log_debug(f"_ordered_send completed OK", self.component_name)
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

                pilgrim_queue = self.core.queues['pilgrim_discord']
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
                    if _heartbeat_counter >= 120:  # ~every 2 min
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