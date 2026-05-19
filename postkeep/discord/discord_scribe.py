import asyncio
import io
import json
import re
import time
import os
import threading
import traceback
from datetime import datetime, timezone
from urllib.parse import urlparse
from typing import Optional, Dict, List

from postkeep.papyrus import (
    log_info, log_error, log_warn, log_success, log_debug,
    BridgeMessage,
    extract_media_urls, extract_tenor_urls, escape_discord_emojis,
    convert_video_to_gif, get_file_type_from_url, normalize_mime_type,
    should_skip_download, extract_ignore_hint_urls,
    is_user_ignored, add_ignored_user, is_user_admin,
    add_downloadable_domain, add_ignored_domain,
    get_cached_avatar, store_avatar_cache, compute_avatar_hash,
    AVATAR_REFRESH_INTERVAL,
    get_privacy_level, set_privacy_level, list_privacy_settings,
    get_privacy_limit, cap_privacy_level,
    format_privacy_levels_token, format_privacy_levels_help,
    format_privacy_levels_phrase,
    compute_incognito_name, ensure_incognito_avatar,
    apply_link_replacements, is_privacy_active,
)


# Pre-compiled match for the privacy command. Used to gate dispatch in
# on_message and to defensively skip command messages in process_discord_message
# (catch-up paths bypass on_message and would otherwise bridge it).
_PRIVACY_CMD_RE = re.compile(r'^-{1,2}privacy(\s|$)')


class DiscordScribe:
    def __init__(self, core):
        self.core = core
        self.bot = core.bot
        self._dlib = core.discord_lib
        self.component_name = 'discord_scribe'
        self._bridge_locks: Dict[str, asyncio.Lock] = {}

        # Cache: is privacy reachable on any bridge? When False we skip the
        # -privacy command dispatch AND every per-message privacy check.
        # Refreshed by core.reload_config when implemented; safe default is to
        # recompute on demand if bridges change at runtime.
        bridges = getattr(core, 'bridges', None)
        self._privacy_active = is_privacy_active(bridges) if bridges else False

    def setup_handlers(self):
        @self.bot.event
        async def on_ready():
            log_success(f"Discord bot logged in as {self.bot.user}")

            if not self.core.rabbitmq_channel or self.core.rabbitmq_channel.is_closed:
                log_info("Waiting for RabbitMQ connection...")
                for _ in range(10):
                    await asyncio.sleep(1)
                    if self.core.rabbitmq_channel and not self.core.rabbitmq_channel.is_closed:
                        break
                else:
                    log_error("RabbitMQ connection timeout")
                    return

            self.core.send_status_update('ready')
            asyncio.create_task(self._second_ready_ping())

            if not self.core.consumer_thread or not self.core.consumer_thread.is_alive():
                import threading
                from postkeep.discord.discord_pilgrim import DiscordPilgrim
                pilgrim = self._get_pilgrim()
                if pilgrim:
                    pilgrim._loop = asyncio.get_event_loop()
                    self.core.consumer_thread = threading.Thread(target=pilgrim.run_consumers, daemon=True)
                    self.core.consumer_thread.start()
                    log_info("Started RabbitMQ consumer thread (pilgrim)")

            asyncio.create_task(self.process_imports_and_missed())
            self._start_weekly_avatar_refresh()

        @self.bot.event
        async def on_message(message):
            # Privacy command works in DMs too (for setting per-channel privacy remotely).
            if isinstance(message.channel, self._dlib.DMChannel):
                if (
                    self._privacy_active
                    and not message.author.bot
                    and _PRIVACY_CMD_RE.match(message.content.strip())
                    # When the global cap is 0 we silently ignore the command
                    # even in DMs, no matter what per-bridge overrides say -
                    # the global setting is the kill switch.
                    and get_privacy_limit() > 0
                ):
                    await self._cmd_privacy(message)
                return

            if message.type == self._dlib.MessageType.new_member:
                await self.handle_system_join(message)
                return

            # Filter out system message types (pins_add, boost, thread_created, etc.)
            # that would otherwise fall through to process_discord_message with empty
            # content and produce "This format is not supported".
            # Only process regular messages and replies (forwards use type default).
            if message.type not in (self._dlib.MessageType.default, self._dlib.MessageType.reply):
                return

            if not message.author.bot:
                if message.content.strip().startswith('-viewraw'):
                    await self._cmd_viewraw(message)
                    return
                if message.content.strip().startswith('-ignoreuser'):
                    await self._cmd_ignoreuser(message)
                    return
                if message.content.strip().startswith('-add_downloadable_domain'):
                    await self._cmd_add_downloadable_domain(message)
                    return
                if message.content.strip().startswith('-add_ignored_domain'):
                    await self._cmd_add_ignored_domain(message)
                    return
                if self._privacy_active and _PRIVACY_CMD_RE.match(message.content.strip()):
                    # Global cap of 0 = the feature is off everywhere. Per-bridge
                    # overrides don't bring back the command in that case.
                    if get_privacy_limit() <= 0:
                        return
                    # Only react in channels the bot is bridging. Outside of a
                    # bridged channel the bot has no business listening - no
                    # error, no help text, just silence.
                    bridge_name, bridge_cfg = self.core.get_bridge_by_discord_channel(message.channel.id)
                    if not bridge_cfg:
                        return
                    # When the effective cap on this channel is 0 (per-bridge
                    # override), behave as if the command doesn't exist.
                    if get_privacy_limit('discord', bridge_cfg) <= 0:
                        return
                    await self._cmd_privacy(message)
                    return

            if not message.author.bot:
                try:
                    if is_user_ignored('discord', message.author.id):
                        return
                except Exception:
                    pass
                # Serialize processing per bridge to preserve message order
                bridge_name, _ = self.core.get_bridge_by_discord_channel(message.channel.id)
                if bridge_name:
                    if bridge_name not in self._bridge_locks:
                        self._bridge_locks[bridge_name] = asyncio.Lock()
                    async with self._bridge_locks[bridge_name]:
                        await self.process_discord_message(message)
                else:
                    await self.process_discord_message(message)

        @self.bot.event
        async def on_message_edit(before, after):
            await self.handle_message_edit(before, after)

        @self.bot.event
        async def on_message_delete(message):
            await self.handle_message_delete(message)

        @self.bot.event
        async def on_guild_channel_pins_update(channel, last_pin):
            await self.handle_pin_update(channel, last_pin)

        @self.bot.event
        async def on_member_remove(member):
            await self.handle_member_remove(member)

    def _get_pilgrim(self):
        return getattr(self.core, '_pilgrim_ref', None)

    def _can_forward_to_others(self, bridge_config):
        platforms = getattr(bridge_config, 'platforms', None) or {}
        return any(
            p and getattr(p, 'can_receive', False)
            for name, p in platforms.items()
            if name != 'discord'
        )

    async def _second_ready_ping(self):
        try:
            await asyncio.sleep(5)
            self.core.send_status_update('ready', 'post-ready ping')
        except Exception as exc:
            log_debug(f"Second ready ping failed: {exc}", self.component_name)

    # ── avatar resolution ─────────────────────────────

    async def _resolve_and_cache_discord_avatar(self, user) -> Optional[str]:
        if user.bot:
            return str(user.display_avatar.url)

        user_id = str(user.id)
        cached = get_cached_avatar(self.core.avatar_db, user_id) if self.core.avatar_db else None

        if cached:
            age = time.time() - (cached.get('last_checked') or 0)
            discord_url = cached.get('discord_url')
            if discord_url and age < AVATAR_REFRESH_INTERVAL:
                log_debug(f"[AVATAR] Cache HIT for Discord user {user_id}: age={age:.0f}s", self.component_name)
                return discord_url

        try:
            avatar_asset = user.display_avatar
            avatar_bytes = await avatar_asset.read()
            new_hash = compute_avatar_hash(avatar_bytes)
            log_debug(f"[AVATAR] Downloaded Discord avatar for {user_id} ({len(avatar_bytes)} bytes, hash={new_hash[:12]}...)", self.component_name)

            if cached and cached.get('avatar_hash') == new_hash and cached.get('discord_url'):
                log_debug(f"[AVATAR] Hash unchanged for Discord user {user_id}, updating last_checked", self.component_name)
                store_avatar_cache(self.core.avatar_db, user_id, 'discord',
                                   avatar_hash=new_hash, avatar_bytes=avatar_bytes)
                return cached['discord_url']

            upload_channel = self.bot.get_channel(self.core.avatar_upload_channel_id) if self.core.avatar_upload_channel_id else None
            if not upload_channel:
                log_warn(f"[AVATAR] No upload channel configured/found, using direct URL for {user_id}", self.component_name)
                direct_url = str(avatar_asset.url)
                store_avatar_cache(self.core.avatar_db, user_id, 'discord',
                                   avatar_hash=new_hash, avatar_bytes=avatar_bytes,
                                   discord_url=direct_url)
                return direct_url

            file = self._dlib.File(io.BytesIO(avatar_bytes), filename=f"avatar_{user_id}.png")
            msg = await upload_channel.send(file=file)
            if msg.attachments:
                avatar_url = str(msg.attachments[0].url)
                store_avatar_cache(
                    self.core.avatar_db, user_id, 'discord',
                    avatar_hash=new_hash, avatar_bytes=avatar_bytes,
                    discord_url=avatar_url,
                    discord_channel_id=str(upload_channel.id),
                    discord_message_id=str(msg.id),
                )
                log_success(f"[AVATAR] Uploaded Discord avatar for {user_id}: {avatar_url[:60]}...", self.component_name)
                return avatar_url

            log_warn(f"[AVATAR] Upload message had no attachments for {user_id}", self.component_name)
        except Exception as e:
            log_warn(f"[AVATAR] Failed to resolve Discord avatar for {user_id}: {type(e).__name__}: {e}", self.component_name)

        if cached and cached.get('discord_url'):
            return cached['discord_url']
        return str(user.display_avatar.url)

    def _start_weekly_avatar_refresh(self):
        def _refresh_loop():
            while True:
                try:
                    time.sleep(AVATAR_REFRESH_INTERVAL)
                    log_info("[AVATAR-REFRESH] Starting weekly Discord avatar refresh", self.component_name)
                    self._run_discord_avatar_refresh()
                except Exception as e:
                    log_error(f"[AVATAR-REFRESH] Error in refresh loop: {e}", self.component_name)

        t = threading.Thread(target=_refresh_loop, daemon=True)
        t.start()
        log_info("[AVATAR-REFRESH] Weekly Discord avatar refresh thread started", self.component_name)

    def _run_discord_avatar_refresh(self):
        if not self.core.avatar_db:
            return
        try:
            rows = self.core.avatar_db.execute(
                "SELECT user_id, avatar_hash, discord_url, discord_channel_id, discord_message_id "
                "FROM avatar_cache WHERE platform = 'discord' AND discord_url IS NOT NULL"
            ).fetchall()
        except Exception as e:
            log_error(f"[AVATAR-REFRESH] DB query failed: {e}", self.component_name)
            return

        loop = asyncio.new_event_loop()
        try:
            for row in rows:
                uid, old_hash, old_url, ch_id, msg_id = row
                try:
                    loop.run_until_complete(self._refresh_single_discord_avatar(uid, old_hash, old_url, ch_id, msg_id))
                except Exception as e:
                    log_warn(f"[AVATAR-REFRESH] Failed for user {uid}: {e}", self.component_name)
        finally:
            loop.close()

    async def _refresh_single_discord_avatar(self, user_id: str, old_hash: str, old_url: str, ch_id: str, msg_id: str):
        try:
            user = await self.bot.fetch_user(int(user_id))
        except Exception:
            log_debug(f"[AVATAR-REFRESH] Could not fetch user {user_id}, skipping", self.component_name)
            store_avatar_cache(self.core.avatar_db, user_id, 'discord')
            return

        try:
            avatar_bytes = await user.display_avatar.read()
            new_hash = compute_avatar_hash(avatar_bytes)
        except Exception as e:
            log_warn(f"[AVATAR-REFRESH] Could not download avatar for {user_id}: {e}", self.component_name)
            store_avatar_cache(self.core.avatar_db, user_id, 'discord')
            return

        if new_hash == old_hash:
            if ch_id and msg_id:
                try:
                    channel = self.bot.get_channel(int(ch_id))
                    if channel:
                        fetched_msg = await channel.fetch_message(int(msg_id))
                        if fetched_msg and fetched_msg.attachments:
                            fresh_url = str(fetched_msg.attachments[0].url)
                            store_avatar_cache(self.core.avatar_db, user_id, 'discord',
                                               avatar_hash=new_hash, avatar_bytes=avatar_bytes,
                                               discord_url=fresh_url,
                                               discord_channel_id=ch_id, discord_message_id=msg_id)
                            log_debug(f"[AVATAR-REFRESH] Refreshed ?ex= token for {user_id}", self.component_name)
                            return
                except Exception as e:
                    log_debug(f"[AVATAR-REFRESH] Message fetch failed for {user_id}, will re-upload: {e}", self.component_name)
            else:
                store_avatar_cache(self.core.avatar_db, user_id, 'discord',
                                   avatar_hash=new_hash, avatar_bytes=avatar_bytes)
                log_debug(f"[AVATAR-REFRESH] Hash unchanged, no message to refresh for {user_id}", self.component_name)
                return

        upload_channel = self.bot.get_channel(self.core.avatar_upload_channel_id) if self.core.avatar_upload_channel_id else None
        if not upload_channel:
            log_warn(f"[AVATAR-REFRESH] No upload channel for re-upload of {user_id}", self.component_name)
            return

        file = self._dlib.File(io.BytesIO(avatar_bytes), filename=f"avatar_{user_id}.png")
        msg = await upload_channel.send(file=file)
        if msg.attachments:
            new_url = str(msg.attachments[0].url)
            store_avatar_cache(
                self.core.avatar_db, user_id, 'discord',
                avatar_hash=new_hash, avatar_bytes=avatar_bytes,
                discord_url=new_url,
                discord_channel_id=str(upload_channel.id),
                discord_message_id=str(msg.id),
            )
            log_success(f"[AVATAR-REFRESH] Re-uploaded avatar for {user_id}: {new_url[:60]}...", self.component_name)

    # ── commands ────────────────────────────────────────

    async def _cmd_viewraw(self, message):
        try:
            target = message
            if message.reference and message.reference.message_id:
                try:
                    target = await message.channel.fetch_message(message.reference.message_id)
                except Exception:
                    target = message
            data = None
            try:
                if hasattr(target, 'to_dict'):
                    data = target.to_dict()
            except Exception:
                data = None
            if data is None:
                if hasattr(target, 'data') and isinstance(getattr(target, 'data'), dict):
                    data = getattr(target, 'data')
                elif hasattr(target, 'raw_data') and isinstance(getattr(target, 'raw_data'), dict):
                    data = getattr(target, 'raw_data')
            if data is None:
                data = {
                    'id': str(target.id),
                    'channel_id': str(target.channel.id),
                    'content': target.content,
                    'embeds': [e.to_dict() if hasattr(e, 'to_dict') else str(e) for e in getattr(target, 'embeds', [])],
                    'attachments': [
                        {
                            'id': str(getattr(a, 'id', '')),
                            'filename': getattr(a, 'filename', ''),
                            'url': getattr(a, 'url', None),
                            'proxy_url': getattr(a, 'proxy_url', None),
                            'content_type': getattr(a, 'content_type', None)
                        } for a in getattr(target, 'attachments', [])
                    ],
                    'stickerItems': [
                        {'id': str(getattr(s, 'id', '')), 'name': getattr(s, 'name', '')}
                        for s in getattr(target, 'stickers', []) or []
                    ],
                    'messageReference': {
                        'channel_id': getattr(target.reference, 'channel_id', None) if getattr(target, 'reference', None) else None,
                        'message_id': getattr(target.reference, 'message_id', None) if getattr(target, 'reference', None) else None
                    } if getattr(target, 'reference', None) else None
                }
            text = json.dumps(data)
            if len(text) > 1900:
                text = text[:1900] + '...'
            await message.channel.send(f"```json\n{text}\n```", reference=message)
        except Exception as e:
            await message.channel.send(f"Failed to dump raw: {e}")

    async def _cmd_ignoreuser(self, message):
        try:
            parts = message.content.strip().split()
            target_id = None
            if len(parts) > 1:
                target_id = parts[1]
            if not target_id and message.reference and message.reference.message_id:
                try:
                    ref = await message.channel.fetch_message(message.reference.message_id)
                    target_id = str(ref.author.id)
                except Exception:
                    pass
            if not target_id:
                target_id = str(message.author.id)
            added = add_ignored_user('discord', target_id)
            await message.channel.send('User ignored' if added else 'User already ignored', reference=message)
        except Exception as e:
            await message.channel.send(f"Failed to ignore: {e}")

    async def _cmd_reloadconfig(self, message):
        try:
            if not is_user_admin('discord', message.author.id):
                await message.channel.send('Not authorized to reload', reference=message)
                return
            self.core.reload_config()
            await message.channel.send('Discord config reloaded', reference=message)
        except Exception as e:
            await message.channel.send(f"Reload failed: {e}")

    async def _cmd_privacy(self, message):
        try:
            user_id = message.author.id
            chan_id = message.channel.id
            is_dm = isinstance(message.channel, self._dlib.DMChannel)

            text = message.content.strip()
            text = re.sub(r'^-{1,2}privacy\s*', '', text)
            parts = text.split() if text else []

            async def _send(t):
                try:
                    if is_dm:
                        await message.channel.send(t)
                    else:
                        await message.channel.send(t, reference=message)
                except Exception:
                    pass

            # Resolve the bridge (best effort, by current channel_id) up front so
            # help text and validation reflect the actual per-bridge cap.
            def _bridge_for_chan(cid):
                try:
                    cid_int = int(cid)
                except Exception:
                    return None
                for _bn, _bcfg in self.core.bridges.items():
                    _ds = _bcfg.get_platform('discord')
                    if _ds and _ds.channel_id_int == cid_int:
                        return _bcfg
                return None

            current_bridge = None if is_dm else _bridge_for_chan(chan_id)
            limit = get_privacy_limit('discord', current_bridge)
            level_token = format_privacy_levels_token(limit)
            level_phrase = format_privacy_levels_phrase(limit)
            level_help = format_privacy_levels_help(limit)
            invalid_msg = f"Level must be {level_phrase}."

            if not parts:
                settings = list_privacy_settings('discord', user_id)
                if settings:
                    lines = []
                    for scope, lvl in settings:
                        eff = min(lvl, limit)
                        label = {0: 'disabled', 1: 'nickname omitted', 2: 'fully ignored'}[eff]
                        scope_lbl = 'all channels' if scope == 'all' else f'channel {scope}'
                        suffix = f" (capped from {lvl})" if eff != lvl else ''
                        lines.append(f"  {scope_lbl}: {label}{suffix}")
                    await _send("Your privacy settings:\n" + "\n".join(lines))
                elif limit <= 0:
                    await _send("Privacy is disabled on this bridge.")
                else:
                    await _send(
                        "No privacy settings (default: level 0, disabled).\n\n"
                        "Usage:\n"
                        f"  -privacy {level_token}               set for this channel\n"
                        f"  -privacy {level_token} -all          set for every channel\n"
                        f"  -privacy <channel_id> {level_token}  set for a specific channel\n\n"
                        f"Levels: {level_help}"
                    )
                return

            if limit <= 0:
                await _send("Privacy is disabled on this bridge.")
                return

            is_all = '-all' in parts
            if is_all:
                parts = [p for p in parts if p != '-all']

            scope = None
            level = None
            if len(parts) == 1:
                try:
                    level = int(parts[0])
                except ValueError:
                    await _send(invalid_msg)
                    return
                if is_all:
                    scope = 'all'
                elif is_dm:
                    await _send(
                        "In DM, use:\n"
                        "  -privacy <level> -all\n"
                        "  -privacy <channel_id> <level>"
                    )
                    return
                else:
                    scope = str(chan_id)
            elif len(parts) == 2:
                if is_all:
                    await _send("Cannot combine explicit channel_id with -all.")
                    return
                scope = parts[0]
                try:
                    level = int(parts[1])
                except ValueError:
                    await _send(invalid_msg)
                    return
            else:
                await _send("Usage: -privacy <level> [-all] or -privacy <channel_id> <level>")
                return

            scope_bridge = current_bridge if scope == 'all' else _bridge_for_chan(scope)
            scope_limit = get_privacy_limit('discord', scope_bridge)

            # Out-of-range level (negative, above cap, etc.) -> generic syntax
            # error using the cap-aware phrase. No dedicated cap message.
            if level < 0 or level > scope_limit:
                await _send(f"Level must be {format_privacy_levels_phrase(scope_limit)}.")
                return

            set_privacy_level('discord', user_id, scope, level)
            scope_lbl = 'all channels' if scope == 'all' else f'channel {scope}'
            level_lbl = {0: 'disabled', 1: 'nickname omitted', 2: 'fully ignored'}[level]
            await _send(f"Privacy for {scope_lbl}: {level_lbl}.")
        except Exception as e:
            try:
                await message.channel.send(f"Privacy command failed: {e}")
            except Exception:
                pass

    async def _cmd_add_downloadable_domain(self, message):
        try:
            parts = message.content.strip().split()
            if len(parts) < 2:
                await message.channel.send('Usage: -add_downloadable_domain example.com', reference=message)
                return
            ok = add_downloadable_domain(parts[1])
            await message.channel.send('Added' if ok else 'Already exists or invalid', reference=message)
        except Exception as e:
            await message.channel.send(f"Failed: {e}")

    async def _cmd_add_ignored_domain(self, message):
        try:
            parts = message.content.strip().split()
            if len(parts) < 2:
                await message.channel.send('Usage: -add_ignored_domain example.com', reference=message)
                return
            ok = add_ignored_domain(parts[1])
            await message.channel.send('Added' if ok else 'Already exists or invalid', reference=message)
        except Exception as e:
            await message.channel.send(f"Failed: {e}")

    # ── embed extraction ────────────────────────────────

    async def _extract_attachments_from_embeds(self, message) -> List[Dict]:
        results: List[Dict] = []
        if not getattr(message, 'embeds', None):
            return results

        try:
            ignored_hint_urls = set(extract_ignore_hint_urls(getattr(message, 'content', '') or ''))
            ignored_hint_bases = set(u.split('?')[0] for u in ignored_hint_urls)
        except Exception:
            ignored_hint_urls = set()
            ignored_hint_bases = set()

        content_has_wrapper = False
        try:
            text = (getattr(message, 'content', '') or '').lower()
            if any(s in text for s in ['x.com/', 'twitter.com/', 'fxtwitter.com/', 'vxtwitter.com/', 'fixupx.com/']):
                content_has_wrapper = True
        except Exception:
            content_has_wrapper = False

        for i, embed in enumerate(message.embeds):
            try:
                is_dict = isinstance(embed, dict)
                image_obj = (embed.get('image') if is_dict else getattr(embed, 'image', None))
                thumb_obj = (embed.get('thumbnail') if is_dict else getattr(embed, 'thumbnail', None))
                video_obj = (embed.get('video') if is_dict else getattr(embed, 'video', None))

                def pick(obj, keys):
                    if not obj:
                        return None
                    if isinstance(obj, dict):
                        for k in keys:
                            if obj.get(k):
                                return obj.get(k)
                    else:
                        for k in keys:
                            if hasattr(obj, k) and getattr(obj, k):
                                return getattr(obj, k)
                    return None

                image_url = pick(image_obj, ['proxy_url', 'proxyURL', 'url'])
                thumb_url = pick(thumb_obj, ['proxy_url', 'proxyURL', 'url'])
                video_url = pick(video_obj, ['proxy_url', 'proxyURL', 'url'])
                direct_url = (embed.get('url') if is_dict else getattr(embed, 'url', None))

                content_type = None
                type_field = None
                if is_dict:
                    content_type = embed.get('contentType') or embed.get('content_type') or embed.get('mimeType')
                    type_field = embed.get('type')
                else:
                    content_type = getattr(embed, 'content_type', None) or getattr(embed, 'contentType', None)
                    type_field = getattr(embed, 'type', None)

                if not content_type:
                    content_type = pick(video_obj, ['contentType', 'content_type', 'mimeType']) or pick(image_obj, ['contentType', 'content_type', 'mimeType'])

                chosen_url = None
                fallback_url = None
                type_hint = None
                ext = None

                if content_type:
                    ct = str(content_type).lower()
                    if ct.startswith('video/'):
                        chosen_url = video_url or direct_url or image_url or thumb_url
                        fallback_url = (direct_url if chosen_url and (chosen_url == video_url and direct_url and direct_url != chosen_url) else None)
                        type_hint = 'video'
                        ext = '.mp4' if ct == 'video/mp4' else f".{ct.split('/')[-1]}"
                    elif ct == 'image/gif' or str(type_field).lower() == 'gifv':
                        chosen_url = video_url or image_url or direct_url or thumb_url
                        fallback_url = (image_url if image_url and image_url != chosen_url else None)
                        type_hint = 'gif'
                        ext = '.gif'
                    elif ct.startswith('image/'):
                        chosen_url = image_url or thumb_url or direct_url
                        fallback_url = (thumb_url if thumb_url and thumb_url != chosen_url else None)
                        type_hint = 'image'
                        ext = os.path.splitext((chosen_url or '').split('?')[0])[1] or f".{ct.split('/')[-1]}" or '.jpg'

                if not chosen_url:
                    tf = str(type_field).lower() if type_field else ''
                    if tf == 'video' or (video_url and getattr(self.core, 'tenor_preferred_format', 'gif') == 'mp4'):
                        chosen_url = video_url or image_url or thumb_url or direct_url
                        fallback_url = (direct_url if direct_url and direct_url != chosen_url else None)
                        type_hint = 'video'
                        ext = '.mp4'
                    elif tf == 'gifv' or (image_url and ('.gif' in str(image_url).lower())):
                        chosen_url = video_url or image_url or thumb_url or direct_url
                        fallback_url = (image_url if image_url and image_url != chosen_url else None)
                        type_hint = 'gif'
                        ext = '.gif'
                    else:
                        chosen_url = image_url or thumb_url or video_url or direct_url
                        fallback_url = (thumb_url if thumb_url and thumb_url != chosen_url else None)
                        type_hint = 'image'
                        ext = os.path.splitext((chosen_url or '').split('?')[0])[1] or '.png'

                if not chosen_url:
                    continue

                if type_hint == 'gif':
                    mime = 'image/gif'
                elif type_hint == 'video':
                    mime = 'video/mp4' if ext == '.mp4' else f"video/{ext.lstrip('.')}" if ext else 'video/*'
                else:
                    if ext in ('.jpg', '.jpeg'):
                        mime = 'image/jpeg'
                    elif ext == '.png':
                        mime = 'image/png'
                    elif ext == '.webp':
                        mime = 'image/webp'
                    elif ext == '.gif':
                        mime = 'image/gif'
                    else:
                        mime = 'image/*'

                from urllib.parse import urlparse as _urlparse
                parsed_name = os.path.basename((chosen_url or '').split('?')[0])
                if not parsed_name or parsed_name.strip('.') == '':
                    try:
                        src = direct_url or chosen_url
                        if src:
                            p = _urlparse(src)
                            segments = [seg for seg in p.path.split('/') if seg]
                            base = segments[-1] if segments else p.netloc.split(':')[0]
                            parsed_name = f"{base}{ext if not base.lower().endswith(ext or '') else ''}"
                    except Exception:
                        parsed_name = f"embed_{i}{ext}"
                else:
                    root, cur_ext = os.path.splitext(parsed_name)
                    if not cur_ext and ext:
                        parsed_name = f"{parsed_name}{ext}"
                    elif ext and cur_ext.lower() != ext.lower() and type_hint != 'gif':
                        pass

                try:
                    etype = (str(type_field).lower() if type_field else '')
                    if etype in ('rich', 'article', 'link'):
                        log_debug('Skipping embed download due to type rich/article/link')
                        continue
                    candidate_for_policy = chosen_url or direct_url or video_url or image_url or thumb_url
                    check_url = self._unwrap_external_url(candidate_for_policy) if candidate_for_policy else candidate_for_policy
                    try:
                        base_cmp = (check_url or '').split('?')[0]
                        if base_cmp in ignored_hint_bases:
                            log_debug(f"Inline ignore for embed media: {check_url}")
                            continue
                    except Exception:
                        pass
                    if check_url and should_skip_download(check_url):
                        log_debug(f"Domain policy skip for embed media, keeping link only: {check_url}")
                        continue
                except Exception:
                    pass

                if (direct_url and ('cdn.discordapp.com' in str(direct_url) or 'media.discordapp.net' in str(direct_url)) and ('?ex=' in str(direct_url) or '&ex=' in str(direct_url))):
                    if type_hint in ('video', 'gif'):
                        candidates = [direct_url, video_url, chosen_url, fallback_url, image_url, thumb_url]
                    else:
                        candidates = [direct_url, image_url, chosen_url, thumb_url, fallback_url, video_url]
                else:
                    if type_hint in ('video', 'gif'):
                        candidates = [video_url, chosen_url, fallback_url, direct_url, image_url, thumb_url]
                    else:
                        candidates = [image_url, chosen_url, thumb_url, fallback_url, direct_url, video_url]

                result = await self.core._try_download_candidates(candidates, parsed_name, message=message)
                file_path = result['path'] if result else None
                if result:
                    chosen_url = result['url']

                if (not file_path or (type_hint == 'gif' and not parsed_name.lower().endswith('.gif'))):
                    if str(type_field).lower() == 'gifv' or (content_type and str(content_type).lower() == 'image/gif'):
                        tenor_id = None
                        try:
                            embed_url_src = direct_url or chosen_url or ''
                            m = re.search(r"tenor\.com/(?:view/.*-|)(\d+)", str(embed_url_src))
                            if m:
                                tenor_id = m.group(1)
                        except Exception:
                            tenor_id = None

                        video_source = video_url or chosen_url or fallback_url
                        if video_source:
                            mp4_name = f"{tenor_id}.mp4" if tenor_id else (parsed_name.replace('.gif', '.mp4') if parsed_name else 'tenor.mp4')
                            mp4_res = await self.core._try_download_candidates([video_source], mp4_name, message=message)
                            mp4_path = mp4_res['path'] if mp4_res else None
                            if mp4_path and os.path.exists(mp4_path):
                                try:
                                    gif_path = await convert_video_to_gif(mp4_path)
                                    if gif_path and os.path.exists(gif_path):
                                        try:
                                            if tenor_id:
                                                target_dir = os.path.dirname(gif_path)
                                                desired = os.path.join(target_dir, f"{tenor_id}.gif")
                                                if os.path.abspath(gif_path) != os.path.abspath(desired):
                                                    try:
                                                        os.replace(gif_path, desired)
                                                        gif_path = desired
                                                    except Exception:
                                                        pass
                                        except Exception:
                                            pass
                                        results.append({
                                            'url': video_source,
                                            'filename': os.path.basename(gif_path),
                                            'type': 'image/gif',
                                            'local_path': gif_path,
                                            'page_url': direct_url
                                        })
                                        try:
                                            os.remove(mp4_path)
                                        except Exception:
                                            pass
                                        continue
                                except Exception:
                                    pass

                if file_path:
                    results.append({
                        'url': chosen_url,
                        'filename': parsed_name,
                        'type': mime,
                        'local_path': file_path,
                        'page_url': direct_url
                    })
            except Exception:
                continue
        return results

    # ── shared helpers ──────────────────────────────────────

    @staticmethod
    def _get_raw_data(message) -> Optional[dict]:
        for attr in ('data', 'raw_data'):
            val = getattr(message, attr, None)
            if isinstance(val, dict):
                return val
        return None

    @staticmethod
    def _unwrap_external_url(url: str) -> str:
        try:
            if url and '/external/' in url and ('discordapp.net' in url or 'discordapp.com' in url):
                tail = url.split('/external/', 1)[1].split('/', 1)
                if len(tail) == 2:
                    rest = tail[1]
                    if rest.startswith('http/'):
                        return 'http://' + rest[len('http/'):]
                    if rest.startswith('https/'):
                        return 'https://' + rest[len('https/'):]
        except Exception:
            pass
        return url

    async def _download_sticker(self, sid: str, sname: str) -> Optional[Dict]:
        if not sid:
            return None
        url = f"https://media.discordapp.net/stickers/{sid}.png?size=160&name={sname}"
        fp = await self.core.download_media(url)
        if fp:
            return {'url': url, 'filename': f"{sname}.png", 'type': 'image/png', 'local_path': fp}
        return None

    async def _collect_stickers(self, items, allow_external: bool) -> List[Dict]:
        results = []
        if not items or not allow_external:
            return results
        for item in items:
            try:
                if isinstance(item, dict):
                    sid, sname = str(item.get('id', '')), item.get('name') or 'sticker'
                else:
                    sid, sname = str(getattr(item, 'id', '')), getattr(item, 'name', None) or 'sticker'
                att = await self._download_sticker(sid, sname)
                if att:
                    results.append(att)
            except Exception:
                continue
        return results

    async def _fetch_ref_attachments(self, ref_msg) -> List[Dict]:
        atts = []
        for ratt in getattr(ref_msg, 'attachments', []) or []:
            try:
                primary_url = ratt.proxy_url if hasattr(ratt, 'proxy_url') and ratt.proxy_url else ratt.url
                if primary_url:
                    fp = await self.core.download_with_recovery(primary_url, ratt.filename, message=ref_msg)
                    if fp:
                        atts.append({
                            'url': primary_url,
                            'filename': ratt.filename,
                            'type': normalize_mime_type(getattr(ratt, 'content_type', None), filename=ratt.filename, url=primary_url),
                            'local_path': fp
                        })
            except Exception:
                continue
        try:
            for att in await self._extract_attachments_from_embeds(ref_msg):
                if att.get('local_path'):
                    atts.append(att)
        except Exception:
            pass
        return atts

    async def _resolve_channel_ref(self, ref_ch_id: int, ref_msg_id: Optional[int],
                                   current_message) -> tuple:
        same_server_ref = None
        forward_info = None
        cross_server = False
        if not ref_ch_id or ref_ch_id == current_message.channel.id:
            return same_server_ref, forward_info, cross_server
        try:
            ref_channel = self.bot.get_channel(ref_ch_id) or await self.bot.fetch_channel(ref_ch_id)
            cur_guild = getattr(current_message, 'guild', None)
            ref_guild = getattr(ref_channel, 'guild', None)
            if cur_guild and ref_guild and ref_guild.id == cur_guild.id:
                if ref_msg_id:
                    try:
                        ref_msg = await ref_channel.fetch_message(ref_msg_id)
                        same_server_ref = {
                            'content': getattr(ref_msg, 'content', '') or '',
                            'attachments': await self._fetch_ref_attachments(ref_msg),
                        }
                    except Exception:
                        same_server_ref = None
            else:
                forward_info = {'from_user': f"#{getattr(ref_channel, 'name', 'Unknown')}"}
                cross_server = True
        except Exception:
            forward_info = {'from_user': 'Forwarded'}
            cross_server = True
        return same_server_ref, forward_info, cross_server

    # ── main message processing ─────────────────────────

    async def process_discord_message(self, message):
        bridge_name, bridge_config = self.core.get_bridge_by_discord_channel(message.channel.id)
        if not bridge_config:
            return
        ds = bridge_config.get_platform('discord')
        if not ds or not ds.can_send:
            return

        can_forward_anywhere = self._can_forward_to_others(bridge_config)
        if not can_forward_anywhere:
            return

        if ds.ignore_bots and message.author.bot:
            return
        if ds.ignore_webhooks and getattr(message, 'webhook_id', None):
            return

        # Defensive: catch-up paths (process_imports_and_missed) call here
        # directly, bypassing on_message. Skip privacy commands so they never
        # bridge regardless of which entrypoint surfaced them.
        try:
            content_stripped = (message.content or '').strip()
            if content_stripped and _PRIVACY_CMD_RE.match(content_stripped):
                return
        except Exception:
            pass

        privacy_level = 0
        if self._privacy_active:
            privacy_level = get_privacy_level(
                'discord', message.author.id, message.channel.id, bridge=bridge_config
            )
            if privacy_level >= 2:
                log_debug(
                    f"Privacy level 2: dropping msg={message.id} from user {message.author.id}",
                    self.component_name
                )
                return

        # Track native Discord message timestamp for prefix collapse
        self.core._last_native_dc_msg[bridge_name] = time.time()

        log_info(f"Processing Discord message from {message.author.name}")

        try:
            now = datetime.now(timezone.utc)
            created = getattr(message, 'created_at', None)
            if created:
                if created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                age = (now - created).total_seconds()
                if age < 0.6:
                    await asyncio.sleep(0.7 - age if age < 0.7 else 0)
        except Exception:
            pass

        attachments = []
        forward_info = None
        seen_urls = set()
        consumed_urls = []

        allow_embeds = True
        allow_external_emojis = True
        allow_external_stickers = True
        try:
            guild = getattr(message, 'guild', None)
            if guild and hasattr(message.channel, 'permissions_for'):
                author = message.author
                # permissions_for(User) only sees @everyone overwrites; role-based perms
                # (which most servers use to gate embed_links) are missed. Promote to Member
                # whenever possible so the check is accurate.
                Member = getattr(self._dlib, 'Member', None)
                if Member is not None and not isinstance(author, Member):
                    member = guild.get_member(author.id)
                    if member is None:
                        try:
                            member = await guild.fetch_member(author.id)
                        except Exception as fetch_err:
                            log_debug(
                                f"[PERMS] fetch_member failed for {author.id}: "
                                f"{type(fetch_err).__name__}: {fetch_err}",
                                self.component_name
                            )
                            member = None
                    if member is not None:
                        author = member
                perms = message.channel.permissions_for(author)
                allow_embeds = getattr(perms, 'embed_links', True)
                allow_external_emojis = getattr(perms, 'use_external_emojis', True)
                allow_external_stickers = getattr(perms, 'use_external_stickers', True)
                log_debug(
                    f"[PERMS] author={getattr(author, 'name', author)} "
                    f"type={type(author).__name__} "
                    f"embed_links={allow_embeds} "
                    f"ext_emojis={allow_external_emojis} "
                    f"ext_stickers={allow_external_stickers}",
                    self.component_name
                )
        except Exception as e:
            log_warn(
                f"[PERMS] resolution failed for msg={message.id}: "
                f"{type(e).__name__}: {e}",
                self.component_name
            )

        sticker_atts = await self._collect_stickers(getattr(message, 'stickers', None), allow_external_stickers)
        for att in sticker_atts:
            attachments.append(att)
            consumed_urls.append(att['url'])

        cross_server_forward = False
        same_server_ref = None
        poll_dict = None
        try:
            raw = self._get_raw_data(message)
            if raw:
                try:
                    ref = raw.get('messageReference') or raw.get('message_reference')
                    if ref and isinstance(ref, dict) and not forward_info:
                        ref_ch = int(ref.get('channel_id')) if ref.get('channel_id') else None
                        ref_msg_id = int(ref.get('message_id')) if ref.get('message_id') else None
                        ssr, fi, cs = await self._resolve_channel_ref(ref_ch, ref_msg_id, message)
                        if ssr:
                            same_server_ref = ssr
                        if fi:
                            forward_info = fi
                        if cs:
                            cross_server_forward = True
                except Exception:
                    pass

                try:
                    rp = raw.get('poll') or raw.get('message', {}).get('poll') if isinstance(raw, dict) else None
                    if rp and isinstance(rp, dict):
                        q = (rp.get('question') or {}).get('text') if isinstance(rp.get('question'), dict) else rp.get('question')
                        answers = []
                        for ans in rp.get('answers', []) or []:
                            media = ans.get('poll_media') or {}
                            txt = media.get('text') if isinstance(media, dict) else None
                            if txt:
                                answers.append(txt)
                        poll_dict = {
                            'question': q or 'Poll',
                            'options': answers,
                            'allows_multiple': bool(rp.get('allow_multiselect', False)),
                            'expiry': rp.get('expiry')
                        }
                except Exception:
                    poll_dict = None

                for att in await self._collect_stickers(raw.get('stickerItems', []), allow_external_stickers):
                    attachments.append(att)
                    consumed_urls.append(att['url'])

                snapshots = raw.get('messageSnapshots')
                if snapshots and isinstance(snapshots, list) and snapshots:
                    snap_msg = snapshots[0].get('message', {})

                    # messageSnapshots = Discord forward feature.
                    # The snapshot already contains the forwarded content,
                    # so we don't need to fetch the original message.
                    if not forward_info:
                        try:
                            ref = raw.get('messageReference') or raw.get('message_reference')
                            ref_ch = int(ref.get('channel_id')) if ref and ref.get('channel_id') else None
                            if ref_ch and ref_ch != message.channel.id:
                                try:
                                    ref_channel = self.bot.get_channel(ref_ch) or await self.bot.fetch_channel(ref_ch)
                                    forward_info = {'from_user': f"#{getattr(ref_channel, 'name', 'Unknown')}"}
                                except Exception:
                                    forward_info = {'from_user': 'Forwarded'}
                            else:
                                forward_info = {'from_user': f"#{getattr(message.channel, 'name', 'this channel')}"}
                        except Exception:
                            forward_info = {'from_user': 'Forwarded'}
                        # We have the snapshot data, so this is NOT a cross-server issue
                        cross_server_forward = False

                    for att in await self._collect_stickers(snap_msg.get('stickerItems', []), allow_external_stickers):
                        attachments.append(att)
                        consumed_urls.append(att['url'])

                    if snap_msg.get('content') and not message.content and not same_server_ref:
                        message.content = snap_msg['content']

                    for att in snap_msg.get('attachments', []) or []:
                        try:
                            u = att.get('proxy_url') or att.get('url')
                            fn = att.get('filename') or 'file'
                            if u:
                                check_url = self._unwrap_external_url(u)
                                if should_skip_download(check_url):
                                    continue
                                fp = await self.core.download_with_recovery(u, fn, message=message)
                                if fp:
                                    attachments.append({
                                        'url': u,
                                        'filename': fn,
                                        'type': normalize_mime_type(att.get('content_type'), filename=fn, url=u),
                                        'local_path': fp
                                    })
                                    consumed_urls.append(u)
                        except Exception:
                            continue
        except Exception:
            pass

        try:
            if not forward_info and not same_server_ref:
                links = re.findall(r"https?://(?:canary\.|ptb\.)?discord\.com/channels/\d+/(\d+)/(\d+)", message.content or '')
                for ch_id_str, msg_id_str in links:
                    try:
                        ch_id, msg_id = int(ch_id_str), int(msg_id_str)
                        ssr, fi, cs = await self._resolve_channel_ref(ch_id, msg_id, message)
                        if ssr:
                            same_server_ref = ssr
                            consumed_urls.append(f"https://discord.com/channels/{getattr(message.guild, 'id', 0)}/{ch_id}/{msg_id}")
                            break
                        if fi:
                            forward_info = fi
                        if cs:
                            cross_server_forward = True
                            break
                    except Exception:
                        continue
        except Exception:
            pass

        try:
            raw = self._get_raw_data(message)
            if raw and raw.get('messageSnapshots'):
                if not forward_info:
                    forward_info = None
                try:
                    snapshots = raw.get('messageSnapshots')
                    snap_msg = snapshots[0].get('message', {}) if snapshots else {}
                    faux = type('M', (), {'embeds': snap_msg.get('embeds', [])})
                    snap_embed_atts = await self._extract_attachments_from_embeds(faux)
                    for att in snap_embed_atts:
                        if att['url'] in seen_urls:
                            continue
                        attachments.append(att)
                        seen_urls.add(att['url'])
                        consumed_urls.append(att['url'])
                        if att.get('page_url'):
                            consumed_urls.append(att['page_url'])
                            seen_urls.add(att['page_url'])
                except Exception:
                    pass
        except Exception:
            pass

        for attachment in message.attachments:
            log_debug(f"Processing attachment: {attachment.filename}")
            primary_url = attachment.proxy_url if hasattr(attachment, 'proxy_url') and attachment.proxy_url else attachment.url
            fallback_url = attachment.url if hasattr(attachment, 'proxy_url') and attachment.proxy_url else None

            if primary_url in seen_urls:
                continue

            file_path = await self.core.download_with_recovery(primary_url, attachment.filename, message=message)
            if not file_path and fallback_url and fallback_url not in seen_urls:
                file_path = await self.core.download_with_recovery(fallback_url, attachment.filename, message=message)
            if not file_path:
                await asyncio.sleep(0.8)
                file_path = await self.core.download_with_recovery(primary_url, attachment.filename, message=message)
                if file_path:
                    primary_url = fallback_url

            if file_path:
                att_dict = {
                    'url': primary_url,
                    'filename': attachment.filename,
                    'type': normalize_mime_type(attachment.content_type, filename=attachment.filename, url=primary_url),
                    'local_path': file_path
                }
                if attachment.is_spoiler():
                    att_dict['spoiler'] = True
                attachments.append(att_dict)
                seen_urls.add(primary_url)
                if primary_url:
                    consumed_urls.append(primary_url)
                log_success(f"Downloaded: {attachment.filename}")
            else:
                log_error(f"Failed to download attachment: {attachment.filename} from {primary_url}")

        try:
            embed_attachments = [] if not allow_embeds else await self._extract_attachments_from_embeds(message)
            for att in embed_attachments:
                if att['url'] in seen_urls:
                    continue
                attachments.append(att)
                seen_urls.add(att['url'])
                consumed_urls.append(att['url'])
                if att.get('page_url'):
                    consumed_urls.append(att['page_url'])
                    seen_urls.add(att['page_url'])
            if allow_embeds and not embed_attachments and getattr(message, 'embeds', None):
                await asyncio.sleep(0.8)
                embed_attachments = await self._extract_attachments_from_embeds(message)
                for att in embed_attachments:
                    if att['url'] in seen_urls:
                        continue
                    attachments.append(att)
                    seen_urls.add(att['url'])
                    consumed_urls.append(att['url'])
                    if att.get('page_url'):
                        consumed_urls.append(att['page_url'])
                        seen_urls.add(att['page_url'])
                if not embed_attachments and getattr(message, 'embeds', None):
                    for e in message.embeds:
                        try:
                            v = getattr(e, 'video', None)
                            p = getattr(v, 'proxy_url', None) or getattr(v, 'proxyURL', None)
                            if p:
                                referer = self._unwrap_external_url(str(p))
                                if referer != str(p) and not should_skip_download(referer):
                                    fp = await self.core.download_with_recovery(referer, None, message=message)
                                    if not fp:
                                        fp = await self.core.download_media(referer)
                                    if fp:
                                        fname = os.path.basename(referer.split('?')[0]) or f"media_{int(time.time())}"
                                        ftype = get_file_type_from_url(referer, fname)
                                        attachments.append({'url': referer, 'filename': fname, 'type': ftype, 'local_path': fp})
                                        consumed_urls.append(referer)
                        except Exception:
                            continue
        except Exception as _e:
            log_debug(f"Embed extraction failed: {_e}")

        try:
            if not attachments and (getattr(message, 'embeds', None) or ('http' in (message.content or ''))):
                await asyncio.sleep(1.0)
                try:
                    fresh = await message.channel.fetch_message(message.id)
                except Exception:
                    fresh = None
                if fresh:
                    try:
                        embed_attachments = await self._extract_attachments_from_embeds(fresh)
                        for att in embed_attachments:
                            if att['url'] in seen_urls:
                                continue
                            attachments.append(att)
                            seen_urls.add(att['url'])
                            consumed_urls.append(att['url'])
                            if att.get('page_url'):
                                consumed_urls.append(att['page_url'])
                    except Exception:
                        pass
                    try:
                        fresh_media_urls = extract_media_urls(fresh.content)
                        for url in fresh_media_urls:
                            if url in seen_urls:
                                continue
                            is_discord_cdn_text = any(domain in url for domain in ['cdn.discordapp.com', 'media.discordapp.net', '/attachments/'])
                            if is_discord_cdn_text:
                                file_path = await self.core.download_with_recovery(url, None, message=fresh)
                                if file_path:
                                    filename = os.path.basename(url.split('?')[0]) or f"media_{int(time.time())}"
                                    file_type = get_file_type_from_url(url, filename)
                                    attachments.append({'url': url, 'filename': filename, 'type': file_type, 'local_path': file_path})
                                    seen_urls.add(url)
                                    consumed_urls.append(url)
                                    continue
                            if should_skip_download(url):
                                continue
                            file_path = await self.core.download_media(url)
                            if file_path:
                                filename = os.path.basename(url.split('?')[0]) or f"media_{int(time.time())}"
                                file_type = get_file_type_from_url(url, filename)
                                attachments.append({'url': url, 'filename': filename, 'type': file_type, 'local_path': file_path})
                                seen_urls.add(url)
                                consumed_urls.append(url)
                    except Exception:
                        pass
        except Exception:
            pass

        media_urls = [] if not allow_embeds else extract_media_urls(message.content)
        ignored_hint_urls = set(extract_ignore_hint_urls(message.content))
        tenor_urls = extract_tenor_urls(message.content)
        skipped_links = []
        failed_downloads = 0
        for url in media_urls:
            if url in seen_urls:
                continue
            if url in ignored_hint_urls:
                skipped_links.append(url)
                continue
            is_discord_cdn_text = any(domain in url for domain in ['cdn.discordapp.com', 'media.discordapp.net', '/attachments/'])
            if is_discord_cdn_text:
                file_path = await self.core.download_with_recovery(url, None, message=message)
                if file_path:
                    filename = os.path.basename(url.split('?')[0]) or f"media_{int(time.time())}"
                    file_type = get_file_type_from_url(url, filename)
                    attachments.append({'url': url, 'filename': filename, 'type': file_type, 'local_path': file_path})
                    seen_urls.add(url)
                    consumed_urls.append(url)
                    continue
                else:
                    continue
            if should_skip_download(url):
                skipped_links.append(url)
                continue

            file_path = await self.core.download_media(url)
            if file_path:
                filename = os.path.basename(url.split('?')[0]) or f"media_{int(time.time())}"
                file_type = get_file_type_from_url(url, filename)
                attachments.append({'url': url, 'filename': filename, 'type': file_type, 'local_path': file_path})
                seen_urls.add(url)
                consumed_urls.append(url)
            else:
                await asyncio.sleep(0.6)
                file_path = await self.core.download_media(url)
                if file_path:
                    filename = os.path.basename(url.split('?')[0]) or f"media_{int(time.time())}"
                    file_type = get_file_type_from_url(url, filename)
                    attachments.append({'url': url, 'filename': filename, 'type': file_type, 'local_path': file_path})
                    seen_urls.add(url)
                    consumed_urls.append(url)
                else:
                    failed_downloads += 1

        reply_to_id = None
        if message.reference and message.reference.message_id and not forward_info:
            try:
                if hasattr(message.reference, 'channel_id') and message.reference.channel_id != message.channel.id:
                    try:
                        ref_channel = self.bot.get_channel(message.reference.channel_id) or await self.bot.fetch_channel(message.reference.channel_id)
                        forward_info = {'from_user': f"#{getattr(ref_channel, 'name', 'Unknown')}"}
                    except Exception:
                        forward_info = {'from_user': 'Forwarded'}
                else:
                    reply_to_id = str(message.reference.message_id)
            except Exception:
                pass

        # Forward detection: Discord forwards have type=default, a reference,
        # but empty content on the outer message. If we haven't already resolved
        # the referenced content (e.g. raw data wasn't available), fetch it now.
        if (message.type == self._dlib.MessageType.default
                and message.reference and message.reference.message_id
                and not same_server_ref
                and not (message.content or '').strip()
                and not cross_server_forward):
            ref_channel_id = getattr(message.reference, 'channel_id', None) or message.channel.id
            try:
                ref_ch = self.bot.get_channel(ref_channel_id) or await self.bot.fetch_channel(ref_channel_id)
                ref_guild = getattr(ref_ch, 'guild', None)
                cur_guild = getattr(message, 'guild', None)
                if ref_guild and cur_guild and ref_guild.id == cur_guild.id:
                    try:
                        ref_msg = await ref_ch.fetch_message(message.reference.message_id)
                        same_server_ref = {
                            'content': getattr(ref_msg, 'content', '') or '',
                            'attachments': await self._fetch_ref_attachments(ref_msg),
                        }
                    except Exception as e:
                        log_debug(f"Failed to fetch forwarded message: {e}", self.component_name)
                else:
                    cross_server_forward = True
                    if not forward_info:
                        try:
                            forward_info = {'from_user': f"#{getattr(ref_ch, 'name', 'Unknown')}"}
                        except Exception:
                            forward_info = {'from_user': 'Forwarded'}
            except Exception:
                cross_server_forward = True
                if not forward_info:
                    forward_info = {'from_user': 'Forwarded'}

        content = self.core._sanitize_discord_mentions(message, message.content)
        if not allow_embeds:
            try:
                content = re.sub(r'https?://[^\s]+', '[REDACTED]', content or '').strip()
            except Exception:
                pass
        downloaded_urls = media_urls + tenor_urls + consumed_urls
        for url in downloaded_urls:
            content = content.replace(url, '').strip()
        if skipped_links:
            extra = ' '.join(skipped_links)
            content = f"{content} {extra}".strip()

        try:
            emoji_mode = getattr(ds, 'custom_emoji', 0)
            emoji_matches = re.findall(r"<:([a-zA-Z0-9_~]+):(\d+)>", content or '')
            for ename, eid in emoji_matches:
                tag = f"<:{ename}:{eid}>"
                if emoji_mode == 2:
                    content = content.replace(tag, '')
                else:
                    content = content.replace(tag, f":{ename}:")
                emoji_url = f"https://cdn.discordapp.com/emojis/{eid}.webp"
                seen_urls.add(emoji_url)
                consumed_urls.append(emoji_url)
                if emoji_mode == 0 and allow_external_emojis:
                    fp = await self.core.download_media(emoji_url, f"{ename}.webp")
                    if fp:
                        attachments.append({'url': emoji_url, 'filename': f"{ename}.webp", 'type': 'image/webp', 'local_path': fp})
        except Exception:
            pass

        if forward_info:
            content = forward_info.get('content', content)

        try:
            if same_server_ref:
                ref_preview = (same_server_ref.get('content') or '').strip()
                if not ref_preview and same_server_ref.get('attachments'):
                    ref_preview = '[media]'
                header = 'user referenced this message: '
                content = f"{header}{ref_preview}\n{content or ''}".strip()
                for att in same_server_ref.get('attachments', []):
                    attachments.append(att)
        except Exception:
            pass

        if ds.ignore_text and not cross_server_forward:
            url_pattern = r'https?://[^\s]+'
            all_urls = re.findall(url_pattern, message.content, re.IGNORECASE)
            remaining_urls = [url for url in all_urls if url not in downloaded_urls]
            if remaining_urls:
                content = ' '.join(remaining_urls)
            else:
                content = ''

        if ds.ignore_text and not content and not attachments:
            return

        if cross_server_forward:
            attachments = []
            content = "Discord forwards are not supported between servers"

        if not attachments and failed_downloads > 0 and (message.content or '').strip():
            content = (content + "\n" if content else "") + (message.content or '')

        if not attachments and not (content or '').strip():
            content = "This format is not supported"

        content = apply_link_replacements(content)

        try:
            if privacy_level == 1:
                effective_author_name = compute_incognito_name(
                    message.channel.id, message.author.id
                )
                bg = self.core.settings.get('INCOGNITO_AVATAR_BG', 'transparent')
                effective_author_id = ensure_incognito_avatar(
                    self.core.avatar_db, 'discord',
                    message.channel.id, message.author.id, bg_color=bg,
                )
            else:
                await self._resolve_and_cache_discord_avatar(message.author)
                effective_author_name = message.author.name
                effective_author_id = str(message.author.id)

            bridge_msg = BridgeMessage(
                bridge_name=bridge_name,
                message_id=str(message.id),
                channel_id=str(message.channel.id),
                author_name=effective_author_name,
                author_id=effective_author_id,
                content=content,
                attachments=attachments,
                reply_to_id=reply_to_id,
                is_forward=bool(forward_info),
                forward_from=forward_info.get('from_user') if forward_info else None,
                timestamp=time.time(),
                source='discord',
                channel_name=getattr(message.channel, 'name', None),
                poll=poll_dict if 'poll_dict' in dir() else None
            )
            json_body = bridge_msg.to_json()
            log_debug(f"BridgeMessage created OK, json_len={len(json_body)}", self.component_name)
        except Exception as e:
            log_error(f"BridgeMessage creation failed: {e}", self.component_name)
            import traceback; traceback.print_exc()
            return

        log_debug(f"Publishing to '{self.core.queues['scribe_discord']}', msg_id={bridge_msg.message_id}, author={bridge_msg.author_name}", self.component_name)
        self.core._safe_publish(self.core.queues['scribe_discord'], json_body)
        log_debug(f"_safe_publish returned OK", self.component_name)
        await asyncio.sleep(0.05)

    # ── edit / delete / pin ─────────────────────────────

    async def handle_message_edit(self, before, after):
        bridge_name, bridge_config = self.core.get_bridge_by_discord_channel(after.channel.id)
        if not bridge_config or not self._can_forward_to_others(bridge_config):
            return
        ds = bridge_config.get_platform('discord')
        if ds and ds.ignore_bots and after.author.bot:
            return

        # Only process actual user/bot edits, not embed loads or attachment URL updates
        if after.edited_at is None:
            return

        def norm_text(msg_obj) -> str:
            try:
                base = escape_discord_emojis(self.core._sanitize_discord_mentions(msg_obj, getattr(msg_obj, 'content', '') or ''))
                # Strip leading ```ini / ```diff code-block prefix used by the
                # bot-mode bridge to render source-platform attribution. The
                # pilgrim's edit cache strips the same way; if we don't match
                # it here, our own bot edits will echo back through.
                base = re.sub(r"^```(?:diff|ini)?\s*[\s\S]*?```\s*", "", base, flags=re.M)
                base = base.replace('\u200b', '').replace('\u200c', '').replace('\xa0', ' ')
                base = re.sub(r"\s+", " ", base).strip()
                return base
            except Exception:
                return (getattr(msg_obj, 'content', '') or '').strip()

        try:
            before_content = norm_text(before)
            after_content = norm_text(after)

            if hasattr(self.core, '_inbound_edit_cache'):
                cached = self.core._inbound_edit_cache.get(after.id)
                if cached and cached[0] == after_content:
                    return

            if before_content == after_content:
                return
        except Exception:
            pass

        db = self.core.bridge_dbs[bridge_name]
        mappings = db.get_all_mappings('discord', str(after.id))
        if mappings:
            clean_content = escape_discord_emojis(self.core._sanitize_discord_mentions(after, after.content or ''))
            media_urls = extract_media_urls(after.content or '')
            tenor_urls = extract_tenor_urls(after.content or '')
            for url in media_urls + tenor_urls:
                clean_content = clean_content.replace(url, '').strip()

            channel_name = ''
            try:
                channel_name = after.channel.name or ''
            except Exception:
                pass

            # Pack all target IDs into a single body. Arbiter routes the
            # one body to each pilgrim queue and each pilgrim picks out
            # its own *_message_id. Publishing one body per mapping would
            # send N copies (each routed to N pilgrims = N*N deliveries)
            # and produce N "discord -> matrix, telegram, stoatchat"
            # lines in the arbiter log per source edit.
            edit_msg = {
                'type': 'edit',
                'source': 'discord',
                'bridge_name': bridge_name,
                'new_text': clean_content,
                'author_name': after.author.name,
                'channel_name': channel_name,
            }
            for platform, platform_id in mappings.items():
                edit_msg[f'{platform}_message_id'] = platform_id
            self.core._safe_publish(self.core.queues['scribe_discord'], json.dumps(edit_msg))

    async def handle_message_delete(self, message):
        bridge_name, bridge_config = self.core.get_bridge_by_discord_channel(message.channel.id)
        if not bridge_config or not self._can_forward_to_others(bridge_config):
            return

        # Best-effort author of the deleted message. on_message_delete only
        # fires for cached messages, so message.author is normally present.
        # Use .name (internal username) to match what normal messages and
        # edits send - .display_name is the per-server nickname.
        author_obj = getattr(message, 'author', None)
        author_name = (
            getattr(author_obj, 'name', None)
            or getattr(author_obj, 'display_name', None)
            or ''
        )

        db = self.core.bridge_dbs[bridge_name]
        mappings = db.get_all_mappings('discord', str(message.id))
        if mappings:
            # Single body with all target IDs (see edit handler for why).
            delete_msg = {
                'type': 'delete',
                'source': 'discord',
                'bridge_name': bridge_name,
                'author_name': author_name,
            }
            for platform, platform_id in mappings.items():
                delete_msg[f'{platform}_message_id'] = platform_id
            self.core._safe_publish(self.core.queues['scribe_discord'], json.dumps(delete_msg))

    async def handle_pin_update(self, channel, last_pin):
        bridge_name, bridge_config = self.core.get_bridge_by_discord_channel(channel.id)
        if not bridge_config or not self._can_forward_to_others(bridge_config):
            return

        try:
            pins = await channel.pins()
            if pins:
                latest_pin = pins[0]

                pinner_name = None
                try:
                    if channel.guild:
                        now = datetime.now(timezone.utc)
                        async for entry in channel.guild.audit_logs(action=self._dlib.AuditLogAction.message_pin, limit=5):
                            if (hasattr(entry, 'extra') and entry.extra
                                    and getattr(entry.extra, 'message_id', None) == latest_pin.id):
                                if (now - entry.created_at).total_seconds() < 30:
                                    pinner_name = entry.user.name
                                break
                except Exception:
                    pass

                if pinner_name:
                    pin_label = f"📌 {pinner_name} pinned a message"
                else:
                    pin_label = "📌 Message pinned"

                pin_notification = BridgeMessage(
                    bridge_name=bridge_name,
                    message_id=str(int(time.time())),
                    channel_id=str(channel.id),
                    author_name=pin_label,
                    author_id=None,
                    content="",
                    attachments=[],
                    reply_to_id=str(latest_pin.id),
                    is_forward=False,
                    forward_from=None,
                    timestamp=time.time(),
                    source='discord'
                )
                self.core._safe_publish(self.core.queues['scribe_discord'], pin_notification.to_json())
        except Exception as e:
            log_error(f"Failed to handle pin update: {e}")

    async def handle_system_join(self, message):
        try:
            bridge_name, bridge_config = self.core.get_bridge_by_discord_channel(message.channel.id)
            if not bridge_config or not self._can_forward_to_others(bridge_config):
                return
            ds = bridge_config.get_platform('discord')
            if not ds or not ds.forward_member_events:
                return

            display_text = getattr(message, 'system_content', None) or f"{message.author.display_name} joined the server."

            system_msg = BridgeMessage(
                bridge_name=bridge_name,
                message_id=str(message.id),
                channel_id=str(message.channel.id),
                author_name="Discord System",
                author_id=None,
                content=f"➡️ {display_text}",
                attachments=[],
                reply_to_id=None,
                is_forward=False,
                forward_from=None,
                timestamp=time.time(),
                source='discord',
                metadata={'is_system_message': True}
            )
            self.core._safe_publish(self.core.queues['scribe_discord'], system_msg.to_json())
            log_info(f"System join message published for {message.author.display_name} on bridge {bridge_name}", self.component_name)
        except Exception as e:
            log_error(f"Failed to handle system join: {e}", self.component_name)

    async def handle_member_remove(self, member):
        if member.bot:
            return
        try:
            for bridge_name, bridge_config in self.core.bridges.items():
                if not self._can_forward_to_others(bridge_config):
                    continue
                ds = bridge_config.get_platform('discord')
                if not ds or not ds.forward_member_events:
                    continue
                channel = self.bot.get_channel(ds.channel_id_int)
                if not channel or not getattr(channel, 'guild', None):
                    continue
                if channel.guild.id != member.guild.id:
                    continue

                system_msg = BridgeMessage(
                    bridge_name=bridge_name,
                    message_id=str(int(time.time() * 1000)),
                    channel_id=str(ds.channel_id_int),
                    author_name="Discord System",
                    author_id=None,
                    content=f"⬅️ **{member.display_name}** left the server.",
                    attachments=[],
                    reply_to_id=None,
                    is_forward=False,
                    forward_from=None,
                    timestamp=time.time(),
                    source='discord',
                    metadata={'is_system_message': True}
                )
                self.core._safe_publish(self.core.queues['scribe_discord'], system_msg.to_json())
                log_info(f"Member leave event published for {member.display_name} on bridge {bridge_name}", self.component_name)
        except Exception as e:
            log_error(f"Failed to handle member remove: {e}", self.component_name)

    # ── import / catch-up ───────────────────────────────

    async def process_imports_and_missed(self):
        if getattr(self, '_import_worker_running', False):
            log_warn("[IMPORT] Worker already running; skipping duplicate invocation.", self.component_name)
            return

        self._import_worker_running = True
        try:
            bridge_count = len(self.core.bridges or {})
            log_info(f"[IMPORT] Worker queued (bridges={bridge_count}). Awaiting Discord ready event...", self.component_name)

            try:
                await self.bot.wait_until_ready()
            except Exception as ready_exc:
                log_error(f"[IMPORT] wait_until_ready failed: {ready_exc}", self.component_name)
                traceback.print_exc()
                return

            log_info("[IMPORT] Discord ready event received; sleeping 2s for cache warm-up.", self.component_name)
            await asyncio.sleep(2)

            if not self.core.bridges:
                log_warn("[IMPORT] No bridges configured; worker exiting.", self.component_name)
                return

            link_pattern = re.compile(r"/channels/(?:@me|\d+)/(?P<channel>\d+)/(?P<message>\d+)", re.IGNORECASE)

            def _parse_link(link):
                if not link:
                    return None, None
                match = link_pattern.search(str(link).strip())
                if not match:
                    return None, None
                try:
                    return int(match.group('channel')), int(match.group('message'))
                except Exception:
                    return None, None

            for bridge_name, bridge in (self.core.bridges or {}).items():
                ds = bridge.get_platform('discord')
                if not ds:
                    continue

                if not self._can_forward_to_others(bridge):
                    continue

                try:
                    bridge_channel_id = ds.channel_id_int
                except Exception:
                    continue
                if bridge_channel_id <= 0:
                    continue

                db = self.core.bridge_dbs.get(bridge_name)
                if not db:
                    continue

                start_link = ds.import_start or ''
                end_link = ds.import_end or ''
                explicit_import = bool(start_link.strip())
                end_bound_id = None

                if explicit_import:
                    start_channel_id, start_msg_id = _parse_link(start_link)
                    if start_channel_id != bridge_channel_id or not start_msg_id:
                        continue

                    if end_link:
                        end_channel_id, end_msg_id = _parse_link(end_link)
                        if not end_channel_id or end_channel_id != bridge_channel_id:
                            pass
                        else:
                            end_bound_id = end_msg_id

                    try:
                        import uuid as _uuid
                        with db.conn:
                            db.conn.execute(
                                "DELETE FROM message_map WHERE platform = 'discord' AND "
                                "(direction = 'dc2tg' OR direction IS NULL OR TRIM(direction) = '')"
                            )
                            db.conn.execute(
                                "INSERT OR REPLACE INTO message_map "
                                "(platform, platform_id, group_id, direction, timestamp) "
                                "VALUES (?, ?, ?, ?, ?)",
                                ('discord', str(start_msg_id - 1), str(_uuid.uuid4()), 'dc2tg', time.time())
                            )
                        last_processed = start_msg_id - 1
                    except Exception as seed_exc:
                        log_error(f"[IMPORT] Bridge '{bridge_name}': failed to seed: {seed_exc}", self.component_name)
                        continue
                else:
                    raw_last = db.get_last_platform_id('discord', direction='dc2tg')
                    if raw_last is None:
                        raw_last = db.get_last_platform_id('discord')
                    try:
                        last_processed = int(str(raw_last).strip()) if raw_last not in (None, '') else 0
                    except Exception:
                        last_processed = 0

                # Discord snowflake sanity check: IDs below this threshold
                # predate 2020 and are almost certainly corrupted DB values.
                # Snowflake 700000000000000000 ≈ mid-2019.
                _MIN_VALID_SNOWFLAKE = 700_000_000_000_000_000
                if 0 < last_processed < _MIN_VALID_SNOWFLAKE and not explicit_import:
                    log_warn(
                        f"[IMPORT] Bridge '{bridge_name}': last_processed={last_processed} "
                        f"looks like a corrupted value (below minimum snowflake). "
                        f"Treating as fresh database.",
                        self.component_name,
                    )
                    last_processed = 0

                history_params = {'limit': None, 'oldest_first': True}
                if last_processed > 0:
                    history_params['after'] = self._dlib.Object(id=last_processed)

                try:
                    channel = self.bot.get_channel(bridge_channel_id) or await self.bot.fetch_channel(bridge_channel_id)
                except Exception:
                    continue

                # Ensure import uses the same per-bridge lock as on_message
                if bridge_name not in self._bridge_locks:
                    self._bridge_locks[bridge_name] = asyncio.Lock()
                import_lock = self._bridge_locks[bridge_name]

                if last_processed <= 0 and not explicit_import:
                    log_info(f"[IMPORT] Bridge '{bridge_name}': fresh database, seeding with newest message.", self.component_name)
                    try:
                        newest_messages = [message async for message in channel.history(limit=1, oldest_first=False)]
                    except Exception:
                        continue
                    if not newest_messages:
                        continue
                    newest = newest_messages[0]
                    if (ds.ignore_bots and newest.author.bot) or \
                       (ds.ignore_webhooks and newest.webhook_id):
                        import uuid as _uuid
                        with db.conn:
                            db.conn.execute(
                                "INSERT OR REPLACE INTO message_map "
                                "(platform, platform_id, group_id, direction, timestamp) "
                                "VALUES (?, ?, ?, ?, ?)",
                                ('discord', str(newest.id), str(_uuid.uuid4()), 'dc2tg', time.time())
                            )
                    else:
                        async with import_lock:
                            await self.process_discord_message(newest)
                    continue

                # For non-explicit catch-up, cap at 5000 messages to prevent
                # runaway floods if something goes wrong. Explicit imports
                # have no cap (user intentionally wants full history).
                _CATCHUP_LIMIT = 5000
                processed = 0
                try:
                    async for msg in channel.history(**history_params):
                        if end_bound_id and msg.id > end_bound_id:
                            break
                        if not explicit_import and processed >= _CATCHUP_LIMIT:
                            log_warn(
                                f"[IMPORT] Bridge '{bridge_name}': catch-up limit reached "
                                f"({_CATCHUP_LIMIT} messages). Stopping to prevent flood. "
                                f"Use explicit import if you need more.",
                                self.component_name,
                            )
                            break
                        if ds.ignore_bots and msg.author.bot:
                            continue
                        if ds.ignore_webhooks and msg.webhook_id:
                            continue
                        try:
                            if db.get_all_mappings('discord', str(msg.id)):
                                continue
                        except Exception:
                            pass
                        async with import_lock:
                            await self.process_discord_message(msg)
                        processed += 1
                        if processed % 25 == 0:
                            log_info(f"[IMPORT] Bridge '{bridge_name}': processed {processed} messages so far.", self.component_name)
                        await asyncio.sleep(0.2)
                except Exception as history_exc:
                    log_error(f"[IMPORT] Bridge '{bridge_name}': history retrieval failed: {history_exc}", self.component_name)

                if processed:
                    log_success(f"[IMPORT] Bridge '{bridge_name}': finished. {processed} messages forwarded.", self.component_name)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log_error(f"[IMPORT] Worker crashed: {exc}", self.component_name)
            traceback.print_exc()
        finally:
            self._import_worker_running = False
