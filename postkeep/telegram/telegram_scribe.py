import asyncio
import json
import os
import re
import time
import traceback
from html import escape as html_escape
from typing import Optional, Dict, TYPE_CHECKING

from telegram import Update, constants
from telegram.ext import MessageHandler, filters, ContextTypes, ApplicationHandlerStop

from postkeep.papyrus import (
    ARBITER_QUEUES, apply_namespace, get_namespace,
    log_info, log_error, log_warn, log_success, log_debug,
    BridgeMessage, ensure_cache_dir,
    escape_discord_emojis, is_user_ignored, add_ignored_user, is_user_admin,
    get_cached_avatar, store_avatar_cache, compute_avatar_hash,
    AVATAR_REFRESH_INTERVAL,
    get_privacy_level, set_privacy_level, list_privacy_settings,
    get_privacy_limit, cap_privacy_level,
    format_privacy_levels_token, format_privacy_levels_help,
    format_privacy_levels_phrase,
    compute_incognito_name, ensure_incognito_avatar,
    apply_link_replacements, is_privacy_active,
    normalize_mime_type,
)

if TYPE_CHECKING:
    from postkeep.telegram.telegram_core import TelegramCore


def _norm_text(s: str) -> str:
    s = (s or '').replace('\u200b', '').replace('\u200c', '').replace('\xa0', ' ')
    return re.sub(r'\s+', ' ', s).strip()


class TelegramScribe:
    def __init__(self, core: 'TelegramCore'):
        self.core = core
        self.bot = core.bot
        self.app = core.app

        self._text_cache = {}

        self._media_group_buffers: Dict[str, list] = {}
        self._media_group_tasks: Dict[str, asyncio.Task] = {}

        # Throttle concurrent file downloads to avoid TG API rate-limiting during catch-up
        self._download_semaphore = asyncio.Semaphore(3)

        # Admin list cache for signature avatar resolution: {chat_id: (timestamp, [admin_members])}
        self._admin_cache: Dict[int, tuple] = {}
        _ADMIN_CACHE_TTL = 600  # 10 minutes
        self._ADMIN_CACHE_TTL = _ADMIN_CACHE_TTL

        # Build set of configured chat IDs for fast unconfigured-channel rejection
        self._configured_chat_ids: set = set()
        for _name, bridge in core.bridges.items():
            tg = bridge.get_platform('telegram')
            if tg and getattr(tg, 'channel_id_int', None):
                self._configured_chat_ids.add(tg.channel_id_int)

        # Track chats we already attempted to leave so we don't spam the API
        self._left_chats: set = set()

        # Cache: is privacy reachable on any bridge? When False we skip the
        # /privacy handler registration AND every per-message privacy check.
        # Refreshed by core.reload_config().
        self._privacy_active = is_privacy_active(core.bridges)

        ns = core._namespace
        queues = apply_namespace(ARBITER_QUEUES, ns)
        self._queues = queues
        self._scribe_queue = queues.get('scribe_telegram', 'scribe_telegram')

        self._rabbitmq_connection = None
        self._rabbitmq_channel = None
        self._connect_rabbitmq()

    def _connect_rabbitmq(self):
        try:
            self._rabbitmq_connection = self.core._new_rabbitmq_connection()
            self._rabbitmq_channel = self._rabbitmq_connection.channel()
            self._rabbitmq_channel.queue_declare(queue=self._scribe_queue, durable=True)

            ns = self.core._namespace
            namespaced = apply_namespace(ARBITER_QUEUES, ns)
            for q in namespaced.values():
                try:
                    self._rabbitmq_channel.queue_declare(queue=q, durable=True)
                except Exception:
                    pass
            log_success("Scribe connected to RabbitMQ")
        except Exception as e:
            log_error(f"Scribe failed to connect to RabbitMQ: {e}")
            self._rabbitmq_connection = None
            self._rabbitmq_channel = None

    def _safe_publish(self, routing_key: str, body: str):
        try:
            if not self._rabbitmq_connection or self._rabbitmq_connection.is_closed:
                self._connect_rabbitmq()
            if self._rabbitmq_channel and self._rabbitmq_channel.is_open:
                self._rabbitmq_channel.basic_publish(
                    exchange='',
                    routing_key=routing_key,
                    body=body,
                    properties=__import__('pika').BasicProperties(delivery_mode=2)
                )
            else:
                log_error("Scribe: RabbitMQ channel not open after reconnect.", 'telegram_scribe')
        except Exception as e:
            log_warn(f"Scribe: publish failed ({e}), reconnecting...", 'telegram_scribe')
            try:
                self._connect_rabbitmq()
                if self._rabbitmq_channel and self._rabbitmq_channel.is_open:
                    self._rabbitmq_channel.basic_publish(
                        exchange='',
                        routing_key=routing_key,
                        body=body,
                        properties=__import__('pika').BasicProperties(delivery_mode=2)
                    )
                else:
                    log_error("Scribe: publish failed even after reconnect.", 'telegram_scribe')
            except Exception as e2:
                log_error(f"Scribe: publish failed after reconnect: {e2}", 'telegram_scribe')

    def _do_publish(self, routing_key: str, body: str):
        try:
            if self._rabbitmq_channel and self._rabbitmq_channel.is_open:
                self._rabbitmq_channel.basic_publish(
                    exchange='',
                    routing_key=routing_key,
                    body=body,
                    properties=__import__('pika').BasicProperties(delivery_mode=2)
                )
            else:
                log_error("Scribe: RabbitMQ channel not open.", 'telegram_scribe')
        except Exception as e:
            log_error(f"Scribe: _do_publish failed: {e}", 'telegram_scribe')

    # ── handlers registration ────────────────────────

    def setup_handlers(self):
        self.app.add_handler(MessageHandler(
            filters.COMMAND & filters.Regex(r'^/topicid$'), self.handle_topicid
        ), 0)
        self.app.add_handler(MessageHandler(
            filters.TEXT & filters.Regex(r'^-topicid'), self.handle_topicid
        ), 0)
        self.app.add_handler(MessageHandler(
            filters.TEXT & filters.Regex(r'^-viewraw'), self.handle_viewraw
        ), 0)
        self.app.add_handler(MessageHandler(
            filters.TEXT & filters.Regex(r'^-ignoreuser'), self.handle_ignoreuser
        ), 0)
        self.app.add_handler(MessageHandler(
            filters.TEXT & filters.Regex(r'^-reloadconfig'), self.handle_reload
        ), 0)
        if self._privacy_active:
            self.app.add_handler(MessageHandler(
                filters.COMMAND & filters.Regex(r'^/privacy(\s|$|@)'), self.handle_privacy
            ), 0)
        else:
            log_info("Privacy is fully disabled; skipping /privacy handler registration.", 'telegram_scribe')

        self.app.add_handler(MessageHandler(
            filters.ALL & ~filters.UpdateType.EDITED, self.handle_message
        ), 1)

        edited_filter = (
            filters.UpdateType.EDITED_MESSAGE | filters.UpdateType.EDITED_CHANNEL_POST
        )
        self.app.add_handler(MessageHandler(edited_filter, self.handle_edited_message))

    def _can_forward_to_others(self, bridge_config):
        platforms = getattr(bridge_config, 'platforms', None) or {}
        return any(
            p and getattr(p, 'can_receive', False)
            for name, p in platforms.items()
            if name != 'telegram'
        )

    # ── bridge lookup ────────────────────────────────

    def get_bridge_by_telegram_channel(self, channel_id):
        for name, bridge in self.core.bridges.items():
            tg = bridge.get_platform('telegram')
            if tg and tg.channel_id_int == channel_id:
                return name, bridge
        return None, None

    def get_bridge_by_telegram_message(self, message):
        chat_id = getattr(message, 'chat_id', None)
        if chat_id is None and getattr(message, 'chat', None):
            chat_id = message.chat.id

        topic_id = getattr(message, 'message_thread_id', None)

        candidates = []
        for name, bridge in self.core.bridges.items():
            tg = bridge.get_platform('telegram')
            if tg and tg.channel_id_int == chat_id:
                candidates.append((name, bridge))
        if not candidates:
            return None, None

        if topic_id is not None:
            topic_matches = [
                (name, b)
                for name, b in candidates
                if getattr(b.get_platform('telegram'), 'topic_id', None) == topic_id
            ]
            if topic_matches:
                return topic_matches[0]

            for name, bridge in candidates:
                tg = bridge.get_platform('telegram')
                if getattr(tg, 'topic_id', None) in (None, 0):
                    log_debug(
                        f"[TG] No exact topic match for thread {topic_id}; "
                        f"using non-topic bridge '{name}'",
                        'telegram_scribe'
                    )
                    return name, bridge

            log_warn(
                f"[TG] No bridge matches topic_id={topic_id} for chat_id={chat_id}; "
                f"dropping message.",
                'telegram_scribe'
            )
            return None, None

        for name, bridge in candidates:
            tg = bridge.get_platform('telegram')
            if getattr(tg, 'topic_id', None) in (None, 0):
                return name, bridge

        name, bridge = candidates[0]
        tg = bridge.get_platform('telegram')
        log_debug(
            f"[TG] No non-topic bridge for chat_id={chat_id}; using '{name}' "
            f"(topic_id={getattr(tg, 'topic_id', None)})",
            'telegram_scribe'
        )
        return name, bridge

    # ── identity helpers ──────────────────────────────

    def _compute_webhook_identity(self, message):
        display_name = None
        prefer_chat_avatar = False
        entity_id = None

        try:
            signature = getattr(message, 'author_signature', None)

            if getattr(message, 'sender_chat', None):
                # Auto-forwarded channel posts carry signature in forward_origin
                if not signature:
                    fwd = getattr(message, 'forward_origin', None)
                    if fwd:
                        signature = getattr(fwd, 'author_signature', None)
                prefer_chat_avatar = True
                entity_id = int(abs(message.sender_chat.id))
                chat_title = getattr(message.sender_chat, 'title', None)
                if signature and chat_title and signature != chat_title:
                    display_name = f"{chat_title} - {signature}"
                elif signature:
                    display_name = signature
                else:
                    display_name = chat_title

            if not prefer_chat_avatar and not getattr(message, 'from_user', None) and getattr(message, 'chat', None):
                prefer_chat_avatar = True
                entity_id = int(abs(message.chat.id))
                if not display_name:
                    display_name = getattr(message.chat, 'title', None)

            if not prefer_chat_avatar and getattr(message, 'from_user', None):
                u = message.from_user
                display_name = display_name or signature or (u.username or u.full_name)
                entity_id = int(u.id)
        except Exception:
            pass

        if not display_name:
            try:
                if getattr(message, 'chat', None) and getattr(message.chat, 'title', None):
                    display_name = message.chat.title
                elif getattr(message, 'from_user', None):
                    u = message.from_user
                    display_name = u.username or u.full_name
            except Exception:
                pass

        return display_name or "Anonymous", entity_id, prefer_chat_avatar

    async def get_telegram_forward_info(self, message):
        forward_info = ""

        if hasattr(message, 'forward_origin') and message.forward_origin:
            if hasattr(message.forward_origin, 'type'):
                origin_type = message.forward_origin.type

                if origin_type == 'user' and hasattr(message.forward_origin, 'sender_user'):
                    user = message.forward_origin.sender_user
                    forward_info = f"{user.full_name}"
                elif origin_type == 'hidden_user' and hasattr(message.forward_origin, 'sender_user_name'):
                    forward_info = f"{message.forward_origin.sender_user_name}"
                elif origin_type == 'chat' and hasattr(message.forward_origin, 'sender_chat'):
                    chat = message.forward_origin.sender_chat
                    forward_info = f"{chat.title}"
                elif origin_type == 'channel' and hasattr(message.forward_origin, 'chat'):
                    chat = message.forward_origin.chat
                    forward_info = f"{chat.title}"
                else:
                    forward_info = "Unknown Source"
            return forward_info

        if hasattr(message, 'forward_date') and message.forward_date:
            if hasattr(message, 'forward_from') and message.forward_from:
                forward_info = f"{message.from_user.full_name}"
            elif hasattr(message, 'forward_from_chat') and message.forward_from_chat:
                forward_info = f"{message.forward_from_chat.title}"
            elif hasattr(message, 'forward_sender_name') and message.forward_sender_name:
                forward_info = f"{message.forward_sender_name}"
            else:
                forward_info = "Unknown Source"

        return forward_info

    # ── avatar caching ───────────────────────────────

    async def _cache_telegram_avatar(self, message, entity_id, is_channel: bool = False):
        """Download and cache Telegram avatar bytes. Each pilgrim uploads to its own platform."""
        if not entity_id or not self.core.avatar_db:
            return

        cache_key = str(entity_id)
        cached = get_cached_avatar(self.core.avatar_db, cache_key)
        if cached:
            age = time.time() - (cached.get('last_checked') or 0)
            if cached.get('avatar_bytes') and age < AVATAR_REFRESH_INTERVAL:
                log_debug(f"[AVATAR] Cache HIT for {cache_key}: age={age:.0f}s", 'telegram_scribe')
                return

        try:
            local_path = await self._download_avatar_via_bot_api(message, entity_id, is_channel=is_channel)
            if local_path and os.path.exists(local_path):
                with open(local_path, 'rb') as f:
                    avatar_bytes = f.read()
                new_hash = compute_avatar_hash(avatar_bytes)
                log_debug(f"[AVATAR] Downloaded OK ({len(avatar_bytes)} bytes, hash={new_hash[:12]}...)", 'telegram_scribe')
                store_avatar_cache(self.core.avatar_db, cache_key, 'telegram',
                                   avatar_hash=new_hash, avatar_bytes=avatar_bytes)
                try:
                    os.remove(local_path)
                except Exception:
                    pass
            else:
                log_debug(f"[AVATAR] No avatar available for entity {entity_id}", 'telegram_scribe')
        except Exception as e:
            log_warn(f"[AVATAR] Failed to cache avatar for {cache_key}: {e}", 'telegram_scribe')

    async def _download_avatar_via_bot_api(self, message, user_id: int, is_channel: bool) -> Optional[str]:
        log_debug(f"[AVATAR-DL] Starting: user_id={user_id}, is_channel={is_channel}", 'telegram_scribe')
        try:
            cache_dir = ensure_cache_dir()
            log_debug(f"[AVATAR-DL] Cache dir: {cache_dir}", 'telegram_scribe')
        except Exception as e:
            log_error(f"[AVATAR-DL] ensure_cache_dir FAILED: {e}", 'telegram_scribe')
            return None

        if not is_channel and user_id:
            log_debug(f"[AVATAR-DL] Trying get_user_profile_photos for user_id={user_id}...", 'telegram_scribe')
            try:
                photos = await self.bot.get_user_profile_photos(user_id=user_id, limit=1)
                total = getattr(photos, 'total_count', 0)
                log_debug(f"[AVATAR-DL] get_user_profile_photos returned: total_count={total}", 'telegram_scribe')
                if photos and total > 0 and photos.photos:
                    sizes = photos.photos[0]
                    if sizes:
                        log_debug(f"[AVATAR-DL] Got {len(sizes)} size(s), downloading largest...", 'telegram_scribe')
                        try:
                            file = await sizes[-1].get_file()
                            file_path = os.path.join(cache_dir, f"tg_avatar_{user_id}_{int(time.time())}.jpg")
                            await file.download_to_drive(file_path)
                            if os.path.exists(file_path):
                                fsize = os.path.getsize(file_path)
                                log_debug(f"[AVATAR-DL] User avatar saved: {file_path} ({fsize} bytes)", 'telegram_scribe')
                                return file_path
                            else:
                                log_warn(f"[AVATAR-DL] download_to_drive returned but file missing: {file_path}", 'telegram_scribe')
                        except Exception as e:
                            log_warn(f"[AVATAR-DL] get_file/download_to_drive FAILED: {type(e).__name__}: {e}", 'telegram_scribe')
                    else:
                        log_debug(f"[AVATAR-DL] photos.photos[0] is empty", 'telegram_scribe')
                else:
                    log_debug(f"[AVATAR-DL] User has no visible profile photos (total_count={total})", 'telegram_scribe')
            except Exception as e:
                log_warn(f"[AVATAR-DL] get_user_profile_photos FAILED: {type(e).__name__}: {e}", 'telegram_scribe')

        chat_id = None
        try:
            if getattr(message, 'sender_chat', None):
                chat_id = message.sender_chat.id
            elif getattr(message, 'chat', None):
                chat_id = message.chat.id
        except Exception:
            chat_id = None

        if chat_id:
            log_debug(f"[AVATAR-DL] Trying chat photo fallback for chat_id={chat_id}...", 'telegram_scribe')
            try:
                chat_obj = await self.bot.get_chat(chat_id)
                has_photo = bool(getattr(chat_obj, 'photo', None))
                log_debug(f"[AVATAR-DL] get_chat OK: title={getattr(chat_obj, 'title', '?')}, has_photo={has_photo}", 'telegram_scribe')
                if has_photo:
                    file_id = getattr(chat_obj.photo, 'big_file_id', None) or getattr(chat_obj.photo, 'small_file_id', None)
                    if file_id:
                        log_debug(f"[AVATAR-DL] Downloading chat photo file_id={file_id[:20]}...", 'telegram_scribe')
                        try:
                            fobj = await self.bot.get_file(file_id)
                            file_path = os.path.join(cache_dir, f"tg_chat_avatar_{abs(chat_obj.id)}_{int(time.time())}.jpg")
                            await fobj.download_to_drive(file_path)
                            if os.path.exists(file_path):
                                fsize = os.path.getsize(file_path)
                                log_debug(f"[AVATAR-DL] Chat avatar saved: {file_path} ({fsize} bytes)", 'telegram_scribe')
                                return file_path
                            else:
                                log_warn(f"[AVATAR-DL] Chat avatar download_to_drive returned but file missing: {file_path}", 'telegram_scribe')
                        except Exception as e:
                            log_warn(f"[AVATAR-DL] Chat photo download FAILED: {type(e).__name__}: {e}", 'telegram_scribe')
                    else:
                        log_debug(f"[AVATAR-DL] Chat photo exists but no file_id found", 'telegram_scribe')
                else:
                    log_debug(f"[AVATAR-DL] Chat has no photo", 'telegram_scribe')
            except Exception as e:
                log_warn(f"[AVATAR-DL] get_chat FAILED for {chat_id}: {type(e).__name__}: {e}", 'telegram_scribe')
        else:
            log_debug(f"[AVATAR-DL] No chat_id available for photo fallback", 'telegram_scribe')

        log_warn(f"[AVATAR-DL] All avatar sources exhausted for user_id={user_id}, chat_id={chat_id} - returning None", 'telegram_scribe')
        return None

    # ── signature → admin resolution ─────────────────

    async def _resolve_signature_admin(self, chat_id: int, signature: str):
        """Match a channel-post signature to a chat admin.
        Returns (user_obj, is_bot) or (None, False)."""
        if not signature:
            return None, False

        cached = self._admin_cache.get(chat_id)
        if cached and (time.time() - cached[0]) < self._ADMIN_CACHE_TTL:
            admins = cached[1]
        else:
            try:
                admins = await self.bot.get_chat_administrators(chat_id)
                self._admin_cache[chat_id] = (time.time(), list(admins))
                log_debug(f"[SIG] Fetched {len(admins)} admins for chat {chat_id}", 'telegram_scribe')
            except Exception as e:
                log_warn(f"[SIG] Failed to fetch admins for {chat_id}: {e}", 'telegram_scribe')
                return None, False

        # Match by name: full_name first, then first_name
        for admin in admins:
            user = admin.user
            if getattr(user, 'full_name', None) == signature:
                log_debug(f"[SIG] Matched signature '{signature}' → admin {user.id} (full_name)", 'telegram_scribe')
                return user, getattr(user, 'is_bot', False)

        for admin in admins:
            user = admin.user
            if getattr(user, 'first_name', None) == signature:
                log_debug(f"[SIG] Matched signature '{signature}' → admin {user.id} (first_name)", 'telegram_scribe')
                return user, getattr(user, 'is_bot', False)

        log_debug(f"[SIG] No admin matched signature '{signature}' in chat {chat_id}", 'telegram_scribe')
        return None, False

    # ── blockquote formatting ─────────────────────────

    @staticmethod
    def _apply_blockquote_formatting(text, entities, source_label='Telegram'):
        """Style Telegram blockquotes: ini header above, quoted text below as regular text."""
        blockquotes = sorted(
            [e for e in (entities or []) if e.type in ('blockquote', 'expandable_blockquote')],
            key=lambda e: e.offset
        )
        if not blockquotes:
            return text

        # Work in UTF-16 space (Telegram entity offsets are UTF-16 code units)
        encoded = text.encode('utf-16-le')
        result_parts = []
        last_end = 0

        for entity in blockquotes:
            start = entity.offset * 2
            end = (entity.offset + entity.length) * 2

            before = encoded[last_end:start].decode('utf-16-le')
            if before:
                result_parts.append(before)

            bq_text = encoded[start:end].decode('utf-16-le')
            result_parts.append(f"```ini\n[{source_label}]\n```\n{bq_text}")

            last_end = end

        remaining = encoded[last_end:].decode('utf-16-le')
        if remaining:
            result_parts.append(remaining)

        return ''.join(result_parts)

    # ── spoiler formatting ─────────────────────────────

    @staticmethod
    def _apply_spoiler_formatting(text, entities):
        """Wrap Telegram spoiler entities in ||spoiler|| markers."""
        spoilers = sorted(
            [e for e in (entities or []) if e.type == 'spoiler'],
            key=lambda e: e.offset
        )
        if not spoilers:
            return text

        encoded = text.encode('utf-16-le')
        result_parts = []
        last_end = 0

        for entity in spoilers:
            start = entity.offset * 2
            end = (entity.offset + entity.length) * 2

            before = encoded[last_end:start].decode('utf-16-le')
            if before:
                result_parts.append(before)

            spoiler_text = encoded[start:end].decode('utf-16-le')
            result_parts.append(f"||{spoiler_text}||")

            last_end = end

        remaining = encoded[last_end:].decode('utf-16-le')
        if remaining:
            result_parts.append(remaining)

        return ''.join(result_parts)

    # ── message handlers ──────────────────────────────

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        message = update.effective_message
        if not message:
            log_debug("Received update without message payload", 'telegram_scribe')
            return

        chat_id = getattr(message, 'chat_id', None)
        msg_id = getattr(message, 'message_id', None)
        is_reply = bool(getattr(message, 'reply_to_message', None))

        self.core._trace(f"incoming message chat={chat_id} id={msg_id} reply={is_reply}")

        bridge_name, bridge_config = self.get_bridge_by_telegram_message(message)

        if not bridge_name or not bridge_config:
            chat_type = getattr(getattr(message, 'chat', None), 'type', None)

            # Private chat (DM with bot): can't leave, show info message instead.
            if chat_type == 'private':
                if chat_id and chat_id not in self._left_chats:
                    self._left_chats.add(chat_id)
                    info_msg = self.core.settings.get(
                        'TG_PRIVATE_CHAT_MESSAGE',
                        "👋 Hi! I'm a CHATAFT bridge bot and I don't operate in private messages. "
                        "Visit the project page for more info: "
                        "https://github.com/ilikeshiny/CHATAFT/"
                    )
                    try:
                        if info_msg:
                            await context.bot.send_message(chat_id=chat_id, text=info_msg)
                    except Exception as e:
                        log_debug(f"Could not send private chat info to {chat_id}: {e}", 'telegram_scribe')
                return

            if chat_id and chat_id not in self._configured_chat_ids and chat_id not in self._left_chats:
                log_warn(f"Unconfigured chat {chat_id}; attempting to leave.", 'telegram_scribe')
                self._left_chats.add(chat_id)
                leave_msg = self.core.settings.get(
                    'TG_UNCONFIGURED_LEAVE_MESSAGE',
                    "⚠️ Detected unconfigured bridge - leaving the chat. "
                    "For more information visit bot's github page - "
                    "https://github.com/ilikeshiny/CHATAFT/"
                )
                try:
                    if leave_msg:
                        await context.bot.send_message(chat_id=chat_id, text=leave_msg)
                except Exception as e:
                    log_debug(f"Could not send leave message to {chat_id}: {e}", 'telegram_scribe')
                try:
                    await context.bot.leave_chat(chat_id)
                    log_info(f"Left unconfigured chat {chat_id}", 'telegram_scribe')
                except Exception as e:
                    log_debug(f"Could not leave chat {chat_id}: {e}", 'telegram_scribe')
            return

        tg = bridge_config.get_platform('telegram')
        listener = (tg.listener if tg else 'bot')
        if listener != 'bot':
            log_info(
                f"Bridge '{bridge_name}' is handled by {listener}; ignoring msg={msg_id}",
                'telegram_scribe'
            )
            return

        if not self._can_forward_to_others(bridge_config):
            log_info(
                f"Bridge '{bridge_name}' has no receiving platforms; skipping msg={msg_id}",
                'telegram_scribe'
            )
            return

        media_group_id = getattr(message, 'media_group_id', None)
        if media_group_id:
            buf_key = f"{bridge_name}:{media_group_id}"
            if buf_key not in self._media_group_buffers:
                self._media_group_buffers[buf_key] = []
            self._media_group_buffers[buf_key].append((message, bridge_name, bridge_config))
            log_debug(
                f"Buffered media group msg={msg_id} group={media_group_id} "
                f"(count={len(self._media_group_buffers[buf_key])})",
                'telegram_scribe'
            )

            existing_task = self._media_group_tasks.get(buf_key)
            if existing_task and not existing_task.done():
                existing_task.cancel()
            self._media_group_tasks[buf_key] = asyncio.create_task(
                self._flush_media_group(buf_key)
            )
            return

        await self.process_bridge_message(message, bridge_name, bridge_config)

    async def _flush_media_group(self, buf_key: str):
        try:
            await asyncio.sleep(1.5)
        except asyncio.CancelledError:
            return

        entries = self._media_group_buffers.pop(buf_key, [])
        self._media_group_tasks.pop(buf_key, None)

        if not entries:
            return

        entries.sort(key=lambda e: e[0].message_id)
        primary_msg, bridge_name, bridge_config = entries[0]
        extra_messages = [e[0] for e in entries[1:]]

        log_debug(
            f"Flushing media group {buf_key}: {len(entries)} message(s), "
            f"primary={primary_msg.message_id}",
            'telegram_scribe'
        )

        await self.process_bridge_message(
            primary_msg, bridge_name, bridge_config,
            extra_media_messages=extra_messages
        )

    async def process_bridge_message(self, message, bridge_name, bridge_config,
                                     extra_media_messages=None):
        try:
            system_content = None
            tg = bridge_config.get_platform('telegram')
            if message.new_chat_members:
                if tg and tg.forward_member_events:
                    names = [user.full_name for user in message.new_chat_members if not user.is_bot]
                    if names:
                        system_content = f"➡️ **{' and '.join(names)}** joined the group."
                else:
                    return
            elif message.left_chat_member:
                if tg and tg.forward_member_events:
                    if not message.left_chat_member.is_bot:
                        system_content = f"⬅️ **{message.left_chat_member.full_name}** left the group."
                else:
                    return
            elif message.new_chat_title:
                system_content = f"ℹ️ Group title was changed to **{message.new_chat_title}**."
            elif message.new_chat_photo:
                system_content = "🖼️ Group photo was updated."
            elif message.delete_chat_photo:
                system_content = "🖼️ Group photo was removed."
            elif getattr(message, 'pinned_message', None):
                pinned = message.pinned_message
                pinner = ""
                if message.from_user:
                    pinner = message.from_user.full_name
                elif message.sender_chat:
                    pinner = message.sender_chat.title or ""
                if pinner:
                    system_content = f"📌 **{pinner}** pinned a message"
                else:
                    system_content = f"📌 A message was pinned"

            if system_content:
                log_info(f"Processing system message for {bridge_name}: {system_content}")

                # For pin notifications, set reply_to_id to the pinned message
                # so the bot replies to it on the target platform
                pin_reply_id = None
                if getattr(message, 'pinned_message', None):
                    pin_reply_id = str(message.pinned_message.message_id)

                bridge_msg = BridgeMessage(
                    bridge_name=bridge_name,
                    message_id=str(message.message_id),
                    channel_id=str(message.chat_id),
                    author_name="Telegram System",
                    author_id=None,
                    content=system_content,
                    attachments=[],
                    reply_to_id=pin_reply_id,
                    is_forward=False,
                    forward_from=None,
                    timestamp=time.time(),
                    source='telegram',
                    metadata={'is_system_message': True}
                )
                self._safe_publish(self._scribe_queue, bridge_msg.to_json())
                log_success(f"Published system message from {bridge_name} to scribe queue.")
                return

            if message.from_user and message.from_user.id == self.bot.id:
                return

            # Record native Telegram message for prefix collapsing
            try:
                self.core._last_native_tg_msg[bridge_name] = time.time()
            except Exception:
                pass

            sender_id = (message.from_user.id if message.from_user
                         else (message.sender_chat.id if message.sender_chat else None))
            if sender_id and is_user_ignored('telegram', sender_id):
                return

            display_name, entity_id, prefer_chat_avatar = self._compute_webhook_identity(message)
            forward_from = await self.get_telegram_forward_info(message)
            is_forward = bool(forward_from)

            # Auto-forwarded channel posts to linked groups aren't user-initiated forwards
            if is_forward and getattr(message, 'is_automatic_forward', False):
                forward_from = None
                is_forward = False

            # ── signature avatar resolution ──
            signature = getattr(message, 'author_signature', None)
            if not signature and getattr(message, 'sender_chat', None):
                fwd = getattr(message, 'forward_origin', None)
                if fwd:
                    signature = getattr(fwd, 'author_signature', None)
            sig_mode = getattr(tg, 'parse_channel_signatures', 0) if tg else 0
            log_debug(
                f"[SIG-DIAG] msg={message.message_id} signature={signature!r} "
                f"prefer_chat_avatar={prefer_chat_avatar} sig_mode={sig_mode} "
                f"display_name={display_name!r}",
                'telegram_scribe'
            )
            resolved_admin_user_id = None
            if signature and prefer_chat_avatar and sig_mode > 0:
                try:
                    chat_id_for_admins = message.sender_chat.id if message.sender_chat else message.chat.id
                    admin_user, admin_is_bot = await self._resolve_signature_admin(chat_id_for_admins, signature)
                    if admin_user:
                        if sig_mode == 2 and admin_is_bot:
                            # Mode 2 + bot: keep channel avatar
                            log_debug(f"[SIG] Bot signature '{signature}' → using channel avatar", 'telegram_scribe')
                        else:
                            # Mode 1 or human in mode 2: use admin's avatar
                            entity_id = int(admin_user.id)
                            prefer_chat_avatar = False
                            resolved_admin_user_id = int(admin_user.id)
                            log_debug(f"[SIG] Resolved '{signature}' → user {entity_id}", 'telegram_scribe')
                except Exception as e:
                    log_warn(f"[SIG] Signature resolution failed: {e}", 'telegram_scribe')

            # ── privacy (after signature resolution so channel posts route to admin) ──
            # Use the actual user identity: from_user for normal posts, the resolved
            # admin user_id for signed channel posts. Anonymous group admins / unresolved
            # signatures don't have a real user_id and therefore can't be matched.
            effective_user_id = None
            if message.from_user:
                effective_user_id = message.from_user.id
            elif resolved_admin_user_id is not None:
                effective_user_id = resolved_admin_user_id

            if effective_user_id is not None and self._privacy_active:
                privacy_level = get_privacy_level(
                    'telegram', effective_user_id, message.chat_id,
                    bridge=bridge_config,
                )
                if privacy_level >= 2:
                    log_debug(
                        f"Privacy level 2: dropping msg={message.message_id} "
                        f"from user {effective_user_id}",
                        'telegram_scribe'
                    )
                    return
                if privacy_level == 1:
                    display_name = compute_incognito_name(message.chat_id, effective_user_id)
                    # Generate (or reuse) a deterministic identicon, then route
                    # avatar lookups through the synthetic incognito ID so we
                    # don't leak the real user's avatar.
                    bg = self.core.settings.get('INCOGNITO_AVATAR_BG', 'transparent')
                    entity_id = ensure_incognito_avatar(
                        self.core.avatar_db, 'telegram',
                        message.chat_id, effective_user_id, bg_color=bg,
                    )
                    prefer_chat_avatar = False

            db_lookup = self.core.bridge_dbs.get(bridge_name)
            if db_lookup is None:
                log_warn(f"DB handle missing for {bridge_name}; creating on the fly", 'telegram_scribe')
                db_lookup = __import__('common').BridgeDatabase(bridge_config.db_path)
                self.core.bridge_dbs[bridge_name] = db_lookup

            reply_to_id: Optional[str] = None
            reply_metadata: Optional[Dict[str, str]] = None
            if message.reply_to_message:
                replied_to_tg_id = message.reply_to_message.message_id
                reply_to_id = str(replied_to_tg_id)
                reply_metadata = {
                    'has_reply': 'true',
                    'raw_reply_to': str(replied_to_tg_id),
                    'source_reply_platform': 'telegram',
                    'source_reply_id': str(replied_to_tg_id),
                }

            attachments = []
            is_animation = False
            actual_media = None

            is_voice = False
            is_audio = False
            is_static_sticker = False

            if message.animation:
                is_animation = True
                actual_media = message.animation
            elif message.sticker:
                actual_media = message.sticker
                if message.sticker.is_animated or message.sticker.is_video:
                    is_animation = True
                else:
                    is_static_sticker = True
            elif message.photo:
                actual_media = message.photo[-1]
            elif message.video:
                actual_media = message.video
            elif message.video_note:
                actual_media = message.video_note
            elif message.audio:
                actual_media = message.audio
                is_audio = True
            elif message.voice:
                actual_media = message.voice
                is_voice = True
            elif message.document:
                actual_media = message.document

            if actual_media:
                try:
                    if hasattr(actual_media, 'get_file'):
                        # Throttle concurrent downloads to avoid TG API rate-limiting
                        async with self._download_semaphore:
                            file = await actual_media.get_file()
                            cache_dir = ensure_cache_dir()

                            # Prefer original filename for audio (preserves song title)
                            original_name = getattr(actual_media, 'file_name', None)
                            if original_name:
                                filename = original_name
                            elif file.file_path:
                                filename = os.path.basename(file.file_path)
                            else:
                                filename = f"tg_media_{actual_media.file_unique_id}"

                            if is_animation and not filename.lower().endswith('.mp4'):
                                base, _ = os.path.splitext(filename)
                                filename = base + ".mp4"

                            # Ensure voice messages have .ogg extension
                            if is_voice and not filename.lower().endswith(('.ogg', '.oga', '.opus')):
                                base, _ = os.path.splitext(filename)
                                filename = base + ".ogg"

                            # Ensure static stickers have .webp extension
                            if is_static_sticker and not filename.lower().endswith('.webp'):
                                base, _ = os.path.splitext(filename)
                                filename = base + ".webp"

                            local_path = os.path.join(cache_dir, f"tgb_{message.chat_id}_{message.message_id}_{filename}")
                            await file.download_to_drive(local_path)

                        if os.path.exists(local_path):
                            raw_mime = getattr(actual_media, 'mime_type', None)
                            if is_static_sticker:
                                att_mime = 'image/webp'
                            else:
                                att_mime = normalize_mime_type(raw_mime, filename=filename)
                            attachments.append({
                                'url': '', 'filename': filename,
                                'type': att_mime,
                                'local_path': local_path
                            })
                except Exception as e:
                    log_error(f"(telegram_scribe) attachment download failed: {e}")

            if extra_media_messages:
                for extra_msg in extra_media_messages:
                    extra_media = None
                    extra_is_anim = False
                    if extra_msg.animation:
                        extra_is_anim = True
                        extra_media = extra_msg.animation
                    elif extra_msg.sticker:
                        extra_media = extra_msg.sticker
                        if extra_msg.sticker.is_animated or extra_msg.sticker.is_video:
                            extra_is_anim = True
                    elif extra_msg.photo:
                        extra_media = extra_msg.photo[-1]
                    elif extra_msg.video:
                        extra_media = extra_msg.video
                    elif extra_msg.video_note:
                        extra_media = extra_msg.video_note
                    elif extra_msg.audio:
                        extra_media = extra_msg.audio
                    elif extra_msg.voice:
                        extra_media = extra_msg.voice
                    elif extra_msg.document:
                        extra_media = extra_msg.document

                    if extra_media and hasattr(extra_media, 'get_file'):
                        try:
                            async with self._download_semaphore:
                                efile = await extra_media.get_file()
                                cache_dir = ensure_cache_dir()
                                extra_original = getattr(extra_media, 'file_name', None)
                                if extra_original:
                                    efname = extra_original
                                elif efile.file_path:
                                    efname = os.path.basename(efile.file_path)
                                else:
                                    efname = f"tg_media_{extra_media.file_unique_id}"
                                if extra_is_anim and not efname.lower().endswith('.mp4'):
                                    base, _ = os.path.splitext(efname)
                                    efname = base + ".mp4"
                                elocal = os.path.join(cache_dir, f"tgb_{extra_msg.chat_id}_{extra_msg.message_id}_{efname}")
                                await efile.download_to_drive(elocal)
                            if os.path.exists(elocal):
                                extra_raw_mime = getattr(extra_media, 'mime_type', None)
                                attachments.append({
                                    'url': '', 'filename': efname,
                                    'type': normalize_mime_type(extra_raw_mime, filename=efname),
                                    'local_path': elocal
                                })
                                if extra_is_anim:
                                    is_animation = True
                        except Exception as e:
                            log_error(f"(telegram_scribe) media group extra download failed: {e}")

                    extra_caption = extra_msg.text or extra_msg.caption or ''
                    if extra_caption:
                        existing_text = message.text or message.caption or ''
                        if extra_caption.strip() != existing_text.strip():
                            if not hasattr(message, '_extra_captions'):
                                message._extra_captions = []
                            message._extra_captions.append(extra_caption)

            text_content = message.text or message.caption or ''
            text_entities = message.entities if message.text else (
                message.caption_entities if message.caption else None
            )
            text_content = self._apply_blockquote_formatting(text_content, text_entities)
            text_content = self._apply_spoiler_formatting(text_content, text_entities)

            # Flag media spoilers on attachments
            if getattr(message, 'has_media_spoiler', False) and attachments:
                for att in attachments:
                    att['spoiler'] = True

            extra_captions = getattr(message, '_extra_captions', None)
            if extra_captions:
                text_content = '\n'.join([text_content] + extra_captions).strip()

            text_content = apply_link_replacements(text_content)

            message_metadata = {}
            if is_animation:
                message_metadata['is_animation'] = True
            if is_voice:
                message_metadata['is_voice'] = True
            if is_audio:
                message_metadata['is_audio'] = True
                if hasattr(actual_media, 'performer') and actual_media.performer:
                    message_metadata['audio_performer'] = actual_media.performer
                if hasattr(actual_media, 'title') and actual_media.title:
                    message_metadata['audio_title'] = actual_media.title
                if hasattr(actual_media, 'duration') and actual_media.duration:
                    message_metadata['audio_duration'] = actual_media.duration

            if extra_media_messages:
                message_metadata['media_group_ids'] = [
                    str(m.message_id) for m in extra_media_messages
                ]

            # Don't run the Telegram avatar download on synthetic incognito IDs
            # (they're already populated by ensure_incognito_avatar above).
            if not (isinstance(entity_id, str) and entity_id.startswith('incognito_')):
                await self._cache_telegram_avatar(message, entity_id, is_channel=prefer_chat_avatar)

            bridge_msg = BridgeMessage(
                bridge_name=bridge_name,
                message_id=str(message.message_id),
                channel_id=str(message.chat_id),
                author_name=display_name,
                author_id=str(entity_id) if entity_id else None,
                content=text_content,
                attachments=attachments,
                reply_to_id=reply_to_id,
                is_forward=is_forward,
                forward_from=forward_from,
                timestamp=time.time(),
                source='telegram',
                metadata=message_metadata if message_metadata else None,
                reply_metadata=reply_metadata,
            )

            try:
                payload = bridge_msg.to_json()
                self._safe_publish(self._scribe_queue, payload)

                log_success(f"Published msg={message.message_id} to scribe queue")
            except Exception as e:
                log_error(f"Publish failed: {e}")
                raise

            self._text_cache[int(message.message_id)] = text_content

        except Exception as e:
            log_error(f"process_bridge_message failed: {e}")
            traceback.print_exc()

    async def handle_edited_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            message = getattr(update, 'edited_message', None) or getattr(update, 'edited_channel_post', None)
            if not message:
                return

            bridge_name, bridge_config = self.get_bridge_by_telegram_message(message)
            if not bridge_config or not self._can_forward_to_others(bridge_config):
                return

            try:
                exp = self.core.pilgrim._local_tg_edits.get(int(message.message_id))
                if exp and exp > time.time():
                    return
                elif exp and exp <= time.time():
                    del self.core.pilgrim._local_tg_edits[int(message.message_id)]
            except Exception:
                pass

            db = self.core.bridge_dbs.get(bridge_name)
            if not db:
                return
            mappings = db.get_all_mappings('telegram', str(message.message_id))
            if not mappings:
                log_debug(f"No mappings for edited Telegram message {message.message_id}")
                return

            new_text = (message.text or message.caption or '') or ''

            old_raw = self._text_cache.get(int(message.message_id))
            if old_raw is not None and _norm_text(old_raw) == _norm_text(new_text):
                return
            self._text_cache[int(message.message_id)] = new_text

            author_name = ''
            try:
                if message.from_user:
                    author_name = message.from_user.first_name or ''
                    if message.from_user.last_name:
                        author_name += f" {message.from_user.last_name}"
                elif message.sender_chat:
                    author_name = message.sender_chat.title or ''
            except Exception:
                pass

            channel_name = ''
            try:
                if message.chat:
                    channel_name = message.chat.title or ''
            except Exception:
                pass

            # Single body with all target IDs - arbiter routes once per
            # target queue, each pilgrim picks out its own *_message_id.
            # Publishing one body per mapping would multiply log lines
            # and RabbitMQ traffic by len(mappings).
            payload = {
                'type': 'edit',
                'source': 'telegram',
                'bridge_name': bridge_name,
                'new_text': new_text,
                'author_name': author_name,
                'channel_name': channel_name,
            }
            for platform, platform_id in mappings.items():
                payload[f'{platform}_message_id'] = str(platform_id)
            self._safe_publish(self._scribe_queue, json.dumps(payload))

        except Exception as e:
            log_error(f"Failed to handle Telegram edit: {e}")

    # ── utility commands ──────────────────────────────

    async def handle_topicid(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            m = update.effective_message
            if not m:
                return
            tid = getattr(m, 'message_thread_id', None)
            if tid is None:
                await m.reply_text("No topic/thread id on this message (send inside a forum topic).")
            else:
                await m.reply_text(f"message_thread_id: {tid}")
        except Exception as e:
            await update.effective_chat.send_message(f"Failed: {e}")

    async def handle_viewraw(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            message = update.effective_message
            if not message:
                return
            target = message
            if message.reply_to_message:
                target = message.reply_to_message
            raw = target.to_dict()
            text = json.dumps(raw, indent=2)
            if len(text) > 3500:
                text = text[:3500] + '...'
            safe = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            await message.reply_text(f"<pre>{safe}</pre>", parse_mode=constants.ParseMode.HTML)
        except Exception as e:
            await update.effective_chat.send_message(f"Failed to dump raw: {e}")

    async def handle_ignoreuser(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            message = update.effective_message
            if not message:
                return
            parts = (message.text or '').strip().split()
            target_id = None
            if len(parts) > 1:
                target_id = parts[1]
            if not target_id and message.reply_to_message and message.reply_to_message.from_user:
                target_id = str(message.reply_to_message.from_user.id)
            if not target_id and message.from_user:
                target_id = str(message.from_user.id)
            added = add_ignored_user('telegram', target_id)
            await message.reply_text('User ignored' if added else 'User already ignored')
        except Exception as e:
            await update.effective_chat.send_message(f"Failed to ignore: {e}")

    async def handle_privacy(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Outer wrapper guarantees ApplicationHandlerStop ALWAYS fires - even
        # when the body short-circuits via `return`. Without this split, every
        # `return` inside the body skipped the trailing raise and the message
        # would fall through to handle_message in group 1 and get bridged.
        try:
            await self._do_handle_privacy(update, context)
        except ApplicationHandlerStop:
            raise
        except Exception as e:
            try:
                await update.effective_chat.send_message(f"Privacy command failed: {e}")
            except Exception:
                pass
        # Don't let this message also flow into handle_message and get bridged.
        raise ApplicationHandlerStop

    async def _do_handle_privacy(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        message = update.effective_message
        if not message or not message.from_user:
            return
        user_id = message.from_user.id
        chat_id = message.chat_id
        chat_type = getattr(getattr(message, 'chat', None), 'type', None)

        # Resolve the bridge (best effort) up front so help text and
        # validation both reflect the actual per-bridge cap.
        current_bridge = None
        try:
            _bn, current_bridge = self.get_bridge_by_telegram_channel(chat_id)
        except Exception:
            current_bridge = None
        limit = get_privacy_limit('telegram', current_bridge)

        level_token = format_privacy_levels_token(limit)         # e.g. '0|1'
        level_phrase = format_privacy_levels_phrase(limit)       # e.g. '0 or 1'
        level_help = format_privacy_levels_help(limit)           # e.g. '0=disabled, 1=omit nickname'
        invalid_msg = f"Level must be {level_phrase}."

        text = (message.text or '').strip()
        text = re.sub(r'^/privacy(@\S+)?\s*', '', text)
        parts = text.split() if text else []

        # No args: show current settings.
        if not parts:
            settings = list_privacy_settings('telegram', user_id)
            if settings:
                lines = []
                for scope, lvl in settings:
                    # Reflect the runtime cap in the listing.
                    eff = min(lvl, limit)
                    label = {0: 'disabled', 1: 'nickname omitted', 2: 'fully ignored'}[eff]
                    scope_lbl = 'all chats' if scope == 'all' else f'chat {scope}'
                    suffix = f" (capped from {lvl})" if eff != lvl else ''
                    lines.append(f"  {scope_lbl}: {label}{suffix}")
                msg_text = "Your privacy settings:\n" + "\n".join(lines)
            elif limit <= 0:
                msg_text = "Privacy is disabled on this bridge."
            else:
                msg_text = (
                    "No privacy settings (default: level 0, disabled).\n\n"
                    "Usage:\n"
                    f"  /privacy {level_token}            set for this chat\n"
                    f"  /privacy {level_token} -all       set for every chat\n"
                    f"  /privacy <chat_id> {level_token}  set for a specific chat\n\n"
                    f"Levels: {level_help}"
                )
            await message.reply_text(msg_text)
            return

        # If the bridge has privacy disabled outright, refuse early so the
        # user gets a meaningful answer instead of a confusing range error.
        if limit <= 0:
            await message.reply_text("Privacy is disabled on this bridge.")
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
                await message.reply_text(invalid_msg)
                return
            if is_all:
                scope = 'all'
            elif chat_type == 'private':
                await message.reply_text(
                    "In DM, use:\n"
                    "  /privacy <level> -all\n"
                    "  /privacy <chat_id> <level>"
                )
                return
            else:
                scope = str(chat_id)
        elif len(parts) == 2:
            if is_all:
                await message.reply_text("Cannot combine explicit chat_id with -all.")
                return
            scope = parts[0]
            try:
                level = int(parts[1])
            except ValueError:
                await message.reply_text(invalid_msg)
                return
        else:
            await message.reply_text(
                "Usage: /privacy <level> [-all] or /privacy <chat_id> <level>"
            )
            return

        # Re-resolve the bridge for the actual scope (might differ from the
        # current chat) so we apply the right per-bridge cap.
        scope_bridge = current_bridge
        if scope != 'all':
            try:
                _bn, scope_bridge = self.get_bridge_by_telegram_channel(int(scope))
            except Exception:
                pass
        scope_limit = get_privacy_limit('telegram', scope_bridge)

        # Out-of-range level (negative, above cap, etc.) -> generic syntax
        # error using the cap-aware phrase. No dedicated cap message.
        if level < 0 or level > scope_limit:
            await message.reply_text(
                f"Level must be {format_privacy_levels_phrase(scope_limit)}."
            )
            return

        set_privacy_level('telegram', user_id, scope, level)
        scope_lbl = 'all chats' if scope == 'all' else f'chat {scope}'
        level_lbl = {0: 'disabled', 1: 'nickname omitted', 2: 'fully ignored'}[level]
        await message.reply_text(f"Privacy for {scope_lbl}: {level_lbl}.")

    async def handle_reload(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            message = update.effective_message
            if not message:
                return
            authorized = is_user_admin('telegram', message.from_user.id)
            if not authorized:
                await message.reply_text('Not authorized to reload')
                return
            self.core.reload_config()
            await message.reply_text('Telegram config reloaded')
        except Exception as e:
            await update.effective_chat.send_message(f"Reload failed: {e}")
