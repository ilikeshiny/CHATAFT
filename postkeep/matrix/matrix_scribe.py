import asyncio
import pika
import json
import time
import os
import re
import aiohttp
from typing import Optional
from collections import deque

import sys
_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _root not in sys.path:
    sys.path.insert(0, _root)

from postkeep.papyrus import (
    log_info, log_error, log_warn, log_success, log_debug,
    BridgeMessage, ensure_cache_dir,
    get_cached_avatar, store_avatar_cache, compute_avatar_hash,
    is_user_ignored,
    normalize_mime_type,
)


class MatrixScribe:
    # Events older than (session_start - SLACK) are treated as catch-up replay
    # from the initial /sync and ignored. Prevents historical redactions / edits
    # from re-firing across the bridge on every restart.
    STALE_EVENT_SLACK_SEC = 5

    def __init__(self, core):
        self.core = core
        self._rabbitmq_conn = None
        self._rabbitmq_channel = None
        self._recently_seen_events = deque(maxlen=500)
        self._session_start_ts = time.time()

    def _is_stale_event(self, event) -> bool:
        """True if the event predates this scribe session (i.e. it arrived via
        initial sync catch-up). We deliberately drop offline-window deletes and
        edits because replaying them on restart causes spurious 'message not
        found' errors on target platforms."""
        ts_ms = getattr(event, 'timestamp', None)
        if not ts_ms:
            return False
        event_ts = ts_ms / 1000.0
        return event_ts < (self._session_start_ts - self.STALE_EVENT_SLACK_SEC)

    # ========== Bot Mode Handler Registration ==========

    def setup_bot_handlers(self, client):
        """Register event handlers on the mautrix Client (bot mode sync)."""
        from mautrix.types import EventType

        @client.on(EventType.ROOM_MESSAGE)
        async def _on_room_message(event):
            try:
                await self._dispatch_event(event)
            except Exception as e:
                log_error(f"[matrix-scribe] Error handling message: {e}")
                import traceback; traceback.print_exc()

        @client.on(EventType.ROOM_REDACTION)
        async def _on_redaction(event):
            try:
                await self._handle_redaction(event)
            except Exception as e:
                log_error(f"[matrix-scribe] Error handling redaction: {e}")

        @client.on(EventType.STICKER)
        async def _on_sticker(event):
            try:
                await self._handle_sticker(event)
            except Exception as e:
                log_error(f"[matrix-scribe] Error handling sticker: {e}")

        @client.on(EventType.ROOM_MEMBER)
        async def _on_member(event):
            try:
                await self._handle_member_event(event)
            except Exception as e:
                log_error(f"[matrix-scribe] Error handling member event: {e}")

        log_info("Matrix scribe handlers registered (bot mode)")

    # ========== Event Dispatch ==========

    async def _dispatch_event(self, event):
        from mautrix.types import RelationType as _RelationType

        event_id = str(event.event_id)

        # Dedup
        if event_id in self._recently_seen_events:
            return
        self._recently_seen_events.append(event_id)

        # Echo prevention: skip our own messages
        if self._is_own_user(str(event.sender)):
            return

        # Skip if pilgrim recently sent this event_id
        pilgrim_sent = getattr(self.core, '_pilgrim_sent_event_ids', None)
        if pilgrim_sent and event_id in pilgrim_sent:
            return

        room_id = str(event.room_id)
        bridge_name, bridge = self.core.get_bridge_by_matrix_room(room_id)
        if not bridge:
            return

        mx_cfg = bridge.get_platform('matrix')
        if not mx_cfg:
            return

        # Check for edit (m.replace relation)
        relates_to = getattr(event.content, 'relates_to', None)
        if relates_to:
            rel_type = getattr(relates_to, 'rel_type', None)
            if rel_type and rel_type == _RelationType.REPLACE:
                await self._handle_edit(event, bridge)
                return

        await self._handle_message(event, bridge, mx_cfg)

    # ========== Message Processing ==========

    async def _handle_message(self, event, bridge, platform_cfg):
        from mautrix.types import MessageType

        sender = str(event.sender)

        # Ignore check
        if is_user_ignored('matrix', sender):
            return

        # Track native Matrix message timestamp for prefix collapse
        self.core._last_native_mx_msg[bridge.name] = time.time()

        msgtype = event.content.msgtype

        # ── Text content ──
        content = ''
        if msgtype in (MessageType.TEXT, MessageType.NOTICE, MessageType.EMOTE):
            # Prefer formatted_body for spoiler detection, fall back to plain body
            formatted = getattr(event.content, 'formatted_body', None) or ''
            body_format = getattr(event.content, 'format', None)
            if formatted and body_format and 'data-mx-spoiler' in formatted:
                # Convert Matrix spoiler spans to ||spoiler|| markers
                content = re.sub(
                    r'<span[^>]*data-mx-spoiler[^>]*>(.*?)</span>',
                    r'||\1||',
                    formatted,
                )
                # Strip remaining HTML tags
                content = re.sub(r'<[^>]+>', '', content)
                # Decode HTML entities
                from html import unescape
                content = unescape(content)
            else:
                content = event.content.body or ''

            if msgtype == MessageType.EMOTE:
                display_name = await self._get_display_name(sender) or sender
                content = f"* {display_name} {content}"
            # Strip reply fallback
            content = self._strip_reply_fallback(content)

        # ── Attachments ──
        attachments = []
        if msgtype in (MessageType.IMAGE, MessageType.FILE, MessageType.AUDIO, MessageType.VIDEO):
            att = await self._download_matrix_media(event)
            if att:
                # Check for media spoiler (data-mx-spoiler in formatted_body wrapping media)
                formatted = getattr(event.content, 'formatted_body', None) or ''
                if 'data-mx-spoiler' in formatted:
                    att['spoiler'] = True
                attachments.append(att)
            # Caption text - only use body as content if it's NOT just the filename
            if event.content.body and msgtype != MessageType.TEXT:
                body = event.content.body or ''
                att_filename = att.get('filename', '') if att else ''
                if body and body != att_filename:
                    if not content:
                        content = body

        # ── Reply detection ──
        reply_to_id = None
        reply_metadata = None
        relates_to = getattr(event.content, 'relates_to', None)
        if relates_to:
            in_reply_to = getattr(relates_to, 'in_reply_to', None)
            if in_reply_to:
                reply_event_id = str(in_reply_to.event_id)
                reply_to_id = reply_event_id
                reply_metadata = {
                    'has_reply': True,
                    'raw_reply_to': reply_event_id,
                    'source_reply_platform': 'matrix',
                    'source_reply_id': reply_event_id,
                }

        # Nothing to bridge
        if not content and not attachments:
            return

        # Skip if already bridged (catch-up dedup after restart)
        event_id = str(event.event_id)
        db = self.core.bridge_dbs.get(bridge.name)
        if db and db.get_all_mappings('matrix', event_id):
            return

        # ── Author info ──
        display_name = await self._get_display_name(sender) or sender
        author_id = sender  # Matrix user ID like @user:server.com

        # ── Avatar caching ──
        await self._cache_matrix_avatar(sender)

        # ── Build BridgeMessage ──
        room_id = str(event.room_id)
        timestamp = event.timestamp / 1000.0 if event.timestamp else time.time()

        channel_name = None
        # Try to get room name for channel attribution
        try:
            if self.core.mode == 'bot' and self.core.client:
                # Room name from state (cached during sync)
                room_state = getattr(self.core.client, 'rooms', {})
                if hasattr(room_state, 'get'):
                    room = room_state.get(event.room_id)
                    if room:
                        channel_name = getattr(room, 'name', None)
        except Exception:
            pass

        bridge_msg = BridgeMessage(
            bridge_name=bridge.name,
            message_id=event_id,
            channel_id=room_id,
            author_name=display_name,
            author_id=author_id,
            content=content if content else None,
            attachments=attachments,
            reply_to_id=reply_to_id,
            is_forward=False,
            forward_from=None,
            timestamp=timestamp,
            source='matrix',
            channel_name=channel_name,
            metadata=None,
            reply_metadata=reply_metadata,
        )

        self._publish_to_scribe(bridge_msg)
        log_debug(f"[matrix-scribe] Published: {display_name} in {bridge.name} ({len(content or '')} chars, {len(attachments)} att)")

    # ========== Sticker Handler ==========

    async def _handle_sticker(self, event):
        """Handle m.sticker events - treat as image attachments."""
        event_id = str(event.event_id)
        if event_id in self._recently_seen_events:
            return
        self._recently_seen_events.append(event_id)

        if self._is_own_user(str(event.sender)):
            return

        room_id = str(event.room_id)
        bridge_name, bridge = self.core.get_bridge_by_matrix_room(room_id)
        if not bridge:
            return

        # Skip if already bridged (catch-up dedup)
        db = self.core.bridge_dbs.get(bridge_name)
        if db and db.get_all_mappings('matrix', event_id):
            return

        att = await self._download_matrix_media(event)
        if not att:
            return

        sender = str(event.sender)
        display_name = await self._get_display_name(sender) or sender
        await self._cache_matrix_avatar(sender)

        timestamp = event.timestamp / 1000.0 if event.timestamp else time.time()

        bridge_msg = BridgeMessage(
            bridge_name=bridge.name,
            message_id=event_id,
            channel_id=room_id,
            author_name=display_name,
            author_id=sender,
            content=None,
            attachments=[att],
            reply_to_id=None,
            is_forward=False,
            forward_from=None,
            timestamp=timestamp,
            source='matrix',
        )
        self._publish_to_scribe(bridge_msg)

    # ========== Edit Handler ==========

    async def _handle_edit(self, event, bridge):
        # Drop historical edits replayed by initial sync (same reasoning as
        # _handle_redaction - re-firing edits hits 'message can't be edited').
        if self._is_stale_event(event):
            return

        # Check echo prevention for edits
        local_edits = getattr(self.core, '_local_matrix_edits', None)
        original_event_id_attr = getattr(event.content.relates_to, 'event_id', None)
        if not original_event_id_attr:
            return
        original_event_id = str(original_event_id_attr)

        if local_edits and original_event_id in local_edits:
            log_debug(f"[matrix-scribe] Skipping own edit for {original_event_id}")
            del local_edits[original_event_id]
            return

        # Get new content
        new_content = getattr(event.content, 'new_content', None)
        new_text = new_content.body if new_content else event.content.body
        if new_text:
            new_text = self._strip_reply_fallback(new_text)

        if not new_text:
            return

        db = self.core.bridge_dbs.get(bridge.name)
        if not db:
            return

        all_mappings = db.get_all_mappings('matrix', original_event_id)
        if not all_mappings:
            return

        sender = str(event.sender)
        author_name = await self._get_display_name(sender) or sender

        # Single body with all target IDs - arbiter routes once per
        # target queue, each pilgrim picks its own *_message_id. The
        # matrix_message_id is the source ID (informational - matrix
        # pilgrim is excluded by arbiter as the source).
        targets = {p: pid for p, pid in all_mappings.items() if p != 'matrix'}
        if targets:
            control_msg = {
                'type': 'edit',
                'source': 'matrix',
                'bridge_name': bridge.name,
                'matrix_message_id': original_event_id,
                'new_text': new_text,
                'author_name': author_name,
            }
            for platform, platform_id in targets.items():
                control_msg[f'{platform}_message_id'] = platform_id
            self._publish_raw(json.dumps(control_msg))
            log_debug(f"[matrix-scribe] Published edit for {original_event_id} -> {','.join(targets.keys())}")

    # ========== Redaction (Delete) Handler ==========

    async def _handle_redaction(self, event):
        # Drop historical redactions replayed by initial sync. Otherwise every
        # restart re-fires the most recent delete and target platforms 404.
        if self._is_stale_event(event):
            return

        redacted_event_id = str(event.redacts) if hasattr(event, 'redacts') and event.redacts else None
        if not redacted_event_id:
            return

        if self._is_own_user(str(event.sender)):
            return

        room_id = str(event.room_id)
        bridge_name, bridge = self.core.get_bridge_by_matrix_room(room_id)
        if not bridge:
            return

        mx_cfg = bridge.get_platform('matrix')
        if not mx_cfg or not mx_cfg.cross_delete:
            return

        db = self.core.bridge_dbs.get(bridge_name)
        if not db:
            return

        # The redaction event's sender is the user doing the delete (not the
        # original author - Matrix doesn't carry that back through redactions).
        redactor_name = await self._get_display_name(str(event.sender)) or str(event.sender)

        all_mappings = db.get_all_mappings('matrix', redacted_event_id)
        targets = {p: pid for p, pid in all_mappings.items() if p != 'matrix'}
        if targets:
            # Single body with all target IDs (see edit handler for why).
            control_msg = {
                'type': 'delete',
                'source': 'matrix',
                'bridge_name': bridge_name,
                'author_name': redactor_name,
                'matrix_message_id': redacted_event_id,
            }
            for platform, platform_id in targets.items():
                control_msg[f'{platform}_message_id'] = platform_id
            self._publish_raw(json.dumps(control_msg))
            log_debug(f"[matrix-scribe] Published delete for {redacted_event_id} -> {','.join(targets.keys())}")

    # ========== Member Events ==========

    async def _handle_member_event(self, event):
        # Skip stale member events (e.g. from initial sync catch-up)
        event_ts = event.timestamp / 1000.0 if event.timestamp else 0
        if event_ts and (time.time() - event_ts) > 60:
            return

        room_id = str(event.room_id)
        bridge_name, bridge = self.core.get_bridge_by_matrix_room(room_id)
        if not bridge:
            return

        mx_cfg = bridge.get_platform('matrix')
        if not mx_cfg or not mx_cfg.forward_member_events:
            return

        if self._is_own_user(str(event.sender)):
            return

        membership = getattr(event.content, 'membership', None)
        if not membership:
            return

        sender = str(event.state_key) if hasattr(event, 'state_key') else str(event.sender)
        display_name = await self._get_display_name(sender) or sender

        prev_membership = None
        if hasattr(event, 'prev_content') and event.prev_content:
            prev_membership = getattr(event.prev_content, 'membership', None)

        content = None
        if str(membership) == 'join' and str(prev_membership) != 'join':
            content = f"👋 {display_name} joined the room"
        elif str(membership) == 'leave':
            if sender == str(event.sender):
                content = f"👋 {display_name} left the room"
            else:
                kicker = await self._get_display_name(str(event.sender)) or str(event.sender)
                content = f"👢 {display_name} was removed by {kicker}"

        if not content:
            return

        timestamp = event.timestamp / 1000.0 if event.timestamp else time.time()

        bridge_msg = BridgeMessage(
            bridge_name=bridge.name,
            message_id=str(event.event_id),
            channel_id=room_id,
            author_name=display_name,
            author_id=sender,
            content=content,
            attachments=[],
            reply_to_id=None,
            is_forward=False,
            forward_from=None,
            timestamp=timestamp,
            source='matrix',
            metadata={'is_system_message': True},
        )
        self._publish_to_scribe(bridge_msg)

    # ========== Echo Prevention ==========

    def _is_own_user(self, sender: str) -> bool:
        """Returns True if the sender is our bot or one of our ghost users."""
        server = self.core.settings.get('MATRIX_SERVER_NAME', '')
        bot_localpart = self.core.settings.get('MATRIX_BOT_LOCALPART', '_chataft_bot')

        # Bot user (both modes)
        bot_user = self.core.settings.get('MATRIX_BOT_USER', '')
        if bot_user and sender == bot_user:
            return True

        # Bot by localpart + server
        if server and sender == f"@{bot_localpart}:{server}":
            return True

        # Ghost user namespace (AS mode)
        if sender.startswith("@_chataft_"):
            if server and sender.endswith(f":{server}"):
                return True
            # Also catch any @_chataft_ user to prevent Matrix→Matrix loops
            return True

        return False

    # ========== Display Name Resolution ==========

    async def _get_display_name(self, user_id: str) -> Optional[str]:
        try:
            if self.core.mode == 'bot' and self.core.client:
                profile = await self.core.client.get_profile(user_id)
                return profile.displayname if profile else None
            elif self.core.mode == 'appservice' and self.core.appservice:
                profile = await self.core.appservice.intent.get_profile(user_id)
                return profile.displayname if profile else None
        except Exception:
            pass
        return None

    # ========== Avatar Caching ==========

    async def _cache_matrix_avatar(self, user_id: str):
        """Download a Matrix user's avatar and cache bytes for other platform pilgrims."""
        try:
            if not self.core.avatar_db:
                return

            # Get profile
            profile = None
            if self.core.mode == 'bot' and self.core.client:
                profile = await self.core.client.get_profile(user_id)
            elif self.core.mode == 'appservice' and self.core.appservice:
                profile = await self.core.appservice.intent.get_profile(user_id)

            if not profile or not profile.avatar_url:
                return

            mxc_url = str(profile.avatar_url)

            # Check cache - if same mxc URL, skip
            cached = get_cached_avatar(self.core.avatar_db, user_id)
            if cached and cached.get('matrix_url') == mxc_url and cached.get('avatar_bytes'):
                return

            # Convert mxc:// to HTTP download URL
            http_url = self._mxc_to_http(mxc_url)
            if not http_url:
                return

            # Download avatar bytes (authenticated)
            headers = self._get_auth_headers()
            async with aiohttp.ClientSession() as session:
                async with session.get(http_url, headers=headers) as resp:
                    if resp.status != 200:
                        log_debug(f"[matrix-scribe] Avatar download HTTP {resp.status} for {user_id}")
                        return
                    avatar_bytes = await resp.read()

            if not avatar_bytes:
                return

            avatar_hash = compute_avatar_hash(avatar_bytes)

            store_avatar_cache(
                self.core.avatar_db,
                user_id=user_id,
                platform='matrix',
                avatar_hash=avatar_hash,
                avatar_bytes=avatar_bytes,
                matrix_url=mxc_url,
            )
            log_debug(f"[matrix-scribe] Cached avatar for {user_id}")

        except Exception as e:
            log_debug(f"[matrix-scribe] Avatar cache failed for {user_id}: {e}")

    def _mxc_to_http(self, mxc_url: str) -> Optional[str]:
        """Convert mxc://server/media_id to an authenticated HTTP download URL."""
        if not mxc_url or not mxc_url.startswith('mxc://'):
            return None
        homeserver = self.core.settings['MATRIX_HOMESERVER_URL'].rstrip('/')
        # mxc://server_name/media_id -> /_matrix/client/v1/media/download/server_name/media_id
        parts = mxc_url[6:]  # strip 'mxc://'
        return f"{homeserver}/_matrix/client/v1/media/download/{parts}"

    def _get_auth_headers(self) -> dict:
        """Return Authorization header for authenticated media requests.

        Reads the live access token from the active mautrix client (which the
        background refresher mutates in place when MAS rotates tokens). Falls
        back to the bootstrap MATRIX_BOT_TOKEN only if the client isn't up yet.
        """
        token = ''
        client = getattr(self.core, 'client', None)
        if client is not None and getattr(client, 'api', None) is not None:
            token = getattr(client.api, 'token', '') or ''
        if not token:
            token = self.core.settings.get('MATRIX_BOT_TOKEN', '') or ''
        return {'Authorization': f'Bearer {token}'} if token else {}

    # ========== Media Download ==========

    async def _download_matrix_media(self, event) -> Optional[dict]:
        """Download media from the Matrix content repository (mxc:// URL)."""
        mxc_url = getattr(event.content, 'url', None)
        if not mxc_url:
            return None

        mxc_url = str(mxc_url)
        http_url = self._mxc_to_http(mxc_url)
        if not http_url:
            return None

        filename = event.content.body or 'file'
        info = getattr(event.content, 'info', None)
        raw_mime = info.mimetype if info and hasattr(info, 'mimetype') and info.mimetype else ''
        mimetype = normalize_mime_type(raw_mime, filename=filename, url=mxc_url)

        # Sanitize filename
        safe_event_id = str(event.event_id).replace('$', '').replace(':', '_')[:40]
        safe_filename = re.sub(r'[<>:"/\\|?*]', '_', filename)[:100]
        cache_dir = ensure_cache_dir()
        local_path = os.path.join(cache_dir, f"mx_{safe_event_id}_{safe_filename}")

        try:
            headers = self._get_auth_headers()
            async with aiohttp.ClientSession() as session:
                async with session.get(http_url, headers=headers) as resp:
                    if resp.status != 200:
                        log_warn(f"[matrix-scribe] Media download failed: HTTP {resp.status} for {mxc_url}")
                        return None
                    with open(local_path, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            f.write(chunk)
        except Exception as e:
            log_error(f"[matrix-scribe] Media download error: {e}")
            return None

        return {
            'url': http_url,
            'filename': filename,
            'type': mimetype,
            'local_path': local_path,
            'mimetype': mimetype,
        }

    # ========== Reply Fallback Stripping ==========

    def _strip_reply_fallback(self, text: str) -> str:
        """Remove the '> <@user:server> quoted text' reply fallback from body."""
        if not text:
            return text
        lines = text.split('\n')
        while lines and lines[0].startswith('> '):
            lines.pop(0)
        # Remove the blank line after the fallback
        if lines and lines[0] == '':
            lines.pop(0)
        return '\n'.join(lines)

    # ========== RabbitMQ Publishing ==========

    def _publish_to_scribe(self, bridge_msg: BridgeMessage):
        self._publish_raw(bridge_msg.to_json())

    def _publish_raw(self, body: str):
        try:
            if not self._rabbitmq_conn or self._rabbitmq_conn.is_closed:
                self._reconnect_rabbitmq()
            self._rabbitmq_channel.basic_publish(
                exchange='',
                routing_key=self.core.queues['scribe_matrix'],
                body=body.encode('utf-8'),
                properties=pika.BasicProperties(delivery_mode=2),
            )
        except Exception:
            try:
                self._reconnect_rabbitmq()
                self._rabbitmq_channel.basic_publish(
                    exchange='',
                    routing_key=self.core.queues['scribe_matrix'],
                    body=body.encode('utf-8'),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
            except Exception as e:
                log_error(f"[matrix-scribe] RabbitMQ publish failed: {e}")

    def _reconnect_rabbitmq(self):
        try:
            if self._rabbitmq_conn and not self._rabbitmq_conn.is_closed:
                self._rabbitmq_conn.close()
        except Exception:
            pass
        self._rabbitmq_conn = self.core._new_rabbitmq_connection()
        self._rabbitmq_channel = self._rabbitmq_conn.channel()
