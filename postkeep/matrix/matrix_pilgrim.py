import asyncio
import pika
import json
import re
import time
import os
import threading
import traceback
import aiohttp
from collections import deque

import sys
_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _root not in sys.path:
    sys.path.insert(0, _root)

from postkeep.papyrus import (
    log_info, log_error, log_warn, log_success, log_debug,
    BridgeMessage, make_direction, get_file_type_from_url,
)


class MatrixPilgrim:
    def __init__(self, core):
        self.core = core
        self.component_name = 'matrix-pilgrim'
        self._loop = None
        self._consumer_thread = None

        # Ordered send buffers
        self._order_buffers = {}
        self._order_locks = {}
        self._last_sent_ts = {}

        # Prefix collapsing state (per bridge_name)
        self._last_bridged_author = {}  # {bridge_name: "source:author_name"}
        self._last_bridged_time = {}    # {bridge_name: timestamp}

        # Echo prevention: event IDs we sent (scribe checks this)
        self._recently_sent_event_ids = deque(maxlen=500)
        # Expose on core so scribe can access it
        self.core._pilgrim_sent_event_ids = self._recently_sent_event_ids

        # Edit echo prevention
        self._local_matrix_edits = {}
        self.core._local_matrix_edits = self._local_matrix_edits

        # File cleanup tracking
        self._pending_cleanup = []

    def _update_collapse_tracking(self, bridge_msg: BridgeMessage):
        """Update prefix-collapse state after a successful bridged send."""
        bname = bridge_msg.bridge_name
        author_key = f"{bridge_msg.source}:{bridge_msg.author_name}"
        self._last_bridged_author[bname] = author_key
        self._last_bridged_time[bname] = time.time()

    # ========== Start / Stop ==========

    def start(self):
        self._loop = self.core.main_loop or asyncio.get_event_loop()
        self._consumer_thread = threading.Thread(
            target=self._run_consumers,
            daemon=True,
            name='matrix-pilgrim-consumer',
        )
        self._consumer_thread.start()
        log_info("Matrix pilgrim consumer thread started")

    def stop(self):
        pass  # Thread is daemon, dies with process

    # ========== RabbitMQ Consumer ==========

    def _run_consumers(self):
        while True:
            try:
                conn = self.core._new_rabbitmq_connection()
                ch = conn.channel()
                ch.basic_qos(prefetch_count=10)

                pilgrim_queue = self.core.queues['pilgrim_matrix']
                ch.queue_declare(queue=pilgrim_queue, durable=True)
                ch.basic_consume(
                    queue=pilgrim_queue,
                    on_message_callback=self._handle_incoming,
                )
                log_info(f"Matrix pilgrim consuming from {pilgrim_queue}")

                _heartbeat_counter = 0
                while conn and not conn.is_closed:
                    conn.process_data_events(time_limit=1)
                    time.sleep(0.01)
                    _heartbeat_counter += 1
                    if _heartbeat_counter >= 120:  # ~every 2 min
                        _heartbeat_counter = 0
                        try:
                            self.core.send_status_update('ready', 'heartbeat')
                        except Exception:
                            pass

            except pika.exceptions.AMQPConnectionError:
                log_warn("[matrix-pilgrim] RabbitMQ connection lost, reconnecting in 5s...")
                time.sleep(5)
            except Exception as e:
                log_error(f"[matrix-pilgrim] Consumer error: {e}")
                traceback.print_exc()
                time.sleep(5)

    def _handle_incoming(self, ch, method, properties, body):
        try:
            if not self._loop:
                log_warn("[matrix-pilgrim] Event loop not ready, requeuing", self.component_name)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return

            raw = body.decode('utf-8')
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # Control messages (edit/delete)
            msg_type = data.get('type')
            if msg_type == 'edit':
                fut = asyncio.run_coroutine_threadsafe(
                    self._handle_edit(data), self._loop
                )
                fut.add_done_callback(self._log_errors)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            elif msg_type == 'delete':
                fut = asyncio.run_coroutine_threadsafe(
                    self._handle_delete(data), self._loop
                )
                fut.add_done_callback(self._log_errors)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # Regular BridgeMessage
            bridge_msg = BridgeMessage.from_json(raw)
            fut = asyncio.run_coroutine_threadsafe(
                self._ordered_send(bridge_msg), self._loop
            )
            fut.add_done_callback(self._log_errors)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            log_error(f"[matrix-pilgrim] Error handling inbound: {e}")
            traceback.print_exc()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _log_errors(self, fut):
        try:
            fut.result()
        except Exception as e:
            log_error(f"[matrix-pilgrim] Async task failed: {e}")
            traceback.print_exc()

    # ========== Ordered Send ==========

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
            id_int = hash(bridge_msg.message_id) & 0xFFFFFFFF

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
                    await self._send_message(msg0)
                except Exception as e:
                    log_error(f"[matrix-pilgrim] Send failed: {e}")
                    traceback.print_exc()

                if ts0 > last_ts:
                    last_ts = ts0
                    self._last_sent_ts[key] = last_ts

    # ========== Message Sending ==========

    async def _send_message(self, bridge_msg: BridgeMessage):
        """Route to ghost or bot send based on config."""
        bridge = self.core.bridges.get(bridge_msg.bridge_name)
        if not bridge:
            log_warn(f"[matrix-pilgrim] Unknown bridge: {bridge_msg.bridge_name}")
            return

        mx_cfg = bridge.get_platform('matrix')
        if not mx_cfg:
            return

        # For now, always use bot mode (legacy)
        # Ghost mode will be added when AS mode is implemented
        if self.core.mode == 'appservice' and mx_cfg.use_ghosts:
            # TODO: _send_as_ghost when AS mode is built
            await self._send_as_bot(bridge_msg, mx_cfg)
        else:
            await self._send_as_bot(bridge_msg, mx_cfg)

    async def _send_as_bot(self, bridge_msg: BridgeMessage, bridge_cfg):
        """Send message as the bot user with author attribution prefix."""
        from mautrix.types import (
            EventType, TextMessageEventContent, MessageType,
            RelatesTo, InReplyTo, Format,
        )

        if self.core.mode == 'bot':
            client = self.core.client
        else:
            # AS mode but use_ghosts=False -> use bot intent
            client = self.core.appservice.intent

        room_id = bridge_cfg.channel_id

        from html import escape as html_escape

        # ── Build prefix ──
        prefix = ''
        prefix_html = ''
        skip_prefix = False
        if bridge_cfg.use_prefixes:
            # Check if prefix should be collapsed
            bname = bridge_msg.bridge_name
            if getattr(bridge_cfg, 'collapse_prefixes', False) and not bridge_msg.attachments and not bridge_msg.is_forward:
                author_key = f"{bridge_msg.source}:{bridge_msg.author_name}"
                now = time.time()
                last_author = self._last_bridged_author.get(bname)
                last_time = self._last_bridged_time.get(bname, 0.0)
                native_ts = self.core._last_native_mx_msg.get(bname, 0.0)
                if (last_author == author_key
                        and (now - last_time) < 120
                        and native_ts < last_time):
                    skip_prefix = True

            if not skip_prefix:
                source_label = bridge_msg.source.capitalize()
                author = bridge_msg.author_name or 'Unknown'
                channel = ''
                if bridge_cfg.add_channel_name and bridge_msg.channel_name:
                    channel = f", #{bridge_msg.channel_name}"
                prefix = f"[{source_label} | {author}{channel}]\n"
                prefix_html = f"<code>[{html_escape(source_label)} | {html_escape(author)}{html_escape(channel)}]</code><br>"

        # ── Reply ──
        relates_to = None
        if bridge_msg.reply_to_id:
            db = self.core.bridge_dbs.get(bridge_msg.bridge_name)
            if db:
                mx_event_id = db.get_mapped_id(
                    bridge_msg.source, bridge_msg.reply_to_id, 'matrix'
                )
                if mx_event_id:
                    relates_to = RelatesTo(
                        in_reply_to=InReplyTo(event_id=mx_event_id)
                    )

        # Also check reply_metadata for cross-platform reply resolution
        if not relates_to and bridge_msg.reply_metadata:
            rm = bridge_msg.reply_metadata
            if rm.get('has_reply') and rm.get('source_reply_platform') and rm.get('source_reply_id'):
                db = self.core.bridge_dbs.get(bridge_msg.bridge_name)
                if db:
                    mx_event_id = db.get_mapped_id(
                        rm['source_reply_platform'], rm['source_reply_id'], 'matrix'
                    )
                    if mx_event_id:
                        relates_to = RelatesTo(
                            in_reply_to=InReplyTo(event_id=mx_event_id)
                        )

        sent_event_id = None

        # ── Forward attribution ──
        forward_line = ''
        forward_html = ''
        if bridge_msg.is_forward and bridge_msg.forward_from:
            forward_line = f"Forwarded from {bridge_msg.forward_from}\n"
            forward_html = f"<em>Forwarded from {html_escape(bridge_msg.forward_from)}</em><br>"

        # ── System messages ──
        is_system = bridge_msg.metadata and bridge_msg.metadata.get('is_system_message')

        # ── Attachments ──
        if bridge_msg.attachments:
            for i, att in enumerate(bridge_msg.attachments):
                media_content = await self._build_media_content(att, client)
                if media_content:
                    # Add reply to first message only
                    if i == 0 and relates_to:
                        media_content.relates_to = relates_to

                    eid = await client.send_message_event(
                        room_id, EventType.ROOM_MESSAGE, media_content
                    )
                    eid_str = str(eid)
                    self._recently_sent_event_ids.append(eid_str)
                    if i == 0:
                        sent_event_id = eid_str

        # ── Text content ──
        text = bridge_msg.content
        if text or (is_system and bridge_msg.content):
            # Build plain text body (fallback for clients)
            body = ''
            if prefix and not is_system:
                body += prefix
            if forward_line:
                body += forward_line
            body += text or ''

            # Strip ||spoiler|| markers from plain body
            plain_body = re.sub(r'\|\|(.+?)\|\|', r'\1', body)

            # Build HTML formatted_body
            html_body = ''
            if prefix_html and not is_system:
                html_body += prefix_html
            if forward_html:
                html_body += forward_html

            content_html = html_escape(text or '')
            # Convert ||spoiler|| to Matrix spoiler HTML
            content_html = re.sub(r'\|\|(.+?)\|\|', r'<span data-mx-spoiler>\1</span>', content_html)
            # Convert **bold** to <strong>
            content_html = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', content_html)
            # Convert *italic* to <em> (but not inside <strong>)
            content_html = re.sub(r'(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)', r'<em>\1</em>', content_html)
            # Convert `code` to <code>
            content_html = re.sub(r'`([^`]+?)`', r'<code>\1</code>', content_html)
            # Convert newlines to <br>
            content_html = content_html.replace('\n', '<br>')
            html_body += content_html

            msgtype = MessageType.NOTICE if is_system else MessageType.TEXT
            text_content = TextMessageEventContent(
                msgtype=msgtype,
                body=plain_body,
                format=Format.HTML,
                formatted_body=html_body,
            )

            # Only add reply if no attachments were sent (reply goes on first item)
            if not bridge_msg.attachments and relates_to:
                text_content.relates_to = relates_to

            eid = await client.send_message_event(
                room_id, EventType.ROOM_MESSAGE, text_content
            )
            eid_str = str(eid)
            self._recently_sent_event_ids.append(eid_str)
            if not sent_event_id:
                sent_event_id = eid_str

        # ── Poll rendering (text fallback) ──
        if bridge_msg.poll and not sent_event_id:
            poll = bridge_msg.poll
            question = poll.get('question', 'Poll')
            options = poll.get('options', [])
            lines = [f"**📊 {question}**"]
            for idx, opt in enumerate(options):
                opt_text = opt if isinstance(opt, str) else opt.get('text', str(opt))
                lines.append(f"  {idx + 1}. {opt_text}")
            poll_body = f"{prefix}{chr(10).join(lines)}" if prefix else '\n'.join(lines)
            text_content = TextMessageEventContent(
                msgtype=MessageType.TEXT,
                body=poll_body,
            )
            eid = await client.send_message_event(
                room_id, EventType.ROOM_MESSAGE, text_content
            )
            eid_str = str(eid)
            self._recently_sent_event_ids.append(eid_str)
            sent_event_id = eid_str

        # ── Store mapping ──
        if sent_event_id:
            self._store_mapping(bridge_msg, sent_event_id)
            self._update_collapse_tracking(bridge_msg)

        # ── Schedule file cleanup ──
        if bridge_msg.attachments:
            self._schedule_file_cleanup(bridge_msg.attachments)

        if sent_event_id:
            log_debug(f"[matrix-pilgrim] Sent to {room_id}: {bridge_msg.author_name} ({bridge_msg.source})")

    # ========== Media Upload ==========

    async def _build_media_content(self, attachment: dict, client):
        """Upload attachment to Matrix and build MediaMessageEventContent."""
        from mautrix.types import (
            MediaMessageEventContent, MessageType,
            ImageInfo, VideoInfo, AudioInfo, FileInfo,
        )

        local_path = attachment.get('local_path')
        url = attachment.get('url')
        filename = attachment.get('filename', 'file')

        # Resolve MIME type: try 'mimetype' field first (Matrix scribe sets this),
        # then 'type' field (other scribes put actual MIME types like 'image/jpeg' here),
        # then infer from filename extension.
        mimetype = attachment.get('mimetype') or ''
        if not mimetype or mimetype == 'application/octet-stream':
            att_type = attachment.get('type', '')
            if isinstance(att_type, str) and '/' in att_type:
                mimetype = att_type
            else:
                mimetype = get_file_type_from_url(url or '', filename)

        # Get file bytes
        file_bytes = None
        if local_path and os.path.exists(local_path):
            with open(local_path, 'rb') as f:
                file_bytes = f.read()
        elif url:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            file_bytes = await resp.read()
            except Exception as e:
                log_warn(f"[matrix-pilgrim] Failed to download attachment: {e}")

        if not file_bytes:
            log_warn(f"[matrix-pilgrim] No file bytes for {filename}")
            return None

        # Upload to Matrix content repository
        try:
            mxc_uri = await client.upload_media(
                data=file_bytes,
                mime_type=mimetype,
                filename=filename,
            )
        except Exception as e:
            log_error(f"[matrix-pilgrim] Media upload failed: {e}")
            return None

        # Determine message type and info
        if mimetype.startswith('image/'):
            msgtype = MessageType.IMAGE
            info = ImageInfo(mimetype=mimetype, size=len(file_bytes))
            try:
                from PIL import Image
                import io
                img = Image.open(io.BytesIO(file_bytes))
                info.width, info.height = img.size
            except Exception:
                pass
        elif mimetype.startswith('video/'):
            msgtype = MessageType.VIDEO
            info = VideoInfo(mimetype=mimetype, size=len(file_bytes))
        elif mimetype.startswith('audio/'):
            msgtype = MessageType.AUDIO
            info = AudioInfo(mimetype=mimetype, size=len(file_bytes))
        else:
            msgtype = MessageType.FILE
            info = FileInfo(mimetype=mimetype, size=len(file_bytes))

        return MediaMessageEventContent(
            msgtype=msgtype,
            body=filename,
            url=mxc_uri,
            info=info,
        )

    # ========== Edit Handler ==========

    async def _handle_edit(self, data: dict):
        """Handle cross-platform edit -> Matrix."""
        from mautrix.types import (
            EventType, TextMessageEventContent, MessageType,
        )

        bridge_name = data.get('bridge_name')
        bridge = self.core.bridges.get(bridge_name)
        if not bridge:
            return
        mx_cfg = bridge.get_platform('matrix')
        if not mx_cfg or not mx_cfg.cross_edit:
            return

        # Resolve Matrix event ID
        mx_event_id = data.get('matrix_message_id')
        if not mx_event_id:
            source = data.get('source', '')
            for key, val in data.items():
                if key.endswith('_message_id') and key != 'matrix_message_id':
                    db = self.core.bridge_dbs.get(bridge_name)
                    if db:
                        platform = key.replace('_message_id', '')
                        mx_event_id = db.get_mapped_id(platform, val, 'matrix')
                    break
        if not mx_event_id:
            return

        new_text = data.get('new_text', '')

        # In bot mode, reconstruct prefix + new text
        if self.core.mode == 'bot':
            client = self.core.client
        else:
            client = self.core.appservice.intent

        # Build prefix for the edit
        prefix = ''
        if mx_cfg.use_prefixes:
            source_label = (data.get('source', '') or '').capitalize()
            author = data.get('author_name', 'Unknown')
            channel = ''
            channel_name = data.get('channel_name', '')
            if mx_cfg.add_channel_name and channel_name:
                channel = f", #{channel_name}"
            prefix = f"`[{source_label} | {author}{channel}]`\n"

        body = f"{prefix}{new_text}" if prefix else new_text

        edit_content = TextMessageEventContent(
            msgtype=MessageType.TEXT,
            body=f"* {body}",
        )
        edit_content.new_content = TextMessageEventContent(
            msgtype=MessageType.TEXT,
            body=body,
        )
        edit_content.set_edit(mx_event_id)

        room_id = mx_cfg.channel_id

        # Record in echo prevention before sending
        self._local_matrix_edits[mx_event_id] = time.time()

        try:
            await client.send_message_event(
                room_id, EventType.ROOM_MESSAGE, edit_content
            )
            log_debug(f"[matrix-pilgrim] Edited {mx_event_id}")
        except Exception as e:
            log_warn(f"[matrix-pilgrim] Edit failed for {mx_event_id}: {e}")
            # Clean up echo prevention on failure
            self._local_matrix_edits.pop(mx_event_id, None)

    # ========== Delete Handler ==========

    async def _handle_delete(self, data: dict):
        """Handle cross-platform delete -> Matrix redaction."""
        bridge_name = data.get('bridge_name')
        bridge = self.core.bridges.get(bridge_name)
        if not bridge:
            return
        mx_cfg = bridge.get_platform('matrix')
        if not mx_cfg or not mx_cfg.cross_delete:
            return

        # Resolve Matrix event ID
        mx_event_id = data.get('matrix_message_id')
        if not mx_event_id:
            for key, val in data.items():
                if key.endswith('_message_id') and key != 'matrix_message_id':
                    db = self.core.bridge_dbs.get(bridge_name)
                    if db:
                        platform = key.replace('_message_id', '')
                        mx_event_id = db.get_mapped_id(platform, val, 'matrix')
                    break
        if not mx_event_id:
            return

        room_id = mx_cfg.channel_id

        if self.core.mode == 'bot':
            client = self.core.client
        else:
            client = self.core.appservice.intent

        try:
            await client.redact(room_id, mx_event_id, reason="Deleted on bridged platform")
            log_debug(f"[matrix-pilgrim] Redacted {mx_event_id}")
        except Exception as e:
            log_warn(f"[matrix-pilgrim] Redaction failed for {mx_event_id}: {e}")

    # ========== Message Mapping ==========

    def _store_mapping(self, bridge_msg: BridgeMessage, matrix_event_id: str):
        """Store source -> matrix message ID mapping in bridge DB."""
        db = self.core.bridge_dbs.get(bridge_msg.bridge_name)
        if not db:
            return

        from postkeep.papyrus import make_direction
        direction = make_direction(bridge_msg.source, 'matrix')

        db.store_mapping(
            source_platform=bridge_msg.source,
            source_id=bridge_msg.message_id,
            target_platform='matrix',
            target_id=matrix_event_id,
            direction=direction,
        )

    # ========== File Cleanup ==========

    def _schedule_file_cleanup(self, attachments):
        """Schedule cleanup of local attachment files after a delay."""
        paths = [att.get('local_path') for att in attachments if att.get('local_path')]
        if not paths:
            return

        def _cleanup():
            time.sleep(120)
            for p in paths:
                try:
                    if os.path.exists(p):
                        os.remove(p)
                except Exception:
                    pass

        t = threading.Thread(target=_cleanup, daemon=True)
        t.start()
