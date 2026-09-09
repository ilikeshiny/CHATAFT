import asyncio
import json
import os
import re
import time
import threading
import traceback
import pika
from html import escape as html_escape
from typing import Optional, TYPE_CHECKING

from PIL import Image
from telegram import InputMediaPhoto, InputMediaVideo, constants
from telegram.error import RetryAfter

from postkeep.papyrus import (
    ARBITER_QUEUES, apply_namespace, get_namespace,
    log_info, log_error, log_warn, log_success, log_debug,
    log_transient, is_transient_error,
    BridgeMessage, BridgeDatabase,
    escape_discord_emojis, convert_video_to_gif, discord_snowflake_to_unix,
    ensure_cache_dir, enforce_cache_quota,
    make_direction,
)


if TYPE_CHECKING:
    from postkeep.telegram.telegram_core import TelegramCore


def _norm_text(s: str) -> str:
    s = (s or '').replace('\u200b', '').replace('\u200c', '').replace('\xa0', ' ')
    return re.sub(r'\s+', ' ', s).strip()


class TelegramPilgrim:
    MAX_MEDIA_GROUP = 10

    def __init__(self, core: 'TelegramCore'):
        self.core = core
        self.bot = core.bot

        self._last_edit_cache = {}
        self._local_tg_edits = {}

        self._order_buffers = {}
        self._order_locks = {}
        self._last_sent_ts = {}

        # Prefix collapsing state (per bridge_name)
        self._last_bridged_author = {}   # {bridge_name: "author_name"}
        self._last_bridged_time = {}     # {bridge_name: timestamp}
        self._collapsed_msg_ids = set()  # telegram msg IDs sent without prefix

        self._rabbitmq_connection = None
        self._rabbitmq_channel = None

        ns = core._namespace
        queues = apply_namespace(ARBITER_QUEUES, ns)
        self._queues = queues
        self._pilgrim_queue = queues.get('pilgrim_telegram', 'pilgrim_telegram')

        self._consumer_thread = None

    async def _with_flood_retry(self, send, attempts: int = 4, reset=None):
        """Run a single Telegram API call, waiting out flood limits.

        Only safe for atomic calls: RetryAfter means Telegram rejected the
        request outright, so nothing was delivered and a retry cannot
        duplicate. Without this the send is dropped and its mapping never
        written, leaving the message to be re-sent by a later catch-up.

        `reset` rewinds any file handle the call uploads from; a retry would
        otherwise send zero bytes from an already-consumed stream.
        """
        for attempt in range(1, attempts + 1):
            try:
                if reset is not None:
                    reset()
                return await send()
            except RetryAfter as e:
                if attempt == attempts:
                    raise
                wait = float(getattr(e, 'retry_after', 5)) + 1
                log_warn(
                    f"Flood control: waiting {wait:.0f}s then retrying "
                    f"(attempt {attempt}/{attempts})",
                    'telegram_pilgrim',
                )
                await asyncio.sleep(wait)

    def _store_mapping(self, db, telegram_msg_id, bridge_msg):
        source = (bridge_msg.source or '').lower()
        direction = make_direction(source, 'telegram') if source else None
        try:
            db.store_mapping(source, bridge_msg.message_id, 'telegram', str(telegram_msg_id), direction)
            for extra_id in (bridge_msg.metadata or {}).get('media_group_ids', []):
                db.store_mapping(source, extra_id, 'telegram', str(telegram_msg_id), direction)
        except Exception as e:
            log_debug(f"Failed to store mapping: {e}", 'telegram_pilgrim')

    def _update_collapse_tracking(self, bridge_msg: BridgeMessage):
        """Update prefix-collapse state after a successful bridged send."""
        bname = bridge_msg.bridge_name
        author_key = f"{bridge_msg.source}:{bridge_msg.author_name}"
        self._last_bridged_author[bname] = author_key
        self._last_bridged_time[bname] = time.time()

    def start(self):
        self._consumer_thread = threading.Thread(target=self._run_consumers, daemon=True)
        self._consumer_thread.start()

    def _connect_rabbitmq(self):
        try:
            self._rabbitmq_connection = self.core._new_rabbitmq_connection()
            self._rabbitmq_channel = self._rabbitmq_connection.channel()

            self._rabbitmq_channel.queue_declare(queue=self._pilgrim_queue, durable=True)

            for q in self._queues.values():
                try:
                    self._rabbitmq_channel.queue_declare(queue=q, durable=True)
                except Exception:
                    pass

            log_success("Pilgrim connected to RabbitMQ")
            log_debug(f"Connected OK, pilgrim_queue='{self._pilgrim_queue}'", 'telegram_pilgrim')
            return True
        except Exception as e:
            log_error(f"Pilgrim failed to connect to RabbitMQ: {e}")
            self._rabbitmq_connection = None
            self._rabbitmq_channel = None
            return False

    def _safe_publish(self, routing_key: str, body: str):
        try:
            if self._rabbitmq_connection and self._rabbitmq_connection.is_open:
                self._rabbitmq_connection.add_callback_threadsafe(
                    lambda: self._do_publish(routing_key, body)
                )
            else:
                log_error("Pilgrim: RabbitMQ connection not open.", 'telegram_pilgrim')
        except Exception as e:
            log_error(f"Pilgrim: failed to schedule publish: {e}", 'telegram_pilgrim')

    def _do_publish(self, routing_key: str, body: str):
        try:
            if self._rabbitmq_channel and self._rabbitmq_channel.is_open:
                self._rabbitmq_channel.basic_publish(
                    exchange='',
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
            else:
                log_error("Pilgrim: RabbitMQ channel not open.", 'telegram_pilgrim')
        except Exception as e:
            log_error(f"Pilgrim: _do_publish failed: {e}", 'telegram_pilgrim')

    # ── consumer thread ──────────────────────────────

    def _run_consumers(self):
        log_info("Pilgrim consumer thread started.", 'telegram_pilgrim')

        while True:
            try:
                if not self._rabbitmq_connection or self._rabbitmq_connection.is_closed:
                    log_info("Pilgrim attempting to connect to RabbitMQ...", 'telegram_pilgrim')
                    if not self._connect_rabbitmq():
                        log_warn("Pilgrim connection failed, retrying in 5s...", 'telegram_pilgrim')
                        time.sleep(5)
                        continue

                log_info("Pilgrim connected. Setting up consumers...", 'telegram_pilgrim')

                self._rabbitmq_channel.basic_qos(prefetch_count=10)

                self._rabbitmq_channel.basic_consume(
                    queue=self._pilgrim_queue,
                    on_message_callback=self._handle_incoming,
                    auto_ack=False
                )

                log_success("Pilgrim consumers are set up. Starting consumption loop.", 'telegram_pilgrim')
                log_debug(f"Entering process_data_events loop. Listening on: {self._pilgrim_queue}", 'telegram_pilgrim')

                _heartbeat_counter = 0
                while self._rabbitmq_connection and not self._rabbitmq_connection.is_closed:
                    self._rabbitmq_connection.process_data_events(time_limit=1.0)
                    time.sleep(0.01)
                    _heartbeat_counter += 1
                    if _heartbeat_counter >= 120:  # ~every 2 min
                        _heartbeat_counter = 0
                        try:
                            self.core.send_status_update('ready', 'heartbeat')
                        except Exception:
                            pass

            except (pika.exceptions.StreamLostError, pika.exceptions.ConnectionClosedByBroker, pika.exceptions.AMQPConnectionError) as e:
                log_warn(f"Pilgrim RabbitMQ connection lost: {e}. Reconnecting in 5s...", 'telegram_pilgrim')
            except Exception as e:
                log_error(f"Pilgrim consumer thread error: {e}. Reconnecting in 5s...", 'telegram_pilgrim')

            log_info("Pilgrim cleaning up before reconnect...", 'telegram_pilgrim')
            try:
                if self._rabbitmq_channel and self._rabbitmq_channel.is_open:
                    self._rabbitmq_channel.close()
                if self._rabbitmq_connection and self._rabbitmq_connection.is_open:
                    self._rabbitmq_connection.close()
            except Exception as ce:
                log_error(f"Error during pilgrim cleanup: {ce}", 'telegram_pilgrim')

            self._rabbitmq_connection = None
            self._rabbitmq_channel = None
            time.sleep(5)

    # ── incoming dispatch ────────────────────────────

    def _handle_incoming(self, ch, method, properties, body):
        try:
            raw = body.decode()
            log_debug(f"Received message on pilgrim_telegram, len={len(raw)}", 'telegram_pilgrim')

            try:
                data = json.loads(raw)
            except Exception:
                log_error("Pilgrim received non-JSON message, dropping.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            is_control = (
                isinstance(data, dict)
                and data.get('type') in ('edit', 'delete')
            )

            if is_control:
                log_debug(f"Control message: type={data['type']}", 'telegram_pilgrim')
                if not self.core.main_loop:
                    log_error("No main loop for control callback")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    return

                if data['type'] == 'edit':
                    future = asyncio.run_coroutine_threadsafe(
                        self._handle_edit(data),
                        self.core.main_loop
                    )
                else:
                    future = asyncio.run_coroutine_threadsafe(
                        self._handle_delete(data),
                        self.core.main_loop
                    )

                def _control_done(fut):
                    try:
                        fut.result()
                    except Exception as e:
                        log_error(f"Control task failed: {e}")
                future.add_done_callback(_control_done)

                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            bridge_msg = BridgeMessage.from_json(raw)
            log_debug(f"BridgeMessage parsed OK: bridge={bridge_msg.bridge_name}, author={bridge_msg.author_name}, msg_id={bridge_msg.message_id}", 'telegram_pilgrim')

            if not self.core.main_loop:
                log_error("No main loop available for message callback")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return

            log_debug(f"Scheduling _ordered_send on main loop...", 'telegram_pilgrim')
            future = asyncio.run_coroutine_threadsafe(
                self._ordered_send(bridge_msg),
                self.core.main_loop
            )

            def _done_cb(fut):
                try:
                    fut.result()
                    log_debug(f"_ordered_send completed OK", 'telegram_pilgrim')
                except Exception as e:
                    log_error(f"Ordered send task failed: {e}")
                    traceback.print_exc()
            future.add_done_callback(_done_cb)

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            log_error(f"Error handling incoming message: {e}")
            traceback.print_exc()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    # ── edit / delete ───────────────────────────────

    async def _handle_edit(self, data):
        try:
            bridge = self.core.bridges.get(data['bridge_name'])
            if not bridge:
                return
            tg = bridge.get_platform('telegram')
            if not getattr(tg, 'cross_edit', True):
                log_debug(f"cross_edit disabled for telegram in bridge '{data['bridge_name']}', skipping", 'telegram_pilgrim')
                return
            chat_id = tg.channel_id_int
            msg_id_raw = data.get('telegram_message_id')
            if not msg_id_raw:
                return
            msg_id = int(msg_id_raw)
            raw_text = data['new_text'] or ''

            # Pilgrim constructs prefix from payload metadata
            source = data.get('source', 'unknown')
            source_cfg = bridge.get_platform(source) or tg
            content = html_escape(escape_discord_emojis(raw_text))
            new_text = content

            if source_cfg.use_prefixes and msg_id not in self._collapsed_msg_ids:
                author_name = data.get('author_name', '')
                channel_name = data.get('channel_name', '')
                if author_name:
                    source_label = source.capitalize()
                    channel_suffix = ''
                    if source_cfg.add_channel_name and channel_name:
                        channel_suffix = f", #{channel_name}"
                    safe_author = html_escape(author_name)
                    ch_suffix = html_escape(channel_suffix)
                    prefix = f"<blockquote>[{source_label}] {safe_author}{ch_suffix}</blockquote>\n"
                    new_text = prefix + content

            last = self._last_edit_cache.get(msg_id)
            if last is not None and _norm_text(last) == _norm_text(new_text):
                return

            edit_applied = False
            last_error_msg = ''
            # Telegram errors that mean "this edit is a no-op or impossible"
            # and emphatically should NOT trigger a fallback [Edited] reply.
            # Pasting a duplicate notification into the chat for these would
            # be pure noise. They cover: identical content, target deleted,
            # target too old, target is a non-editable system/service msg.
            _BENIGN_ERRS = (
                'message is not modified',
                "message can't be edited",
                'message to edit not found',
                'there is no text in the message to edit',
                'message_id_invalid',
            )

            def _is_benign(err) -> bool:
                s = str(err).lower()
                return any(token in s for token in _BENIGN_ERRS)

            async def _try_edit_text():
                return await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=new_text,
                    parse_mode=constants.ParseMode.HTML
                )

            async def _try_edit_caption():
                return await self.bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=msg_id,
                    caption=new_text,
                    parse_mode=constants.ParseMode.HTML
                )

            for attempt in range(2):
                try:
                    await asyncio.wait_for(_try_edit_text(), timeout=5.0)
                    edit_applied = True
                    break
                except Exception as _e:
                    last_error_msg = str(_e)
                    if (isinstance(_e, asyncio.TimeoutError) or 'Timed out' in str(_e)) and attempt == 0:
                        await asyncio.sleep(0.5)
                        continue
                    break

            if not edit_applied:
                for attempt in range(2):
                    try:
                        await asyncio.wait_for(_try_edit_caption(), timeout=5.0)
                        edit_applied = True
                        break
                    except Exception as _e2:
                        last_error_msg = str(_e2)
                        if (isinstance(_e2, asyncio.TimeoutError) or 'Timed out' in str(_e2)) and attempt == 0:
                            await asyncio.sleep(0.5)
                            continue
                        break

            if not edit_applied:
                if _is_benign(last_error_msg):
                    # Silent skip - the user doesn't need to know we tried
                    # to no-op an already-up-to-date or too-old message.
                    log_debug(f"Skipped fallback [Edited] reply ({last_error_msg})", 'telegram_pilgrim')
                else:
                    try:
                        await self.bot.send_message(
                            chat_id=chat_id,
                            text=f"[Edited]\n{new_text}",
                            parse_mode=constants.ParseMode.HTML,
                            reply_to_message_id=msg_id
                        )
                        edit_applied = True
                    except Exception as _e3:
                        log_error(f"Failed to edit Telegram message: {_e3}")

            if edit_applied:
                self._last_edit_cache[msg_id] = new_text
                self._local_tg_edits[msg_id] = time.time() + 15.0

        except Exception as e:
            log_error(f"Failed to edit Telegram message: {e}")
        finally:
            try:
                marker = {
                    'type': 'telegram_edit_applied',
                    'bridge_name': data.get('bridge_name'),
                    'telegram_message_id': data.get('telegram_message_id'),
                    'timestamp': time.time()
                }
                self._safe_publish(
                    self._queues.get('import_coordination', 'import_coordination'),
                    json.dumps(marker)
                )
            except Exception as _e_marker:
                log_debug(f"Failed to send edit coordination marker: {_e_marker}")

    async def _handle_delete(self, data):
        try:
            bridge = self.core.bridges.get(data.get('bridge_name'))
            if not bridge:
                return
            tg = bridge.get_platform('telegram')
            if not getattr(tg, 'cross_delete', True):
                log_debug(f"cross_delete disabled for telegram in bridge '{data.get('bridge_name')}', skipping", 'telegram_pilgrim')
                return
            chat_id = tg.channel_id_int

            msg_id = data.get('telegram_message_id')
            if not msg_id:
                # Try to look up from source mapping
                source = data.get('source', '')
                src_id = None
                for key in data:
                    if key.endswith('_message_id') and key != 'telegram_message_id':
                        src_id = data[key]
                        if not source:
                            source = key.replace('_message_id', '')
                        break
                if source and src_id:
                    db = self.core.bridge_dbs.get(data.get('bridge_name'))
                    if db:
                        msg_id = db.get_mapped_id(source, str(src_id), 'telegram')
            if not msg_id:
                return
            msg_id = int(msg_id)

            async def _try_delete():
                return await self.bot.delete_message(chat_id=chat_id, message_id=msg_id)

            for attempt in range(2):
                try:
                    await asyncio.wait_for(_try_delete(), timeout=3.0)
                    log_debug(f"Deleted Telegram message {msg_id}")
                    return
                except Exception as _e:
                    if ('Timed out' in str(_e) or isinstance(_e, asyncio.TimeoutError)) and attempt == 0:
                        await asyncio.sleep(0.5)
                        continue
                    break
            try:
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text="[Deleted]",
                    parse_mode=constants.ParseMode.HTML
                )
                log_debug(f"Marked Telegram message {msg_id} as deleted")
            except Exception as _e2:
                log_error(f"Failed to delete or mark Telegram message: {_e2}")
        except Exception as e:
            log_error(f"Failed to delete Telegram message: {e}")

    # ── ordered send ─────────────────────────────────

    async def _ordered_send(self, bridge_msg: BridgeMessage):
        key = bridge_msg.bridge_name
        if key not in self._order_locks:
            self._order_locks[key] = asyncio.Lock()
        if key not in self._order_buffers:
            self._order_buffers[key] = []
        if key not in self._last_sent_ts:
            self._last_sent_ts[key] = 0.0

        try:
            if (bridge_msg.source or '').lower() == 'discord':
                try:
                    sid = int(bridge_msg.message_id)
                    ts = discord_snowflake_to_unix(sid)
                except Exception:
                    ts = float(bridge_msg.timestamp or 0.0) or time.time()
            else:
                ts = float(bridge_msg.timestamp or 0.0) or time.time()
        except Exception:
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
                    log_debug(f"Calling _send_to_telegram for msg_id={msg0.message_id}", 'telegram_pilgrim')
                    await self._send_to_telegram(msg0)
                    log_debug(f"_send_to_telegram returned for msg_id={msg0.message_id}", 'telegram_pilgrim')
                    self._update_collapse_tracking(msg0)
                except Exception as e:
                    log_error(f"Ordered send failed: {e}")

                if ts0 > last_ts:
                    last_ts = ts0
                    self._last_sent_ts[key] = last_ts

    # ── send to telegram ─────────────────────────────

    async def _send_to_telegram(self, bridge_msg: BridgeMessage):
        try:
            log_debug(f"_send_to_telegram called: bridge={bridge_msg.bridge_name}, author={bridge_msg.author_name}", 'telegram_pilgrim')
            bridge_config = self.core.bridges.get(bridge_msg.bridge_name)
            if not bridge_config:
                log_debug(f"ABORT: bridge '{bridge_msg.bridge_name}' not found in core.bridges! Available: {list(self.core.bridges.keys())}", 'telegram_pilgrim')
                return

            source_platform = bridge_config.get_platform(bridge_msg.source) if bridge_msg.source else None
            tg = bridge_config.get_platform('telegram')
            source_cfg = source_platform or tg
            source_label = (bridge_msg.source or 'Unknown').capitalize()

            log_debug(f"bridge_config found, chat_id={tg.channel_id_int}, use_prefixes={source_cfg.use_prefixes}", 'telegram_pilgrim')
            topic_id = tg.topic_id

            if getattr(bridge_msg, 'poll', None):
                prefix_text = ''
                if source_cfg.use_prefixes:
                    channel_suffix = ''
                    if source_cfg.add_channel_name and bridge_msg.channel_name:
                        channel_suffix = f", #{bridge_msg.channel_name}"
                    prefix_text = f"<blockquote>[{source_label}] {html_escape(bridge_msg.author_name or '')}{html_escape(channel_suffix)}</blockquote>\n"
                poll = bridge_msg.poll or {}
                question = html_escape(str(poll.get('question') or 'Poll'))
                options = [html_escape(str(o)) for o in (poll.get('options') or []) if str(o).strip()]
                allows_multiple = bool(poll.get('allows_multiple', False))
                lines = [f"\U0001f4ca Poll: {question}"]
                for opt in options:
                    lines.append(f"- {opt}")
                if allows_multiple:
                    lines.append("(Multiple answers allowed)")
                summary_text = prefix_text + "\n".join(lines)
                reply_to = None
                if bridge_msg.reply_to_id:
                    db_tmp = self.core.bridge_dbs.get(bridge_msg.bridge_name)
                    if db_tmp:
                        try:
                            reply_to = db_tmp.get_mapped_id(bridge_msg.source, bridge_msg.reply_to_id, 'telegram')
                            if reply_to:
                                reply_to = int(reply_to)
                        except Exception:
                            pass
                try:
                    sent = await self.bot.send_message(
                        chat_id=tg.channel_id_int,
                        text=summary_text,
                        parse_mode=constants.ParseMode.HTML,
                        reply_to_message_id=reply_to,
                        message_thread_id=topic_id
                    )
                except Exception as e_first:
                    if 'Message thread not found' in str(e_first):
                        log_warn(f"Topic {topic_id} not found; sending without thread", 'telegram_pilgrim')
                        sent = await self.bot.send_message(
                            chat_id=tg.channel_id_int,
                            text=summary_text,
                            parse_mode=constants.ParseMode.HTML,
                            reply_to_message_id=reply_to
                        )
                    else:
                        raise
                if bridge_msg.bridge_name in self.core.bridge_dbs:
                    db = self.core.bridge_dbs[bridge_msg.bridge_name]
                    self._store_mapping(db, sent.message_id, bridge_msg)
                return

            content = escape_discord_emojis(bridge_msg.content or '')
            content = html_escape(content)
            # Convert ||spoiler|| markers to native TG spoiler tags
            content = re.sub(r'\|\|(.+?)\|\|', r'<tg-spoiler>\1</tg-spoiler>', content)

            prefix_html = ''
            skip_prefix = False
            if source_cfg.use_prefixes:
                # Check if prefix should be collapsed (same author, no interruption)
                bname = bridge_msg.bridge_name
                if getattr(tg, 'collapse_prefixes', False) and not bridge_msg.attachments and not bridge_msg.is_forward:
                    author_key = f"{bridge_msg.source}:{bridge_msg.author_name}"
                    now = time.time()
                    last_author = self._last_bridged_author.get(bname)
                    last_time = self._last_bridged_time.get(bname, 0.0)
                    native_ts = self.core._last_native_tg_msg.get(bname, 0.0)
                    # Skip prefix if: same author, within 120s, no native TG msg since last bridge
                    if (last_author == author_key
                            and (now - last_time) < 120
                            and native_ts < last_time):
                        skip_prefix = True

                if skip_prefix:
                    text = content
                else:
                    channel_suffix = ''
                    if source_cfg.add_channel_name and bridge_msg.channel_name:
                        channel_suffix = f", #{bridge_msg.channel_name}"
                    safe_author = html_escape(bridge_msg.author_name or '')
                    ch_suffix = html_escape(channel_suffix)
                    prefix_html = f"<blockquote>[{source_label}] {safe_author}{ch_suffix}"
                    if bridge_msg.is_forward:
                        prefix_html += f", forwarded from: {html_escape(bridge_msg.forward_from or '')}"
                    if source_cfg.add_filenames and bridge_msg.attachments:
                        filenames = [att['filename'] for att in bridge_msg.attachments]
                        prefix_html += f", {html_escape(', '.join(filenames))}"
                    prefix_html += "</blockquote>\n"
                    text = prefix_html + content
            else:
                text = content

            reply_to = None
            if bridge_msg.reply_to_id:
                db_tmp = self.core.bridge_dbs.get(bridge_msg.bridge_name)
                if db_tmp:
                    try:
                        reply_to = db_tmp.get_mapped_id(bridge_msg.source, bridge_msg.reply_to_id, 'telegram')
                        if reply_to:
                            reply_to = int(reply_to)
                    except Exception:
                        pass

            att_count = len(bridge_msg.attachments)
            has_local = bool(bridge_msg.attachments and bridge_msg.attachments[0].get('local_path'))
            log_debug(f"att_count={att_count}, has_local={has_local}, text_len={len(text)}, reply_to={reply_to}, topic_id={topic_id}", 'telegram_pilgrim')

            if len(bridge_msg.attachments) > 1:
                log_debug(f"-> sending media group ({len(bridge_msg.attachments)} attachments)", 'telegram_pilgrim')
                await self._send_media_group(bridge_msg, bridge_config, text, reply_to, topic_id)
                return

            if bridge_msg.attachments and bridge_msg.attachments[0].get('local_path'):
                log_debug(f"-> sending single attachment: {bridge_msg.attachments[0].get('filename')}", 'telegram_pilgrim')
                await self._send_single_attachment(bridge_msg, bridge_config, text, reply_to, topic_id)
                return

            log_debug(f"-> sending text-only message", 'telegram_pilgrim')
            send_kwargs = dict(
                chat_id=tg.channel_id_int,
                text=text if text.strip() else "This format is not supported",
                parse_mode=constants.ParseMode.HTML,
                reply_to_message_id=reply_to,
                message_thread_id=topic_id
            )
            try:
                sent = await self._with_flood_retry(
                    lambda: self.bot.send_message(**send_kwargs)
                )
            except Exception as e_text:
                if 'Message thread not found' in str(e_text):
                    log_warn(f"Topic {topic_id} not found; sending without thread", 'telegram_pilgrim')
                    send_kwargs.pop('message_thread_id', None)
                    sent = await self._with_flood_retry(
                        lambda: self.bot.send_message(**send_kwargs)
                    )
                else:
                    raise
            log_debug(f"Text message sent OK, sent.message_id={sent.message_id}", 'telegram_pilgrim')

            if bridge_msg.bridge_name in self.core.bridge_dbs:
                db = self.core.bridge_dbs[bridge_msg.bridge_name]
                self._store_mapping(db, sent.message_id, bridge_msg)

            if skip_prefix:
                self._collapsed_msg_ids.add(sent.message_id)

        except Exception as e:
            log_debug(f"_send_to_telegram EXCEPTION: {e}", 'telegram_pilgrim')
            if is_transient_error(e):
                # A network blip, not a fault. Note this is NOT retried: unlike
                # RetryAfter, a timeout may mean the send actually landed and we
                # never saw the reply, so resending could duplicate.
                log_transient(f"Error sending to Telegram: {e}", 'telegram_pilgrim')
            else:
                log_error(f"Error sending to Telegram: {e}")
                traceback.print_exc()

    # ── media group ──────────────────────────────────

    async def _send_media_group(self, bridge_msg, bridge_config, text, reply_to, topic_id):
        tg = bridge_config.get_platform('telegram')
        media_group = []
        extras_as_documents = []
        try:
            for i, attachment in enumerate(bridge_msg.attachments):
                file_path = attachment.get('local_path')
                if not (file_path and os.path.exists(file_path)):
                    continue
                fname = attachment.get('filename', '').lower()
                is_image = ('image' in attachment.get('type', '').lower()) or fname.endswith(('.jpg', '.jpeg', '.png', '.webp'))
                is_video = ('video' in attachment.get('type', '').lower()) or fname.endswith(('.mp4', '.avi', '.mov', '.webm'))

                if is_image:
                    converted = await self._convert_image_to_png(file_path)
                    file_path = converted or file_path
                    if self._is_tg_photo_too_small(file_path):
                        extras_as_documents.append((file_path, attachment.get('filename')))
                        continue
                    if i == 0:
                        media_group.append(InputMediaPhoto(
                            media=open(file_path, 'rb'),
                            caption=text,
                            parse_mode=constants.ParseMode.HTML
                        ))
                    else:
                        media_group.append(InputMediaPhoto(media=open(file_path, 'rb')))
                elif is_video:
                    if any(ext in fname for ext in ['.avi', '.mov', '.webm']):
                        converted = await self._convert_video_to_mp4(file_path)
                        file_path = converted or file_path
                    if i == 0:
                        media_group.append(InputMediaVideo(
                            media=open(file_path, 'rb'),
                            caption=text,
                            parse_mode=constants.ParseMode.HTML
                        ))
                    else:
                        media_group.append(InputMediaVideo(media=open(file_path, 'rb')))
                else:
                    extras_as_documents.append((file_path, attachment.get('filename')))

            await asyncio.sleep(0.2)

            sent_messages = None
            if media_group:
                chunks = [media_group[i:i + self.MAX_MEDIA_GROUP]
                          for i in range(0, len(media_group), self.MAX_MEDIA_GROUP)]
                for idx, chunk in enumerate(chunks):
                    chunk_reply = reply_to if idx == 0 else None
                    chunk_topic = topic_id
                    try:
                        result = await self._with_flood_retry(
                            lambda: self.bot.send_media_group(
                                chat_id=tg.channel_id_int,
                                media=chunk,
                                reply_to_message_id=chunk_reply,
                                message_thread_id=chunk_topic
                            )
                        )
                    except Exception as e_group:
                        if 'Message thread not found' in str(e_group):
                            log_warn(f"Topic {topic_id} not found (group); sending without thread", 'telegram_pilgrim')
                            result = await self._with_flood_retry(
                                lambda: self.bot.send_media_group(
                                    chat_id=tg.channel_id_int,
                                    media=chunk,
                                    reply_to_message_id=chunk_reply
                                )
                            )
                        else:
                            raise
                    if idx == 0:
                        sent_messages = result

            if sent_messages and bridge_msg.bridge_name in self.core.bridge_dbs:
                db = self.core.bridge_dbs[bridge_msg.bridge_name]
                self._store_mapping(db, sent_messages[0].message_id, bridge_msg)

            first_extra_sent_id = None
            for idx, (p, original_name) in enumerate(extras_as_documents):
                cap = text if (not sent_messages and idx == 0) else None
                pm = constants.ParseMode.HTML if (not sent_messages and idx == 0 and text) else None
                rto = reply_to if (not sent_messages and idx == 0) else None
                tid = topic_id if sent_messages is None else None
                fn_lower = (original_name or '').lower()
                is_extra_audio = fn_lower.endswith(('.mp3', '.m4a', '.flac', '.wav', '.aac', '.wma'))
                is_extra_voice = fn_lower.endswith(('.ogg', '.oga', '.opus'))
                with open(p, 'rb') as f:
                    try:
                        if is_extra_voice:
                            extra = await self._with_flood_retry(
                                lambda: self.bot.send_voice(
                                    chat_id=tg.channel_id_int, voice=f,
                                    caption=cap, parse_mode=pm,
                                    reply_to_message_id=rto, message_thread_id=tid
                                ),
                                reset=lambda: f.seek(0),
                            )
                        elif is_extra_audio:
                            extra = await self._with_flood_retry(
                                lambda: self.bot.send_audio(
                                    chat_id=tg.channel_id_int, audio=f,
                                    caption=cap, parse_mode=pm,
                                    reply_to_message_id=rto, message_thread_id=tid
                                ),
                                reset=lambda: f.seek(0),
                            )
                        else:
                            extra = await self._with_flood_retry(
                                lambda: self.bot.send_document(
                                    chat_id=tg.channel_id_int, document=f,
                                    caption=cap, parse_mode=pm,
                                    reply_to_message_id=rto, message_thread_id=tid
                                ),
                                reset=lambda: f.seek(0),
                            )
                        if first_extra_sent_id is None:
                            first_extra_sent_id = extra.message_id
                    except Exception as e_extra:
                        if 'Message thread not found' in str(e_extra):
                            if is_extra_voice:
                                extra = await self._with_flood_retry(
                                    lambda: self.bot.send_voice(
                                        chat_id=tg.channel_id_int, voice=f,
                                        caption=cap, parse_mode=pm,
                                        reply_to_message_id=rto
                                    ),
                                    reset=lambda: f.seek(0),
                                )
                            elif is_extra_audio:
                                extra = await self._with_flood_retry(
                                    lambda: self.bot.send_audio(
                                        chat_id=tg.channel_id_int, audio=f,
                                        caption=cap, parse_mode=pm,
                                        reply_to_message_id=rto
                                    ),
                                    reset=lambda: f.seek(0),
                                )
                            else:
                                extra = await self._with_flood_retry(
                                    lambda: self.bot.send_document(
                                        chat_id=tg.channel_id_int, document=f,
                                        caption=cap, parse_mode=pm,
                                        reply_to_message_id=rto
                                    ),
                                    reset=lambda: f.seek(0),
                                )
                            if first_extra_sent_id is None:
                                first_extra_sent_id = extra.message_id

            if (not sent_messages) and first_extra_sent_id and bridge_msg.bridge_name in self.core.bridge_dbs:
                db = self.core.bridge_dbs[bridge_msg.bridge_name]
                self._store_mapping(db, first_extra_sent_id, bridge_msg)

        except Exception:
            raise
        finally:
            for im in media_group:
                try:
                    if hasattr(im.media, 'close'):
                        im.media.close()
                except Exception:
                    pass

    # ── single attachment ────────────────────────────

    async def _send_single_attachment(self, bridge_msg, bridge_config, text, reply_to, topic_id):
        attachment = bridge_msg.attachments[0]
        file_path = attachment.get('local_path')
        if not (file_path and os.path.exists(file_path)):
            sent = await self._send_text_fallback(bridge_config, text, reply_to, topic_id)
            if bridge_msg.bridge_name in self.core.bridge_dbs:
                db = self.core.bridge_dbs[bridge_msg.bridge_name]
                self._store_mapping(db, sent.message_id, bridge_msg)
            return

        filename = attachment.get('filename', '')
        att_type = attachment.get('type', '').lower()
        fname_lower = filename.lower()
        is_image = ('image' in att_type) or ('x-tgsticker' in att_type) or fname_lower.endswith(('.jpg', '.jpeg', '.png', '.webp'))
        is_video = ('video' in att_type) or fname_lower.endswith(('.mp4', '.avi', '.mov', '.webm'))
        is_gif = fname_lower.endswith('.gif') or 'gif' in att_type
        is_audio = ('audio' in att_type) or fname_lower.endswith(('.mp3', '.m4a', '.flac', '.wav', '.aac', '.wma'))
        is_voice = fname_lower.endswith(('.ogg', '.oga', '.opus')) or 'ogg' in att_type

        meta = getattr(bridge_msg, 'metadata', None) or {}
        if meta.get('is_voice'):
            is_voice = True
            is_audio = False
        elif meta.get('is_audio'):
            is_audio = True
            is_voice = False

        is_giflike_video = 'tenor' in attachment.get('url', '').lower() or (fname_lower.endswith('.mp4') and 'animation' in filename.lower())

        if is_image and not is_gif:
            converted = await self._convert_image_to_png(file_path)
            if converted:
                file_path = converted
        elif is_video and any(ext in filename.lower() for ext in ['.avi', '.mov', '.webm']):
            converted = await self._convert_video_to_mp4(file_path)
            if converted:
                file_path = converted

        if is_giflike_video and not is_gif:
            try:
                gif_path = await convert_video_to_gif(file_path)
                if gif_path:
                    file_path = gif_path
                    is_gif = True
                    is_video = False
            except Exception as e:
                log_warn(f"Failed to convert video to GIF, sending as video: {e}")

        with open(file_path, 'rb') as f:
            try:
                sent = await self._with_flood_retry(
                    lambda: self._send_file(
                        f, file_path, bridge_config, text, reply_to, topic_id,
                        is_gif=is_gif, is_image=is_image, is_video=is_video,
                        is_audio=is_audio, is_voice=is_voice, metadata=meta,
                        has_spoiler=attachment.get('spoiler', False)
                    ),
                    reset=lambda: f.seek(0),
                )
            except Exception as e_file:
                if 'Message thread not found' in str(e_file):
                    log_warn(f"Topic {topic_id} not found; sending without thread", 'telegram_pilgrim')
                    sent = await self._with_flood_retry(
                        lambda: self._send_file(
                            f, file_path, bridge_config, text, reply_to, None,
                            is_gif=is_gif, is_image=is_image, is_video=is_video,
                            is_audio=is_audio, is_voice=is_voice, metadata=meta,
                            has_spoiler=attachment.get('spoiler', False)
                        ),
                        reset=lambda: f.seek(0),
                    )
                else:
                    raise

        await asyncio.sleep(0.15)

        # Delay file deletion so other pilgrims have time to read the same file
        async def _deferred_remove():
            await asyncio.sleep(120)
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                if file_path.endswith('_converted.png') or file_path.endswith('_converted.mp4'):
                    original = file_path.replace('_converted.png', '').replace('_converted.mp4', '')
                    if os.path.exists(original):
                        os.remove(original)
            except Exception:
                pass

        asyncio.create_task(_deferred_remove())

        if bridge_msg.bridge_name in self.core.bridge_dbs:
            db = self.core.bridge_dbs[bridge_msg.bridge_name]
            self._store_mapping(db, sent.message_id, bridge_msg)

    async def _send_file(self, f, file_path, bridge_config, text, reply_to, topic_id,
                         is_gif=False, is_image=False, is_video=False,
                         is_audio=False, is_voice=False, metadata=None,
                         has_spoiler=False):
        tg = bridge_config.get_platform('telegram')
        chat_id = tg.channel_id_int
        metadata = metadata or {}

        if is_gif:
            return await self.bot.send_animation(
                chat_id=chat_id, animation=f,
                caption=text, parse_mode=constants.ParseMode.HTML,
                reply_to_message_id=reply_to, message_thread_id=topic_id,
                has_spoiler=has_spoiler
            )
        elif is_voice:
            return await self.bot.send_voice(
                chat_id=chat_id, voice=f,
                caption=text, parse_mode=constants.ParseMode.HTML,
                reply_to_message_id=reply_to, message_thread_id=topic_id,
                duration=metadata.get('audio_duration')
            )
        elif is_audio:
            return await self.bot.send_audio(
                chat_id=chat_id, audio=f,
                caption=text, parse_mode=constants.ParseMode.HTML,
                reply_to_message_id=reply_to, message_thread_id=topic_id,
                performer=metadata.get('audio_performer'),
                title=metadata.get('audio_title'),
                duration=metadata.get('audio_duration')
            )
        elif is_image and not self._is_tg_photo_too_small(file_path):
            try:
                return await self.bot.send_photo(
                    chat_id=chat_id, photo=f,
                    caption=text, parse_mode=constants.ParseMode.HTML,
                    reply_to_message_id=reply_to, message_thread_id=topic_id,
                    has_spoiler=has_spoiler
                )
            except Exception as e_photo:
                if 'PHOTO_INVALID_DIMENSIONS' in str(e_photo).upper() or 'PHOTO_INVALID' in str(e_photo).upper():
                    f.seek(0)
                    return await self.bot.send_document(
                        chat_id=chat_id, document=f,
                        caption=text, parse_mode=constants.ParseMode.HTML,
                        reply_to_message_id=reply_to, message_thread_id=topic_id
                    )
                raise
        elif is_video and not file_path.lower().endswith('.gif'):
            return await self.bot.send_video(
                chat_id=chat_id, video=f,
                caption=text, parse_mode=constants.ParseMode.HTML,
                reply_to_message_id=reply_to, message_thread_id=topic_id,
                has_spoiler=has_spoiler
            )
        else:
            return await self.bot.send_document(
                chat_id=chat_id, document=f,
                caption=text, parse_mode=constants.ParseMode.HTML,
                reply_to_message_id=reply_to, message_thread_id=topic_id
            )

    async def _send_text_fallback(self, bridge_config, text, reply_to, topic_id):
        tg = bridge_config.get_platform('telegram')
        send_kwargs = dict(
            chat_id=tg.channel_id_int,
            text=text if text.strip() else "This format is not supported",
            parse_mode=constants.ParseMode.HTML,
            reply_to_message_id=reply_to,
            message_thread_id=topic_id
        )
        try:
            return await self.bot.send_message(**send_kwargs)
        except Exception as e:
            if 'Message thread not found' in str(e):
                log_warn(f"Topic {topic_id} not found; sending without thread", 'telegram_pilgrim')
                send_kwargs.pop('message_thread_id', None)
                return await self.bot.send_message(**send_kwargs)
            raise

    # ── media conversion ─────────────────────────────

    async def _convert_image_to_png(self, file_path):
        try:
            output_path = file_path.rsplit('.', 1)[0] + '_converted.png'
            with Image.open(file_path) as img:
                if img.mode not in ('RGBA', 'RGB', 'L'):
                    img = img.convert('RGBA')
                max_size = 1280
                if img.width > max_size or img.height > max_size:
                    img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
                img.save(output_path, 'PNG', optimize=True)
            return output_path
        except Exception as e:
            log_error(f"Image conversion failed: {e}")
            return None

    async def _convert_video_to_mp4(self, file_path):
        try:
            output_path = file_path.rsplit('.', 1)[0] + '_converted.mp4'
            cmd = [
                'ffmpeg', '-i', file_path,
                '-c:v', 'libx264', '-c:a', 'aac',
                '-movflags', '+faststart',
                '-preset', 'medium', '-crf', '23',
                '-y', output_path
            ]
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=300)
            if process.returncode == 0 and os.path.exists(output_path):
                return output_path
            return None
        except Exception as e:
            log_error(f"Video conversion failed: {e}")
            return None

    def _is_tg_photo_too_small(self, file_path: str, min_side: int = 50) -> bool:
        try:
            with Image.open(file_path) as im:
                w, h = im.size
                return (w < min_side) or (h < min_side)
        except Exception:
            return False
