import stoat
import asyncio
import pika
import json
import time
import os
import aiohttp
import threading
from typing import Optional, Dict

import sys
_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _root not in sys.path:
    sys.path.insert(0, _root)

from postkeep.papyrus import (
    log_info, log_error, log_warn, log_success, log_debug,
    load_global_settings, ensure_cache_dir,
    BridgeMessage, StatusUpdate,
    BridgeDatabase, CONFIG_FILE,
    enforce_cache_quota, should_skip_download,
    ARBITER_QUEUES, apply_namespace, get_namespace,
    GatewayConfig, load_all_gateways,
    get_avatar_db,
)

CITADEL_ROOT = os.path.join(_root, 'citadel')
CODEX_FILE = os.path.join(_root, 'codex.ini')
CONFIG_PATH = CODEX_FILE if os.path.exists(CODEX_FILE) else CONFIG_FILE


class BridgeClient(stoat.Client):
    def __init__(self, *args, **kwargs):
        self._core = None
        self._scribe = None
        self._pilgrim = None
        self._last_event_ts = time.time()
        self._ready_fired = False
        self._watchdog_running = False
        super().__init__(*args, **kwargs)

    async def on_ready(self, event):
        is_reconnect = self._ready_fired
        if is_reconnect:
            log_info(f"Stoatchat reconnected as {event.me.tag} (re-ready)")
        else:
            log_success(f"Stoatchat bot logged in as {event.me.tag}")
        self._ready_fired = True
        self._last_event_ts = time.time()

        core = self._core
        if not core:
            return

        if not core.rabbitmq_channel or core.rabbitmq_channel.is_closed:
            log_info("Waiting for RabbitMQ connection...")
            for _ in range(10):
                await asyncio.sleep(1)
                if core.rabbitmq_channel and not core.rabbitmq_channel.is_closed:
                    break
            else:
                log_error("RabbitMQ connection timeout")
                return

        core.send_status_update('ready')

        pilgrim = self._pilgrim
        if pilgrim:
            # Always keep event loop reference current
            pilgrim._loop = asyncio.get_event_loop()
            if not core.consumer_thread or not core.consumer_thread.is_alive():
                core.consumer_thread = threading.Thread(target=pilgrim.run_consumers, daemon=True)
                core.consumer_thread.start()
                log_info("Started RabbitMQ consumer thread (pilgrim)")
                pilgrim._start_weekly_avatar_refresh()

        if not is_reconnect:
            asyncio.create_task(self._second_ready_ping())
        if not self._watchdog_running:
            self._watchdog_running = True
            asyncio.create_task(self._connection_watchdog())

    async def _second_ready_ping(self):
        try:
            await asyncio.sleep(5)
            if self._core:
                self._core.send_status_update('ready', 'post-ready ping')
        except Exception as exc:
            log_debug(f"Second ready ping failed: {exc}", 'stoatchat')

    async def _connection_watchdog(self):
        """Monitor event flow. Only force-exit if REST API is also dead for extended period."""
        STARTUP_GRACE = 120   # 2 min grace after ready
        CHECK_INTERVAL = 60   # check every 60s
        DEAD_THRESHOLD = 600  # 10 min with no events = check health
        FATAL_THRESHOLD = 1200  # 20 min with no events AND no API = force exit
        _consecutive_api_failures = 0

        await asyncio.sleep(STARTUP_GRACE)
        log_info("[WATCHDOG] Stoatchat connection watchdog active", 'stoatchat')

        while True:
            await asyncio.sleep(CHECK_INTERVAL)

            elapsed = time.time() - self._last_event_ts
            if elapsed < DEAD_THRESHOLD:
                _consecutive_api_failures = 0
                continue

            log_debug(f"[WATCHDOG] No events for {elapsed:.0f}s, checking health...", 'stoatchat')

            # Try a REST API call to verify we're still authenticated
            api_works = False
            try:
                if self.me:
                    user = await asyncio.wait_for(self.fetch_user(self.me.id), timeout=10.0)
                    if user:
                        api_works = True
                        _consecutive_api_failures = 0
                else:
                    log_debug("[WATCHDOG] self.me is None", 'stoatchat')
            except Exception as e:
                log_debug(f"[WATCHDOG] Health check failed: {e}", 'stoatchat')

            if api_works:
                # API works, WS is probably fine - just a quiet channel
                # Try nudging the WS connection
                try:
                    ws = getattr(self, 'ws', None) or getattr(self, '_ws', None)
                    if ws:
                        await ws.close()
                    await asyncio.sleep(5)
                except Exception:
                    pass
                continue

            # API also failing
            _consecutive_api_failures += 1
            if elapsed > FATAL_THRESHOLD and _consecutive_api_failures >= 3:
                log_error("[WATCHDOG] API unreachable for extended period, forcing restart.", 'stoatchat')
                if self._core:
                    self._core.send_status_update('error', 'Watchdog: API unreachable, restarting')
                os._exit(1)

    async def on_message(self, message):
        self._last_event_ts = time.time()
        log_debug(f"[RAW] on_message fired: channel={getattr(message, 'channel_id', '?')}, author={getattr(message, 'author_id', '?')}", 'stoatchat')
        if self._scribe:
            try:
                await self._scribe.process_stoatchat_message(message)
            except Exception as e:
                log_error(f"[SCRIBE] process_stoatchat_message crashed: {type(e).__name__}: {e}", 'stoatchat')
                import traceback; traceback.print_exc()

    async def on_message_update(self, event):
        self._last_event_ts = time.time()
        if self._scribe:
            try:
                await self._scribe.handle_message_edit(event)
            except Exception as e:
                log_error(f"[SCRIBE] handle_message_edit crashed: {type(e).__name__}: {e}", 'stoatchat')

    async def on_message_delete(self, event):
        self._last_event_ts = time.time()
        if self._scribe:
            try:
                await self._scribe.handle_message_delete(event)
            except Exception as e:
                log_error(f"[SCRIBE] handle_message_delete crashed: {type(e).__name__}: {e}", 'stoatchat')

    async def on_member_join(self, event):
        self._last_event_ts = time.time()
        if self._scribe:
            try:
                await self._scribe.handle_member_join(event)
            except Exception as e:
                log_error(f"[SCRIBE] handle_member_join crashed: {type(e).__name__}: {e}", 'stoatchat')

    async def on_member_remove(self, event):
        self._last_event_ts = time.time()
        if self._scribe:
            try:
                await self._scribe.handle_member_leave(event)
            except Exception as e:
                log_error(f"[SCRIBE] handle_member_leave crashed: {type(e).__name__}: {e}", 'stoatchat')


class StoatchatCore:
    def __init__(self):
        self.component_name = 'stoatchat'
        self.settings = load_global_settings(CONFIG_PATH, 'stoatchat_bot')
        self.bridges = self._build_bridges_from_gateways()
        self.bridge_dbs: Dict[str, BridgeDatabase] = {}

        self._init_citadel()

        # Use a custom shard factory to disable aggressive reconnect-on-pong-mismatch.
        # The stoat library reconnects on ANY missed pong nonce (every ~60s on flaky networks).
        # With reconnect_on_timeout=False, missed pongs are logged as warnings but don't
        # trigger reconnects. Our watchdog (5-min dead threshold) handles actual dead connections.
        def _make_shard(client, state):
            from stoat.shard import ShardImpl
            from stoat.client import ClientEventHandler
            return ShardImpl(
                self.settings['STOATCHAT_TOKEN'],
                handler=ClientEventHandler(client),
                state=state,
                reconnect_on_timeout=False,
            )

        self.client = BridgeClient(
            token=self.settings['STOATCHAT_TOKEN'],
            shard=_make_shard,
        )
        self.client._core = self

        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self._pub_connection = None
        self._pub_channel = None
        self.consumer_thread = None

        self._namespace = get_namespace() or str(self.settings.get('QUEUE_NAMESPACE', '')).strip()
        self.queues = apply_namespace(ARBITER_QUEUES, self._namespace)

        # Native message timestamps for prefix collapse (set by scribe, read by pilgrim)
        self._last_native_sc_msg = {}  # {bridge_name: timestamp}

        self.connect_rabbitmq()

        self.avatar_upload_channel_id = self.settings.get('STOATCHAT_AVATAR_UPLOAD_CHANNEL_ID')
        if self.avatar_upload_channel_id:
            self.avatar_upload_channel_id = str(self.avatar_upload_channel_id).strip()
            if self.avatar_upload_channel_id:
                log_info(f"Stoatchat avatar upload channel ID: {self.avatar_upload_channel_id}")
            else:
                self.avatar_upload_channel_id = None

        try:
            self.avatar_db = get_avatar_db()
        except Exception:
            self.avatar_db = None

    # ========== Citadel ==========

    def _init_citadel(self):
        os.makedirs(CITADEL_ROOT, exist_ok=True)
        for name, bridge in self.bridges.items():
            bridge_dir = os.path.join(CITADEL_ROOT, name)
            os.makedirs(bridge_dir, exist_ok=True)
            db_path = bridge.db_path
            self.bridge_dbs[name] = BridgeDatabase(db_path)
            log_info(f"Initialized database for '{name}': {db_path}", self.component_name)

    def _load_gateways(self) -> Dict:
        return load_all_gateways(CITADEL_ROOT)

    def _build_bridges_from_gateways(self) -> Dict:
        gateways = self._load_gateways()
        bridges = {}
        for name, gw in gateways.items():
            sc = gw.get_platform('stoatchat')
            if not sc or not sc.channel_id:
                continue
            bridges[name] = gw
        if not bridges:
            log_warn("No citadel gateways with stoatchat platform found", self.component_name)
        return bridges

    # ========== RabbitMQ ==========

    def connect_rabbitmq(self):
        try:
            credentials = pika.PlainCredentials(
                self.settings['RABBITMQ_USER'],
                self.settings['RABBITMQ_PASS']
            )
            parameters = pika.ConnectionParameters(
                host=self.settings['RABBITMQ_HOST'],
                port=self.settings['RABBITMQ_PORT'],
                credentials=credentials,
                heartbeat=60,
                blocked_connection_timeout=120,
                socket_timeout=10,
            )
            self.rabbitmq_connection = pika.BlockingConnection(parameters)
            self.rabbitmq_channel = self.rabbitmq_connection.channel()

            all_queues = set(self.queues.values())
            for qn in all_queues:
                self.rabbitmq_channel.queue_declare(queue=qn, durable=True)

            log_success(f"Stoatchat connected to RabbitMQ (ns='{self._namespace or 'default'}')")
        except Exception as e:
            log_error(f"Failed to connect to RabbitMQ: {e}")

    def _safe_publish(self, routing_key, body):
        try:
            if not self._pub_connection or self._pub_connection.is_closed:
                self._open_pub_connection()
            self._pub_channel.basic_publish(
                exchange='',
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(delivery_mode=2)
            )
        except Exception as e:
            log_debug(f"Publish failed: {e}, reconnecting...", self.component_name)
            try:
                self._open_pub_connection()
                self._pub_channel.basic_publish(
                    exchange='',
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
            except Exception as e2:
                log_error(f"_safe_publish FAILED even after reconnect: {e2}", self.component_name)

    def _open_pub_connection(self):
        try:
            if self._pub_connection and not self._pub_connection.is_closed:
                self._pub_connection.close()
        except Exception:
            pass
        self._pub_connection = None
        self._pub_channel = None
        credentials = pika.PlainCredentials(
            self.settings['RABBITMQ_USER'],
            self.settings['RABBITMQ_PASS']
        )
        params = pika.ConnectionParameters(
            host=self.settings['RABBITMQ_HOST'],
            port=self.settings['RABBITMQ_PORT'],
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=120,
            socket_timeout=10,
        )
        self._pub_connection = pika.BlockingConnection(params)
        self._pub_channel = self._pub_connection.channel()

    def send_status_update(self, status: str, message: Optional[str] = None):
        try:
            msg = message
            if self._namespace:
                msg = f"{message or ''} [ns={self._namespace}]".strip()
            update = StatusUpdate(
                component='stoatchat',
                status=status,
                message=msg,
                timestamp=time.time()
            )
            credentials = pika.PlainCredentials(
                self.settings['RABBITMQ_USER'],
                self.settings['RABBITMQ_PASS']
            )
            params = pika.ConnectionParameters(
                host=self.settings['RABBITMQ_HOST'],
                port=self.settings['RABBITMQ_PORT'],
                credentials=credentials,
                heartbeat=60,
                blocked_connection_timeout=120,
                socket_timeout=10,
            )
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            try:
                ch.queue_declare(queue=self.queues['status_updates'], durable=True)
            except Exception:
                pass
            ch.basic_publish(
                exchange='',
                routing_key=self.queues['status_updates'],
                body=update.to_json(),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            try:
                conn.close()
            except Exception:
                pass
        except Exception as e:
            log_debug(f"Failed to send status update: {e}", self.component_name)

    # ========== Bridge Lookups ==========

    def get_bridge_by_stoatchat_channel(self, channel_id: str):
        for name, bridge in self.bridges.items():
            sc = bridge.get_platform('stoatchat')
            if sc and sc.channel_id == channel_id:
                return name, bridge
        return None, None

    # ========== Download Utilities ==========

    async def download_media(self, url: str, filename: Optional[str] = None) -> Optional[str]:
        cache_dir = ensure_cache_dir()
        try:
            timeout = aiohttp.ClientTimeout(total=300)
            headers = {
                'User-Agent': 'StoatchatBridge/1.0',
                'Accept': '*/*'
            }
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                log_debug(f"HTTP GET {url}")
                resp = await session.get(url)
                if resp.status != 200:
                    status = resp.status
                    try:
                        await resp.release()
                    except Exception:
                        pass
                    if status in (403, 404, 429, 500, 502, 503):
                        await asyncio.sleep(0.7)
                        resp = await session.get(url)
                async with resp:
                    if resp.status != 200:
                        log_error(f"Failed to download {url}: HTTP {resp.status}")
                        return None

                    if not filename:
                        filename = os.path.basename(url.split('?')[0]) or f"file_{int(time.time())}"

                    file_path = os.path.join(cache_dir, f"{int(time.time())}_{filename}")

                    with open(file_path, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            f.write(chunk)

                    try:
                        enforce_cache_quota()
                    except Exception:
                        pass
                    return file_path
        except Exception as e:
            log_error(f"Failed to download {url}: {e}")
            return None


if __name__ == '__main__':
    import sys
    _root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if _root not in sys.path:
        sys.path.insert(0, _root)

    from postkeep.stoatchat.stoatchat_scribe import StoatchatScribe
    from postkeep.stoatchat.stoatchat_pilgrim import StoatchatPilgrim

    core = StoatchatCore()

    scribe = StoatchatScribe(core)
    pilgrim = StoatchatPilgrim(core)

    core.client._scribe = scribe
    core.client._pilgrim = pilgrim

    # Startup watchdog: if on_ready doesn't fire within 30s, exit for restart
    def _startup_watchdog():
        time.sleep(30)
        if not core.client._ready_fired:
            log_error("[WATCHDOG] on_ready did not fire within 30s. Exiting for restart.", 'stoatchat')
            core.send_status_update('error', 'Startup watchdog: on_ready timeout')
            os._exit(1)
        else:
            log_info("[WATCHDOG] Startup OK, on_ready fired.", 'stoatchat')

    watchdog = threading.Thread(target=_startup_watchdog, daemon=True)
    watchdog.start()

    core.send_status_update('starting')
    core.client.run()
