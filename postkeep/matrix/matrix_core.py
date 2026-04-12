import asyncio
import pika
import time
import os
import aiohttp
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
    enforce_cache_quota,
    ARBITER_QUEUES, apply_namespace, get_namespace,
    GatewayConfig, load_all_gateways,
    get_avatar_db,
)

CITADEL_ROOT = os.path.join(_root, 'citadel')
CODEX_FILE = os.path.join(_root, 'codex.ini')
CONFIG_PATH = CODEX_FILE if os.path.exists(CODEX_FILE) else CONFIG_FILE


class MatrixCore:
    def __init__(self):
        self.component_name = 'matrix'
        self.settings = load_global_settings(CONFIG_PATH, 'matrix')

        # ── Determine mode ──
        if self.settings.get('MATRIX_AS_TOKEN'):
            self.mode = 'appservice'
        elif self.settings.get('MATRIX_BOT_TOKEN'):
            self.mode = 'bot'
        else:
            raise RuntimeError("No Matrix credentials configured (need MATRIX_AS_TOKEN or MATRIX_BOT_TOKEN)")

        log_info(f"Matrix mode: {self.mode}")

        self.bridges = self._build_bridges_from_gateways()
        self.bridge_dbs: Dict[str, BridgeDatabase] = {}
        self._init_citadel()

        # ── RabbitMQ ──
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self._pub_connection = None
        self._pub_channel = None
        self.consumer_thread = None

        self._namespace = get_namespace() or str(self.settings.get('QUEUE_NAMESPACE', '')).strip()
        self.queues = apply_namespace(ARBITER_QUEUES, self._namespace)

        # Native message timestamps for prefix collapse (set by scribe, read by pilgrim)
        self._last_native_mx_msg = {}  # {bridge_name: timestamp}

        self.connect_rabbitmq()

        # ── Avatar DB ──
        try:
            self.avatar_db = get_avatar_db()
        except Exception:
            self.avatar_db = None

        # ── Matrix client objects (set during run) ──
        self.appservice = None  # AS mode: mautrix AppService
        self.client = None      # Bot mode: mautrix Client
        self.main_loop = None   # asyncio event loop

    # ========== Citadel ==========

    def _init_citadel(self):
        os.makedirs(CITADEL_ROOT, exist_ok=True)
        for name, bridge in self.bridges.items():
            bridge_dir = os.path.join(CITADEL_ROOT, name)
            os.makedirs(bridge_dir, exist_ok=True)
            db_path = bridge.db_path
            self.bridge_dbs[name] = BridgeDatabase(db_path)
            log_info(f"Initialized database for '{name}': {db_path}", self.component_name)

    def _build_bridges_from_gateways(self) -> Dict:
        gateways = load_all_gateways(CITADEL_ROOT)
        bridges = {}
        for name, gw in gateways.items():
            mx = gw.get_platform('matrix')
            if not mx or not mx.channel_id:
                continue
            bridges[name] = gw
        if not bridges:
            log_warn("No citadel gateways with matrix platform found", self.component_name)
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
                blocked_connection_timeout=30
            )
            self.rabbitmq_connection = pika.BlockingConnection(parameters)
            self.rabbitmq_channel = self.rabbitmq_connection.channel()

            all_queues = set(self.queues.values())
            for qn in all_queues:
                self.rabbitmq_channel.queue_declare(queue=qn, durable=True)

            log_success(f"Matrix connected to RabbitMQ (ns='{self._namespace or 'default'}')")
        except Exception as e:
            log_error(f"Failed to connect to RabbitMQ: {e}")

    def _new_rabbitmq_connection(self):
        credentials = pika.PlainCredentials(
            self.settings['RABBITMQ_USER'],
            self.settings['RABBITMQ_PASS']
        )
        params = pika.ConnectionParameters(
            host=self.settings['RABBITMQ_HOST'],
            port=self.settings['RABBITMQ_PORT'],
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=30
        )
        return pika.BlockingConnection(params)

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
        self._pub_connection = self._new_rabbitmq_connection()
        self._pub_channel = self._pub_connection.channel()

    def send_status_update(self, status: str, message: Optional[str] = None):
        try:
            msg = message
            if self._namespace:
                msg = f"{message or ''} [ns={self._namespace}]".strip()
            update = StatusUpdate(
                component='matrix',
                status=status,
                message=msg,
                timestamp=time.time()
            )
            conn = self._new_rabbitmq_connection()
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

    def get_bridge_by_matrix_room(self, room_id: str):
        for name, bridge in self.bridges.items():
            mx = bridge.get_platform('matrix')
            if mx and mx.channel_id == room_id:
                return name, bridge
        return None, None

    # ========== Download Utilities ==========

    async def download_media(self, url: str, filename: Optional[str] = None) -> Optional[str]:
        cache_dir = ensure_cache_dir()
        try:
            timeout = aiohttp.ClientTimeout(total=300)
            headers = {'User-Agent': 'MatrixBridge/1.0', 'Accept': '*/*'}
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                resp = await session.get(url)
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


async def _run_bot_mode(core):
    """Run Matrix in bot mode using mautrix Client with sync loop."""
    from mautrix.client import Client
    from mautrix.api import HTTPAPI

    from postkeep.matrix.matrix_scribe import MatrixScribe
    from postkeep.matrix.matrix_pilgrim import MatrixPilgrim

    homeserver = core.settings['MATRIX_HOMESERVER_URL'].rstrip('/')
    token = core.settings['MATRIX_BOT_TOKEN']
    bot_user = core.settings.get('MATRIX_BOT_USER', '')

    api = HTTPAPI(base_url=homeserver, token=token)
    core.main_loop = asyncio.get_event_loop()

    # If bot_user not configured, resolve via raw API call
    if not bot_user:
        try:
            async with aiohttp.ClientSession() as session:
                headers = {'Authorization': f'Bearer {token}'}
                async with session.get(f"{homeserver}/_matrix/client/v3/account/whoami", headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        bot_user = data.get('user_id', '')
                    else:
                        log_error(f"whoami failed: HTTP {resp.status}")
                        return
            if not bot_user:
                log_error("Could not resolve bot user identity")
                return
            core.settings['MATRIX_BOT_USER'] = bot_user
            log_info(f"Resolved bot user: {bot_user}")
        except Exception as e:
            log_error(f"Failed to resolve bot user identity: {e}")
            return

    core.client = Client(mxid=bot_user, api=api)

    scribe = MatrixScribe(core)
    pilgrim = MatrixPilgrim(core)

    # Register scribe event handlers on the sync client
    scribe.setup_bot_handlers(core.client)

    # Start pilgrim consumer thread
    pilgrim.start()

    # Auto-join all configured rooms
    for name, bridge in core.bridges.items():
        mx_cfg = bridge.get_platform('matrix')
        if mx_cfg and mx_cfg.channel_id:
            try:
                await core.client.join_room(mx_cfg.channel_id)
                log_info(f"Joined room {mx_cfg.channel_id} (bridge: {name})")
            except Exception as e:
                log_warn(f"Could not join room {mx_cfg.channel_id}: {e} (make sure the bot is invited)")

    core.send_status_update('ready')
    log_success(f"Matrix bot mode ready (user={bot_user})")

    # Start sync loop (this blocks until shutdown)
    try:
        await core.client.start(None)
    except Exception as e:
        log_error(f"Matrix sync loop crashed: {e}")
        core.send_status_update('error', str(e))


async def _run_appservice_mode(core):
    """Run Matrix in AS mode (ghost puppeteering). Stub for future implementation."""
    log_warn("AS mode is not yet implemented. Falling back to error state.")
    core.send_status_update('error', 'AS mode not yet implemented')


if __name__ == '__main__':
    core = MatrixCore()
    core.send_status_update('starting')

    if core.mode == 'bot':
        asyncio.run(_run_bot_mode(core))
    elif core.mode == 'appservice':
        asyncio.run(_run_appservice_mode(core))
