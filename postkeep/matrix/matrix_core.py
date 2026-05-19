import asyncio
import json
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
    CITADEL_GLOBAL_ROOT,
)

CITADEL_ROOT = os.path.join(_root, 'citadel')
CODEX_FILE = os.path.join(_root, 'codex.ini')
CONFIG_PATH = CODEX_FILE if os.path.exists(CODEX_FILE) else CONFIG_FILE

MATRIX_SESSION_FILE = os.path.join(CITADEL_GLOBAL_ROOT, 'matrix_session.json')
# Refresh the access token this many seconds before its actual expiry, so we never
# present an expired token mid-sync. MAS access tokens are typically valid 5 min - 24 h.
MATRIX_REFRESH_LEEWAY_SEC = 60


class MatrixCore:
    def __init__(self):
        self.component_name = 'matrix'
        self.settings = load_global_settings(CONFIG_PATH, 'matrix')

        # ── Determine mode ──
        if self.settings.get('MATRIX_AS_TOKEN'):
            self.mode = 'appservice'
        elif (self.settings.get('MATRIX_BOT_TOKEN')
              or self.settings.get('MATRIX_BOT_PASSWORD')
              or os.path.exists(MATRIX_SESSION_FILE)):
            self.mode = 'bot'
        else:
            raise RuntimeError(
                "No Matrix credentials configured (need MATRIX_AS_TOKEN, "
                "or MATRIX_BOT_TOKEN, or MATRIX_BOT_USERNAME + MATRIX_BOT_PASSWORD)"
            )

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


def _load_matrix_session() -> Optional[dict]:
    if not os.path.exists(MATRIX_SESSION_FILE):
        return None
    try:
        with open(MATRIX_SESSION_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        log_warn(f"Failed to load matrix session file ({e}), will re-login")
        return None


def _save_matrix_session(session: dict) -> None:
    os.makedirs(CITADEL_GLOBAL_ROOT, exist_ok=True)
    tmp = MATRIX_SESSION_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(session, f, indent=2)
    os.replace(tmp, MATRIX_SESSION_FILE)


async def _matrix_login(homeserver: str, username: str, password: str,
                        device_id: Optional[str] = None) -> dict:
    """Password login against the homeserver. Requests a refresh token so we can
    survive MAS-style access-token expiry."""
    url = f"{homeserver}/_matrix/client/v3/login"
    body = {
        'type': 'm.login.password',
        'identifier': {'type': 'm.id.user', 'user': username},
        'password': password,
        'refresh_token': True,
        'initial_device_display_name': 'CHATAFT Bridge',
    }
    if device_id:
        body['device_id'] = device_id
    async with aiohttp.ClientSession() as s:
        async with s.post(url, json=body) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"Matrix login failed: HTTP {resp.status}: {text}")
            data = json.loads(text)
    now = time.time()
    expires_in_ms = data.get('expires_in_ms')
    expires_at = now + (expires_in_ms / 1000.0) if expires_in_ms else 0.0
    return {
        'homeserver': homeserver,
        'user_id': data.get('user_id', ''),
        'device_id': data.get('device_id', ''),
        'access_token': data.get('access_token', ''),
        'refresh_token': data.get('refresh_token', ''),
        'expires_at': expires_at,
    }


async def _matrix_refresh(session: dict) -> dict:
    """Trade the refresh token for a new access token (and a rotated refresh token)."""
    homeserver = session['homeserver']
    url = f"{homeserver}/_matrix/client/v3/refresh"
    body = {'refresh_token': session['refresh_token']}
    async with aiohttp.ClientSession() as s:
        async with s.post(url, json=body) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"Matrix refresh failed: HTTP {resp.status}: {text}")
            data = json.loads(text)
    now = time.time()
    expires_in_ms = data.get('expires_in_ms')
    expires_at = now + (expires_in_ms / 1000.0) if expires_in_ms else 0.0
    return {
        **session,
        'access_token': data.get('access_token', session['access_token']),
        # Refresh token rotation: if the server sends a new one, use it; else keep the old.
        'refresh_token': data.get('refresh_token', session['refresh_token']),
        'expires_at': expires_at,
    }


async def _ensure_matrix_session(core, homeserver: str) -> dict:
    """Return a usable session, refreshing or re-logging-in as needed."""
    username = (core.settings.get('MATRIX_BOT_USERNAME') or '').strip()
    password = (core.settings.get('MATRIX_BOT_PASSWORD') or '').strip()
    bootstrap_token = (core.settings.get('MATRIX_BOT_TOKEN') or '').strip()
    configured_user = (core.settings.get('MATRIX_BOT_USER') or '').strip()

    session = _load_matrix_session()

    # If we have a saved session, try to use / refresh it.
    if session and session.get('access_token'):
        now = time.time()
        expires_at = session.get('expires_at') or 0
        if expires_at and (expires_at - now) < MATRIX_REFRESH_LEEWAY_SEC and session.get('refresh_token'):
            try:
                session = await _matrix_refresh(session)
                _save_matrix_session(session)
                log_info("Refreshed Matrix access token from saved session")
            except Exception as e:
                log_warn(f"Matrix token refresh failed ({e}), falling back to password login")
                session = None
        else:
            log_info("Using saved Matrix session")

    # No usable session - log in fresh.
    if not session:
        if username and password:
            session = await _matrix_login(homeserver, username, password)
            _save_matrix_session(session)
            log_success(f"Matrix password login succeeded (user={session.get('user_id')}, device={session.get('device_id')})")
        elif bootstrap_token:
            log_warn("No saved session and no password configured - using MATRIX_BOT_TOKEN as a one-shot. Token will not auto-refresh.")
            session = {
                'homeserver': homeserver,
                'user_id': configured_user,
                'device_id': '',
                'access_token': bootstrap_token,
                'refresh_token': '',
                'expires_at': 0.0,
            }
        else:
            raise RuntimeError("No Matrix credentials available (need username+password or a bot token)")

    return session


async def _token_refresher(core, homeserver: str):
    """Background task: refresh the access token before it expires, in a loop."""
    while True:
        try:
            session = _load_matrix_session()
            if not session or not session.get('refresh_token'):
                # Nothing to refresh (either a non-expiring token, or we lost the file).
                # Sleep an hour and re-check.
                await asyncio.sleep(3600)
                continue

            expires_at = session.get('expires_at') or 0
            if not expires_at:
                # Server didn't tell us an expiry. Try refresh on a conservative cadence.
                await asyncio.sleep(3600)
                refresh_now = True
            else:
                wait = max(15.0, (expires_at - time.time()) - MATRIX_REFRESH_LEEWAY_SEC)
                await asyncio.sleep(wait)
                refresh_now = True

            if refresh_now:
                try:
                    new_session = await _matrix_refresh(session)
                    _save_matrix_session(new_session)
                    if core.client and getattr(core.client, 'api', None):
                        core.client.api.token = new_session['access_token']
                    log_info("Matrix access token refreshed")
                except Exception as e:
                    log_error(f"Matrix token refresh failed in background loop: {e}. Retrying in 60s.")
                    await asyncio.sleep(60)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log_error(f"Matrix refresher loop hiccup: {e}")
            await asyncio.sleep(60)


async def _run_bot_mode(core):
    """Run Matrix in bot mode using mautrix Client with sync loop."""
    from mautrix.client import Client
    from mautrix.api import HTTPAPI

    from postkeep.matrix.matrix_scribe import MatrixScribe
    from postkeep.matrix.matrix_pilgrim import MatrixPilgrim

    homeserver = core.settings['MATRIX_HOMESERVER_URL'].rstrip('/')
    core.main_loop = asyncio.get_event_loop()

    # ── Acquire a valid session (login / refresh / cached) ──
    try:
        session = await _ensure_matrix_session(core, homeserver)
    except Exception as e:
        log_error(f"Could not establish Matrix session: {e}")
        core.send_status_update('error', str(e))
        return

    token = session['access_token']
    bot_user = session.get('user_id') or core.settings.get('MATRIX_BOT_USER', '')

    api = HTTPAPI(base_url=homeserver, token=token)

    # Resolve bot_user if still missing (e.g. bootstrap-token path with no MATRIX_BOT_USER set)
    if not bot_user:
        try:
            async with aiohttp.ClientSession() as s:
                headers = {'Authorization': f'Bearer {token}'}
                async with s.get(f"{homeserver}/_matrix/client/v3/account/whoami", headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        bot_user = data.get('user_id', '')
                    else:
                        log_error(f"whoami failed: HTTP {resp.status}")
                        return
            if not bot_user:
                log_error("Could not resolve bot user identity")
                return
        except Exception as e:
            log_error(f"Failed to resolve bot user identity: {e}")
            return

    core.settings['MATRIX_BOT_USER'] = bot_user
    log_info(f"Resolved bot user: {bot_user}")

    core.client = Client(mxid=bot_user, api=api)

    scribe = MatrixScribe(core)
    pilgrim = MatrixPilgrim(core)
    scribe.setup_bot_handlers(core.client)
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

    # Background token refresher
    refresher_task = asyncio.create_task(_token_refresher(core, homeserver))

    core.send_status_update('ready')
    log_success(f"Matrix bot mode ready (user={bot_user})")

    try:
        await core.client.start(None)
    except Exception as e:
        log_error(f"Matrix sync loop crashed: {e}")
        core.send_status_update('error', str(e))
    finally:
        refresher_task.cancel()


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
