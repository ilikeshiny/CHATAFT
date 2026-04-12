import asyncio
import inspect
import pika
import json
import time
import os
import sys
import threading
import uuid
from typing import Optional, Dict

_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _root not in sys.path:
    sys.path.insert(0, _root)

from telegram import Bot
from telegram.ext import ApplicationBuilder

from postkeep.papyrus import (
    ARBITER_QUEUES, apply_namespace, get_namespace, log_info, 
    log_error, log_warn, log_success, log_debug,
    load_global_settings,
    BridgeDatabase, StatusUpdate, CONFIG_FILE,
    get_avatar_db, CITADEL_ROOT,
    GatewayConfig, load_all_gateways,
)

CODEX_FILE = os.path.join(_root, 'codex.ini')
CONFIG_PATH = CODEX_FILE if os.path.exists(CODEX_FILE) else CONFIG_FILE


class TelegramCore:
    def __init__(self):
        self.component_name = 'telegram'
        self.settings = load_global_settings(CONFIG_PATH)
        self.bridges = load_all_gateways()
        self.bridge_dbs: Dict[str, BridgeDatabase] = {}
        self.avatar_db = get_avatar_db()

        if not self.bridges:
            log_warn("No citadel gateways found; telegram has no bridges configured.")

        for name, bridge in self.bridges.items():
            tg = bridge.get_platform('telegram')
            if not tg:
                continue
            db_path = bridge.db_path
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            self.bridge_dbs[name] = BridgeDatabase(db_path)
            log_info(f"Initialized database for {name}: {db_path}")

        self.bot = Bot(token=self.settings['TELEGRAM_BOT_TOKEN'])
        self.app = ApplicationBuilder().token(self.settings['TELEGRAM_BOT_TOKEN']).build()

        self._namespace = get_namespace() or str(self.settings.get('QUEUE_NAMESPACE', '')).strip()
        self._queues_namespaced = False
        self._queues = apply_namespace(ARBITER_QUEUES, self._namespace) if self._namespace else dict(ARBITER_QUEUES)
        self._apply_namespace()

        self.main_loop = None

        # Shared state for prefix collapsing: scribe writes, pilgrim reads
        self._last_native_tg_msg = {}   # {bridge_name: timestamp}

        from postkeep.telegram.telegram_scribe import TelegramScribe
        from postkeep.telegram.telegram_pilgrim import TelegramPilgrim

        self.scribe = TelegramScribe(self)
        self.pilgrim = TelegramPilgrim(self)

    def _apply_namespace(self):
        ns = self._namespace
        if ns and not self._queues_namespaced:
            self._queues_namespaced = True
            log_info(f"Applied queue namespace: {ns}")

    def _trace(self, message: str):
        log_debug(f"[TRACE] {message}", self.component_name)

    # ===== RabbitMQ Helpers =====

    def _new_rabbitmq_connection(self, heartbeat=60, timeout=120):
        credentials = pika.PlainCredentials(
            self.settings['RABBITMQ_USER'],
            self.settings['RABBITMQ_PASS']
        )
        parameters = pika.ConnectionParameters(
            host=self.settings['RABBITMQ_HOST'],
            port=self.settings['RABBITMQ_PORT'],
            credentials=credentials,
            heartbeat=heartbeat,
            blocked_connection_timeout=timeout,
            socket_timeout=10,
        )
        return pika.BlockingConnection(parameters)

    # ===== Status =====

    def send_status_update(self, status: str, message: Optional[str] = None):
        try:
            self._apply_namespace()
            msg = message
            if self._namespace:
                msg = f"{message or ''} [ns={self._namespace}]".strip()
            update = StatusUpdate(
                component=self.component_name,
                status=status,
                message=msg,
                timestamp=time.time()
            )
            conn = self._new_rabbitmq_connection(heartbeat=60, timeout=120)
            ch = conn.channel()
            try:
                ch.queue_declare(queue=self._queues['status_updates'], durable=True)
            except Exception:
                pass
            ch.basic_publish(
                exchange='',
                routing_key=self._queues['status_updates'],
                body=update.to_json(),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            try:
                conn.close()
            except Exception:
                pass
            log_info(f"Status update sent: {status}{' (' + msg + ')' if msg else ''}")
        except Exception as e:
            log_error(f"Failed to send status update: {e}")

    # ===== Config Reload =====

    def reload_config(self):
        try:
            new_settings = load_global_settings(CONFIG_PATH)
            new_bridges = load_all_gateways()

            for name, bridge in new_bridges.items():
                if name not in self.bridge_dbs:
                    db_path = bridge.db_path
                    os.makedirs(os.path.dirname(db_path), exist_ok=True)
                    self.bridge_dbs[name] = BridgeDatabase(db_path)
                    log_info(f"Initialized database for {name}")
            for name in list(self.bridge_dbs.keys()):
                if name not in new_bridges:
                    try:
                        self.bridge_dbs[name].conn.close()
                    except Exception:
                        pass
                    del self.bridge_dbs[name]
                    log_warn(f"Bridge removed on reload: {name}")

            self.settings = new_settings
            self.bridges = new_bridges
            log_success('Telegram config reloaded')
        except Exception as e:
            log_error(f"Failed to reload Telegram config: {e}")

    # ===== Main Loop =====

    async def run(self):
        self.send_status_update('starting')

        try:
            self.main_loop = asyncio.get_running_loop()
        except RuntimeError:
            log_error("Could not get running loop; async callbacks may fail.")
            self.main_loop = None

        self.scribe.setup_handlers()
        self.pilgrim.start()

        try:
            me = await self.bot.get_me()
            log_success(f"Telegram bot logged in as @{getattr(me, 'username', None) or me.id}")
        except Exception as e:
            log_error(f"Telegram Bot token validation failed: {e}")
            self.send_status_update('error', f"token_invalid: {e}")
            return

        try:
            rp = getattr(self.app, 'run_polling', None)
            if rp and inspect.iscoroutinefunction(rp):
                self.send_status_update('ready')
                log_info("Starting Telegram bot polling (run_polling async)...")
                await rp()
                return

            await self.app.initialize()
            await self.app.start()

            self.send_status_update('ready')
            log_info("Starting Telegram bot polling...")

            upd = getattr(self.app, 'updater', None)
            if not upd or not hasattr(upd, 'start_polling'):
                log_error("No available polling method on this PTB version.")
                self.send_status_update('error', 'polling_unavailable')
                return

            if inspect.iscoroutinefunction(upd.start_polling):
                await upd.start_polling()
            else:
                upd.start_polling()

            wait_closed = getattr(self.app, 'wait_closed', None)
            if wait_closed and inspect.iscoroutinefunction(wait_closed):
                await wait_closed()
            else:
                while True:
                    await asyncio.sleep(1.0)

        except Exception as e:
            log_error(f"Telegram bot error: {e}")
            self.send_status_update('error', str(e))
        finally:
            try:
                upd = getattr(self.app, 'updater', None)
                if upd and hasattr(upd, 'stop'):
                    if inspect.iscoroutinefunction(upd.stop):
                        await upd.stop()
                    else:
                        upd.stop()
            except Exception:
                pass
            try:
                await self.app.stop()
            except Exception:
                pass
            try:
                await self.app.shutdown()
            except Exception:
                pass
            self.send_status_update('stopped')


if __name__ == '__main__':
    core = TelegramCore()
    asyncio.run(core.run())
