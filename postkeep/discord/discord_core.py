import discord
from discord.ext import commands
from discord import Webhook
import asyncio
import pika
import json
import re
import time
import os
import aiohttp
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse
from typing import Optional, Dict, List

import sys
_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _root not in sys.path:
    sys.path.insert(0, _root)

from postkeep.papyrus import (
    log_info, log_error, log_warn, log_success, log_debug,
    load_global_settings, ensure_cache_dir,
    BridgeMessage, StatusUpdate,
    BridgeDatabase, CONFIG_FILE,
    extract_media_urls, extract_tenor_urls, escape_discord_emojis,
    convert_video_to_gif, get_file_type_from_url,
    enforce_cache_quota, should_skip_download, extract_ignore_hint_urls,
    is_user_ignored, add_ignored_user, is_user_admin, get_avatar_db,
    add_downloadable_domain, add_ignored_domain,
    ARBITER_QUEUES, apply_namespace, get_namespace,
    GatewayConfig, load_all_gateways,
)

CITADEL_ROOT = os.path.join(_root, 'citadel')
CODEX_FILE = os.path.join(_root, 'codex.ini')
CONFIG_PATH = CODEX_FILE if os.path.exists(CODEX_FILE) else CONFIG_FILE


class DiscordCore:
    def __init__(self):
        self.component_name = 'discord'
        self.settings = load_global_settings(CONFIG_PATH, 'discord_bot')
        self.enable_history_parsing = bool(self.settings.get('ENABLE_HISTORY_PARSING', False))
        self.tenor_preferred_format = str(self.settings.get('TENOR_PREFERRED_FORMAT', 'gif')).lower()
        self.bridges = self._build_bridges_from_gateways()
        self.bridge_dbs: Dict[str, BridgeDatabase] = {}

        self._init_citadel()

        intents = discord.Intents.default()
        intents.messages = True
        intents.message_content = True
        intents.dm_messages = True
        intents.guild_messages = True
        intents.members = True
        self.bot = commands.Bot(command_prefix="!", intents=intents)

        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self._pub_connection = None
        self._pub_channel = None
        self.consumer_thread = None

        self._namespace = get_namespace() or str(self.settings.get('QUEUE_NAMESPACE', '')).strip()
        self.queues = apply_namespace(ARBITER_QUEUES, self._namespace)

        # Native message timestamps for prefix collapse (set by scribe, read by pilgrim)
        self._last_native_dc_msg = {}  # {bridge_name: timestamp}

        self.connect_rabbitmq()

        self.avatar_upload_channel_id = self.settings.get('DISCORD_AVATAR_UPLOAD_CHANNEL_ID')
        if self.avatar_upload_channel_id:
            self.avatar_upload_channel_id = int(self.avatar_upload_channel_id)
            log_info(f"Avatar upload channel ID: {self.avatar_upload_channel_id}")

        try:
            self.avatar_db = get_avatar_db()
        except Exception:
            self.avatar_db = None

    # ── citadel ─────────────────────────────────────────

    def _init_citadel(self):
        os.makedirs(CITADEL_ROOT, exist_ok=True)
        for name, bridge in self.bridges.items():
            bridge_dir = os.path.join(CITADEL_ROOT, name)
            os.makedirs(bridge_dir, exist_ok=True)
            db_path = bridge.db_path
            self.bridge_dbs[name] = BridgeDatabase(db_path)
            log_info(f"Initialized database for '{name}': {db_path}", self.component_name)

        avatar_db_dir = os.path.join(CITADEL_ROOT, '_avatars')
        os.makedirs(avatar_db_dir, exist_ok=True)

    def _load_gateways(self) -> Dict:
        return load_all_gateways(CITADEL_ROOT)

    def _build_bridges_from_gateways(self) -> Dict:
        gateways = self._load_gateways()
        bridges = {}
        for name, gw in gateways.items():
            ds = gw.get_platform('discord')
            if not ds or not ds.channel_id_int:
                continue
            bridges[name] = gw
        if not bridges:
            log_warn("No citadel gateways with discord platform found", self.component_name)
        return bridges

    # ── rabbitmq ────────────────────────────────────────

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

            log_success(f"Discord connected to RabbitMQ (ns='{self._namespace or 'default'}')")
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
                component='discord',
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

    # ── bridge lookups ──────────────────────────────────

    def get_bridge_by_discord_channel(self, channel_id):
        for name, bridge in self.bridges.items():
            ds = bridge.get_platform('discord')
            if ds and ds.channel_id_int == channel_id:
                return name, bridge
        return None, None

    # ── download utilities ──────────────────────────────

    @staticmethod
    def _is_discord_cdn(url: str) -> bool:
        if not url:
            return False
        url = str(url)
        return ('cdn.discordapp.com' in url) or ('media.discordapp.net' in url) or ('/attachments/' in url)

    async def download_media(self, url: str, filename: Optional[str] = None, referer: Optional[str] = None) -> Optional[str]:
        cache_dir = ensure_cache_dir()
        try:
            timeout = aiohttp.ClientTimeout(total=300)
            headers = {
                'User-Agent': 'DiscordBridge/1.0',
                'Accept': '*/*'
            }
            if referer:
                headers['Referer'] = referer
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                log_debug(f"HTTP GET {url} referer={referer or ''}")

                async def do_request():
                    return await session.get(url)

                resp = await do_request()
                if resp.status != 200:
                    status = resp.status
                    try:
                        await resp.release()
                    except Exception:
                        pass
                    if status in (403, 404, 429, 500, 502, 503):
                        await asyncio.sleep(0.7)
                        resp = await do_request()
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

    async def download_with_recovery(self, url: str, filename: str = None, message=None, **_kwargs):
        """Download media, with a simple fallback to the message's own fresh attachment URLs.
        No history scanning - just tries the message object's attachments if the primary URL fails."""
        try:
            file_path = await self.download_media(url, filename)
            if file_path:
                return file_path
        except Exception:
            pass

        if not self._is_discord_cdn(url) or message is None:
            return None

        m = re.search(r'/attachments/(\d+)/(\d+)/', url)
        attachment_id = m.group(2) if m else None

        try:
            for att in getattr(message, 'attachments', None) or []:
                try:
                    if (str(getattr(att, 'id', '')) == str(attachment_id)) or (getattr(att, 'filename', None) and getattr(att, 'filename') in url):
                        fresh = getattr(att, 'url', None) or getattr(att, 'proxy_url', None)
                        if fresh:
                            log_debug('Found fresh attachment URL from message object; attempting download')
                            fp = await self.download_media(fresh, filename)
                            if fp:
                                return fp
                except Exception:
                    continue
        except Exception:
            pass

        return None

    async def _try_download_candidates(self, candidates: List[str], filename: str, message=None) -> Optional[Dict]:
        for cand in [u for u in candidates if u]:
            log_debug(f"Trying candidate media URL: {cand}")
            referer = None
            original_url_for_policy = None
            try:
                if '/external/' in cand and 'discordapp.net' in cand:
                    parts = cand.split('/external/', 1)[1].split('/', 1)
                    if len(parts) == 2:
                        scheme_and_rest = parts[1]
                        if scheme_and_rest.startswith('http/'):
                            referer = 'http://' + scheme_and_rest[len('http/'):]
                            original_url_for_policy = referer
                        elif scheme_and_rest.startswith('https/'):
                            referer = 'https://' + scheme_and_rest[len('https/'):]
                            original_url_for_policy = referer
            except Exception:
                referer = None

            try:
                check_url = original_url_for_policy or cand
                if should_skip_download(check_url):
                    log_debug(f"Domain policy skip for candidate: {check_url}")
                    continue
            except Exception:
                pass

            if self._is_discord_cdn(cand):
                fp = await self.download_with_recovery(cand, filename, message=message)
            else:
                fp = await self.download_media(cand, filename, referer=referer)

            if fp:
                return {'url': cand, 'path': fp}
        return None

    @staticmethod
    def _sanitize_discord_mentions(message, content: str) -> str:
        try:
            result = content or ''
            id_to_name = {}
            for m in getattr(message, 'mentions', []) or []:
                try:
                    id_to_name[str(m.id)] = '@' + (m.display_name if hasattr(m, 'display_name') and m.display_name else m.name)
                except Exception:
                    continue
            role_to_name = {}
            for r in getattr(message, 'role_mentions', []) or []:
                try:
                    role_to_name[str(r.id)] = '@' + r.name
                except Exception:
                    continue
            ch_to_name = {}
            for ch in getattr(message, 'channel_mentions', []) or []:
                try:
                    ch_to_name[str(ch.id)] = '#' + ch.name
                except Exception:
                    continue

            def repl_user(match):
                return id_to_name.get(match.group(1), '@user')
            def repl_role(match):
                return role_to_name.get(match.group(1), '@role')
            def repl_channel(match):
                return ch_to_name.get(match.group(1), '#channel')

            result = re.sub(r"<@!?(\d+)>", repl_user, result)
            result = re.sub(r"<@&(\d+)>", repl_role, result)
            result = re.sub(r"<#(\d+)>", repl_channel, result)
            return result
        except Exception:
            return content

    def _search_cache_for_filename(self, filename: str) -> Optional[str]:
        try:
            cache_dir = ensure_cache_dir()
            for root, _, files in os.walk(cache_dir):
                if filename in files:
                    return os.path.join(root, filename)
        except Exception:
            pass
        return None

    # ── run ─────────────────────────────────────────────

    async def run(self):
        self.send_status_update('starting')

        loop = asyncio.get_running_loop()
        if hasattr(self, '_pilgrim_ref') and self._pilgrim_ref:
            self._pilgrim_ref._loop = loop

        try:
            await self.bot.start(self.settings['DISCORD_TOKEN'])
        except Exception as e:
            log_error(f"Discord bot error: {e}")
            self.send_status_update('error', str(e))
        finally:
            self.send_status_update('stopped')


if __name__ == '__main__':
    import sys
    _root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if _root not in sys.path:
        sys.path.insert(0, _root)

    from postkeep.discord.discord_scribe import DiscordScribe
    from postkeep.discord.discord_pilgrim import DiscordPilgrim

    core = DiscordCore()

    scribe = DiscordScribe(core)
    pilgrim = DiscordPilgrim(core)

    core._pilgrim_ref = pilgrim
    core._scribe_ref = scribe

    scribe.setup_handlers()

    import threading
    if core.rabbitmq_channel and not core.rabbitmq_channel.is_closed:
        core.consumer_thread = threading.Thread(target=pilgrim.run_consumers, daemon=True)
        core.consumer_thread.start()

    asyncio.run(core.run())
