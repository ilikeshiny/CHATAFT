import os
import re
import sys
import json
import time
import uuid
import sqlite3
import asyncio
import hashlib
import logging
import configparser
from dataclasses import dataclass, asdict, fields
from typing import Optional, List, Dict, Tuple
from urllib.parse import urlparse, unquote
from colorama import Fore, Style

if sys.platform == 'win32':
    # On Windows, colorama must wrap stdout to convert ANSI codes to Win32 calls.
    from colorama import init as _colorama_init
    _colorama_init(autoreset=True, convert=True)
# On Linux/macOS, ANSI codes work natively - skip colorama.init() entirely
# to avoid its AnsiToWin32 stdout wrapper (which breaks Rich and cursor ops).

if sys.platform == 'win32':
    if sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
    if sys.stderr.encoding != 'utf-8':
        sys.stderr.reconfigure(encoding='utf-8')

# ── path anchoring ───────────────────────────────────

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(_THIS_DIR)
CITADEL_ROOT = os.path.join(PROJECT_ROOT, 'citadel')
CODEX_FILE = os.path.join(PROJECT_ROOT, 'codex.ini')
LEGACY_CONFIG_FILE = os.path.join(PROJECT_ROOT, 'config.ini')
CONFIG_FILE = CODEX_FILE if os.path.exists(CODEX_FILE) else LEGACY_CONFIG_FILE

IGNORED_USERS_FILE = os.path.join(PROJECT_ROOT, 'ignored_users.txt')
ADMINS_FILE = os.path.join(PROJECT_ROOT, 'admins.txt')
DOWNLOAD_DOMAINS_FILE = os.path.join(PROJECT_ROOT, 'download_domains.txt')
IGNORED_DOMAINS_FILE = os.path.join(PROJECT_ROOT, 'ignored_domains.txt')

# ── queue definitions ────────────────────────────────

ARBITER_QUEUES = {
    'scribe_discord':           'scribe_discord',
    'scribe_telegram':          'scribe_telegram',
    'scribe_stoatchat':         'scribe_stoatchat',
    'scribe_matrix':            'scribe_matrix',
    'pilgrim_discord':          'pilgrim_discord',
    'pilgrim_telegram':         'pilgrim_telegram',
    'pilgrim_stoatchat':        'pilgrim_stoatchat',
    'pilgrim_matrix':           'pilgrim_matrix',
    'status_updates':           'status_updates',
}

# ── namespace helpers ────────────────────────────────

def apply_namespace(queues: dict, namespace: str) -> dict:
    if not namespace:
        return dict(queues)
    ns = namespace.strip().rstrip('_')
    return {k: f"{ns}_{v}" for k, v in queues.items()}


def get_namespace():
    return (os.environ.get('BRIDGE_NS') or '').strip()

# ── direction helpers ────────────────────────────────

PLATFORM_ABBREV = {'discord': 'dc', 'telegram': 'tg', 'stoatchat': 'sc', 'matrix': 'mx'}


def make_direction(source: str, target: str) -> str:
    src = PLATFORM_ABBREV.get(source, source[:2])
    tgt = PLATFORM_ABBREV.get(target, target[:2])
    return f"{src}2{tgt}"

# ── logging ──────────────────────────────────────────

LOG_LEVEL = logging.DEBUG
_log_level_set = False
_initialization_messages = []
_initialization_complete = False
_initialization_phase = True
_initialization_start = time.time()
_INIT_PHASE_MAX_SECONDS = 30

# ── file logger (rotating, in citadel/_logs/) ────────
_file_logger = None


def _get_file_logger():
    global _file_logger
    if _file_logger is not None:
        return _file_logger
    try:
        from logging.handlers import RotatingFileHandler
        logs_dir = os.path.join(CITADEL_ROOT, '_logs')
        os.makedirs(logs_dir, exist_ok=True)
        log_path = os.path.join(logs_dir, 'bridge.log')
        handler = RotatingFileHandler(
            log_path, maxBytes=5 * 1024 * 1024, backupCount=5, encoding='utf-8',
        )
        handler.setFormatter(logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
        ))
        logger = logging.getLogger('chataft_file')
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.propagate = False
        _file_logger = logger
    except Exception:
        _file_logger = False  # Mark as failed, don't retry
    return _file_logger


def _log_to_file(level: str, msg: str):
    logger = _get_file_logger()
    if not logger:
        return
    try:
        log_fn = getattr(logger, level, logger.info)
        log_fn(msg)
    except Exception:
        pass


def _is_init_phase():
    global _initialization_phase
    if not _initialization_phase:
        return False
    import time as _t
    if _t.time() - _initialization_start > _INIT_PHASE_MAX_SECONDS:
        _initialization_phase = False
        return False
    return True


def set_log_level(level_str: str):
    global LOG_LEVEL, _log_level_set
    if _log_level_set:
        return
    levels = {
        'DEBUG':   logging.DEBUG,
        'INFO':    logging.INFO,
        'SUCCESS': logging.INFO + 1,
        'WARNING': logging.WARNING,
        'ERROR':   logging.ERROR,
    }
    LOG_LEVEL = levels.get(level_str.upper(), logging.INFO)
    _log_level_set = True



def log_debug(msg, component=None):
    if LOG_LEVEL > logging.DEBUG:
        return
    _log_to_file('debug', msg)
    if component and _is_init_phase():
        _initialization_messages.append(('debug', component, msg))
    else:
        print(f"{Fore.WHITE}[DEBUG] {msg}{Style.RESET_ALL}", flush=True)
        sys.stdout.flush()


def log_info(msg, component=None):
    if LOG_LEVEL > logging.INFO:
        return
    _log_to_file('info', msg)
    if component and _is_init_phase():
        _initialization_messages.append(('info', component, msg))
    else:
        print(f"{Fore.CYAN}[INFO] {msg}{Style.RESET_ALL}", flush=True)
        sys.stdout.flush()


def log_warn(msg, component=None):
    if LOG_LEVEL > logging.WARNING:
        return
    _log_to_file('warning', msg)
    if component and _is_init_phase():
        _initialization_messages.append(('warn', component, msg))
    else:
        print(f"{Fore.YELLOW}[WARN] {msg}{Style.RESET_ALL}", flush=True)
        sys.stdout.flush()


def log_error(msg, component=None):
    _log_to_file('error', msg)
    if component and _is_init_phase():
        _initialization_messages.append(('error', component, msg))
    else:
        print(f"{Fore.RED}[ERROR] {msg}{Style.RESET_ALL}", flush=True)
        sys.stdout.flush()


def log_success(msg, component=None):
    if LOG_LEVEL > logging.INFO + 1:
        return
    _log_to_file('info', f"[OK] {msg}")
    if component and _is_init_phase():
        _initialization_messages.append(('success', component, msg))
    else:
        print(f"{Fore.GREEN}[OK] {msg}{Style.RESET_ALL}", flush=True)
        sys.stdout.flush()



# ── media patterns ───────────────────────────────────

MEDIA_URL_PATTERNS = [
    r"https?://[^\s]+?\.(?:jpg|jpeg|png|gif|webp|mp4|webm|mov|avi|mkv)(?:\?[^\s]*)?"
]
TENOR_URL_PATTERN = r'https://tenor\.com/view/[^\s]+'
DISCORD_CUSTOM_EMOJI_PATTERN = r'<(a?):(\w+):(\d+)>'

# ── dataclasses ──────────────────────────────────────

@dataclass
class BridgeMessage:
    bridge_name: str
    message_id: str
    channel_id: str
    author_name: str
    author_id: Optional[str]
    content: Optional[str]
    attachments: List[Dict]
    reply_to_id: Optional[str]
    is_forward: bool
    forward_from: Optional[str]
    timestamp: float
    source: str
    channel_name: Optional[str] = None
    poll: Optional[Dict] = None
    metadata: Optional[Dict] = None
    extra: Optional[Dict] = None
    reply_metadata: Optional[Dict] = None

    def to_json(self):
        try:
            return json.dumps(asdict(self))
        except Exception as e:
            log_error(
                f"BridgeMessage serialization failed "
                f"(bridge={self.bridge_name}, message_id={self.message_id}): {e}"
            )
            raise

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        allowed = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in allowed}
        instance = cls(**filtered)
        unknown = {k: v for k, v in data.items() if k not in allowed}
        if unknown:
            try:
                setattr(instance, '_unknown_fields', unknown)
            except Exception:
                pass
        return instance


@dataclass
class StatusUpdate:
    component: str
    status: str
    message: Optional[str]
    timestamp: float

    def to_json(self):
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str):
        return cls(**json.loads(json_str))

# ── gateway configuration ────────────────────────────

_BOOL_TRUE_SET = {'true', '1', 'yes', 'on'}

def _gw_bool(val, default=False):
    if val is None:
        return default
    return str(val).strip().lower() in _BOOL_TRUE_SET

def _gw_int(val, default=0):
    if val is None:
        return default
    try:
        v = str(val).strip()
        if v.lower() in ('', 'none', 'null'):
            return default
        return int(v)
    except (ValueError, TypeError):
        return default


class PlatformSection:
    def __init__(self, platform: str, data: dict):
        self.platform = platform
        self.channel_id = data.get('channel_id', '')
        self.mode = data.get('mode', 'both').lower()
        self.use_prefixes = _gw_bool(data.get('use_prefixes'), True)
        self.add_filenames = _gw_bool(data.get('add_filenames'), True)
        self.add_channel_name = _gw_bool(data.get('add_channel_name'), False)
        self.collapse_prefixes = _gw_bool(data.get('collapse_prefixes'), False)
        self.ignore_text = _gw_bool(data.get('ignore_text'), False)
        self.ignore_bots = _gw_bool(data.get('ignore_bots'), True)
        self.forward_member_events = _gw_bool(data.get('forward_member_events'), False)
        self.file_size_notifications = _gw_bool(data.get('file_size_notifications'), True)

        self.cross_edit = _gw_bool(data.get('cross_edit'), True)
        self.cross_delete = _gw_bool(data.get('cross_delete'), True)

        self.import_start = (data.get('import_start', '') or '').strip() or None
        self.import_end = (data.get('import_end', '') or '').strip() or None
        self.import_receiver = _gw_bool(data.get('import_receiver'), False)

        self.listener = (data.get('listener', '') or 'bot').strip().lower()

        if platform == 'telegram':
            raw_topic = data.get('topic_id', '') or ''
            if raw_topic.strip() and raw_topic.strip().lower() not in ('none', '0', ''):
                try:
                    self.topic_id = int(raw_topic)
                except ValueError:
                    self.topic_id = None
            else:
                self.topic_id = None
            self.topic_label = data.get('topic_label', '') or ''

            # parse_channel_signatures: 0/false=off, 1/true=resolve admin avatars,
            #   2=also use channel pfp for bot signatures
            raw_sig = (data.get('parse_channel_signatures', 'true') or 'true').strip().lower()
            if raw_sig in ('false', 'no', 'off', '0', 'disabled'):
                self.parse_channel_signatures = 0
            elif raw_sig in ('2', 'mode2'):
                self.parse_channel_signatures = 2
            else:
                self.parse_channel_signatures = 1

        if platform == 'discord':
            self.use_webhooks = _gw_bool(data.get('use_webhooks'), False)
            self.webhook_url = data.get('webhook_url', '') or ''
            self.ignore_webhooks = _gw_bool(data.get('ignore_webhooks'), True)
            self.webhook_bypass_reply = _gw_bool(data.get('webhook_bypass_reply'), False)
            self.webhook_add_channel_in_name = _gw_bool(data.get('webhook_add_channel_in_name'), False)
            self.webhook_forward_in_name = _gw_bool(data.get('webhook_forward_in_name'), False)
            self.webhook_show_forwards = _gw_bool(data.get('webhook_show_forwards'), True)

        if platform == 'stoatchat':
            self.use_masquerade = _gw_bool(data.get('use_masquerade'), True)
            self.masquerade_colour = (data.get('masquerade_colour', '') or '').strip() or None
            self.stoatchat_api_url = (data.get('api_url', '') or 'https://stoat.chat/api').strip()
            self.ignore_self = _gw_bool(data.get('ignore_self'), True)

        if platform == 'matrix':
            self.use_ghosts = _gw_bool(data.get('use_ghosts'), True)
            self.room_alias = data.get('room_alias', '')

    @property
    def channel_id_int(self):
        return _gw_int(self.channel_id)

    @property
    def can_send(self):
        return self.mode in ('send', 'both')

    @property
    def can_receive(self):
        return self.mode in ('receive', 'both')


class GatewayConfig:
    KNOWN_PLATFORMS = {'telegram', 'discord', 'matrix', 'stoatchat', 'teamspeak'}

    def __init__(self, name: str, cfg):
        self.name = name
        self.platforms: Dict[str, PlatformSection] = {}
        self._raw_cfg = cfg

        if isinstance(cfg, configparser.ConfigParser):
            self._parse_ini(cfg)
        elif isinstance(cfg, dict):
            self._parse_legacy_dict(cfg)

        bridge_dir = os.path.join(CITADEL_ROOT, name)
        self.db_path = os.path.join(bridge_dir, 'bridge.db')

    def _parse_ini(self, cfg: configparser.ConfigParser):
        sections_lower = {s.lower(): s for s in cfg.sections()}
        has_platform_sections = any(
            p in sections_lower for p in self.KNOWN_PLATFORMS
        )

        if has_platform_sections:
            for platform in self.KNOWN_PLATFORMS:
                if platform in sections_lower:
                    data = dict(cfg[sections_lower[platform]])
                    self.platforms[platform] = PlatformSection(platform, data)
        elif 'bridge' in sections_lower:
            self._parse_legacy_dict(dict(cfg[sections_lower['bridge']]))

    def _parse_legacy_dict(self, data: dict):
        direction = data.get('direction', 'both').lower()

        if data.get('telegram_channel_id') and str(data.get('telegram_channel_id', '0')).strip() not in ('', '0'):
            tg_data = {
                'channel_id': data['telegram_channel_id'],
                'mode': self._direction_to_mode(direction, 'telegram'),
                'use_prefixes': data.get('use_prefixes'),
                'add_filenames': data.get('add_filenames'),
                'add_channel_name': data.get('add_channel_name'),
                'ignore_text': data.get('ignore_text'),
                'ignore_bots': data.get('telegram_ignore_bots', data.get('ignore_bots')),
                'forward_member_events': data.get('forward_member_events'),
                'topic_id': data.get('telegram_topic_id'),
                'topic_label': data.get('telegram_topic_label'),
                'import_start': data.get('import_from_telegram_start') or data.get('import_from_telegram'),
                'import_end': data.get('import_from_telegram_end'),
            }
            self.platforms['telegram'] = PlatformSection('telegram', {k: v for k, v in tg_data.items() if v is not None})

        if data.get('discord_channel_id') and str(data.get('discord_channel_id', '0')).strip() not in ('', '0'):
            ds_data = {
                'channel_id': data['discord_channel_id'],
                'mode': self._direction_to_mode(direction, 'discord'),
                'use_webhooks': data.get('use_webhooks'),
                'webhook_url': data.get('webhook_url'),
                'use_prefixes': data.get('use_prefixes'),
                'add_filenames': data.get('add_filenames'),
                'add_channel_name': data.get('add_channel_name'),
                'ignore_text': data.get('ignore_text'),
                'ignore_bots': data.get('discord_ignore_bots', data.get('ignore_bots')),
                'ignore_webhooks': data.get('discord_ignore_webhooks', data.get('ignore_webhooks')),
                'forward_member_events': data.get('forward_member_events'),
                'webhook_bypass_reply': data.get('webhook_bypass_reply'),
                'webhook_add_channel_in_name': data.get('webhook_add_channel_in_name'),
                'webhook_forward_in_name': data.get('webhook_forward_in_name'),
                'webhook_show_forwards': data.get('webhook_show_forwards'),
                'import_start': data.get('import_from_discord_start') or data.get('import_from_discord'),
                'import_end': data.get('import_from_discord_end'),
            }
            self.platforms['discord'] = PlatformSection('discord', {k: v for k, v in ds_data.items() if v is not None})

    @staticmethod
    def _direction_to_mode(direction: str, platform: str) -> str:
        if direction == 'both':
            return 'both'
        abbrev = PLATFORM_ABBREV.get(platform, platform[:2])
        parts = direction.split('2', 1)
        if len(parts) == 2:
            src_abbr, tgt_abbr = parts[0], parts[1]
            if tgt_abbr == abbrev or tgt_abbr == platform[0]:
                return 'receive'
            if src_abbr == abbrev or src_abbr == platform[0]:
                return 'send'
        return 'both'

    def get_platform(self, platform: str) -> Optional[PlatformSection]:
        return self.platforms.get(platform)

    def has_platform(self, platform: str) -> bool:
        return platform in self.platforms

    def can_forward(self, source: str, target: str) -> bool:
        src = self.platforms.get(source)
        tgt = self.platforms.get(target)
        if not src or not tgt:
            return False
        return src.can_send and tgt.can_receive

def load_gateway_config(gateway_dir: str) -> Optional[GatewayConfig]:
    name = os.path.basename(gateway_dir)
    gw_path = os.path.join(gateway_dir, 'gateway.ini')
    if not os.path.exists(gw_path):
        return None
    cfg = configparser.ConfigParser()
    cfg.read(gw_path)
    try:
        return GatewayConfig(name, cfg)
    except Exception as e:
        log_error(f"Failed to load gateway '{name}': {e}")
        return None


def load_all_gateways(citadel_root: str = None) -> Dict[str, GatewayConfig]:
    base = citadel_root or CITADEL_ROOT
    gateways = {}
    if not os.path.isdir(base):
        return gateways
    for entry in os.listdir(base):
        if entry.startswith('_'):
            continue
        bridge_dir = os.path.join(base, entry)
        if os.path.isdir(bridge_dir):
            gw = load_gateway_config(bridge_dir)
            if gw:
                gateways[entry] = gw
    return gateways

# ── config loading ───────────────────────────────────

def load_global_settings(config_file=None, component=None):
    config_file = config_file or CONFIG_FILE
    config = configparser.ConfigParser()
    config.read(config_file)

    global_settings = {
        'CACHE_DIR': 'cache',
        'CACHE_MAX_SIZE_MB': 300,
        'DISCORD_IGNORE_BOTS': True,
        'DISCORD_IGNORE_WEBHOOKS': True,
        'RABBITMQ_HOST': 'localhost',
        'RABBITMQ_PORT': 5672,
        'RABBITMQ_USER': 'guest',
        'RABBITMQ_PASS': 'guest',
        'QUEUE_NAMESPACE': '',
        'DISCORD_SKIP_DOWNLOAD_DOMAINS': [
            'x.com', 'twitter.com', 'fixupx.com', 'vxtwitter.com', 'fxtwitter.com',
        ],
    }

    if 'Credentials' in config:
        creds = config['Credentials']
        global_settings.update({
            'DISCORD_TOKEN':            creds.get('DISCORD_TOKEN', ''),
            'TELEGRAM_BOT_TOKEN':       creds.get('TELEGRAM_BOT_TOKEN', ''),
            'DISCORD_BOT_ID':           creds.get('DISCORD_BOT_ID', ''),
            'STOATCHAT_TOKEN':          creds.get('STOATCHAT_TOKEN', ''),
            # Matrix - AS mode
            'MATRIX_HOMESERVER_URL':    creds.get('MATRIX_HOMESERVER_URL', ''),
            'MATRIX_SERVER_NAME':       creds.get('MATRIX_SERVER_NAME', ''),
            'MATRIX_AS_TOKEN':          creds.get('MATRIX_AS_TOKEN', ''),
            'MATRIX_HS_TOKEN':          creds.get('MATRIX_HS_TOKEN', ''),
            'MATRIX_BOT_LOCALPART':     creds.get('MATRIX_BOT_LOCALPART', '_chataft_bot'),
            'MATRIX_AS_PORT':           creds.get('MATRIX_AS_PORT', '29328'),
            # Matrix - Bot mode
            'MATRIX_BOT_TOKEN':         creds.get('MATRIX_BOT_TOKEN', ''),
            'MATRIX_BOT_USER':          creds.get('MATRIX_BOT_USER', ''),
        })

    if 'Features' in config:
        features = config['Features']
        global_settings['DISCORD_IGNORE_BOTS'] = features.getboolean('DISCORD_IGNORE_BOTS', True)
        global_settings['DISCORD_IGNORE_WEBHOOKS'] = features.getboolean('DISCORD_IGNORE_WEBHOOKS', True)
        global_settings['CACHE_DIR'] = features.get('CACHE_DIR', 'cache')
        global_settings['CACHE_MAX_SIZE_MB'] = int(
            features.get('CACHE_MAX_SIZE_MB', str(global_settings['CACHE_MAX_SIZE_MB']))
        )
        if 'DISCORD_SKIP_DOWNLOAD_DOMAINS' in features:
            try:
                domains = [d.strip() for d in features.get('DISCORD_SKIP_DOWNLOAD_DOMAINS', '').split(',') if d.strip()]
                if domains:
                    global_settings['DISCORD_SKIP_DOWNLOAD_DOMAINS'] = domains
            except Exception:
                pass

        global_settings['RABBITMQ_HOST'] = features.get('RABBITMQ_HOST', 'localhost')
        global_settings['RABBITMQ_PORT'] = int(features.get('RABBITMQ_PORT', '5672'))
        global_settings['RABBITMQ_USER'] = features.get('RABBITMQ_USER', 'guest')
        global_settings['RABBITMQ_PASS'] = features.get('RABBITMQ_PASS', 'guest')
        global_settings['QUEUE_NAMESPACE'] = features.get('QUEUE_NAMESPACE', '')

        log_level = features.get('LOG_LEVEL', 'INFO')
        set_log_level(log_level)

        if 'DISCORD_AVATAR_UPLOAD_CHANNEL_ID' in features:
            global_settings['DISCORD_AVATAR_UPLOAD_CHANNEL_ID'] = features.get('DISCORD_AVATAR_UPLOAD_CHANNEL_ID')

        if 'STOATCHAT_AVATAR_UPLOAD_CHANNEL_ID' in features:
            global_settings['STOATCHAT_AVATAR_UPLOAD_CHANNEL_ID'] = features.get('STOATCHAT_AVATAR_UPLOAD_CHANNEL_ID')

    if 'Global' in config:
        for key, value in config['Global'].items():
            key_upper = key.upper()
            if key_upper in ['DISCORD_IGNORE_BOTS', 'DISCORD_IGNORE_WEBHOOKS']:
                global_settings[key_upper] = config.getboolean('Global', key)
            else:
                global_settings[key_upper] = value

    return global_settings

# ── database: bridge message mappings ─────────────────

class BridgeDatabase:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        try:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA busy_timeout=5000")
        except Exception:
            pass
        self._setup_tables()

    def _setup_tables(self):
        c = self.conn.cursor()

        c.execute('''CREATE TABLE IF NOT EXISTS message_map (
            platform    TEXT NOT NULL,
            platform_id TEXT NOT NULL,
            group_id    TEXT NOT NULL,
            direction   TEXT,
            timestamp   REAL,
            PRIMARY KEY (platform, platform_id)
        )''')
        c.execute("CREATE INDEX IF NOT EXISTS idx_mm_group ON message_map (group_id)")

        c.execute('''CREATE TABLE IF NOT EXISTS media_groups (
            group_id TEXT,
            message_id TEXT,
            platform TEXT,
            timestamp INTEGER
        )''')

        self.conn.commit()

        # ── generic mapping api ─────────────────────────────

    def store_mapping(self, source_platform: str, source_id: str,
                      target_platform: str, target_id: str, direction: str = None):
        now = time.time()
        src_id = str(source_id)
        tgt_id = str(target_id)

        row = self.conn.execute(
            "SELECT group_id FROM message_map WHERE platform = ? AND platform_id = ?",
            (source_platform, src_id),
        ).fetchone()

        if not row:
            row = self.conn.execute(
                "SELECT group_id FROM message_map WHERE platform = ? AND platform_id = ?",
                (target_platform, tgt_id),
            ).fetchone()

        gid = row[0] if row else str(uuid.uuid4())

        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO message_map "
                "(platform, platform_id, group_id, direction, timestamp) VALUES (?, ?, ?, ?, ?)",
                (source_platform, src_id, gid, direction, now),
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO message_map "
                "(platform, platform_id, group_id, direction, timestamp) VALUES (?, ?, ?, ?, ?)",
                (target_platform, tgt_id, gid, direction, now),
            )

    def get_mapped_id(self, from_platform: str, from_id: str,
                      to_platform: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT m2.platform_id FROM message_map m2 "
            "INNER JOIN message_map m1 ON m1.group_id = m2.group_id "
            "WHERE m1.platform = ? AND m1.platform_id = ? AND m2.platform = ?",
            (from_platform, str(from_id), to_platform),
        ).fetchone()
        return row[0] if row else None

    def get_all_mappings(self, platform: str, platform_id: str) -> Dict[str, str]:
        rows = self.conn.execute(
            "SELECT m2.platform, m2.platform_id FROM message_map m2 "
            "INNER JOIN message_map m1 ON m1.group_id = m2.group_id "
            "WHERE m1.platform = ? AND m1.platform_id = ? AND m2.platform != ?",
            (platform, str(platform_id), platform),
        ).fetchall()
        return {row[0]: row[1] for row in rows}

    def get_last_platform_id(self, platform: str,
                             direction: str = None) -> Optional[str]:
        if direction:
            row = self.conn.execute(
                "SELECT platform_id FROM message_map "
                "WHERE platform = ? AND direction = ? "
                "ORDER BY timestamp DESC LIMIT 1",
                (platform, direction),
            ).fetchone()
            if row:
                return row[0]
        row = self.conn.execute(
            "SELECT platform_id FROM message_map "
            "WHERE platform = ? ORDER BY timestamp DESC LIMIT 1",
            (platform,),
        ).fetchone()
        return row[0] if row else None

# ── database: avatar cache ──────────────────────────────

AVATAR_REFRESH_INTERVAL = 7 * 24 * 3600  # 1 week in seconds


def get_avatar_db(citadel_root: str = None):
    base = citadel_root or CITADEL_ROOT
    avatars_dir = os.path.join(base, '_avatars')
    os.makedirs(avatars_dir, exist_ok=True)
    db_path = os.path.join(avatars_dir, 'avatar_cache.db')

    db = sqlite3.connect(db_path, check_same_thread=False)
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA busy_timeout=5000")
    except Exception:
        pass

    cursor = db.cursor()

    # Check if old schema exists and needs replacement
    _needs_rebuild = False
    try:
        cursor.execute("PRAGMA table_info(avatar_cache)")
        cols = {row[1] for row in cursor.fetchall()}
        if cols and 'avatar_hash' not in cols:
            _needs_rebuild = True
    except Exception:
        pass

    if _needs_rebuild:
        try:
            cursor.execute("DROP TABLE IF EXISTS avatar_cache")
        except Exception:
            pass
    elif cols and 'matrix_url' not in cols:
        try:
            cursor.execute("ALTER TABLE avatar_cache ADD COLUMN matrix_url TEXT")
            db.commit()
        except Exception:
            pass

    cursor.execute('''CREATE TABLE IF NOT EXISTS avatar_cache (
        user_id TEXT PRIMARY KEY,
        platform TEXT,
        avatar_hash TEXT,
        avatar_bytes BLOB,
        discord_url TEXT,
        stoatchat_url TEXT,
        matrix_url TEXT,
        discord_channel_id TEXT,
        discord_message_id TEXT,
        stoatchat_channel_id TEXT,
        stoatchat_message_id TEXT,
        last_checked INTEGER
    )''')
    db.commit()
    return db


def compute_avatar_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def get_cached_avatar(db, user_id) -> Optional[Dict]:
    try:
        row = db.execute(
            "SELECT user_id, platform, avatar_hash, avatar_bytes, discord_url, "
            "stoatchat_url, matrix_url, discord_channel_id, discord_message_id, "
            "stoatchat_channel_id, stoatchat_message_id, last_checked "
            "FROM avatar_cache WHERE user_id = ?",
            (str(user_id),)
        ).fetchone()
        if not row:
            return None
        return {
            'user_id': row[0],
            'platform': row[1],
            'avatar_hash': row[2],
            'avatar_bytes': row[3],
            'discord_url': row[4],
            'stoatchat_url': row[5],
            'matrix_url': row[6],
            'discord_channel_id': row[7],
            'discord_message_id': row[8],
            'stoatchat_channel_id': row[9],
            'stoatchat_message_id': row[10],
            'last_checked': row[11],
        }
    except Exception:
        return None


def store_avatar_cache(db, user_id, platform: str,
                       avatar_hash: str = None, avatar_bytes: bytes = None,
                       discord_url: str = None, stoatchat_url: str = None,
                       matrix_url: str = None,
                       discord_channel_id: str = None, discord_message_id: str = None,
                       stoatchat_channel_id: str = None, stoatchat_message_id: str = None):
    try:
        existing = get_cached_avatar(db, user_id)
        with db:
            db.execute(
                "INSERT OR REPLACE INTO avatar_cache "
                "(user_id, platform, avatar_hash, avatar_bytes, discord_url, stoatchat_url, "
                "matrix_url, discord_channel_id, discord_message_id, stoatchat_channel_id, "
                "stoatchat_message_id, last_checked) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(user_id),
                    platform,
                    avatar_hash if avatar_hash is not None else (existing or {}).get('avatar_hash'),
                    avatar_bytes if avatar_bytes is not None else (existing or {}).get('avatar_bytes'),
                    discord_url if discord_url is not None else (existing or {}).get('discord_url'),
                    stoatchat_url if stoatchat_url is not None else (existing or {}).get('stoatchat_url'),
                    matrix_url if matrix_url is not None else (existing or {}).get('matrix_url'),
                    discord_channel_id if discord_channel_id is not None else (existing or {}).get('discord_channel_id'),
                    discord_message_id if discord_message_id is not None else (existing or {}).get('discord_message_id'),
                    stoatchat_channel_id if stoatchat_channel_id is not None else (existing or {}).get('stoatchat_channel_id'),
                    stoatchat_message_id if stoatchat_message_id is not None else (existing or {}).get('stoatchat_message_id'),
                    int(time.time()),
                )
            )
    except Exception as e:
        log_debug(f"store_avatar_cache failed: {e}")

# ── portable utilities: media & urls ────────────────────

def extract_media_urls(text):
    if not text:
        return []
    urls = []
    for pattern in MEDIA_URL_PATTERNS:
        urls.extend(re.findall(pattern, text, re.IGNORECASE))
    return list(set(urls))


def extract_tenor_urls(text):
    if not text:
        return []
    return re.findall(TENOR_URL_PATTERN, text, re.IGNORECASE)


def escape_discord_emojis(text):
    if not text:
        return text

    def _replace(match):
        name = match.group(2)
        return f":{name}:"

    return re.sub(DISCORD_CUSTOM_EMOJI_PATTERN, _replace, text)




# ── utility: cache management ─────────────────────────

def ensure_cache_dir(settings: dict = None):
    try:
        settings = settings or load_global_settings()
        cache_dir = settings.get('CACHE_DIR', 'cache')
        cache_dir = os.path.abspath(os.path.expanduser(cache_dir))
        os.makedirs(cache_dir, exist_ok=True)
        return cache_dir
    except Exception:
        cache_dir = os.path.abspath('cache')
        os.makedirs(cache_dir, exist_ok=True)
        return cache_dir


def get_cache_size_bytes(path: str) -> int:
    total = 0
    for root, _, files_list in os.walk(path):
        for f in files_list:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except Exception:
                pass
    return total


def enforce_cache_quota(max_size_mb: int = None):
    try:
        settings = load_global_settings()
        cache_dir = settings.get('CACHE_DIR', 'cache')
        if not os.path.isdir(cache_dir):
            return
        if max_size_mb is None:
            max_size_mb = int(settings.get('CACHE_MAX_SIZE_MB', 300))
        max_bytes = max_size_mb * 1024 * 1024
        size = get_cache_size_bytes(cache_dir)
        if size <= max_bytes:
            return
        entries = []
        for root, _, files_list in os.walk(cache_dir):
            for name in files_list:
                p = os.path.join(root, name)
                try:
                    entries.append((p, os.path.getmtime(p), os.path.getsize(p)))
                except Exception:
                    continue
        entries.sort(key=lambda t: t[1])
        for p, _, s in entries:
            try:
                os.remove(p)
            except Exception:
                pass
            size -= s
            if size <= max_bytes:
                break
    except Exception as e:
        log_debug(f"Cache quota enforcement failed: {e}")

# ── utility: snowflakes & file types ───────────────────

def discord_snowflake_to_unix(snowflake: int) -> float:
    try:
        discord_epoch_ms = 1420070400000
        ts_ms = ((int(snowflake) >> 22) + discord_epoch_ms)
        return ts_ms / 1000.0
    except Exception:
        return 0.0


def should_skip_download(url: str, skip_domains: list = None) -> bool:
    try:
        if not url:
            return False
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if is_domain_ignored(host):
            return True
        if skip_domains:
            for domain in skip_domains:
                d = (domain or '').lower().strip()
                if d and (host == d or host.endswith('.' + d)):
                    return True
        return False
    except Exception:
        return False


def get_file_type_from_url(url: str, filename: str = "") -> str:
    if filename:
        ext = filename.lower().split('.')[-1] if '.' in filename else ''
    else:
        path = url.split('?')[0]
        ext = path.lower().split('.')[-1] if '.' in path else ''

    mime_map = {
        'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
        'gif': 'image/gif', 'webp': 'image/webp',
        'mp4': 'video/mp4', 'webm': 'video/webm',
        'mov': 'video/quicktime', 'avi': 'video/x-msvideo',
        'mp3': 'audio/mp3', 'm4a': 'audio/m4a', 'ogg': 'audio/ogg',
        'wav': 'audio/wav', 'flac': 'audio/flac',
        'opus': 'audio/ogg', 'oga': 'audio/ogg',
    }
    if ext in mime_map:
        return mime_map[ext]
    if any(d in url for d in ['fixupx.com', 'fxtwitter.com']):
        return 'video/mp4'
    return 'application/octet-stream'


def extract_ignore_hint_urls(text: str) -> List[str]:
    if not text:
        return []
    try:
        pattern = r"(https?://[^\s]+)\s+-(?:i|ignore)\b"
        return list({m.strip() for m in re.findall(pattern, text, re.IGNORECASE)})
    except Exception:
        return []

# ── utility: ignored users, admins, domains ────────────

def _ensure_file(path):
    try:
        if not os.path.exists(path):
            with open(path, 'w', encoding='utf-8') as f:
                f.write('')
    except Exception:
        pass


def _read_line_file(path: str) -> List[str]:
    try:
        _ensure_file(path)
        items = []
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                d = line.strip()
                if d:
                    items.append(d)
        return items
    except Exception:
        return []


def _add_line(path: str, value: str) -> bool:
    try:
        value = value.strip()
        if not value:
            return False
        existing = set(_read_line_file(path))
        if value in existing:
            return False
        with open(path, 'a', encoding='utf-8') as f:
            f.write(value + '\n')
        return True
    except Exception:
        return False


def add_ignored_user(platform: str, user_id) -> bool:
    return _add_line(IGNORED_USERS_FILE, f"{platform.lower()}:{user_id}")


def is_user_ignored(platform: str, user_id) -> bool:
    key = f"{platform.lower()}:{user_id}"
    return key in _read_line_file(IGNORED_USERS_FILE)


def is_user_admin(platform: str, user_id) -> bool:
    key = f"{platform.lower()}:{user_id}"
    return key in _read_line_file(ADMINS_FILE)


def _read_domain_file(path: str) -> List[str]:
    return [d.lower() for d in _read_line_file(path)]


def _add_domain(path: str, domain: str) -> bool:
    return _add_line(path, domain.strip().lower())


def is_domain_ignored(host: str) -> bool:
    try:
        host = (host or '').lower()
        for d in _read_domain_file(IGNORED_DOMAINS_FILE):
            if host == d or host.endswith('.' + d):
                return True
        return False
    except Exception:
        return False



def add_downloadable_domain(domain: str) -> bool:
    return _add_domain(DOWNLOAD_DOMAINS_FILE, domain)


def add_ignored_domain(domain: str) -> bool:
    return _add_domain(IGNORED_DOMAINS_FILE, domain)

# ── video conversion helpers ────────────────────────────

async def convert_video_to_gif(video_path: str, max_size_mb: float = 8.0) -> Optional[str]:
    try:
        import subprocess as _sp
        output_path = video_path.rsplit('.', 1)[0] + '.gif'

        probe_cmd = [
            'ffprobe', '-v', 'error', '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height,duration', '-of', 'json', video_path,
        ]
        probe_result = _sp.run(probe_cmd, capture_output=True, text=True)
        scale = "480:-1"
        if probe_result.returncode == 0:
            info = json.loads(probe_result.stdout)
            if info.get('streams'):
                w = info['streams'][0].get('width', 480)
                h = info['streams'][0].get('height', 480)
                mx = 480
                if w > mx or h > mx:
                    scale = f"{mx}:-1" if w > h else f"-1:{mx}"
                else:
                    scale = f"{w}:{h}"

        cmd = [
            'ffmpeg', '-i', video_path,
            '-vf', f"fps=15,scale={scale}:flags=lanczos,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse",
            '-loop', '0', '-y', output_path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()

        if proc.returncode != 0 or not os.path.exists(output_path):
            return None

        gifsicle_available = False
        try:
            chk = await asyncio.create_subprocess_exec(
                'gifsicle', '--version',
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            await chk.communicate()
            gifsicle_available = chk.returncode == 0
        except Exception:
            pass

        if gifsicle_available:
            opt = output_path.replace('.gif', '_optimized.gif')
            g_cmd = [
                'gifsicle', '--optimize=3', '--lossy=80', '--colors=256',
                '--resize-fit', '480x480', output_path, '-o', opt,
            ]
            gp = await asyncio.create_subprocess_exec(
                *g_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            await gp.communicate()
            if gp.returncode == 0 and os.path.exists(opt):
                if os.path.getsize(opt) < os.path.getsize(output_path):
                    os.remove(output_path)
                    os.rename(opt, output_path)
                else:
                    os.remove(opt)

        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        if size_mb > max_size_mb:
            os.remove(output_path)
            cmd2 = [
                'ffmpeg', '-i', video_path,
                '-vf', "fps=10,scale=320:-1:flags=lanczos,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse",
                '-loop', '0', '-y', output_path,
            ]
            proc2 = await asyncio.create_subprocess_exec(
                *cmd2, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            await proc2.communicate()

            if gifsicle_available and os.path.exists(output_path):
                g2 = [
                    'gifsicle', '--optimize=3', '--lossy=100', '--colors=128',
                    output_path, '-o', output_path + '.tmp',
                ]
                gp2 = await asyncio.create_subprocess_exec(
                    *g2, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                await gp2.communicate()
                if gp2.returncode == 0 and os.path.exists(output_path + '.tmp'):
                    os.remove(output_path)
                    os.rename(output_path + '.tmp', output_path)

        if os.path.exists(output_path):
            return output_path
        return None
    except Exception as e:
        log_error(f"Failed to convert video to GIF: {e}")
        return None

