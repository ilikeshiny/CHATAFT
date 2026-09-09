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
CITADEL_GLOBAL_ROOT = os.path.join(CITADEL_ROOT, '_global')
CODEX_FILE = os.path.join(PROJECT_ROOT, 'codex.ini')
LEGACY_CONFIG_FILE = os.path.join(PROJECT_ROOT, 'config.ini')
CONFIG_FILE = CODEX_FILE if os.path.exists(CODEX_FILE) else LEGACY_CONFIG_FILE

# Ensure the global state directory exists (lazy-created files live here).
try:
    os.makedirs(CITADEL_GLOBAL_ROOT, exist_ok=True)
except Exception:
    pass

# Legacy locations (project root) - used as fallback for migration if present.
_LEGACY_IGNORED_USERS_FILE = os.path.join(PROJECT_ROOT, 'ignored_users.txt')
_LEGACY_ADMINS_FILE = os.path.join(PROJECT_ROOT, 'admins.txt')
_LEGACY_DOWNLOAD_DOMAINS_FILE = os.path.join(PROJECT_ROOT, 'download_domains.txt')
_LEGACY_IGNORED_DOMAINS_FILE = os.path.join(PROJECT_ROOT, 'ignored_domains.txt')

def _resolve_global_state(name: str, legacy_path: str) -> str:
    """Prefer citadel/_global/<name>; fall back to legacy root path if it
    exists and the new file does not (one-time auto-migration)."""
    new_path = os.path.join(CITADEL_GLOBAL_ROOT, name)
    if not os.path.exists(new_path) and os.path.exists(legacy_path):
        try:
            with open(legacy_path, 'rb') as src, open(new_path, 'wb') as dst:
                dst.write(src.read())
        except Exception:
            return legacy_path
    return new_path

IGNORED_USERS_FILE = _resolve_global_state('ignored_users.txt', _LEGACY_IGNORED_USERS_FILE)
ADMINS_FILE = _resolve_global_state('admins.txt', _LEGACY_ADMINS_FILE)
DOWNLOAD_DOMAINS_FILE = _resolve_global_state('download_domains.txt', _LEGACY_DOWNLOAD_DOMAINS_FILE)
IGNORED_DOMAINS_FILE = _resolve_global_state('ignored_domains.txt', _LEGACY_IGNORED_DOMAINS_FILE)
# These previously lived at citadel/<file>; auto-migrate to citadel/_global/<file>.
PRIVACY_FILE = _resolve_global_state('privacy.txt', os.path.join(CITADEL_ROOT, 'privacy.txt'))
LINK_REPLACEMENTS_FILE = _resolve_global_state(
    'link_replacements.txt', os.path.join(CITADEL_ROOT, 'link_replacements.txt')
)

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
        # One file per component. Every process previously shared bridge.log
        # with its own RotatingFileHandler, so rotations raced and silently
        # dropped each other's lines.
        component = os.environ.get('BRIDGE_COMPONENT') or ''
        if not component:
            try:
                component = os.path.splitext(os.path.basename(sys.argv[0]))[0]
            except Exception:
                component = ''
        log_path = os.path.join(logs_dir, f"{component or 'bridge'}.log")
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


# ── transient network errors ─────────────────────────
#
# A 2-second API hiccup is not an incident, but it used to emit a full ERROR
# plus traceback per message. These helpers classify such failures and collapse
# repeats so a blip costs one line and a real outage still shows up.

QUIET_TRANSIENT_ERRORS = True
TRANSIENT_SUMMARY_WINDOW = 60.0

_TRANSIENT_PATTERNS = (
    'timed out', 'timeout', 'connection reset', 'connection aborted',
    'connection refused', 'connection closed', 'connection lost',
    'connection_forced', 'connection error', 'cannot connect to host',
    'server disconnected', 'serverdisconnectederror', 'remote end closed',
    'broken pipe', 'network is unreachable', 'temporary failure in name',
    'temporarily unavailable', 'getaddrinfo', 'name or service not known',
    'eof occurred in violation of protocol', 'bad gateway',
    'service unavailable', 'gateway time-out', 'read error',
    'clientconnectorerror', 'incompleteread', 'stream connection lost',
)

# Deliberately NOT treated as transient: certificate/SSL verification failures,
# auth errors (401/403), and 404s. Those are real and must stay loud.
_transient_state = {}


def is_transient_error(err) -> bool:
    """True if `err` looks like a passing network hiccup rather than a fault."""
    s = str(err).lower()
    return any(p in s for p in _TRANSIENT_PATTERNS)


def _transient_key(msg: str) -> str:
    s = re.sub(r'https?://\S+', '<URL>', str(msg))
    return re.sub(r'\d+', 'N', s)[:120]


def log_transient(msg, component=None):
    """Log a network blip, collapsing identical repeats inside a time window.

    The first occurrence is emitted immediately as a WARNING; further copies
    are counted, and the tally is attached to the next line emitted after the
    window closes. A burst that stops entirely holds its tally until the same
    error recurs - acceptable, since the first line already recorded it.
    """
    if not QUIET_TRANSIENT_ERRORS:
        log_error(msg, component)
        return

    key = _transient_key(msg)
    now = time.time()
    state = _transient_state.get(key)

    if state is not None and (now - state['start']) < TRANSIENT_SUMMARY_WINDOW:
        state['count'] += 1
        _log_to_file('debug', f"(suppressed) {msg}")
        return

    suffix = ''
    if state is not None and state['count'] > 1:
        suffix = (f"  (+{state['count'] - 1} more like this in the previous "
                  f"{int(TRANSIENT_SUMMARY_WINDOW)}s)")
    _transient_state[key] = {'start': now, 'count': 1}

    # Keep the table from growing without bound on long uptimes.
    if len(_transient_state) > 256:
        cutoff = now - (TRANSIENT_SUMMARY_WINDOW * 10)
        for k in [k for k, v in _transient_state.items() if v['start'] < cutoff]:
            _transient_state.pop(k, None)

    log_warn(f"{msg}{suffix}", component)


def log_error_smart(msg, component=None):
    """Route to log_transient for network blips, log_error for everything else."""
    if QUIET_TRANSIENT_ERRORS and is_transient_error(msg):
        log_transient(msg, component)
    else:
        log_error(msg, component)



# ── media patterns ───────────────────────────────────

MEDIA_URL_PATTERNS = [
    r"https?://[^\s]+?\.(?:jpg|jpeg|png|gif|webp|mp4|webm|mov|avi|mkv)(?:\?[^\s]*)?"
]

# Hosts that serve the raw media file at any path, with no file extension in the
# URL - the "direct" variants of the social-media embed fixers that
# link_replacements.txt rewrites links into. `https://d.fixupx.com/u/status/1`
# redirects straight to the .mp4, so these are downloadable even though the
# extension-based pattern above can't see them.
#
# Being generous here is cheap: a host that turns out to serve HTML gets
# rejected by download_media's content-type check and the link is left as text,
# which is exactly the behaviour we'd have had anyway.
DIRECT_MEDIA_HOSTS = (
    'd.fixupx.com', 'i.fixupx.com',
    'd.fxtwitter.com', 'i.fxtwitter.com',
    'd.vxtwitter.com', 'i.vxtwitter.com',
    'd.fixvx.com', 'i.fixvx.com',
    'd.kkinstagram.com', 'd.ddinstagram.com',
    'd.tnktok.com', 'd.tiktxk.com',
    'd.rxddit.com', 'd.vxreddit.com',
)

DIRECT_MEDIA_URL_PATTERN = (
    r"https?://(?:" + "|".join(h.replace('.', r'\.') for h in DIRECT_MEDIA_HOSTS) +
    r")/[^\s<>\"'\)\]]+"
)

# Player pages: an embeddable HTML page for a video, not the video itself.
# Discord fills embed.video.url with one of these for any oEmbed provider
# (YouTube, Vimeo, Twitch...), so a naive "type == video, download video.url"
# fetches an HTML document and hands the target platform a broken file.
PLAYER_PAGE_HOSTS = (
    'youtube.com', 'youtu.be', 'youtube-nocookie.com',
    'vimeo.com', 'player.vimeo.com',
    'twitch.tv', 'clips.twitch.tv',
    'dailymotion.com', 'nicovideo.jp', 'bilibili.com',
    'soundcloud.com', 'spotify.com', 'open.spotify.com',
    'kick.com', 'rumble.com', 'odysee.com',
    'twitcasting.tv', 'mixcloud.com', 'bandcamp.com',
)

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

        # privacy_limit: per-platform cap on max user-settable privacy level.
        # Falsy/missing = inherit global PRIVACY_LIMIT. 0/1/2 = explicit cap.
        raw_limit = data.get('privacy_limit', None)
        if raw_limit is None or str(raw_limit).strip() == '':
            self.privacy_limit = None
        else:
            try:
                self.privacy_limit = max(0, min(2, int(str(raw_limit).strip())))
            except Exception:
                self.privacy_limit = None

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

            # custom_emoji: 0/full=text+image (default), 1/text_only=:name: only,
            #   2/disabled=strip both
            raw_emoji = (data.get('custom_emoji', 'full') or 'full').strip().lower()
            if raw_emoji in ('1', 'text_only', 'text-only', 'text'):
                self.custom_emoji = 1
            elif raw_emoji in ('2', 'disabled', 'off', 'false', 'no'):
                self.custom_emoji = 2
            else:
                self.custom_emoji = 0

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
        'PRIVACY_LIMIT': 1,
        'INCOGNITO_AVATAR_BG': 'transparent',
        # Cleanup defaults (overridable via [Cleanup] in codex.ini)
        'CLEANUP_ENABLED': True,
        'CLEANUP_HOUR': 4,
        'MIN_MAPPINGS_PER_BRIDGE': 10000,
        'MAX_MAPPING_AGE_DAYS': 60,
        'AVATAR_RETENTION_DAYS': 90,
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
            'MATRIX_BOT_USERNAME':      creds.get('MATRIX_BOT_USERNAME', ''),
            'MATRIX_BOT_PASSWORD':      creds.get('MATRIX_BOT_PASSWORD', ''),
        })

    if 'Features' in config:
        features = config['Features']
        global_settings['DISCORD_IGNORE_BOTS'] = features.getboolean('DISCORD_IGNORE_BOTS', True)
        global_settings['DISCORD_IGNORE_WEBHOOKS'] = features.getboolean('DISCORD_IGNORE_WEBHOOKS', True)
        global_settings['CACHE_DIR'] = features.get('CACHE_DIR', 'cache')
        global_settings['CACHE_MAX_SIZE_MB'] = int(
            features.get('CACHE_MAX_SIZE_MB', str(global_settings['CACHE_MAX_SIZE_MB']))
        )
        global_settings['RABBITMQ_HOST'] = features.get('RABBITMQ_HOST', 'localhost')
        global_settings['RABBITMQ_PORT'] = int(features.get('RABBITMQ_PORT', '5672'))
        global_settings['RABBITMQ_USER'] = features.get('RABBITMQ_USER', 'guest')
        global_settings['RABBITMQ_PASS'] = features.get('RABBITMQ_PASS', 'guest')
        global_settings['QUEUE_NAMESPACE'] = features.get('QUEUE_NAMESPACE', '')

        if 'PRIVACY_LIMIT' in features:
            try:
                lim = int(features.get('PRIVACY_LIMIT', '1'))
                global_settings['PRIVACY_LIMIT'] = max(0, min(2, lim))
            except Exception:
                global_settings['PRIVACY_LIMIT'] = 1

        if 'INCOGNITO_AVATAR_BG' in features:
            global_settings['INCOGNITO_AVATAR_BG'] = (
                features.get('INCOGNITO_AVATAR_BG', 'transparent') or 'transparent'
            ).strip()

        if 'QUIET_TRANSIENT_ERRORS' in features:
            global QUIET_TRANSIENT_ERRORS
            QUIET_TRANSIENT_ERRORS = str(
                features.get('QUIET_TRANSIENT_ERRORS', 'true')
            ).strip().lower() not in ('false', 'no', 'off', '0')

        if 'TRANSIENT_SUMMARY_WINDOW' in features:
            global TRANSIENT_SUMMARY_WINDOW
            try:
                TRANSIENT_SUMMARY_WINDOW = max(
                    1.0, float(features.get('TRANSIENT_SUMMARY_WINDOW', '60'))
                )
            except Exception:
                pass

        log_level = features.get('LOG_LEVEL', 'INFO')
        set_log_level(log_level)

        if 'DISCORD_AVATAR_UPLOAD_CHANNEL_ID' in features:
            global_settings['DISCORD_AVATAR_UPLOAD_CHANNEL_ID'] = features.get('DISCORD_AVATAR_UPLOAD_CHANNEL_ID')

        if 'DISCORD_MODE' in features:
            global_settings['DISCORD_MODE'] = features.get('DISCORD_MODE')

        if 'STOATCHAT_AVATAR_UPLOAD_CHANNEL_ID' in features:
            global_settings['STOATCHAT_AVATAR_UPLOAD_CHANNEL_ID'] = features.get('STOATCHAT_AVATAR_UPLOAD_CHANNEL_ID')

    if 'Cleanup' in config:
        cleanup = config['Cleanup']
        try:
            global_settings['CLEANUP_ENABLED'] = cleanup.getboolean('enabled', True)
        except Exception:
            pass
        for key, default in (
            ('cleanup_hour', 4),
            ('min_mappings_per_bridge', 10000),
            ('max_mapping_age_days', 60),
            ('avatar_retention_days', 90),
        ):
            try:
                val = int(cleanup.get(key, str(default)))
                global_settings[key.upper()] = max(0, val)
            except Exception:
                pass

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

        c.execute('''CREATE TABLE IF NOT EXISTS import_state (
            platform     TEXT NOT NULL,
            import_start TEXT NOT NULL,
            import_end   TEXT,
            completed_at REAL,
            PRIMARY KEY (platform)
        )''')

        self.conn.commit()

        # ── generic mapping api ─────────────────────────────

    def store_mapping(self, source_platform: str, source_id: str,
                      target_platform: str, target_id: str, direction: str = None):
        now = time.time()
        src_id = str(source_id)
        tgt_id = str(target_id)

        # BEGIN IMMEDIATE takes the write lock before the lookup. Two pilgrims
        # fanning the same source message out to different targets would
        # otherwise both read "no group yet" and mint competing group_ids; the
        # loser's INSERT OR IGNORE was then silently dropped, stranding its
        # target row in a group of one - unreachable for edits/deletes and
        # never reaped by cleanup.
        if self.conn.in_transaction:
            self.conn.commit()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            src_row = self.conn.execute(
                "SELECT group_id FROM message_map WHERE platform = ? AND platform_id = ?",
                (source_platform, src_id),
            ).fetchone()
            tgt_row = self.conn.execute(
                "SELECT group_id FROM message_map WHERE platform = ? AND platform_id = ?",
                (target_platform, tgt_id),
            ).fetchone()

            if src_row and tgt_row and src_row[0] != tgt_row[0]:
                # Both halves already exist but in separate groups - either a
                # race that predates this fix or leftover split data. Fold the
                # target's whole group into the source's so every sibling row
                # moves with it rather than half the group being orphaned.
                gid = src_row[0]
                self.conn.execute(
                    "UPDATE message_map SET group_id = ? WHERE group_id = ?",
                    (gid, tgt_row[0]),
                )
            elif src_row:
                gid = src_row[0]
            elif tgt_row:
                gid = tgt_row[0]
            else:
                gid = str(uuid.uuid4())

            # Upsert rather than INSERT OR IGNORE: an existing row must still be
            # pulled into the winning group, and a row written earlier without a
            # direction gets one. An existing non-empty direction is left alone
            # so a second target can't relabel the first one's row.
            for platform, pid in ((source_platform, src_id), (target_platform, tgt_id)):
                self.conn.execute(
                    "INSERT INTO message_map "
                    "(platform, platform_id, group_id, direction, timestamp) "
                    "VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT(platform, platform_id) DO UPDATE SET "
                    "  group_id = excluded.group_id, "
                    "  direction = COALESCE(NULLIF(message_map.direction, ''), excluded.direction)",
                    (platform, pid, gid, direction, now),
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

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

    def get_last_bridged_id(self, platform: str) -> Optional[str]:
        """Highest `platform` message ID that has a counterpart elsewhere.

        Deliberately ignores `direction`: with more than one target the source
        row carries whichever direction its first pilgrim wrote, so filtering
        on it hides real rows and drags the catch-up watermark backwards into
        already-bridged history. Uses the same "exists on another platform"
        predicate as get_all_mappings, so the watermark and the dedup check
        can never disagree.

        Orders by numeric ID, not insertion time - a backfill writes old
        messages with a fresh timestamp, which makes timestamp ordering point
        at the wrong high-water mark. Only valid for snowflake-style numeric
        IDs (discord, telegram).
        """
        row = self.conn.execute(
            "SELECT m1.platform_id FROM message_map m1 "
            "INNER JOIN message_map m2 ON m1.group_id = m2.group_id "
            "WHERE m1.platform = ? AND m2.platform != ? "
            "AND m1.platform_id GLOB '[0-9]*' "
            "ORDER BY CAST(m1.platform_id AS INTEGER) DESC LIMIT 1",
            (platform, platform),
        ).fetchone()
        return row[0] if row else None

    # ── explicit import bookkeeping ─────────────────────

    def import_already_completed(self, platform: str, import_start: str,
                                 import_end: str) -> bool:
        """True if this exact start/end pair already ran to completion.

        Keyed on the config values themselves so editing import_start to a new
        link re-arms the import without any manual DB surgery.
        """
        row = self.conn.execute(
            "SELECT import_start, import_end FROM import_state "
            "WHERE platform = ? AND completed_at IS NOT NULL",
            (platform,),
        ).fetchone()
        if not row:
            return False
        return row[0] == (import_start or '') and (row[1] or '') == (import_end or '')

    def mark_import_complete(self, platform: str, import_start: str,
                             import_end: str):
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO import_state "
                "(platform, import_start, import_end, completed_at) VALUES (?, ?, ?, ?)",
                (platform, import_start or '', import_end or '', time.time()),
            )

    def clear_import_range(self, platform: str, start_id: int,
                           end_id: Optional[int] = None) -> int:
        """Drop mappings for `platform` messages inside an explicit import range.

        Deletes whole groups (not just the source row) so re-imported messages
        remap cleanly to their new target IDs and no orphan rows are left
        behind. Scoped to the range so history outside the backfill keeps its
        reply/edit/delete mappings.
        """
        upper = end_id if end_id else 9_223_372_036_854_775_807
        with self.conn:
            cur = self.conn.execute(
                """
                DELETE FROM message_map WHERE group_id IN (
                    SELECT group_id FROM message_map
                    WHERE platform = ?
                      AND CAST(platform_id AS INTEGER) BETWEEN ? AND ?
                )
                """,
                (platform, int(start_id), int(upper)),
            )
        return cur.rowcount if cur.rowcount is not None else 0

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
        existing = get_cached_avatar(db, user_id) or {}
        prev_hash = existing.get('avatar_hash')

        # If the underlying avatar bytes changed (new hash), any previously
        # uploaded platform CDN URLs now point to STALE bytes. Null them so
        # each pilgrim re-uploads on next use. Without this the pilgrims'
        # own staleness check (`age < AVATAR_REFRESH_INTERVAL`) sees the
        # shared `last_checked` timestamp reset by this very write and
        # concludes the URL is fresh - forever.
        # Callers that explicitly pass a URL always win (e.g. the pilgrim
        # writing back its just-uploaded URL).
        hash_changed = (
            avatar_hash is not None and prev_hash is not None
            and avatar_hash != prev_hash
        )

        def _url_default(new_val, existing_val):
            if new_val is not None:
                return new_val
            if hash_changed:
                return None
            return existing_val

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
                    avatar_hash if avatar_hash is not None else existing.get('avatar_hash'),
                    avatar_bytes if avatar_bytes is not None else existing.get('avatar_bytes'),
                    _url_default(discord_url, existing.get('discord_url')),
                    _url_default(stoatchat_url, existing.get('stoatchat_url')),
                    _url_default(matrix_url, existing.get('matrix_url')),
                    discord_channel_id if discord_channel_id is not None else existing.get('discord_channel_id'),
                    discord_message_id if discord_message_id is not None else existing.get('discord_message_id'),
                    stoatchat_channel_id if stoatchat_channel_id is not None else existing.get('stoatchat_channel_id'),
                    stoatchat_message_id if stoatchat_message_id is not None else existing.get('stoatchat_message_id'),
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
    # Extensionless direct-media redirectors (d.fixupx.com and friends). These
    # only appear after apply_link_replacements has run, so callers must do the
    # replacement before extracting.
    for m in re.findall(DIRECT_MEDIA_URL_PATTERN, text, re.IGNORECASE):
        urls.append(m.rstrip('.,;:!?'))
    return list(set(urls))


def is_player_page_url(url: str) -> bool:
    """True when `url` is an embeddable player page rather than a media file.

    Discord hands us `embed.video.url` pointing at e.g.
    https://www.youtube.com/embed/<id> for any oEmbed provider. Fetching that
    yields an HTML document, which then gets written out under a .mp4 name and
    arrives at the far end as a broken/empty file.
    """
    try:
        if not url:
            return False
        host = (urlparse(str(url)).netloc or '').lower().split(':')[0]
        if host.startswith('www.'):
            host = host[4:]
        return any(host == h or host.endswith('.' + h) for h in PLAYER_PAGE_HOSTS)
    except Exception:
        return False


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
        # Explicit allow beats every deny below it.
        if is_domain_downloadable(host):
            return False
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


_MIME_MAP = {
    'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
    'gif': 'image/gif', 'webp': 'image/webp',
    'mp4': 'video/mp4', 'webm': 'video/webm',
    'mov': 'video/quicktime', 'avi': 'video/x-msvideo',
    'mp3': 'audio/mp3', 'm4a': 'audio/m4a', 'ogg': 'audio/ogg',
    'wav': 'audio/wav', 'flac': 'audio/flac',
    'opus': 'audio/ogg', 'oga': 'audio/ogg',
}


def infer_mime_type(filename: str = "", url: str = "", fallback: str = "application/octet-stream") -> str:
    """Best-effort MIME inference from filename/url. Returns fallback if nothing matches.

    Tries (in order): custom map (authoritative for our common cases), stdlib mimetypes,
    URL-domain hints, then fallback.
    """
    name = filename or ''
    if not name and url:
        name = url.split('?')[0]
    ext = name.lower().rsplit('.', 1)[-1] if '.' in name else ''

    if ext in _MIME_MAP:
        return _MIME_MAP[ext]

    if name:
        try:
            import mimetypes
            guess, _ = mimetypes.guess_type(name)
            if guess:
                return guess
        except Exception:
            pass

    if url and any(d in url for d in ['fixupx.com', 'fxtwitter.com']):
        return 'video/mp4'

    return fallback


def normalize_mime_type(candidate: str, filename: str = "", url: str = "") -> str:
    """Return `candidate` if it's a real MIME type; otherwise infer from filename/url.

    Treats empty, missing-slash, and 'application/octet-stream' values as unreliable
    and tries to do better via filename inference. Falls back to octet-stream if all
    else fails.
    """
    c = (candidate or '').strip()
    if c and '/' in c and c != 'application/octet-stream':
        return c
    return infer_mime_type(filename=filename, url=url, fallback='application/octet-stream')


def get_file_type_from_url(url: str, filename: str = "") -> str:
    return infer_mime_type(filename=filename, url=url, fallback='application/octet-stream')


def attachment_name_from_download(url: str, file_path: str) -> str:
    """Display filename for a URL we just downloaded.

    Normally the URL's own basename is the best name. But extensionless
    redirectors (d.fixupx.com and friends) give a bare post id, and only
    `download_media` knows the real extension - it resolves one from the
    redirect target or the Content-Type. So when the URL yields no extension,
    fall back to the name actually written to disk, minus the cache's
    `<epoch>_` prefix.
    """
    url_name = os.path.basename((url or '').split('?')[0])
    disk_name = re.sub(r'^\d{9,}_', '', os.path.basename(file_path or ''))
    if disk_name and (not url_name or not os.path.splitext(url_name)[1]):
        return disk_name
    return url_name or disk_name or f"media_{int(time.time())}"


def ensure_filename_extension(filename: str, mime: str = "") -> str:
    """Append a MIME-derived extension to `filename` if it lacks one.

    Telegram's Bot API sometimes returns files with an internal path like
    `documents/file_24390` with no extension. When we know the real MIME (from
    the media object), we can restore the extension so downstream platforms
    render inline previews instead of generic file icons.

    Returns the filename unchanged if it already has an extension, or if no
    extension can be inferred from the mime.
    """
    name = (filename or '').strip()
    if not name:
        return name
    # Already has an extension we recognize (2-5 chars after final dot).
    if '.' in name:
        tail = name.rsplit('.', 1)[-1]
        if 1 <= len(tail) <= 5 and tail.isalnum():
            return name
    mime = (mime or '').strip()
    if not mime or '/' not in mime or mime == 'application/octet-stream':
        return name
    try:
        import mimetypes
        ext = mimetypes.guess_extension(mime)
        if ext:
            return name + ext
    except Exception:
        pass
    return name


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
                # No entry in any of these files legitimately starts with '#'
                # (domains, platform:id keys, privacy tuples), so treat those
                # lines as comments rather than as bogus entries that can never
                # match. Keeps ignored/download_domains.txt documentable, the
                # way link_replacements.txt already is.
                if d and not d.startswith('#'):
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


# ── privacy ───────────────────────────────────────────
# privacy.txt format:  platform:user_id:scope:level
#   scope = channel_id (per-bridge) or "all" (everywhere)
#   level = 0 (disabled), 1 (omit nickname), 2 (fully ignore)
# Channel-specific entry takes precedence over "all"; missing = level 0.

_GLOBAL_PRIVACY_LIMIT_CACHE = None


def _cached_global_privacy_limit() -> int:
    global _GLOBAL_PRIVACY_LIMIT_CACHE
    if _GLOBAL_PRIVACY_LIMIT_CACHE is None:
        try:
            settings = load_global_settings()
            _GLOBAL_PRIVACY_LIMIT_CACHE = int(settings.get('PRIVACY_LIMIT', 1))
        except Exception:
            _GLOBAL_PRIVACY_LIMIT_CACHE = 1
    return _GLOBAL_PRIVACY_LIMIT_CACHE


def refresh_global_privacy_limit():
    """Invalidate the cached global PRIVACY_LIMIT. Call from core.reload_config()."""
    global _GLOBAL_PRIVACY_LIMIT_CACHE
    _GLOBAL_PRIVACY_LIMIT_CACHE = None


def is_privacy_active(bridges) -> bool:
    """True if privacy is reachable anywhere: global cap > 0, OR any bridge has
    an explicit per-platform `privacy_limit > 0` override.
    When False, scribes/cores can skip registering privacy commands and skip
    every per-message privacy check entirely - zero overhead.
    """
    if _cached_global_privacy_limit() > 0:
        return True
    if not bridges:
        return False
    for bridge in bridges.values():
        for plat in ('telegram', 'discord', 'stoatchat', 'matrix'):
            section = bridge.get_platform(plat) if hasattr(bridge, 'get_platform') else None
            if section is None:
                continue
            lim = getattr(section, 'privacy_limit', None)
            if lim is not None and lim > 0:
                return True
    return False


def get_privacy_limit(platform: str = None, bridge: 'GatewayConfig' = None) -> int:
    """Effective max privacy level a user may select.
    Per-platform override (gateway.ini) > global PRIVACY_LIMIT (codex.ini) > 1.
    """
    if bridge is not None and platform:
        section = bridge.get_platform(platform)
        if section is not None and getattr(section, 'privacy_limit', None) is not None:
            return int(section.privacy_limit)
    return _cached_global_privacy_limit()


_PRIVACY_LEVEL_LABELS = {0: 'disabled', 1: 'omit nickname', 2: 'fully ignore'}


def format_privacy_levels_token(limit: int) -> str:
    """Pipe-separated valid-level token for usage strings, e.g. '0|1' or '0|1|2'."""
    return '|'.join(str(i) for i in range(0, max(0, min(2, int(limit))) + 1))


def format_privacy_levels_help(limit: int) -> str:
    """Human-readable per-level legend filtered to the effective cap.
    e.g. limit=1 -> '0=disabled, 1=omit nickname'."""
    lim = max(0, min(2, int(limit)))
    return ', '.join(f"{i}={_PRIVACY_LEVEL_LABELS[i]}" for i in range(0, lim + 1))


def format_privacy_levels_phrase(limit: int) -> str:
    """For error text. limit=0 -> '0', limit=1 -> '0 or 1', limit=2 -> '0, 1, or 2'."""
    lim = max(0, min(2, int(limit)))
    if lim == 0:
        return '0'
    if lim == 1:
        return '0 or 1'
    return '0, 1, or 2'


def cap_privacy_level(level: int, platform: str = None, bridge: 'GatewayConfig' = None) -> int:
    """Clamp a stored/requested level by the effective privacy_limit."""
    try:
        lvl = int(level)
    except Exception:
        return 0
    if lvl < 0:
        return 0
    return min(lvl, get_privacy_limit(platform, bridge))


def get_privacy_level(platform: str, user_id, channel_id=None, bridge=None) -> int:
    # Fast path: if effective cap is 0, privacy is force-disabled here.
    # Skip privacy.txt I/O entirely - this runs per message, so it matters.
    limit = get_privacy_limit(platform, bridge)
    if limit <= 0:
        return 0

    platform = platform.lower()
    user_str = str(user_id)
    chan_str = str(channel_id) if channel_id is not None else None

    chan_lvl = None
    all_lvl = None
    for line in _read_line_file(PRIVACY_FILE):
        parts = line.split(':')
        if len(parts) != 4:
            continue
        p, u, c, lvl = parts
        if p != platform or u != user_str:
            continue
        try:
            lvl_int = int(lvl)
        except ValueError:
            continue
        if lvl_int not in (0, 1, 2):
            continue
        if c == 'all':
            all_lvl = lvl_int
        elif chan_str is not None and c == chan_str:
            chan_lvl = lvl_int

    if chan_lvl is not None:
        raw = chan_lvl
    elif all_lvl is not None:
        raw = all_lvl
    else:
        raw = 0

    # Runtime cap only - txt is not rewritten. If admin lowers the cap later
    # (e.g. 2 -> 1), users with stored level 2 are simply treated as 1; raising
    # the cap back to 2 restores their original level transparently.
    if raw < 0:
        return 0
    return min(raw, limit)


def set_privacy_level(platform: str, user_id, scope, level: int) -> None:
    platform = platform.lower()
    user_str = str(user_id)
    scope_str = str(scope)
    if level not in (0, 1, 2):
        raise ValueError("level must be 0, 1, or 2")

    _ensure_file(PRIVACY_FILE)
    target_prefix = f"{platform}:{user_str}:{scope_str}:"
    keep = []
    try:
        with open(PRIVACY_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                stripped = line.rstrip('\n')
                if not stripped.strip():
                    continue
                if stripped.startswith(target_prefix):
                    continue
                keep.append(stripped)
    except FileNotFoundError:
        pass

    with open(PRIVACY_FILE, 'w', encoding='utf-8') as f:
        for line in keep:
            f.write(line + '\n')
        if level != 0:
            f.write(f"{platform}:{user_str}:{scope_str}:{level}\n")


def list_privacy_settings(platform: str, user_id) -> List[Tuple[str, int]]:
    platform = platform.lower()
    user_str = str(user_id)
    out = []
    for line in _read_line_file(PRIVACY_FILE):
        parts = line.split(':')
        if len(parts) != 4:
            continue
        p, u, c, lvl = parts
        if p != platform or u != user_str:
            continue
        try:
            lvl_int = int(lvl)
        except ValueError:
            continue
        if lvl_int not in (0, 1, 2):
            continue
        out.append((c, lvl_int))
    return out


def compute_incognito_name(channel_id, user_id) -> str:
    """Deterministic 'Incognito #NNNN' name. Same channel + same user = same number;
    different channel (or different platform) = different number."""
    h = hashlib.sha256(f"{channel_id}:{user_id}".encode('utf-8')).hexdigest()
    return f"Incognito #{int(h[:8], 16) % 10000}"


def compute_incognito_id(channel_id, user_id) -> str:
    """Stable synthetic user_id for the avatar cache, so the generated identicon
    is reused across all of one user's incognito messages in a channel."""
    h = hashlib.sha256(f"{channel_id}:{user_id}".encode('utf-8')).hexdigest()
    return f"incognito_{h[:16]}"


def _parse_avatar_bg(bg_color):
    """Resolve the user-configured background to an RGBA tuple.
    'transparent' (default) -> (0,0,0,0). Hex -> RGB+255. Bad input -> #323339 fallback."""
    if bg_color is None:
        return (0, 0, 0, 0)
    s = str(bg_color).strip().lower()
    if s in ('', 'transparent', 'none', 'alpha'):
        return (0, 0, 0, 0)
    s = s.lstrip('#')
    try:
        if len(s) == 6:
            return (int(s[0:2], 16), int(s[2:4], 16), int(s[4:6], 16), 255)
        if len(s) == 3:
            return (int(s[0]*2, 16), int(s[1]*2, 16), int(s[2]*2, 16), 255)
    except Exception:
        pass
    return (50, 51, 57, 255)  # #323339 fallback


def ensure_incognito_avatar(avatar_db, platform: str, channel_id, user_id,
                            bg_color: str = 'transparent') -> Optional[str]:
    """Make sure the avatar cache has an identicon for this incognito user.
    Returns the synthetic user_id to use as `author_id` on the BridgeMessage,
    or None if the avatar cache or Pillow is unavailable.
    Idempotent: only generates+stores if the cache has no entry yet.
    """
    if avatar_db is None:
        return None
    cache_key = compute_incognito_id(channel_id, user_id)
    try:
        existing = get_cached_avatar(avatar_db, cache_key)
    except Exception:
        existing = None
    if existing and existing.get('avatar_bytes'):
        return cache_key
    avatar_bytes = compute_incognito_avatar_bytes(channel_id, user_id, bg_color)
    if not avatar_bytes:
        # Pillow missing - return the cache_key anyway so the synthetic ID
        # remains stable; pilgrims will simply send no avatar.
        return cache_key
    try:
        store_avatar_cache(
            avatar_db, cache_key, platform,
            avatar_hash=compute_avatar_hash(avatar_bytes),
            avatar_bytes=avatar_bytes,
        )
    except Exception as e:
        log_warn(f"Failed to store incognito avatar for {cache_key}: {e}")
    return cache_key


_INCOGNITO_GRID = 16   # cells per side (mirrored on Y axis -> needs GRID/2 bits per row)
_INCOGNITO_CELL = 32   # px per cell -> 16 * 32 = 512px image


def compute_incognito_avatar_bytes(channel_id, user_id, bg_color='transparent') -> Optional[bytes]:
    """Generate a deterministic GitHub-style identicon PNG for an Incognito user.
    16x16 grid mirrored on the Y axis, color derived from sha256(channel:user).

    bg_color: 'transparent' (default), or a hex string like '#323339'.
    Returns PNG bytes, or None if Pillow is not installed.
    Same (channel_id, user_id) always returns the same image.
    """
    try:
        from PIL import Image, ImageDraw
        import colorsys
    except ImportError:
        return None

    # Use a deeper hash so the 128 bits we need (16x8) come from a single source.
    h = hashlib.sha512(f"{channel_id}:{user_id}".encode('utf-8')).digest()

    # Foreground color: vivid HSV from first 3 bytes (saturated, mid-bright).
    hue = h[0] / 255.0
    sat = 0.55 + (h[1] / 255.0) * 0.25  # 0.55 - 0.80
    val = 0.70 + (h[2] / 255.0) * 0.20  # 0.70 - 0.90
    r, g, b = [int(c * 255) for c in colorsys.hsv_to_rgb(hue, sat, val)]
    fg = (r, g, b, 255)

    bg = _parse_avatar_bg(bg_color)

    # GRIDxGRID grid mirrored on Y axis. 16x16 -> 8 left cols x 16 rows = 128 bits.
    grid_n = _INCOGNITO_GRID
    half = grid_n // 2
    grid = [[False] * grid_n for _ in range(grid_n)]
    bit_idx = 0
    src = h[3:]  # 61 bytes left from sha512 - more than enough for 128 bits
    for y in range(grid_n):
        for x in range(half):
            on = bool((src[bit_idx // 8] >> (bit_idx % 8)) & 1)
            grid[y][x] = on
            grid[y][grid_n - 1 - x] = on  # mirror
            bit_idx += 1

    cell = _INCOGNITO_CELL
    size = cell * grid_n  # 512px (no outer padding - the grid IS the avatar)
    img = Image.new('RGBA', (size, size), bg)
    draw = ImageDraw.Draw(img)
    for y in range(grid_n):
        for x in range(grid_n):
            if grid[y][x]:
                x0 = x * cell
                y0 = y * cell
                # No -1 on the right/bottom: cells abut cleanly at this resolution.
                draw.rectangle((x0, y0, x0 + cell, y0 + cell), fill=fg)

    import io as _io
    buf = _io.BytesIO()
    img.save(buf, format='PNG')
    return buf.getvalue()


# ── link replacements ─────────────────────────────────
# link_replacements.txt format:  source_domain:replacement_domain
# Matches the URL host (also www.<source> and *.source) and rewrites it.

def get_link_replacements() -> List[Tuple[str, str]]:
    rules = []
    for line in _read_line_file(LINK_REPLACEMENTS_FILE):
        if ':' not in line or line.lstrip().startswith('#'):
            continue
        src, dst = line.split(':', 1)
        src = src.strip().lower()
        dst = dst.strip()
        if src and dst:
            rules.append((src, dst))
    return rules


def apply_link_replacements(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    rules = get_link_replacements()
    if not rules:
        return text

    # A host that is already some rule's replacement target is left alone, so
    # the pass is idempotent and can be run more than once on the same text.
    # Without this, `x.com:d.fixupx.com` plus `fixupx.com:i.fixupx.com` walks a
    # link down the chain - d.fixupx.com matches the second rule through its
    # subdomain and a video link silently becomes an image one.
    targets = {(dst or '').lower() for _, dst in rules}

    def _swap(match):
        url = match.group(0)
        try:
            parsed = urlparse(url)
            host = (parsed.netloc or '').lower()
            if host in targets or (host.startswith('www.') and host[4:] in targets):
                return url
            for src, dst in rules:
                if host == src or host == 'www.' + src or host.endswith('.' + src):
                    return url.replace(parsed.netloc, dst, 1)
        except Exception:
            pass
        return url

    return re.sub(r'https?://[^\s<>"\)\]]+', _swap, text)


def _read_domain_file(path: str) -> List[str]:
    return [d.lower() for d in _read_line_file(path)]


def _add_domain(path: str, domain: str) -> bool:
    return _add_line(path, domain.strip().lower())


def _host_matches(host: str, domains) -> bool:
    """True if `host` equals, or is a subdomain of, any entry in `domains`."""
    host = (host or '').lower()
    return any(host == d or host.endswith('.' + d) for d in domains)


def is_domain_ignored(host: str) -> bool:
    try:
        return _host_matches(host, _read_domain_file(IGNORED_DOMAINS_FILE))
    except Exception:
        return False


def is_domain_downloadable(host: str) -> bool:
    """True if `host` is on the explicit download allowlist.

    An entry here forces a download the policy would otherwise decline: it
    outranks ignored_domains.txt and the automatic player-page skip. Nothing
    unsafe can come of that, because download_media still refuses to save a
    response that comes back as HTML.
    """
    try:
        return _host_matches(host, _read_domain_file(DOWNLOAD_DOMAINS_FILE))
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

