import json
import time
import pika
import threading
import subprocess
import sys
import os
import signal
import configparser
from datetime import datetime
from typing import Dict, Set, List, Optional
from collections import deque
from io import StringIO

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.layout import Layout
    from rich.text import Text
    from rich import box
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

# Rich (or its dependencies) may call colorama.init() on import, wrapping
# sys.stdout in an AnsiToWin32 converter. On Linux this breaks cursor ops
# since the Win32 terminal backend is None. Undo the wrapping.
if HAS_RICH and sys.platform != 'win32':
    try:
        import colorama
        colorama.deinit()
    except Exception:
        pass

_root = os.path.dirname(os.path.abspath(__file__))
if _root not in sys.path:
    sys.path.insert(0, _root)

from postkeep.papyrus import (
    log_info, log_error, log_warn, log_success, log_debug,
    load_global_settings,
    StatusUpdate,
    ARBITER_QUEUES, apply_namespace, get_namespace,
    CITADEL_ROOT, CONFIG_FILE,
    add_ignored_domain, add_ignored_user, add_downloadable_domain,
    is_domain_ignored, is_user_ignored,
    Fore, Style,
    GatewayConfig, load_all_gateways,
    get_avatar_db,
)


# ----------------------------------------------------------------
#  Plain console shim for headless / no-Rich mode
# ----------------------------------------------------------------
import re
_RICH_MARKUP_RE = re.compile(r'\[/?[a-zA-Z][^\]]*\]')


class _PlainConsole:
    """Minimal console that strips Rich markup and prints plain text."""

    width = 120

    @staticmethod
    def _strip(text):
        if not isinstance(text, str):
            text = str(text)
        return _RICH_MARKUP_RE.sub('', text)

    def print(self, *args, **kwargs):
        parts = []
        for a in args:
            parts.append(self._strip(a) if isinstance(a, str) else str(a))
        print(' '.join(parts))

    def rule(self, *args, **kwargs):
        print('-' * 60)

    def clear(self):
        pass


if HAS_RICH:
    console = Console(force_terminal=True, highlight=False)
else:
    console = _PlainConsole()


class Arbiter:
    def __init__(self):
        self.settings = load_global_settings(CONFIG_FILE)
        self.bridges = load_all_gateways()
        self.active_components: Set[str] = set()
        self.component_status: Dict[str, str] = {}
        self.component_processes: Dict[str, subprocess.Popen] = {}
        self.running = False
        self.connection = None
        self.channel = None
        self.output_lock = threading.Lock()
        self._debug_mode = False
        self._routed_count = 0
        self._pin_active = False

        # Inline command bar state (pin mode)
        self._cmd_mode = False
        self._cmd_buffer = ''
        self._cmd_flash = ''
        self._cmd_flash_until = 0.0

        self._namespace = (
            get_namespace()
            or str(self.settings.get('QUEUE_NAMESPACE', '')).strip()
        )
        self.queues = apply_namespace(ARBITER_QUEUES, self._namespace)
        self._all_queues = set(self.queues.values())

        self._init_citadel()

        self.available_components = {
            'discord':   os.path.join('postkeep', 'discord', 'discord_core.py'),
            'telegram':  os.path.join('postkeep', 'telegram', 'telegram_core.py'),
            'stoatchat': os.path.join('postkeep', 'stoatchat', 'stoatchat_core.py'),
            'matrix':    os.path.join('postkeep', 'matrix', 'matrix_core.py'),
        }

        self._platform_credentials = {
            'discord':   'DISCORD_TOKEN',
            'telegram':  'TELEGRAM_BOT_TOKEN',
            'stoatchat': 'STOATCHAT_TOKEN',
            'matrix':    ['MATRIX_BOT_TOKEN', 'MATRIX_AS_TOKEN', 'MATRIX_BOT_PASSWORD'],  # needs at least one
        }

        self._platforms = list(self.available_components.keys())

        self._routing_table = self._build_routing_table()

        self._recent_routes: deque = deque(maxlen=100)
        self._route_log: deque = deque(maxlen=100)
        self._pending_counts: Dict[str, int] = {}
        self._cached_queue_depths: Dict[str, int] = {}
        self._last_route_time: Optional[datetime] = None
        self._start_time = datetime.now()

        self._terminal_type = self._detect_terminal()
        self._chrome_offset = self._detect_terminal_chrome_offset()
        self._anim_frame = 0
        self._anim_tick = 0
        self._anim_active = False
        self._anim_until: float = 0
        self._pin_refresh_interval = 1.0 if self._terminal_type == 'legacy' else 0.35
        self._status_bar_frames = [
            '>>        ', '>>>>      ', '>>>>>>    ',
            '>>>>>>>>  ', '>>>>>>>>>>', '>>>>>>>>>>', '>>>>>>>>>>',
        ]

        self._error_log: deque = deque(maxlen=500)
        self._warn_log: deque = deque(maxlen=500)
        self._error_counts: Dict[str, int] = {p: 0 for p in self._platforms}
        self._warn_counts: Dict[str, int] = {p: 0 for p in self._platforms}
        self._error_view_active = False
        self._error_view_filter = 'all'  # 'all', 'discord', 'telegram', 'matrix', 'stoatchat'
        self._error_view_until: float = 0

        # Track last status update heard from each component for health monitoring
        self._last_status_heard: Dict[str, float] = {}
        self._COMPONENT_SILENT_THRESHOLD = 300  # 5 min without any status = suspect

        self._arbiter_config = self._load_arbiter_config()
        self._headless = self._arbiter_config.get('headless', False) or not HAS_RICH

        # In headless mode, swap the global console to the plain shim
        if self._headless:
            global console
            console = _PlainConsole()

    # ----------------------------------------------------------------
    #  Arbiter config from [Arbiter] section in codex.ini
    # ----------------------------------------------------------------

    @staticmethod
    def _load_arbiter_config() -> Dict:
        defaults = {
            'min_width': None,
            'min_height': None,
            'auto_dashboard_pin': True,
            'headless': False,
            # auto-restart: bounce all components every N hours during a
            # quiet window to flush stale per-process caches. 0/blank in
            # `auto_restart_timer` disables the feature entirely.
            'auto_restart_timer': 0,        # hours; 0 = disabled
            'auto_restart_clear_msg': 15,   # minutes of idle required
            'auto_restart_max_wait': 12,    # hours; force-restart if no quiet window
        }
        try:
            if not os.path.isfile(CONFIG_FILE):
                return defaults
            parser = configparser.ConfigParser()
            parser.read(CONFIG_FILE, encoding='utf-8')
            if not parser.has_section('Arbiter'):
                return defaults

            raw_w = parser.get('Arbiter', 'min_width', fallback='').strip().lower()
            if raw_w and raw_w not in ('false', 'no', 'off', '0', ''):
                try:
                    defaults['min_width'] = int(raw_w)
                except ValueError:
                    pass

            raw_h = parser.get('Arbiter', 'min_height', fallback='').strip().lower()
            if raw_h and raw_h not in ('false', 'no', 'off', '0', ''):
                try:
                    defaults['min_height'] = int(raw_h)
                except ValueError:
                    pass

            raw_pin = parser.get('Arbiter', 'auto_dashboard_pin', fallback='').strip().lower()
            if raw_pin in ('false', 'no', 'off', '0'):
                defaults['auto_dashboard_pin'] = False
            elif raw_pin in ('true', 'yes', 'on', '1', ''):
                defaults['auto_dashboard_pin'] = True

            raw_hl = parser.get('Arbiter', 'arbiter_headless', fallback='').strip().lower()
            if raw_hl in ('true', 'yes', 'on', '1'):
                defaults['headless'] = True

            # Auto-restart settings. Blank/missing keeps the default above;
            # invalid integers are ignored silently to avoid breaking startup.
            for key, fallback_val in (
                ('auto_restart_timer', defaults['auto_restart_timer']),
                ('auto_restart_clear_msg', defaults['auto_restart_clear_msg']),
                ('auto_restart_max_wait', defaults['auto_restart_max_wait']),
            ):
                raw = parser.get('Arbiter', key, fallback='').strip().lower()
                if raw and raw not in ('false', 'no', 'off', ''):
                    try:
                        defaults[key] = max(0, int(raw))
                    except ValueError:
                        pass

            return defaults
        except Exception:
            return defaults

    # ----------------------------------------------------------------
    #  Citadel
    # ----------------------------------------------------------------

    def _init_citadel(self):
        os.makedirs(CITADEL_ROOT, exist_ok=True)
        for name in (self.bridges or {}):
            bridge_dir = os.path.join(CITADEL_ROOT, name)
            os.makedirs(bridge_dir, exist_ok=True)
        avatars_dir = os.path.join(CITADEL_ROOT, '_avatars')
        os.makedirs(avatars_dir, exist_ok=True)

    def _load_gateways(self) -> Dict:
        gateways = load_all_gateways(CITADEL_ROOT)
        result = {}
        for name, gw in gateways.items():
            entry = {}
            for platform_name, ps in gw.platforms.items():
                entry[f'{platform_name}_channel_id'] = str(ps.channel_id)
                entry[f'{platform_name}_mode'] = ps.mode
            result[name] = entry
        return result

    # ----------------------------------------------------------------
    #  Terminal Detection & Rendering
    # ----------------------------------------------------------------

    @staticmethod
    def _detect_terminal() -> str:
        if sys.platform != 'win32':
            return 'modern'
        if os.environ.get('WT_SESSION'):
            return 'modern'
        if os.environ.get('ConEmuPID') or os.environ.get('CONEMUDIR'):
            return 'modern'
        if os.environ.get('CMDER_ROOT') or os.environ.get('CMDER_INIT'):
            return 'modern'
        term_program = os.environ.get('TERM_PROGRAM', '').lower()
        if term_program in ('vscode', 'hyper', 'tabby', 'wezterm', 'kitty', 'alacritty'):
            return 'modern'
        if os.environ.get('ANSICON'):
            return 'modern'
        if os.environ.get('TERM') and os.environ.get('TERM') != 'dumb':
            return 'modern'
        return 'legacy'

    @staticmethod
    def _detect_terminal_chrome_offset() -> int:
        """Extra lines consumed by the terminal emulator's own chrome (tabs, status bar, etc.).
        Subtracted from os.get_terminal_size().lines to get the actual usable area."""
        if sys.platform != 'win32':
            return 0
        # ConEmu/Cmder: tab bar (~1 line) + status bar (~1 line) + padding (~1 line)
        if os.environ.get('ConEmuPID') or os.environ.get('CONEMUDIR'):
            return 3
        if os.environ.get('CMDER_ROOT') or os.environ.get('CMDER_INIT'):
            return 3
        # Windows Terminal handles dimensions accurately
        if os.environ.get('WT_SESSION'):
            return 0
        # Other modern terminals on Windows - conservative 1-line safety margin
        term_program = os.environ.get('TERM_PROGRAM', '').lower()
        if term_program in ('vscode', 'hyper', 'tabby'):
            return 1
        return 0

    def _get_usable_height(self) -> int:
        """Terminal height minus chrome offset. Always at least 20.

        No extra safety margin: in pinned mode the cursor is hidden so we
        can claim every visible row. The chrome_offset itself accounts for
        terminal-emulator chrome (tabs/status bars) on known terminals.
        """
        try:
            raw = os.get_terminal_size().lines
        except (OSError, ValueError):
            raw = 40
        return max(20, raw - self._chrome_offset)

    def _ensure_console_size(self):
        if sys.platform != 'win32':
            return
        cfg_w = self._arbiter_config.get('min_width')
        cfg_h = self._arbiter_config.get('min_height')
        if cfg_w is None and cfg_h is None:
            return
        try:
            size = os.get_terminal_size()
            need_cols = max(cfg_w or 0, size.columns)
            need_lines = max(cfg_h or 0, size.lines)
            if (cfg_w and size.columns < cfg_w) or (cfg_h and size.lines < cfg_h):
                os.system(f'mode con cols={need_cols} lines={need_lines}')
        except (OSError, ValueError):
            pass

    def _clear_screen(self):
        if self._terminal_type == 'modern':
            sys.stdout.write('\033[H\033[2J\033[3J')
            sys.stdout.flush()
        else:
            os.system('cls' if sys.platform == 'win32' else 'clear')

    def _get_animated_bar(self) -> str:
        if self._terminal_type == 'legacy':
            return '=========='
        if not self._anim_active or time.monotonic() > self._anim_until:
            self._anim_active = False
            self._anim_frame = 0
            return '          '
        return self._status_bar_frames[self._anim_frame % len(self._status_bar_frames)]

    def _trigger_animation(self):
        self._anim_active = True
        self._anim_frame = 0
        self._anim_until = time.monotonic() + 1.2

    def _advance_animation(self):
        if self._anim_active and time.monotonic() < self._anim_until:
            self._anim_frame = (self._anim_frame + 1) % len(self._status_bar_frames)
        else:
            self._anim_active = False

    def _render_frame(self, *renderables, hint: str = '') -> str:
        buf = StringIO()
        width = console.width or 120
        height = self._get_usable_height()
        buf_console = Console(
            file=buf, force_terminal=True, highlight=False,
            width=width, height=height, color_system=console.color_system,
        )
        for r in renderables:
            buf_console.print(r)
        if hint:
            buf_console.print(hint)
        output = buf.getvalue()
        lines = output.split('\n')
        if len(lines) > height:
            lines = lines[:height]
        return '\n'.join(lines)

    def _write_frame(self, frame: str):
        if self._terminal_type == 'modern':
            # Keep the cursor hidden across redraws. Toggling visibility on each
            # frame (which used to happen here) caused a visible flicker in the
            # bottom-right where the cursor lands after a paint. The cursor is
            # restored once when pin mode exits.
            sys.stdout.write('\033[?25l\033[H')
            sys.stdout.write(frame)
            sys.stdout.write('\033[J')
            sys.stdout.flush()
        else:
            os.system('cls' if sys.platform == 'win32' else 'clear')
            sys.stdout.write(frame)
            sys.stdout.flush()

    # ----------------------------------------------------------------
    #  Error & Warn Tracking
    # ----------------------------------------------------------------

    def _record_error(self, component: str, message: str):
        self._error_log.append({
            'time': datetime.now(), 'component': component, 'message': message,
        })
        if component in self._error_counts:
            self._error_counts[component] += 1
        else:
            self._error_counts[component] = 1

    def _record_warn(self, component: str, message: str):
        self._warn_log.append({
            'time': datetime.now(), 'component': component, 'message': message,
        })
        if component in self._warn_counts:
            self._warn_counts[component] += 1
        else:
            self._warn_counts[component] = 1

    _TRANSIENT_PATTERNS = (
        'httpconnectionpool', 'httpsconnectionpool',
        'connectionreseterror', 'connectionrefusederror',
        'readtimeout', 'connecttimeout',
        'remotedisconnected', 'brokenpipeerror',
        'newconnectionerror', 'urlliberror',
        'reconnect', 'retrying', 'retry',
        'serverselectiontimeouterror',
        'connectionerror', 'connection reset',
        'timed out', 'timeout',
    )

    # Stoat library logs reconnect noise as errors - demote all of it to debug
    _STOAT_NOISE_PATTERNS = (
        'missed pong', 'pong', 'nonce', 'reconnect', 'shard',
        'websocket', 'heartbeat', 'watchdog', 'health check',
    )

    def _classify_child_line(self, component: str, line: str):
        lower = line.lower()
        if any(kw in lower for kw in ('error', 'exception', 'traceback', 'failed')):
            # Stoat library reconnect noise - suppress entirely
            if component == 'stoatchat' and any(sp in lower for sp in self._STOAT_NOISE_PATTERNS):
                return
            if any(tp in lower for tp in self._TRANSIENT_PATTERNS):
                self._record_warn(component, line.strip())
            else:
                self._record_error(component, line.strip())
        elif 'warn' in lower:
            # Also suppress stoat reconnect warnings
            if component == 'stoatchat' and any(sp in lower for sp in self._STOAT_NOISE_PATTERNS):
                return
            self._record_warn(component, line.strip())

    def _get_error_summary(self) -> str:
        parts = []
        for comp, count in sorted(self._error_counts.items()):
            if count > 0:
                parts.append(f"{comp}:{count}")
        return ', '.join(parts) if parts else 'none'

    _ERROR_PLATFORM_KEYS = {'d': 'discord', 't': 'telegram', 'm': 'matrix', 's': 'stoatchat', 'a': 'all'}

    def _build_error_overlay(self, platform_filter: str = 'all') -> str:
        errors = list(self._error_log)
        if platform_filter != 'all':
            errors = [e for e in errors if e['component'] == platform_filter]
        if not errors:
            label = platform_filter if platform_filter != 'all' else 'any platform'
            return f"  [dim]No errors recorded for {label}.[/]"
        lines = []
        for err in errors:
            t = err['time'].strftime('%H:%M:%S')
            comp = err['component']
            msg = err['message']
            if len(msg) > 120:
                msg = msg[:117] + '...'
            lines.append(f"  [dim]{t}[/]  [red]x[/] [yellow]{comp}[/]  {msg}")
        return '\n'.join(lines)

    def _clear_issues(self, platform_filter: str = 'all'):
        if platform_filter == 'all':
            self._error_log.clear()
            self._warn_log.clear()
            self._error_counts = {p: 0 for p in self._platforms}
            self._warn_counts = {p: 0 for p in self._platforms}
        else:
            self._error_log = deque(
                (e for e in self._error_log if e['component'] != platform_filter),
                maxlen=500,
            )
            self._warn_log = deque(
                (w for w in self._warn_log if w['component'] != platform_filter),
                maxlen=500,
            )
            self._error_counts[platform_filter] = 0
            self._warn_counts[platform_filter] = 0

    # ----------------------------------------------------------------
    #  Routing
    # ----------------------------------------------------------------

    def _build_routing_table(self) -> Dict:
        table = {}
        gateways = self._load_gateways()
        for name, gw in gateways.items():
            table[name] = dict(gw)
        return table

    def _get_bridge_platforms(self, bridge_name: str) -> List[str]:
        bridge = self._routing_table.get(bridge_name, {})
        platforms = []
        for key, val in bridge.items():
            if key.endswith('_channel_id') and val:
                platform = key.removesuffix('_channel_id')
                if platform in self._platforms:
                    platforms.append(platform)
        return platforms

    def _queue_to_platform(self, queue_name: str, prefix: str) -> Optional[str]:
        for platform in self._platforms:
            if queue_name == self.queues.get(f'{prefix}_{platform}'):
                return platform
        return None

    def _route_message(self, body_bytes: bytes, source_queue: str):
        try:
            data = json.loads(body_bytes.decode())
        except Exception:
            log_debug("Arbiter received non-JSON message, dropping.")
            return

        if not isinstance(data, dict):
            log_debug("Arbiter received non-dict JSON, dropping.")
            return

        bridge_name = data.get('bridge_name', '?')
        author = data.get('author_name', '')
        msg_type = data.get('type', 'message')
        source_platform = self._queue_to_platform(source_queue, 'scribe')

        if not source_platform:
            log_warn(f"Arbiter: unrecognised source queue '{source_queue}', dropping message.")
            return

        bridge_platforms = self._get_bridge_platforms(bridge_name)
        if not bridge_platforms:
            bridge_platforms = [p for p in self._platforms if p != source_platform]

        rt_entry = self._routing_table.get(bridge_name, {})

        targets_hit = []
        for target_platform in bridge_platforms:
            if target_platform == source_platform:
                continue

            src_mode = rt_entry.get(f'{source_platform}_mode', 'both')
            tgt_mode = rt_entry.get(f'{target_platform}_mode', 'both')
            if src_mode not in ('send', 'both'):
                continue
            if tgt_mode not in ('receive', 'both'):
                continue

            target_queue = self.queues.get(f'pilgrim_{target_platform}')
            if not target_queue:
                continue
            if self._publish(target_queue, body_bytes):
                targets_hit.append(target_platform)
                self._routed_count += 1

        if targets_hit:
            targets_str = ', '.join(targets_hit)
            self._last_route_time = datetime.now()
            self._trigger_animation()
            route_entry = {
                'time':    self._last_route_time,
                'source':  source_platform,
                'targets': targets_hit,
                'bridge':  bridge_name,
                'author':  author,
                'msg_type': msg_type,
            }
            self._recent_routes.append(route_entry)
            self._route_log.append(route_entry)

            type_label = ''
            if msg_type == 'edit':
                type_label = ' [yellow]\\[edit][/]'
            elif msg_type == 'delete':
                type_label = ' [red]\\[delete][/]'

            if not self._pin_active:
                anim_bar = self._get_animated_bar()
                with self.output_lock:
                    console.print(
                        f"  [bold cyan]>[/] "
                        f"[white]{source_platform}[/] -> [white]{targets_str}[/]  "
                        f"[dim]\\[{bridge_name}][/] "
                        f"[italic]{author}[/]{type_label}  "
                        f"[green]{anim_bar}[/]"
                    )
        elif not targets_hit and bridge_platforms:
            log_debug(
                f"Arbiter: no valid targets for {source_platform} "
                f"in bridge '{bridge_name}' (modes not aligned)"
            )

    def _publish(self, queue: str, body: bytes) -> bool:
        try:
            if self.channel and self.channel.is_open:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=queue,
                    body=body,
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                log_debug(f"Arbiter published {len(body)}b to '{queue}'")
                return True
            else:
                log_error(f"Cannot publish to '{queue}': channel not open")
                return False
        except Exception as e:
            log_error(f"Arbiter publish to '{queue}' failed: {e}")
            return False

    # ----------------------------------------------------------------
    #  RabbitMQ
    # ----------------------------------------------------------------

    def check_rabbitmq(self):
        try:
            credentials = pika.PlainCredentials(
                self.settings['RABBITMQ_USER'], self.settings['RABBITMQ_PASS'],
            )
            params = pika.ConnectionParameters(
                host=self.settings['RABBITMQ_HOST'],
                port=self.settings['RABBITMQ_PORT'],
                credentials=credentials,
                socket_timeout=10,
            )
            conn = pika.BlockingConnection(params)
            conn.close()
            return True
        except Exception as e:
            log_error(f"RabbitMQ check failed: {e}")
            return False

    def connect_rabbitmq(self):
        try:
            credentials = pika.PlainCredentials(
                self.settings['RABBITMQ_USER'], self.settings['RABBITMQ_PASS'],
            )
            params = pika.ConnectionParameters(
                host=self.settings['RABBITMQ_HOST'],
                port=self.settings['RABBITMQ_PORT'],
                credentials=credentials,
                heartbeat=60,
                blocked_connection_timeout=120,
                socket_timeout=10,
            )
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()
            for queue_name in self._all_queues:
                self.channel.queue_declare(queue=queue_name, durable=True)
            self.channel.queue_declare(queue='component_query', durable=False)

            ns_label = self._namespace or 'default'
            log_success(f"Arbiter connected to RabbitMQ  [namespace: {ns_label}]")
            log_info(f"Declared queues: {', '.join(sorted(self._all_queues))}")
            return True
        except Exception as e:
            log_error(f"Failed to connect to RabbitMQ: {e}")
            return False

    # ----------------------------------------------------------------
    #  Queue depth  (only called from monitor thread - pika is NOT
    #  thread-safe, so every channel operation must stay on the same
    #  thread that owns the connection)
    # ----------------------------------------------------------------

    def _get_queue_depth_unsafe(self, queue_name: str) -> int:
        try:
            if self.channel and self.channel.is_open:
                result = self.channel.queue_declare(queue=queue_name, passive=True)
                return result.method.message_count
        except Exception:
            pass
        return 0

    def _refresh_pending_counts_unsafe(self):
        counts = {}
        for platform in self._platforms:
            pilgrim_q = self.queues.get(f'pilgrim_{platform}')
            if pilgrim_q:
                counts[platform] = self._get_queue_depth_unsafe(pilgrim_q)
        self._pending_counts = counts

    def _refresh_all_queue_depths_unsafe(self):
        depths = {}
        for k, v in self.queues.items():
            depths[v] = self._get_queue_depth_unsafe(v)
        self._cached_queue_depths = depths
        self._refresh_pending_counts_unsafe()

    # ----------------------------------------------------------------
    #  Status updates
    # ----------------------------------------------------------------

    def handle_status_update(self, ch, method, properties, body):
        try:
            status = StatusUpdate.from_json(body.decode())
            self.component_status[status.component] = status.status
            self._last_status_heard[status.component] = time.time()

            if status.status == 'ready':
                if status.component not in self.active_components:
                    self.active_components.add(status.component)
                    with self.output_lock:
                        log_success(f"{status.component} is ready")

            elif status.status == 'stopped':
                self.active_components.discard(status.component)

            elif status.status == 'error':
                with self.output_lock:
                    log_error(f"{status.component} error: {status.message}")
                self._record_error(status.component, status.message or 'unknown error')

            else:
                # Any other non-terminal status (e.g. 'starting') still proves the
                # component is alive and reachable. Make sure it's listed - the
                # initial 'ready' message can race with the consumer attachment
                # or get coalesced by the broker, leaving us stuck on 'starting'.
                if status.component not in self.active_components:
                    self.active_components.add(status.component)

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            log_error(f"Error handling status update: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def handle_component_query(self, ch, method, properties, body):
        try:
            response = {
                'active_components': list(self.active_components),
                'component_status': self.component_status,
            }
            if properties.reply_to:
                ch.basic_publish(
                    exchange='', routing_key=properties.reply_to,
                    body=json.dumps(response),
                    properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            log_error(f"Error handling component query: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    # ----------------------------------------------------------------
    #  Routing consumers
    # ----------------------------------------------------------------

    def _make_routing_callback(self, source_queue):
        def callback(ch, method, properties, body):
            try:
                self._route_message(body, source_queue)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                log_error(f"Arbiter routing error from '{source_queue}': {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return callback

    def start_routing_consumers(self):
        for platform in self._platforms:
            q = self.queues.get(f'scribe_{platform}')
            if q:
                try:
                    self.channel.basic_consume(
                        queue=q,
                        on_message_callback=self._make_routing_callback(q),
                        auto_ack=False,
                    )
                    log_info(f"Routing consumer attached: {q}")
                except Exception as e:
                    log_warn(f"Could not attach routing consumer for '{q}': {e}")

    def start_status_consumer(self):
        self.channel.basic_consume(
            queue=self.queues['status_updates'],
            on_message_callback=self.handle_status_update,
            auto_ack=False,
        )
        self.channel.basic_consume(
            queue='component_query',
            on_message_callback=self.handle_component_query,
            auto_ack=False,
        )

    # ----------------------------------------------------------------
    #  Process management
    # ----------------------------------------------------------------

    def _can_start(self, name: str) -> bool:
        cred_key = self._platform_credentials.get(name)
        if cred_key:
            if isinstance(cred_key, list):
                return any(bool(self.settings.get(k)) for k in cred_key)
            return bool(self.settings.get(cred_key))
        return bool(name in self.available_components)

    def start_component(self, name: str):
        if name in self.component_processes:
            if self.component_processes[name].poll() is None:
                log_warn(f"{name} is already running")
                return True

        script = self.available_components.get(name)
        if not script:
            log_error(f"Unknown component: {name}")
            return False
        if not os.path.exists(script):
            log_error(f"Script not found: {script}")
            return False
        if not self._can_start(name):
            log_warn(f"Missing credentials for {name}, skipping")
            return False

        try:
            env = os.environ.copy()
            env['PYTHONUNBUFFERED'] = '1'
            if self._namespace:
                env['BRIDGE_NS'] = self._namespace
            if self._debug_mode:
                env['LOG_LEVEL'] = 'DEBUG'

            process = subprocess.Popen(
                [sys.executable, '-u', script],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                universal_newlines=True, bufsize=1, env=env,
            )
            self.component_processes[name] = process
            with self.output_lock:
                log_info(f"Starting {name}  (PID {process.pid})")

            t = threading.Thread(
                target=self._read_output, args=(name, process), daemon=True,
            )
            t.start()
            return True
        except Exception as e:
            log_error(f"Failed to start {name}: {e}")
            return False

    def _read_output(self, name: str, process: subprocess.Popen):
        spam = [
            'process_data_events', 'Started RabbitMQ consumers',
            'Starting Telegram bot polling', 'Active components from master router',
            'Checking mode', 'Running in', 'pop from an empty deque',
            'Cannot send status update', 'RabbitMQ channel not ready',
            'Periodic check', 'Mode locked', 'consumer_thread', 'Consumer thread',
            'Event loop not ready',
        ]
        pin_buffer: deque = deque(maxlen=50)
        try:
            while process.poll() is None:
                line = process.stdout.readline()
                if not line:
                    continue
                line = line.rstrip('\r\n')
                if not line or line.isspace():
                    continue
                if not self._debug_mode and any(kw in line for kw in spam):
                    continue

                if self._pin_active:
                    pin_buffer.append(f"  [{name}] {line}")
                    self._classify_child_line(name, line)
                    continue

                if pin_buffer:
                    with self.output_lock:
                        for buffered in pin_buffer:
                            sys.stdout.write(buffered + '\n')
                        sys.stdout.flush()
                    pin_buffer.clear()

                self._classify_child_line(name, line)

                with self.output_lock:
                    sys.stdout.write(f"  [{name}] {line}\n")
                    sys.stdout.flush()
                time.sleep(0.05)

            if pin_buffer:
                with self.output_lock:
                    for buffered in pin_buffer:
                        sys.stdout.write(buffered + '\n')
                    sys.stdout.flush()

            rc = process.poll()
            if rc not in (0, None):
                with self.output_lock:
                    log_error(f"{name} exited with code {rc}")
        except Exception as e:
            with self.output_lock:
                log_error(f"Error reading {name} output: {e}")

    def stop_component(self, name: str):
        if name not in self.component_processes:
            log_warn(f"{name} is not running")
            return
        process = self.component_processes[name]
        if process.poll() is None:
            log_info(f"Stopping {name}...")
            process.terminate()
            try:
                process.wait(timeout=5)
                log_success(f"Stopped {name}")
            except subprocess.TimeoutExpired:
                log_warn(f"Force killing {name}")
                process.kill()
                process.wait()
        del self.component_processes[name]
        self.active_components.discard(name)

    def stop_all_components(self):
        for name in list(self.component_processes.keys()):
            self.stop_component(name)

    def auto_start_components(self):
        started = []
        for comp in self._platforms:
            if self._can_start(comp) and comp in self.available_components:
                if self.start_component(comp):
                    started.append(comp)
                    time.sleep(2)
        if not started:
            log_warn("No components could be started -- check your configuration.")
        else:
            log_success(f"Auto-started: {', '.join(started)}")

    # ----------------------------------------------------------------
    #  Auto-restart (periodic component bounce during idle window)
    # ----------------------------------------------------------------

    def _auto_restart_loop(self):
        """Background thread that bounces all components every N hours.

        - Sleeps for `auto_restart_timer` hours after startup (and after
          each restart).
        - Then polls every 30s waiting for a quiet window: no routed
          message for `auto_restart_clear_msg` minutes.
        - If `auto_restart_max_wait` hours pass without a quiet window
          appearing, force-restarts anyway so busy bridges still get
          their periodic cache flush.
        - Honors `self.running` so it exits cleanly on shutdown.
        """
        timer_hours = self._arbiter_config.get('auto_restart_timer', 0)
        if not timer_hours or timer_hours <= 0:
            return  # feature disabled

        quiet_minutes = self._arbiter_config.get('auto_restart_clear_msg', 15)
        max_wait_hours = self._arbiter_config.get('auto_restart_max_wait', 12)
        timer_seconds = timer_hours * 3600
        quiet_seconds = quiet_minutes * 60
        max_wait_seconds = max_wait_hours * 3600

        log_info(
            f"Auto-restart armed: cycle every {timer_hours}h, "
            f"requires {quiet_minutes}m idle (max wait {max_wait_hours}h)."
        )

        while self.running:
            # Phase 1: wait the full timer window. Sleep in short chunks so
            # shutdown is responsive.
            cycle_start = time.monotonic()
            while self.running and (time.monotonic() - cycle_start) < timer_seconds:
                time.sleep(30)
            if not self.running:
                return

            # Phase 2: wait for a quiet window, up to max_wait_seconds.
            log_info(
                f"Auto-restart cycle elapsed ({timer_hours}h); waiting for "
                f"{quiet_minutes}m of idle traffic before bouncing components."
            )
            wait_start = time.monotonic()
            forced = False
            while self.running:
                idle_for = self._idle_seconds()
                if idle_for >= quiet_seconds:
                    break
                if (time.monotonic() - wait_start) >= max_wait_seconds:
                    forced = True
                    log_warn(
                        f"Auto-restart: no quiet window in {max_wait_hours}h, "
                        f"forcing restart with traffic still flowing."
                    )
                    break
                time.sleep(30)
            if not self.running:
                return

            # Phase 3: perform the restart. Catches its own errors so a
            # failed cycle doesn't kill the timer thread.
            try:
                self._perform_auto_restart(forced=forced)
            except Exception as exc:
                log_error(f"Auto-restart failed: {exc}")

    def _idle_seconds(self) -> float:
        """Seconds since the most recent routed message.

        Returns a large number when nothing has ever routed, so a fresh
        instance qualifies as 'idle' immediately after its timer expires.
        """
        last = self._last_route_time
        if last is None:
            return float('inf')
        return (datetime.now() - last).total_seconds()

    def _perform_auto_restart(self, forced: bool = False):
        """Stop all components, clear stale in-memory state, restart."""
        tag = "forced" if forced else "scheduled"
        with self.output_lock:
            log_info(f"Auto-restart ({tag}): stopping components for cache flush.")

        self.stop_all_components()

        # Clear arbiter's own in-memory caches so the dashboard starts
        # fresh too. We deliberately keep _routed_count cumulative since
        # uptime stats are more useful when continuous.
        self._route_log.clear()
        self._error_log.clear()
        self._warn_log.clear()
        self._error_counts = {p: 0 for p in self._platforms}
        self._warn_counts = {p: 0 for p in self._platforms}
        self._pending_counts.clear()
        self._last_route_time = None

        # Brief pause so subprocess exit / port release settles before
        # we spawn fresh processes that may try to grab the same handles.
        time.sleep(2)

        self.auto_start_components()
        with self.output_lock:
            log_success(f"Auto-restart ({tag}): components back up, caches cleared.")

    # ----------------------------------------------------------------
    #  Rich dashboard
    # ----------------------------------------------------------------

    def _build_dashboard(self):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        uptime = str(datetime.now() - self._start_time).split('.')[0]

        comp_table = Table(
            box=box.SIMPLE_HEAVY, show_header=True,
            header_style="bold white", expand=True, padding=(0, 1),
        )
        comp_table.add_column("", width=2, justify="center")
        comp_table.add_column("Component", style="white", min_width=12)
        comp_table.add_column("Status", width=10)
        comp_table.add_column("PID", width=7, justify="right")
        comp_table.add_column("E", width=3, justify="right", style="red")
        comp_table.add_column("W", width=3, justify="right", style="yellow")

        shown = set()
        for comp in sorted(self.active_components):
            st = self.component_status.get(comp, 'unknown')
            proc = self.component_processes.get(comp)
            pid = str(proc.pid) if proc and hasattr(proc, 'pid') else '--'
            e_count = self._error_counts.get(comp, 0)
            w_count = self._warn_counts.get(comp, 0)
            e_str = f"[red]{e_count}[/]" if e_count > 0 else "[dim]0[/]"
            w_str = f"[yellow]{w_count}[/]" if w_count > 0 else "[dim]0[/]"
            if st == 'ready':
                icon = "[green]*[/]"
                st_styled = f"[green]{st}[/]"
            elif st == 'error':
                icon = "[red]x[/]"
                st_styled = f"[red]{st}[/]"
            else:
                icon = "[yellow]o[/]"
                st_styled = f"[yellow]{st}[/]"
            comp_table.add_row(icon, comp, st_styled, pid, e_str, w_str)
            shown.add(comp)

        for comp in self.component_processes:
            if comp not in shown:
                proc = self.component_processes.get(comp)
                pid = str(proc.pid) if proc and hasattr(proc, 'pid') else '--'
                e_count = self._error_counts.get(comp, 0)
                w_count = self._warn_counts.get(comp, 0)
                e_str = f"[red]{e_count}[/]" if e_count > 0 else "[dim]0[/]"
                w_str = f"[yellow]{w_count}[/]" if w_count > 0 else "[dim]0[/]"
                comp_table.add_row("[yellow]~[/]", comp, "[yellow]starting[/]", pid, e_str, w_str)
                shown.add(comp)

        for comp in self.available_components:
            if comp not in shown:
                if not self._can_start(comp):
                    continue  # Don't show components without credentials
                comp_table.add_row("[dim]o[/]", f"[dim]{comp}[/]", "[dim]idle[/]", "[dim]--[/]", "[dim]0[/]", "[dim]0[/]")

        bridge_table = Table(
            box=box.SIMPLE_HEAVY, show_header=True,
            header_style="bold white", expand=True, padding=(0, 1),
        )
        bridge_table.add_column("", width=2, justify="center")
        bridge_table.add_column("Bridge", style="white")
        bridge_table.add_column("Route")
        bridge_table.add_column("Flags", justify="right")

        for name in self._routing_table:
            bridge_obj = (self.bridges or {}).get(name)
            rt_entry = self._routing_table[name]
            platforms = self._get_bridge_platforms(name)

            if len(platforms) >= 2:
                all_both = all(
                    rt_entry.get(f'{p}_mode', 'both') == 'both'
                    for p in platforms
                )
                if all_both:
                    route_str = ' <-> '.join(p.capitalize() for p in platforms)
                else:
                    senders = [p for p in platforms if rt_entry.get(f'{p}_mode', 'both') in ('send', 'both')]
                    receivers = [p for p in platforms if rt_entry.get(f'{p}_mode', 'both') in ('receive', 'both')]
                    if senders and receivers:
                        route_str = (
                            ', '.join(p.capitalize() for p in senders)
                            + ' -> '
                            + ', '.join(p.capitalize() for p in receivers)
                        )
                    else:
                        route_str = ' | '.join(p.capitalize() for p in platforms)
            else:
                route_str = ', '.join(p.capitalize() for p in platforms) if platforms else '--'

            flags = []
            if bridge_obj:
                ds = bridge_obj.get_platform('discord')
                tg = bridge_obj.get_platform('telegram')
                if ds and getattr(ds, 'use_webhooks', False):
                    flags.append("[green]webhook[/]")
                if tg and getattr(tg, 'topic_id', None):
                    label = getattr(tg, 'topic_label', '') or f'#{tg.topic_id}'
                    flags.append(f"[cyan]topic:{label}[/]")
            flags_str = ' '.join(flags) if flags else '[dim]--[/]'

            bridge_table.add_row("[blue]>[/]", name, route_str, flags_str)

        if not self.bridges and not self._routing_table:
            bridge_table.add_row("[yellow]--[/]", "[dim]No bridges configured[/]", "", "")

        pending_rich = Text()
        pending_rich.append("  Pending  ", style="bold white")
        for plat, cnt in self._pending_counts.items():
            style = "yellow" if cnt > 0 else "dim"
            pending_rich.append(f"{plat}:{cnt}  ", style=style)
        if not self._pending_counts:
            pending_rich.append("none", style="dim")

        activity_lines = []
        if self._error_view_active and time.time() < self._error_view_until:
            error_overlay = self._build_error_overlay(platform_filter=self._error_view_filter)
            activity_lines = error_overlay.split('\n') if error_overlay else ["  [dim]No errors.[/]"]
            filter_label = self._error_view_filter if self._error_view_filter != 'all' else 'all'
            activity_title = f"[bold red]Error Log[/] [dim]({filter_label} - frozen)[/]"
        else:
            self._error_view_active = False
            anim_bar = self._get_animated_bar()
            for entry in list(self._route_log):
                t = entry['time'].strftime('%H:%M:%S')
                src = entry['source']
                tgts = ', '.join(entry['targets'])
                br = entry['bridge']
                auth = entry.get('author', '')
                mt = entry.get('msg_type', 'message')
                bar = f"[green]{anim_bar}[/]"
                type_tag = ''
                if mt == 'edit':
                    type_tag = ' [yellow]\\[edit][/]'
                elif mt == 'delete':
                    type_tag = ' [red]\\[delete][/]'
                activity_lines.append(
                    f"  [dim]{t}[/]  [bold cyan]>[/] {src} -> {tgts}  "
                    f"[dim]\\[{br}][/] [italic]{auth}[/]{type_tag}  {bar}"
                )
            if not activity_lines:
                activity_lines.append("  [dim]No messages routed yet.[/]")
            activity_title = "[bold]Recent Activity[/]"

        ns_label = self._namespace or 'default'
        mode_label = self._bridge_mode_label()

        stats_text = Text()
        stats_text.append("  Namespace  ", style="bold white")
        stats_text.append(f"{ns_label}", style="cyan")
        stats_text.append("    Debug  ", style="bold white")
        stats_text.append(
            f"{'on' if self._debug_mode else 'off'}",
            style="yellow" if self._debug_mode else "dim",
        )
        stats_text.append("    Routed  ", style="bold white")
        stats_text.append(f"{self._routed_count}", style="green")
        stats_text.append("    Uptime  ", style="bold white")
        stats_text.append(f"{uptime}", style="cyan")

        mode_text = Text()
        mode_text.append("  Mode  ", style="bold white")
        mode_text.append(f"{mode_label}", style="cyan")
        mode_text.append("    Queues  ", style="bold white")
        mode_text.append(f"{len(self._all_queues)} declared", style="dim")
        mode_text.append("    Terminal  ", style="bold white")
        mode_text.append(
            f"{self._terminal_type}",
            style="green" if self._terminal_type == 'modern' else "yellow",
        )

        try:
            term_height = self._get_usable_height()
            term_width = os.get_terminal_size().columns
        except (OSError, ValueError):
            term_height = 40
            term_width = 120

        narrow = term_width < 100

        # outer_chrome = 2 accounts for the dashboard Panel's actual top
        # and bottom border rows (box.DOUBLE). Anything larger leaks out
        # as visible empty rows below the dashboard and also pushes the
        # bottom border off the visible area in pinned mode. With this
        # set to 2, dashboard_rows + hint exactly equals term_height,
        # so Mode sits flush against the bottom and both DOUBLE borders
        # are visible at the terminal edges.
        outer_chrome = 2
        hint_line = 1
        fixed_rows = 3 + 3 + 3
        available = term_height - outer_chrome - hint_line - fixed_rows

        # Only count components that can actually start (have credentials)
        startable = [c for c in self.available_components if self._can_start(c)]
        num_comps = max(len(self.active_components), len(self.component_processes), len(startable))
        num_bridges = max(len(self._routing_table), 1)

        # Per-panel chrome: 2 panel borders + 2 table chrome (header + separator)
        # + 1 slack row that Rich consumes for SIMPLE_HEAVY/ROUNDED. Total 5.
        per_table_chrome = 5
        comp_ideal = num_comps + per_table_chrome
        bridge_ideal = num_bridges + per_table_chrome

        # Priority allocation: components > bridges > activity. Activity has
        # NO floor - if components and bridges claim all the room, activity
        # disappears entirely. When room is plentiful, it takes the rest.
        if narrow:
            comp_h = min(comp_ideal, available)
            bridge_h = min(bridge_ideal, max(0, available - comp_h))
            tables_height = comp_h + bridge_h
            activity_space = max(0, available - tables_height)
        else:
            # Side-by-side: both tables share one row height.
            tables_height = min(max(comp_ideal, bridge_ideal), available)
            tables_height = max(2, tables_height)
            activity_space = max(0, available - tables_height)

        # Activity sizing strategy:
        #  - Tables (Components/Bridges) are sized to their ideal height and
        #    never shrink for activity's sake.
        #  - Activity claims ALL leftover space between tables and Mode in
        #    one Panel. It does NOT grow with content - it always fills the
        #    empty room and just shows blank interior rows when entries are
        #    sparse.
        #  - Under severe cramping (< 3 rows leftover) activity disappears
        #    entirely and the orphan rows go back to tables, keeping the
        #    dashboard flush to the terminal bottom.
        if self._error_view_active and activity_space < 3:
            activity_space = 3
            show_activity = True
        elif activity_space < 3:
            # Donate orphan rows to tables so layout sum still equals
            # `available` and the dashboard fills the terminal.
            tables_height += activity_space
            activity_space = 0
            show_activity = False
        else:
            show_activity = True

        # Fit the line buffer to the visible interior. Rich's box.ROUNDED
        # Panel uses 2 chrome rows (top border with embedded title, bottom
        # border). Pad with blanks if we have too few lines so the Panel
        # reaches the full slot height - otherwise the unused rows of the
        # Layout slot appear as a visible gap ABOVE Mode.
        if show_activity and not self._error_view_active:
            visible = max(0, activity_space - 2)
            if len(activity_lines) > visible:
                activity_lines = activity_lines[-visible:]
            while len(activity_lines) < visible:
                activity_lines.append('')

        layout = Layout()
        sections = [
            Layout(name="stats", size=3),
            Layout(name="tables", size=tables_height),
            Layout(name="pending", size=3),
        ]
        if show_activity:
            sections.append(Layout(name="activity", size=activity_space))
        sections.append(Layout(name="mode", size=3))
        layout.split_column(*sections)

        layout["stats"].update(Panel(stats_text, box=box.SIMPLE, style=""))
        layout["pending"].update(Panel(pending_rich, box=box.SIMPLE, style=""))
        layout["mode"].update(Panel(mode_text, box=box.SIMPLE, style=""))

        comp_panel = Panel(
            comp_table, title="[bold]Components[/]",
            border_style="cyan", box=box.ROUNDED,
        )
        bridge_panel = Panel(
            bridge_table, title="[bold]Bridges[/]",
            border_style="cyan", box=box.ROUNDED,
        )

        tables_layout = Layout()
        if narrow:
            # Sized split: components gets its allocated height, bridges
            # takes the remainder. When bridges was squeezed to zero, drop
            # it entirely so components claims the whole row.
            if bridge_h > 0:
                tables_layout.split_column(
                    Layout(comp_panel, size=comp_h),
                    Layout(bridge_panel),
                )
            else:
                tables_layout.update(comp_panel)
        else:
            tables_layout.split_row(
                Layout(comp_panel),
                Layout(bridge_panel),
            )
        layout["tables"].update(tables_layout)

        if show_activity:
            activity_border = "red" if self._error_view_active else "cyan"
            activity_str = '\n'.join(activity_lines)
            layout["activity"].update(
                Panel(
                    activity_str, title=activity_title,
                    border_style=activity_border, box=box.ROUNDED,
                )
            )

        # Explicit height forces Rich to render the Panel at exactly this
        # many rows regardless of what the inner Layout decides to do.
        # dashboard_height + hint(1) = term_height, so the bottom DOUBLE
        # border lands one row above the terminal edge with the hint on the
        # final visible row.
        dashboard_height = term_height - 1
        dashboard = Panel(
            layout,
            title="[bold white] ARBITER DASHBOARD [/]",
            subtitle=f"[dim]{ts}[/]",
            border_style="cyan",
            box=box.DOUBLE,
            padding=(0, 1),
            height=dashboard_height,
        )
        return dashboard

    def _bridge_mode_label(self) -> str:
        active_platforms = [p for p in self._platforms if p in self.active_components]
        if len(active_platforms) >= 2:
            return ' <-> '.join(p.capitalize() for p in active_platforms)
        elif len(active_platforms) == 1:
            p = active_platforms[0]
            others = [x for x in self._platforms if x != p]
            missing = ', '.join(x.capitalize() for x in others) if others else 'peers'
            return f"{p.capitalize()} only (no {missing})"
        return "No active bridge"

    def print_status(self):
        with self.output_lock:
            if self._headless:
                self._print_status_plain()
            else:
                console.print(self._build_dashboard())
                console.print("  [dim]Type /help for commands.[/]\n")

    def _print_status_plain(self):
        """Plain-text status for headless mode."""
        ts = datetime.now().strftime('%H:%M:%S')
        uptime = str(datetime.now() - self._start_time).split('.')[0]
        print(f"\n=== CHATAFT Status [{ts}] uptime={uptime} routed={self._routed_count} ===")
        print("  Components:")
        for comp in sorted(self.active_components):
            st = self.component_status.get(comp, '?')
            pid = self.component_processes.get(comp)
            pid_str = str(pid.pid) if pid else '--'
            e = self._error_counts.get(comp, 0)
            w = self._warn_counts.get(comp, 0)
            print(f"    {comp:<14} {st:<10} PID={pid_str:<7} E={e} W={w}")
        startable = [c for c in self.available_components if self._can_start(c)]
        for comp in startable:
            if comp not in self.active_components and comp not in self.component_processes:
                print(f"    {comp:<14} idle")
        print("  Bridges:")
        for name, targets in self._routing_table.items():
            platforms = sorted({t.split('_')[0] for t in targets})
            print(f"    {name}: {' <-> '.join(p.capitalize() for p in platforms)}")
        print()

    def _pin_dashboard(self):
        self._pin_active = True

        if sys.platform == 'win32':
            import msvcrt
            check_key = lambda: msvcrt.kbhit()
            read_key = lambda: msvcrt.getch()
        else:
            import select
            import tty
            import termios
            old_settings = termios.tcgetattr(sys.stdin)
            tty.setcbreak(sys.stdin.fileno())
            check_key = lambda: select.select([sys.stdin], [], [], 0)[0]
            read_key = lambda: sys.stdin.read(1).encode()

        is_modern = self._terminal_type == 'modern'
        refresh = self._pin_refresh_interval

        self._clear_screen()

        def _build_hint():
            # Command mode: show the command buffer with cursor
            if self._cmd_mode:
                cursor = '█' if int(time.monotonic() * 2) % 2 == 0 else ' '
                return f"  [bold green]> {self._cmd_buffer}{cursor}[/]"
            # Flash mode: show temporary result message
            if self._cmd_flash and time.monotonic() < self._cmd_flash_until:
                return f"  [bold yellow]{self._cmd_flash}[/]"
            # Normal mode: show key hints. [E] and [C] are always present
            # so the affordance is discoverable even when the error log is
            # empty; the error count is appended when nonzero so the user
            # can see at a glance whether anything is wrong.
            self._cmd_flash = ''
            hints = [r"\[/] cmd", "[Q] exit"]
            total_err = sum(self._error_counts.values())
            if total_err > 0:
                hints.append(f"[E] errors ({total_err})")
            else:
                hints.append("[E] errors")
            hints.append("[C] clear")
            return f"  [dim]{'  '.join(hints)}[/]"

        def _render_normal():
            dashboard = self._build_dashboard()
            return self._render_frame(dashboard, hint=_build_hint())

        def _render_flash():
            saved = self._error_view_active
            self._error_view_active = False
            dashboard = self._build_dashboard()
            self._error_view_active = saved
            buf = StringIO()
            width = console.width or 120
            try:
                height = self._get_usable_height()
            except (OSError, ValueError):
                height = 40
            buf_console = Console(
                file=buf, force_terminal=True, highlight=False,
                width=width, height=height, color_system=console.color_system,
            )
            buf_console.print(dashboard)
            buf_console.print("  [bold white on red] >> LOADING ERROR LOG >> [/]")
            output = buf.getvalue()
            lines = output.split('\n')
            if len(lines) > height:
                lines = lines[:height]
            return '\n'.join(lines)

        try:
            while self.running and self._pin_active:
                frame = _render_normal()
                self._write_frame(frame)

                if is_modern:
                    self._advance_animation()

                wait_start = time.monotonic()
                while time.monotonic() - wait_start < refresh:
                    if check_key():
                        key = read_key()

                        # ── Command mode ──
                        if self._cmd_mode:
                            if key in (b'\r', b'\n'):
                                # Execute the command
                                cmd_text = self._cmd_buffer.strip()
                                self._cmd_mode = False
                                self._cmd_buffer = ''
                                if cmd_text:
                                    result = self._dispatch_command(cmd_text)
                                    if result:
                                        # Truncate multi-line output to first line for flash
                                        lines = result.strip().splitlines()
                                        if len(lines) > 1:
                                            self._cmd_flash = lines[0] + f' (+{len(lines)-1} lines)'
                                        else:
                                            self._cmd_flash = result.strip()
                                        self._cmd_flash_until = time.monotonic() + 3.0
                                break

                            elif key == b'\x1b':
                                # Escape: cancel command mode, drain escape sequence
                                self._cmd_mode = False
                                self._cmd_buffer = ''
                                # Drain any trailing escape sequence bytes
                                for _ in range(6):
                                    if check_key():
                                        read_key()
                                    else:
                                        break
                                break

                            elif key in (b'\x08', b'\x7f'):
                                # Backspace: delete last char or exit cmd mode
                                if len(self._cmd_buffer) > 1:
                                    self._cmd_buffer = self._cmd_buffer[:-1]
                                else:
                                    self._cmd_mode = False
                                    self._cmd_buffer = ''
                                break

                            else:
                                # Printable char: append to buffer
                                try:
                                    ch = key.decode('utf-8', errors='ignore')
                                    if ch and ch.isprintable():
                                        self._cmd_buffer += ch
                                except Exception:
                                    pass
                                break

                        # ── Normal mode ──
                        if key in (b'q', b'Q'):
                            self._pin_active = False
                            break

                        elif key == b'\x1b':
                            # Escape in normal mode: exit pin
                            self._pin_active = False
                            # Drain any trailing escape sequence bytes
                            for _ in range(6):
                                if check_key():
                                    read_key()
                                else:
                                    break
                            break

                        elif key == b'/':
                            # Enter command mode
                            self._cmd_mode = True
                            self._cmd_buffer = '/'
                            break

                        elif key in (b'c', b'C'):
                            # Wait for platform key: d/t/m/s/a
                            try:
                                term_h = self._get_usable_height()
                            except (OSError, ValueError):
                                term_h = 40
                            prompt_start = time.monotonic()
                            clear_filter = None
                            while time.monotonic() - prompt_start < 2.0:
                                p_buf = StringIO()
                                p_con = Console(
                                    file=p_buf, force_terminal=True,
                                    highlight=False, width=console.width or 120,
                                    height=term_h,
                                    color_system=console.color_system,
                                )
                                p_con.print(self._build_dashboard())
                                p_con.print(
                                    "  [bold yellow]Clear errors:[/] "
                                    "[dim]d[/]=discord  [dim]t[/]=telegram  "
                                    "[dim]m[/]=matrix  [dim]s[/]=stoat  [dim]a[/]=all"
                                )
                                out = p_buf.getvalue().split('\n')
                                if len(out) > term_h:
                                    out = out[:term_h]
                                self._write_frame('\n'.join(out))
                                for _ in range(5):
                                    if check_key():
                                        pk = read_key().lower()
                                        if pk in (b'd', b't', b'm', b's', b'a'):
                                            clear_filter = self._ERROR_PLATFORM_KEYS.get(pk.decode(), 'all')
                                    if clear_filter:
                                        break
                                    time.sleep(0.05)
                                if clear_filter:
                                    break
                            if clear_filter:
                                self._clear_issues(clear_filter)
                            else:
                                self._clear_issues('all')
                            self._error_view_active = False
                            self._clear_screen()
                            break

                        elif key in (b'e', b'E'):
                            # Always show the platform-selection prompt, even
                            # when the error log is empty - mirrors [C] clear's
                            # behavior. The error overlay itself handles the
                            # "no errors recorded" case with a friendly message.
                            flash_frame = _render_flash()
                            self._write_frame(flash_frame)
                            time.sleep(0.2)

                            self._error_view_filter = 'all'

                            try:
                                term_h = self._get_usable_height()
                            except (OSError, ValueError):
                                term_h = 40

                            # Wait for platform key: d/t/m/s/a
                            prompt_start = time.monotonic()
                            chosen = None
                            while time.monotonic() - prompt_start < 2.0:
                                digit_buf = StringIO()
                                digit_console = Console(
                                    file=digit_buf, force_terminal=True,
                                    highlight=False, width=console.width or 120,
                                    height=term_h,
                                    color_system=console.color_system,
                                )
                                digit_console.print(
                                    self._build_dashboard(),
                                )
                                digit_console.print(
                                    f"  [bold yellow]Show errors:[/] "
                                    f"[dim]d[/]=discord  [dim]t[/]=telegram  "
                                    f"[dim]m[/]=matrix  [dim]s[/]=stoat  [dim]a[/]=all"
                                )
                                out = digit_buf.getvalue()
                                out_lines = out.split('\n')
                                if len(out_lines) > term_h:
                                    out_lines = out_lines[:term_h]
                                self._write_frame('\n'.join(out_lines))

                                for _ in range(5):
                                    if check_key():
                                        dk = read_key().lower()
                                        if dk in (b'd', b't', b'm', b's', b'a'):
                                            self._error_view_filter = self._ERROR_PLATFORM_KEYS.get(dk.decode(), 'all')
                                            chosen = True
                                        elif dk in (b'\r', b'\n', b' '):
                                            chosen = True
                                    if chosen:
                                        break
                                    time.sleep(0.05)
                                if chosen:
                                    break

                            self._error_view_active = True
                            self._error_view_until = time.time() + 5.0
                            break

                    time.sleep(0.025 if is_modern else 0.05)

        except Exception as e:
            log_warn(f"Pin mode error: {e}")
        finally:
            self._pin_active = False
            self._error_view_active = False
            self._cmd_mode = False
            self._cmd_buffer = ''
            self._cmd_flash = ''
            if self._terminal_type == 'modern':
                sys.stdout.write('\033[?25h')
                sys.stdout.flush()
            if sys.platform != 'win32':
                try:
                    termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
                except Exception:
                    pass
            self._clear_screen()
            self._print_banner()
            log_info("Dashboard unpinned -- command mode restored.")

    # ----------------------------------------------------------------
    #  Monitor loop
    # ----------------------------------------------------------------

    def monitor_loop(self):
        heartbeat_count = 0
        depth_refresh_count = 0
        _last_successful_event = time.time()
        _CONN_DEAD_THRESHOLD = 60  # force reconnect if no activity for 1 min
        while self.running:
            try:
                if self.connection and not self.connection.is_closed:
                    self.connection.process_data_events(time_limit=1.0)
                    _last_successful_event = time.time()
                else:
                    time.sleep(0.5)

                # Auto-restart crashed components
                for name, process in list(self.component_processes.items()):
                    if process.poll() is not None:
                        rc = process.poll()
                        log_warn(f"{name} exited (rc={rc}), auto-restart in 5s")
                        del self.component_processes[name]
                        self.active_components.discard(name)
                        self.component_status.pop(name, None)
                        threading.Thread(
                            target=self._delayed_restart_component,
                            args=(name, 5),
                            daemon=True
                        ).start()

                depth_refresh_count += 1
                if depth_refresh_count >= 100:
                    depth_refresh_count = 0
                    self._refresh_all_queue_depths_unsafe()

                heartbeat_count += 1
                if heartbeat_count > 60:
                    heartbeat_count = 0
                    conn_dead = (
                        self.connection is None
                        or self.connection.is_closed
                        or (time.time() - _last_successful_event > _CONN_DEAD_THRESHOLD)
                    )
                    if conn_dead:
                        log_error("RabbitMQ connection lost or stale, attempting to reconnect...")
                        try:
                            if self.connection and not self.connection.is_closed:
                                self.connection.close()
                        except Exception:
                            pass
                        self.connection = None
                        self.channel = None
                        if self.connect_rabbitmq():
                            self.start_status_consumer()
                            self.start_routing_consumers()
                            _last_successful_event = time.time()

                    # Reconcile active_components against recent status traffic.
                    # If the subprocess is alive and we've heard *anything* from
                    # it in the last 180s, treat it as active. Covers the case
                    # where the initial 'ready' raced with consumer attach and
                    # only later heartbeats made it through.
                    now = time.time()
                    for name, process in list(self.component_processes.items()):
                        if process.poll() is not None:
                            continue
                        if name in self.active_components:
                            continue
                        last_heard = self._last_status_heard.get(name)
                        if last_heard and (now - last_heard) < 180:
                            self.active_components.add(name)
                            if self.component_status.get(name) not in ('ready', 'error'):
                                self.component_status[name] = 'ready'
                            with self.output_lock:
                                log_info(f"Reconciled {name} as active (recent status traffic)")

                    # Check for silent components (process alive but no status updates)
                    for name, process in list(self.component_processes.items()):
                        if process.poll() is not None:
                            continue  # Already dead, handled by auto-restart above
                        last_heard = self._last_status_heard.get(name)
                        if last_heard and (now - last_heard) > self._COMPONENT_SILENT_THRESHOLD:
                            log_warn(
                                f"{name} has been silent for "
                                f"{int(now - last_heard)}s (process alive but no status updates). "
                                f"Possible stale RabbitMQ consumer - restarting..."
                            )
                            self.stop_component(name)
                            threading.Thread(
                                target=self._delayed_restart_component,
                                args=(name, 5),
                                daemon=True,
                            ).start()

                time.sleep(0.05)
            except (pika.exceptions.StreamLostError, pika.exceptions.ConnectionClosedByBroker, pika.exceptions.AMQPConnectionError) as e:
                log_warn(f"Monitor loop connection error: {e}, reconnecting...")
                self.connection = None
                self.channel = None
                try:
                    if self.connect_rabbitmq():
                        self.start_status_consumer()
                        self.start_routing_consumers()
                        _last_successful_event = time.time()
                except Exception:
                    pass
                time.sleep(2)
            except Exception as e:
                if self.running:
                    es = str(e).lower()
                    if not any(k in es for k in ('connection reset', 'heartbeat', 'empty deque')):
                        log_error(f"Monitor loop error: {e}")
                time.sleep(0.5)

    def _delayed_restart_component(self, name: str, delay: float):
        time.sleep(delay)
        if self.running and name not in self.component_processes:
            with self.output_lock:
                log_info(f"Auto-restarting {name}...")
            self.start_component(name)

    # ----------------------------------------------------------------
    #  Command dispatch (shared by handle_commands and pin-mode bar)
    # ----------------------------------------------------------------

    def _dispatch_command(self, cmd: str) -> Optional[str]:
        """Execute a command and return display text, or None for silent commands."""
        cmd = cmd.strip()
        if not cmd:
            return None
        cmd_lower = cmd.lower()
        parts = cmd.split()

        if cmd_lower == '/status':
            return None  # no-op in pin mode; handle_commands calls print_status directly

        if cmd_lower in ('/stop', '/exit', '/quit') and len(parts) == 1:
            log_info("Stopping arbiter...")
            self.running = False
            self._pin_active = False
            return None

        if cmd_lower == '/pin':
            return None  # no-op if already pinned

        if cmd_lower.startswith('/start'):
            if len(parts) == 1:
                self.auto_start_components()
                return 'Starting all components...'
            comp = parts[1].lower()
            if comp in self.available_components:
                self.start_component(comp)
                return f'Starting {comp}...'
            return f'Unknown component: {comp}'

        if cmd_lower.startswith('/stop') and len(parts) > 1:
            comp = parts[1].lower()
            if comp in self.available_components:
                self.stop_component(comp)
                return f'Stopped {comp}'
            return f'Unknown component: {comp}'

        if cmd_lower == '/restart':
            log_info("Restarting all components...")
            self.stop_all_components()
            time.sleep(2)
            self._arbiter_config = self._load_arbiter_config()
            self.auto_start_components()
            return 'Restarting all components...'

        if cmd_lower == '/reload':
            self.bridges = load_all_gateways()
            self._routing_table = self._build_routing_table()
            self._init_citadel()
            return 'Configuration reloaded'

        if cmd_lower == '/debug':
            self._debug_mode = not self._debug_mode
            state = 'ON' if self._debug_mode else 'OFF'
            return f'Debug mode: {state}'

        if cmd_lower.startswith('/clearerrors'):
            parts_ce = cmd_lower.split()
            if len(parts_ce) > 1 and parts_ce[1] in self._ERROR_PLATFORM_KEYS:
                pf = self._ERROR_PLATFORM_KEYS[parts_ce[1]]
                self._clear_issues(pf)
                return f'Cleared errors for {pf}'
            self._clear_issues('all')
            return 'Error and warning logs cleared'

        if cmd_lower.startswith('/avatar'):
            return self._dispatch_avatar_command(parts)

        if cmd_lower.startswith('/ignore_domain'):
            if len(parts) < 2:
                return 'Usage: /ignore_domain <domain>'
            domain = parts[1].strip()
            if add_ignored_domain(domain):
                return f'Domain ignored: {domain}'
            return f'Domain already ignored or invalid: {domain}'

        if cmd_lower.startswith('/download_domain'):
            if len(parts) < 2:
                return 'Usage: /download_domain <domain>'
            domain = parts[1].strip()
            if add_downloadable_domain(domain):
                return f'Domain added to download list: {domain}'
            return f'Domain already listed or invalid: {domain}'

        if cmd_lower.startswith('/ignore_user'):
            if len(parts) < 3:
                return 'Usage: /ignore_user <platform> <user_id>'
            platform = parts[1].strip().lower()
            uid = parts[2].strip()
            if platform not in self._platforms:
                return f'Platform must be one of: {", ".join(self._platforms)}'
            if add_ignored_user(platform, uid):
                return f'User ignored: {platform}:{uid}'
            return f'User already ignored or invalid: {platform}:{uid}'

        if cmd_lower.startswith('/errors'):
            pf = 'all'
            if len(parts) > 1:
                key = parts[1].lower()
                if key in self._ERROR_PLATFORM_KEYS:
                    pf = self._ERROR_PLATFORM_KEYS[key]
                elif key in self._ERROR_PLATFORM_KEYS.values():
                    pf = key
                else:
                    return f'Usage: /errors [d|t|m|s|a]  ({", ".join(f"{k}={v}" for k, v in self._ERROR_PLATFORM_KEYS.items())})'
            total_err = sum(self._error_counts.values())
            if total_err == 0:
                return 'No errors recorded.'
            overlay_text = self._build_error_overlay(platform_filter=pf)
            if self._headless:
                return _RICH_MARKUP_RE.sub('', f"--- Error Log ({total_err} total) ---\n{overlay_text}")
            return self._capture_rich(
                Panel(
                    overlay_text,
                    title=f"[bold red]Error Log[/] [dim]({total_err} total)[/]",
                    border_style="red", box=box.ROUNDED,
                )
            )

        if cmd_lower == '/queues':
            if self._headless:
                lines = ["  Queues:"]
                for k, v in sorted(self.queues.items()):
                    depth = self._cached_queue_depths.get(v, 0)
                    lines.append(f"    {k:<30} {v:<40} depth={depth}")
                return '\n'.join(lines)
            q_table = Table(
                title="Declared Queues", box=box.ROUNDED,
                border_style="cyan", show_header=True,
                header_style="bold white",
            )
            q_table.add_column("Key", style="green")
            q_table.add_column("Queue Name", style="white")
            q_table.add_column("Depth", justify="right")
            for k, v in sorted(self.queues.items()):
                depth = self._cached_queue_depths.get(v, 0)
                depth_style = "yellow" if depth > 0 else "dim"
                q_table.add_row(k, v, f"[{depth_style}]{depth}[/]")
            return self._capture_rich(q_table)

        if cmd_lower.startswith('/clear_db'):
            return self._dispatch_clear_db(parts)

        if cmd_lower == '/help':
            if self._headless:
                # Build plain-text help inline
                lines = ["  Commands:"]
                for name, desc in [
                    ('/status', 'Show current system status'),
                    ('/start [component]', 'Start all or specific component'),
                    ('/stop [component]', 'Stop all or specific component'),
                    ('/restart', 'Restart all components'),
                    ('/reload', 'Reload bridge configs'),
                    ('/errors [d|t|m|s|a]', 'Show errors by platform'),
                    ('/clearerrors [d|t|m|s|a]', 'Clear errors by platform'),
                    ('/queues', 'Show queue names and depths'),
                    ('/avatar list|clear <id|all>', 'Manage avatar cache'),
                    ('/clear_db <bridge|all> [plat]', 'Clear bridge database'),
                    ('/exit', 'Exit the arbiter'),
                ]:
                    lines.append(f"    {name:<38} {desc}")
                return '\n'.join(lines)
            return self._capture_rich_help()

        return f'Unknown command: {cmd}  --  type /help'

    def _capture_rich(self, renderable) -> str:
        """Render a Rich object to a string for display."""
        if not HAS_RICH:
            return _RICH_MARKUP_RE.sub('', str(renderable))
        buf = StringIO()
        c = Console(file=buf, force_terminal=False, highlight=False, width=console.width or 120)
        c.print(renderable)
        return buf.getvalue().rstrip()

    def _capture_rich_help(self) -> str:
        """Capture help output as string."""
        buf = StringIO()
        c = Console(file=buf, force_terminal=False, highlight=False, width=console.width or 120)
        help_table = Table(
            title="Commands", box=box.ROUNDED,
            border_style="cyan", show_header=True, header_style="bold white",
        )
        help_table.add_column("Command", style="green", min_width=36)
        help_table.add_column("Description", style="white")
        cmds = [
            ('/start [component]',              'Start all or a specific component'),
            ('/stop [component]',               'Stop all or a specific component'),
            ('/restart',                        'Restart all components'),
            ('/reload',                         'Reload bridge configs'),
            ('/debug',                          'Toggle debug mode'),
            ('/clearerrors [d|t|m|s|a]',         'Clear errors (per-platform or all)'),
            ('/queues',                         'Show queue names and depths'),
            ('/errors [d|t|m|s|a]',             'Show errors by platform'),
            ('/avatar clear <id|all>',          'Clear avatar cache'),
            ('/avatar list',                    'List cached avatars'),
            ('/ignore_domain <domain>',         'Add domain to ignore list'),
            ('/download_domain <domain>',       'Add domain to download list'),
            ('/ignore_user <plat> <id>',        'Ignore a user'),
            ('/clear_db <bridge|all> [plat]',   'Clear bridge database'),
            ('/exit',                           'Exit the arbiter'),
        ]
        for name, desc in cmds:
            help_table.add_row(name, desc)
        c.print(help_table)
        return buf.getvalue().rstrip()

    def _dispatch_avatar_command(self, parts) -> str:
        """Handle /avatar subcommands, return result text."""
        if len(parts) < 2:
            return 'Usage: /avatar clear <user_id> | /avatar clear all | /avatar list'

        action = parts[1].lower()

        if action == 'list':
            try:
                db = get_avatar_db()
                rows = db.execute(
                    "SELECT user_id, platform, avatar_hash, discord_url, stoatchat_url "
                    "FROM avatar_cache ORDER BY user_id"
                ).fetchall()
                if not rows:
                    return 'Avatar cache is empty.'
                if self._headless:
                    lines = [f"  {'User ID':<20} {'Platform':<12} {'Hash':<12} {'Discord URL':<30} {'Stoat URL':<30}"]
                    for uid, plat, ahash, durl, surl in rows:
                        ahash_short = (ahash or '')[:10] if ahash else '--'
                        durl_short = (durl[:28] + '..') if durl and len(durl) > 28 else (durl or '--')
                        surl_short = (surl[:28] + '..') if surl and len(surl) > 28 else (surl or '--')
                        lines.append(f"  {uid:<20} {plat or '--':<12} {ahash_short:<12} {durl_short:<30} {surl_short:<30}")
                    lines.append(f"  {len(rows)} entries total.")
                    return '\n'.join(lines)
                a_table = Table(
                    box=box.ROUNDED, border_style="cyan",
                    show_header=True, header_style="bold white",
                )
                a_table.add_column("User ID", style="white")
                a_table.add_column("Platform", style="cyan")
                a_table.add_column("Hash", style="dim", max_width=12)
                a_table.add_column("Discord URL", style="green", max_width=30)
                a_table.add_column("Stoatchat URL", style="green", max_width=30)
                for uid, plat, ahash, durl, surl in rows:
                    ahash_short = (ahash or '')[:10] + '..' if ahash and len(ahash) > 10 else (ahash or '--')
                    durl_short = (durl[:28] + '..') if durl and len(durl) > 28 else (durl or '--')
                    surl_short = (surl[:28] + '..') if surl and len(surl) > 28 else (surl or '--')
                    a_table.add_row(uid, plat or '--', ahash_short, durl_short, surl_short)
                return self._capture_rich(a_table) + f'\n  {len(rows)} entries total.'
            except Exception as e:
                return f'Failed to list avatar cache: {e}'

        if action != 'clear':
            return f'Unknown avatar action: {action}'

        if len(parts) < 3:
            return 'Usage: /avatar clear <user_id> | /avatar clear all'

        target = parts[2].strip()
        try:
            db = get_avatar_db()
            if target.lower() == 'all':
                count = db.execute("SELECT COUNT(*) FROM avatar_cache").fetchone()[0]
                db.execute("DELETE FROM avatar_cache")
                db.commit()
                return f'Cleared entire avatar cache ({count} entries)'
            existing = db.execute(
                "SELECT user_id FROM avatar_cache WHERE user_id = ?", (target,)
            ).fetchone()
            if existing:
                db.execute("DELETE FROM avatar_cache WHERE user_id = ?", (target,))
                db.commit()
                return f'Cleared avatar cache for: {target}'
            return f'No avatar cache entry found for: {target}'
        except Exception as e:
            return f'Failed to clear avatar cache: {e}'

    def _dispatch_clear_db(self, parts) -> str:
        """Handle /clear_db, return result text."""
        import sqlite3
        available = sorted(self.bridges.keys()) if self.bridges else []
        known_platforms = ('telegram', 'discord', 'stoatchat', 'matrix')

        if len(parts) < 2:
            lines = [
                'Usage:',
                '  /clear_db <bridge>              Clear all message data',
                '  /clear_db <bridge> <platform>   Clear per-platform rows',
                '  /clear_db all                   Clear all bridge databases',
                '  /clear_db all <platform>        Clear platform rows in all',
            ]
            if available:
                lines.append(f'  Bridges: {", ".join(available)}')
            lines.append(f'  Platforms: {", ".join(known_platforms)}')
            return '\n'.join(lines)

        target = parts[1].strip().lower()
        platform_filter = parts[2].strip().lower() if len(parts) >= 3 else None

        if platform_filter and platform_filter not in known_platforms:
            return f'Unknown platform: {platform_filter}. Must be one of: {", ".join(known_platforms)}'

        targets = available if target == 'all' else [target]
        results = []
        for bridge_name in targets:
            db = self.bridge_dbs.get(bridge_name)
            if not db:
                results.append(f'{bridge_name}: not found')
                continue
            try:
                if platform_filter:
                    db.clear_platform(platform_filter)
                    results.append(f'{bridge_name}: cleared {platform_filter} rows')
                else:
                    db.clear_all()
                    results.append(f'{bridge_name}: cleared all data')
            except Exception as e:
                results.append(f'{bridge_name}: error - {e}')
        return '\n'.join(results) if results else 'Nothing to clear'

    # ----------------------------------------------------------------
    #  Console commands
    # ----------------------------------------------------------------

    def handle_commands(self):
        while self.running:
            try:
                cmd = input().strip()
                if not cmd:
                    continue
                cmd_lower = cmd.lower()

                # /pin enters the pin loop directly (not available in headless)
                if cmd_lower == '/pin':
                    if self._headless:
                        print("  Pin mode is not available in headless mode.")
                        continue
                    self._pin_dashboard()
                    continue

                # /status uses rich print_status (not suitable for string capture)
                if cmd_lower == '/status':
                    self.print_status()
                    continue

                # /restart needs auto-pin logic in console context
                if cmd_lower == '/restart':
                    result = self._dispatch_command(cmd)
                    if result:
                        print(f"  {result}")
                    if not self._headless:
                        auto_pin = self._arbiter_config.get('auto_dashboard_pin', True)
                        if self._terminal_type == 'modern' and auto_pin:
                            log_info("Auto-entering pinned dashboard after restart...")
                            time.sleep(1)
                            self._pin_dashboard()
                    continue

                # Everything else delegates to _dispatch_command
                result = self._dispatch_command(cmd)
                if result:
                    print(result)

            except EOFError:
                self.running = False
            except KeyboardInterrupt:
                self.running = False

        else:
            console.print("  [dim]No rows to clear.[/]")

    def _print_help(self):
        cmds = [
            ('/status',                         'Show current system dashboard'),
            ('/pin',                            'Pin live dashboard (/ cmd, Q exit, E errors, C clear)'),
            ('/start',                          'Start all configured components'),
            ('/start <component>',              'Start a specific component'),
            ('/stop',                           'Stop all components'),
            ('/stop <component>',               'Stop a specific component'),
            ('/restart',                        'Restart all components'),
            ('/reload',                         'Reload bridge configs from codex + citadel'),
            ('/debug',                          'Toggle debug mode (verbose child output)'),
            ('/queues',                         'Show all declared queue names and depths'),
            ('/errors [d|t|m|s|a]',             'Show errors (d=discord, t=tg, m=matrix, s=stoat, a=all)'),
            ('/clearerrors [d|t|m|s|a]',        'Clear error/warning logs (per-platform or all)'),
            ('/avatar clear <id>',              'Clear avatar cache for a user'),
            ('/avatar clear all',               'Clear entire avatar cache'),
            ('/avatar list',                    'List all cached avatars'),
            ('/ignore_domain <domain>',         'Add a domain to the ignore list'),
            ('/download_domain <domain>',       'Add a domain to the download list'),
            ('/ignore_user <platform> <id>',    'Ignore a user on a given platform'),
            ('/clear_db <bridge|all> [platform]', 'Clear bridge database (optionally per-platform)'),
            ('/exit',                           'Exit the arbiter'),
            ('/help',                           'Show this help message'),
        ]

        if self._headless:
            print("\n  Commands:")
            for name, desc in cmds:
                print(f"    {name:<40} {desc}")
            platforms_str = ', '.join(self.available_components.keys())
            print(f"\n  Components: {platforms_str}")
            print(f"  Terminal: {self._terminal_type}  Mode: headless")
            print()
            return

        help_table = Table(
            title="Commands", box=box.ROUNDED,
            border_style="cyan", show_header=False, padding=(0, 2),
        )
        help_table.add_column("Command", style="green", min_width=36)
        help_table.add_column("Description", style="white")
        for name, desc in cmds:
            help_table.add_row(name, desc)

        console.print(help_table)
        platforms_str = ', '.join(self.available_components.keys())
        console.print(f"  [yellow]Components:[/] {platforms_str}")
        console.print(f"  [yellow]Terminal:[/] {self._terminal_type}  [yellow]Pin refresh:[/] {self._pin_refresh_interval}s")
        if self._terminal_type == 'modern':
            console.print("  [dim]Pin mode keybinds: [/] command bar, [E] error overlay, [C] clear, [Q] exit.[/]")
        console.print()

    def signal_handler(self, signum, frame):
        print()
        # Second Ctrl+C while already shutting down -> hard exit. Some teardown
        # paths (pika BlockingConnection.close on a half-dead socket, child
        # processes ignoring SIGTERM) can take a long time, and the user
        # should always be able to bail out.
        if not self.running:
            log_warn("Second interrupt received, exiting hard.")
            os._exit(130)
        log_info("Received interrupt signal, shutting down...")
        self.running = False
        self._pin_active = False
        # Raise so the blocking input() in handle_commands unwinds immediately
        # (PEP 475 restarts the syscall otherwise, leaving the arbiter wedged
        # until the user hits Enter).
        raise KeyboardInterrupt()

    def _wait_for_components(self, timeout: float = 30.0):
        started = set(self.component_processes.keys())
        if not started:
            return
        log_info(f"Waiting for components to initialize: {', '.join(sorted(started))}...")
        start = time.monotonic()
        last_progress = 0.0
        while time.monotonic() - start < timeout and self.running:
            ready = {c for c in started if self.component_status.get(c) == 'ready'}
            if ready >= started:
                log_success(f"All components ready ({', '.join(sorted(ready))})")
                return
            elapsed = time.monotonic() - start
            if elapsed - last_progress >= 5.0:
                last_progress = elapsed
                remaining = sorted(started - ready)
                log_info(f"Still waiting on: {', '.join(remaining)} ({int(elapsed)}s)")
            time.sleep(0.5)
        still_missing = sorted(started - {c for c in started if self.component_status.get(c) == 'ready'})
        if still_missing:
            log_warn(f"Timed out waiting for: {', '.join(still_missing)} (proceeding anyway)")

    # ----------------------------------------------------------------
    #  Main
    # ----------------------------------------------------------------

    def run(self):
        if not self._headless:
            self._ensure_console_size()
        self._print_banner()

        if not self.check_rabbitmq():
            log_error("Cannot start without RabbitMQ")
            if self._headless:
                print("  RabbitMQ is unreachable.")
                print("    Docker:  docker run -d -p 5672:5672 rabbitmq")
            else:
                console.print(
                    "\n  [red]RabbitMQ is unreachable.[/]\n"
                    "    Docker:  docker run -d -p 5672:5672 rabbitmq\n"
                    "    Windows: rabbitmq-service start"
                )
            self._hold_open()
            return

        if not self.connect_rabbitmq():
            log_error("Cannot start without RabbitMQ connection")
            self._hold_open()
            return

        self.running = True
        signal.signal(signal.SIGINT, self.signal_handler)

        self.start_status_consumer()
        self.start_routing_consumers()
        self._refresh_all_queue_depths_unsafe()

        monitor = threading.Thread(target=self.monitor_loop, daemon=True)
        monitor.start()

        # Periodic component bounce. Daemon so it dies with the arbiter.
        # Internally no-ops when auto_restart_timer is 0/blank.
        auto_restart = threading.Thread(target=self._auto_restart_loop, daemon=True)
        auto_restart.start()

        self.auto_start_components()

        self._wait_for_components(timeout=30.0)

        if self._headless:
            log_info("Headless mode -- no dashboard. Type /help for commands.")
        else:
            log_info(f"Terminal: {self._terminal_type.upper()} (refresh: {self._pin_refresh_interval}s)")

            auto_pin = self._arbiter_config.get('auto_dashboard_pin', True)

            if auto_pin:
                self.print_status()

            if self._terminal_type == 'modern' and auto_pin:
                log_info("Modern terminal detected -- auto-entering pinned dashboard. Press Q for command mode.")
                time.sleep(1)
                self._pin_dashboard()

        try:
            self.handle_commands()
        except KeyboardInterrupt:
            pass
        finally:
            self.running = False
            log_info("Shutting down...")
            self.stop_all_components()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            log_info("The Arbiter has concluded.")
            self._hold_open()

    def _print_banner(self):
        if self._headless:
            print("CHATAFT - The Arbiter (headless mode)")
            print(f"  Terminal: {self._terminal_type}")
            return
        banner_art = """
      >>       >======>     >=>>=>    >=> >===>>=====> >=======> >======>
     >>=>      >=>    >=>   >>   >=>  >=>      >=>     >=>       >=>    >=>
    >> >=>     >=>    >=>   >>    >=> >=>      >=>     >=>       >=>    >=>
   >=>  >=>    >> >==>      >==>>=>   >=>      >=>     >=====>   >> >==>
  >=====>>=>   >=>  >=>     >>    >=> >=>      >=>     >=>       >=>  >=>
 >=>      >=>  >=>    >=>   >>     >> >=>      >=>     >=>       >=>    >=>
>=>        >=> >=>      >=> >===>>=>  >=>      >=>     >=======> >=>      >=>
"""
        console.print(f"[bold cyan]{banner_art}[/]")
        console.print("[bold cyan]                      The Arbiter - Message Orchestrator[/]")
        terminal_hint = (
            "[green]enhanced[/] -- smooth rendering, animated dashboard"
            if self._terminal_type == 'modern'
            else "[yellow]legacy[/] -- basic rendering (use Windows Terminal for enhanced mode)"
        )
        console.print(f"  [dim]Terminal:[/] {terminal_hint}\n")

    @staticmethod
    def _hold_open():
        if sys.platform == 'win32':
            console.print("\n  [yellow]Press Enter to close this window...[/]")
            try:
                input()
            except (EOFError, KeyboardInterrupt):
                pass


if __name__ == '__main__':
    try:
        arbiter = Arbiter()
        arbiter.run()
    except Exception as exc:
        console.print(f"\n[red bold]\\[FATAL] {exc}[/]", highlight=False)
        import traceback
        traceback.print_exc()
        if sys.platform == 'win32':
            console.print("\n  [yellow]Press Enter to close this window...[/]")
            try:
                input()
            except (EOFError, KeyboardInterrupt):
                pass
