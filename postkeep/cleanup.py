"""Scheduled DB cleanup: prunes old message mappings and stale avatar cache rows.

Design:
- Message mappings are pruned per-bridge using a two-rule policy: keep at least
  the last N logical messages (groups) per bridge OR anything younger than M
  days, whichever preserves more history. This lets slow "art" channels keep
  years of history while firehose channels stay bounded by the age cap.
- Avatar cache is pruned by simple age on `last_checked`. Users returning after
  a long absence just get a re-fetch.
- A daemon thread schedules a single pass per day at CLEANUP_HOUR local time.

Uses SQLite window functions (ROW_NUMBER OVER), requires SQLite >= 3.25 (any
Python >= 3.9 on a mainstream OS ships this).
"""

import glob
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from typing import Optional

from postkeep.papyrus import log_info, log_warn, log_error, log_debug


def _open_ro_rw(db_path: str) -> Optional[sqlite3.Connection]:
    """Open a fresh, short-lived connection to `db_path` for cleanup work.

    Kept independent from any long-lived connections held by the components so
    a long DELETE doesn't fight with their transactions.
    """
    if not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path, timeout=30.0)
        # Wait up to 30s for the file lock rather than raising immediately.
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn
    except Exception as e:
        log_warn(f"[cleanup] could not open {db_path}: {e}", 'cleanup')
        return None


def cleanup_bridge_db(db_path: str, min_groups: int, max_age_days: int) -> dict:
    """Prune old message mappings from a single bridge DB.

    Rule (in SQL): delete a row IFF its group's most recent timestamp is older
    than the age cutoff AND the group is not among the last `min_groups` by
    recency. Both conditions must fail for a row to be kept.

    Returns a small stats dict for logging.
    """
    stats = {'db': db_path, 'groups_deleted': 0, 'rows_deleted': 0, 'error': None}
    conn = _open_ro_rw(db_path)
    if not conn:
        stats['error'] = 'db not found or unopenable'
        return stats

    age_cutoff = time.time() - (max_age_days * 24 * 3600)

    try:
        # Sanity: table exists?
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='message_map'"
        ).fetchone()
        if not exists:
            stats['error'] = 'message_map table missing'
            conn.close()
            return stats

        # Count groups & rows before, for reporting.
        rows_before = conn.execute("SELECT COUNT(*) FROM message_map").fetchone()[0]
        groups_before = conn.execute("SELECT COUNT(DISTINCT group_id) FROM message_map").fetchone()[0]

        # The delete: identify groups that fail BOTH the age check and the
        # "in the last N groups by recency" check, then delete every row
        # belonging to those groups.
        with conn:
            cur = conn.execute(
                """
                DELETE FROM message_map
                WHERE group_id IN (
                    SELECT group_id FROM (
                        SELECT group_id,
                               MAX(timestamp) AS last_ts,
                               ROW_NUMBER() OVER (ORDER BY MAX(timestamp) DESC) AS rn
                        FROM message_map
                        GROUP BY group_id
                    ) AS ranked
                    WHERE rn > ? AND last_ts < ?
                )
                """,
                (min_groups, age_cutoff),
            )
            stats['rows_deleted'] = cur.rowcount if cur.rowcount is not None else 0

        # Also age-prune media_groups (separate table, small, safe to trim).
        try:
            media_exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='media_groups'"
            ).fetchone()
            if media_exists:
                with conn:
                    conn.execute(
                        "DELETE FROM media_groups WHERE timestamp < ?",
                        (int(age_cutoff),),
                    )
        except Exception as e:
            log_debug(f"[cleanup] media_groups prune skipped for {db_path}: {e}", 'cleanup')

        rows_after = conn.execute("SELECT COUNT(*) FROM message_map").fetchone()[0]
        groups_after = conn.execute("SELECT COUNT(DISTINCT group_id) FROM message_map").fetchone()[0]

        stats['rows_before'] = rows_before
        stats['rows_after'] = rows_after
        stats['groups_before'] = groups_before
        stats['groups_after'] = groups_after
        stats['groups_deleted'] = groups_before - groups_after
    except Exception as e:
        stats['error'] = str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return stats


def cleanup_avatar_db(db_path: str, retention_days: int) -> dict:
    """Delete avatar cache rows whose `last_checked` is older than the retention."""
    stats = {'db': db_path, 'rows_deleted': 0, 'error': None}
    conn = _open_ro_rw(db_path)
    if not conn:
        stats['error'] = 'db not found or unopenable'
        return stats

    age_cutoff = int(time.time() - (retention_days * 24 * 3600))

    try:
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='avatar_cache'"
        ).fetchone()
        if not exists:
            stats['error'] = 'avatar_cache table missing'
            conn.close()
            return stats

        rows_before = conn.execute("SELECT COUNT(*) FROM avatar_cache").fetchone()[0]

        with conn:
            cur = conn.execute(
                "DELETE FROM avatar_cache WHERE last_checked IS NOT NULL AND last_checked < ?",
                (age_cutoff,),
            )
            stats['rows_deleted'] = cur.rowcount if cur.rowcount is not None else 0

        rows_after = conn.execute("SELECT COUNT(*) FROM avatar_cache").fetchone()[0]
        stats['rows_before'] = rows_before
        stats['rows_after'] = rows_after
    except Exception as e:
        stats['error'] = str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return stats


def run_full_cleanup(citadel_root: str, settings: dict) -> None:
    """Run one cleanup pass across all bridge DBs plus the avatar DB.

    Called from the scheduler thread once per day. Errors on individual DBs are
    logged but never raise - one bad DB shouldn't skip the others.
    """
    if not settings.get('CLEANUP_ENABLED', True):
        log_debug("[cleanup] disabled via config, skipping", 'cleanup')
        return

    min_groups = int(settings.get('MIN_MAPPINGS_PER_BRIDGE', 10000))
    max_age = int(settings.get('MAX_MAPPING_AGE_DAYS', 60))
    avatar_retention = int(settings.get('AVATAR_RETENTION_DAYS', 90))

    log_info(
        f"[cleanup] starting pass: min_groups={min_groups} max_age={max_age}d "
        f"avatar_retention={avatar_retention}d",
        'cleanup'
    )

    started = time.time()
    total_rows_deleted = 0
    total_groups_deleted = 0
    bridge_db_paths = sorted(glob.glob(os.path.join(citadel_root, '*', 'bridge.db')))

    for db_path in bridge_db_paths:
        bridge_name = os.path.basename(os.path.dirname(db_path))
        stats = cleanup_bridge_db(db_path, min_groups=min_groups, max_age_days=max_age)
        if stats.get('error'):
            log_warn(
                f"[cleanup] bridge '{bridge_name}' had issues: {stats['error']}",
                'cleanup'
            )
            continue
        rd = stats.get('rows_deleted', 0)
        gd = stats.get('groups_deleted', 0)
        total_rows_deleted += rd
        total_groups_deleted += gd
        if rd or gd:
            log_info(
                f"[cleanup] bridge '{bridge_name}': deleted {gd} group(s) / {rd} row(s) "
                f"(kept {stats.get('groups_after', '?')} groups, {stats.get('rows_after', '?')} rows)",
                'cleanup'
            )
        else:
            log_debug(
                f"[cleanup] bridge '{bridge_name}': nothing to prune "
                f"(groups={stats.get('groups_after', '?')})",
                'cleanup'
            )

    avatar_db_path = os.path.join(citadel_root, '_avatars', 'avatar_cache.db')
    a_stats = cleanup_avatar_db(avatar_db_path, retention_days=avatar_retention)
    if a_stats.get('error'):
        log_debug(f"[cleanup] avatar db: {a_stats['error']}", 'cleanup')
    else:
        ard = a_stats.get('rows_deleted', 0)
        if ard:
            log_info(
                f"[cleanup] avatar db: deleted {ard} stale row(s) "
                f"(kept {a_stats.get('rows_after', '?')})",
                'cleanup'
            )

    elapsed = time.time() - started
    log_info(
        f"[cleanup] pass complete in {elapsed:.1f}s: "
        f"{total_groups_deleted} groups / {total_rows_deleted} rows removed across "
        f"{len(bridge_db_paths)} bridge(s)",
        'cleanup'
    )


def _seconds_until_next_run(target_hour: int) -> float:
    """How many seconds from now to the next `target_hour:00` local time.

    Clamps target_hour into [0, 23]. If we're already past today's target,
    schedules for tomorrow's.
    """
    target_hour = max(0, min(23, int(target_hour)))
    now = datetime.now()
    next_run = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
    if next_run <= now:
        next_run = next_run + timedelta(days=1)
    return (next_run - now).total_seconds()


def start_cleanup_scheduler(citadel_root: str, settings_loader,
                             stop_event: Optional[threading.Event] = None) -> threading.Thread:
    """Start a daemon thread that runs the cleanup once per day at CLEANUP_HOUR.

    `settings_loader` is a zero-arg callable returning fresh settings each time
    (so live config edits take effect on the next scheduled run).

    Returns the started thread.
    """
    def _loop():
        # Small initial delay so we don't fight component startup on boot.
        time.sleep(30)
        while stop_event is None or not stop_event.is_set():
            try:
                settings = settings_loader() or {}
            except Exception as e:
                log_warn(f"[cleanup] settings load failed, using defaults: {e}", 'cleanup')
                settings = {}

            if not settings.get('CLEANUP_ENABLED', True):
                # Sleep an hour and re-check; user may flip the flag mid-run.
                if stop_event is not None and stop_event.wait(3600):
                    return
                elif stop_event is None:
                    time.sleep(3600)
                continue

            target_hour = settings.get('CLEANUP_HOUR', 4)
            sleep_for = _seconds_until_next_run(target_hour)
            log_debug(
                f"[cleanup] next pass at hour {target_hour} (in {sleep_for / 3600:.1f}h)",
                'cleanup'
            )

            if stop_event is not None:
                if stop_event.wait(sleep_for):
                    return
            else:
                time.sleep(sleep_for)

            try:
                run_full_cleanup(citadel_root, settings)
            except Exception as e:
                log_error(f"[cleanup] pass crashed: {e}", 'cleanup')

    t = threading.Thread(target=_loop, name='cleanup-scheduler', daemon=True)
    t.start()
    log_info("[cleanup] scheduler thread started", 'cleanup')
    return t
