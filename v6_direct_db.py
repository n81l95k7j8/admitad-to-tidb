import csv
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import mysql.connector
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TiDB_Importer_Budgeted")


ProductRow = Tuple[int, float, int, str, Optional[str]]


class TiDBImporter:
    def __init__(self):
        cert_path = os.path.join(os.path.dirname(__file__), 'isrgrootx1.pem')

        self.config = {
            'host': os.getenv('TIDB_HOST'),
            'port': 4000,
            'user': os.getenv('TIDB_USER'),
            'password': os.getenv('TIDB_PASSWORD'),
            'database': os.getenv('TIDB_DB_NAME'),
            'client_flags': [mysql.connector.ClientFlag.SSL],
            'autocommit': False,
            'connect_timeout': int(os.getenv('DB_CONNECT_TIMEOUT', '60')),
            'read_timeout': int(os.getenv('DB_READ_TIMEOUT', '600')),
            'write_timeout': int(os.getenv('DB_WRITE_TIMEOUT', '600')),
        }
        if os.path.exists(cert_path):
            self.config['ssl_ca'] = cert_path
            logger.info("Using custom CA certificate: isrgrootx1.pem")
        else:
            logger.warning("isrgrootx1.pem not found, using system CA store")

        self.batch_size = int(os.getenv('BATCH_SIZE', '5000'))
        self.delete_batch_size = int(os.getenv('DELETE_BATCH_SIZE', '10000'))
        self.http_timeout = (
            int(os.getenv('HTTP_CONNECT_TIMEOUT', '40')),
            int(os.getenv('HTTP_READ_TIMEOUT', '900')),
        )
        self.monthly_write_limit = int(os.getenv('MONTHLY_WRITE_LIMIT', '50000000'))
        self.feed_scan_batch_size = int(os.getenv('FEED_SCAN_BATCH_SIZE', '20000'))
        self.strict_stale_delete = os.getenv('STRICT_STALE_DELETE', '1') == '1'
        self.max_feed_rows = int(os.getenv('MAX_FEED_ROWS', '15000000'))
        self.max_total_rows_per_day = int(os.getenv('MAX_TOTAL_ROWS_PER_DAY', '5000000'))
        self.daily_row_reserve_chunk = int(os.getenv('DAILY_ROW_RESERVE_CHUNK', str(self.batch_size)))
        self.max_critical_rows_per_day = int(os.getenv('MAX_CRITICAL_ROWS_PER_DAY', '500000'))
        self.min_feed_rows_absolute = int(os.getenv('MIN_FEED_ROWS_ABSOLUTE', '1000'))
        self.min_feed_size_ratio = float(os.getenv('MIN_FEED_SIZE_RATIO', '0.5'))
        self.force_sync = os.getenv('FORCE_SYNC', '0') == '1'
        self.lock_ttl_seconds = int(os.getenv('LOCK_TTL_SECONDS', '3600'))

    def _connect(self):
        return mysql.connector.connect(**self.config)

    def _ensure_budget_table(self, cursor, conn):
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS import_usage_budget (
              period_ym CHAR(7) PRIMARY KEY,
              used_writes BIGINT NOT NULL DEFAULT 0,
              updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()

    def _ensure_feed_state_table(self, cursor, conn):
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS import_feed_state (
              feed_source VARCHAR(50) PRIMARY KEY,
              etag VARCHAR(512) NULL,
              last_modified VARCHAR(255) NULL,
              last_status VARCHAR(16) NULL,
              consecutive_failures INT NOT NULL DEFAULT 0,
              last_total_seen BIGINT NULL,
              lock_token VARCHAR(64) NULL,
              lock_expires_at TIMESTAMP NULL,
              last_run_utc TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()
        for ddl in (
            "ALTER TABLE import_feed_state ADD COLUMN IF NOT EXISTS last_status VARCHAR(16) NULL",
            "ALTER TABLE import_feed_state ADD COLUMN IF NOT EXISTS consecutive_failures INT NOT NULL DEFAULT 0",
            "ALTER TABLE import_feed_state ADD COLUMN IF NOT EXISTS last_total_seen BIGINT NULL",
            "ALTER TABLE import_feed_state ADD COLUMN IF NOT EXISTS lock_token VARCHAR(64) NULL",
            "ALTER TABLE import_feed_state ADD COLUMN IF NOT EXISTS lock_expires_at TIMESTAMP NULL",
        ):
            try:
                cursor.execute(ddl)
                conn.commit()
            except Exception:
                conn.rollback()

    def _ensure_daily_row_budget_table(self, cursor, conn):
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS import_daily_row_budget (
              day_utc CHAR(10) PRIMARY KEY,
              used_rows BIGINT NOT NULL DEFAULT 0,
              used_critical_rows BIGINT NOT NULL DEFAULT 0,
              updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()
        try:
            cursor.execute(
                "ALTER TABLE import_daily_row_budget ADD COLUMN IF NOT EXISTS used_critical_rows BIGINT NOT NULL DEFAULT 0"
            )
            conn.commit()
        except Exception:
            conn.rollback()

    def _current_period(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m")

    def _current_day(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ---------------- Lock ----------------

    def _acquire_lock(self, cursor, conn, source_id: str) -> Optional[str]:
        token = f"{os.getpid()}-{int(time.time())}-{uuid.uuid4().hex[:8]}"
        try:
            cursor.execute(
                "INSERT INTO import_feed_state (feed_source) VALUES (%s) "
                "ON DUPLICATE KEY UPDATE feed_source = feed_source",
                (source_id,),
            )
            cursor.execute(
                """
                UPDATE import_feed_state
                SET lock_token = %s,
                    lock_expires_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s SECOND)
                WHERE feed_source = %s
                  AND (lock_token IS NULL OR lock_expires_at < UTC_TIMESTAMP())
                """,
                (token, self.lock_ttl_seconds, source_id),
            )
            acquired = cursor.rowcount == 1
            conn.commit()
            return token if acquired else None
        except Exception:
            conn.rollback()
            raise

    def _release_lock(self, cursor, conn, source_id: str, token: str):
        try:
            cursor.execute(
                """
                UPDATE import_feed_state
                SET lock_token = NULL, lock_expires_at = NULL
                WHERE feed_source = %s AND lock_token = %s
                """,
                (source_id, token),
            )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

    # ---------------- Feed state ----------------

    def _get_feed_state_headers(self, cursor, source_id: str) -> Dict[str, str]:
        cursor.execute(
            "SELECT etag, last_modified FROM import_feed_state WHERE feed_source = %s",
            (source_id,),
        )
        row = cursor.fetchone()
        headers: Dict[str, str] = {}
        if not row:
            return headers
        etag, last_modified = row
        if etag:
            headers["If-None-Match"] = str(etag)
        if last_modified:
            headers["If-Modified-Since"] = str(last_modified)
        return headers

    def _save_feed_state_headers(
        self,
        cursor,
        conn,
        source_id: str,
        etag: Optional[str],
        last_modified: Optional[str],
    ):
        cursor.execute(
            """
            INSERT INTO import_feed_state (feed_source, etag, last_modified)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
              etag = VALUES(etag),
              last_modified = VALUES(last_modified),
              last_run_utc = CURRENT_TIMESTAMP
            """,
            (source_id, etag, last_modified),
        )
        conn.commit()

    def _save_feed_total_seen(self, cursor, conn, source_id: str, total_seen: int):
        cursor.execute(
            """
            INSERT INTO import_feed_state (feed_source, last_total_seen)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE last_total_seen = VALUES(last_total_seen)
            """,
            (source_id, total_seen),
        )
        conn.commit()

    def _get_feed_last_status(self, cursor, source_id: str) -> Optional[str]:
        cursor.execute(
            "SELECT last_status FROM import_feed_state WHERE feed_source = %s",
            (source_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return row[0]

    def _get_last_total_seen(self, cursor, source_id: str) -> Optional[int]:
        cursor.execute(
            "SELECT last_total_seen FROM import_feed_state WHERE feed_source = %s",
            (source_id,),
        )
        row = cursor.fetchone()
        if not row or row[0] is None:
            return None
        return int(row[0])

    def _mark_feed_status(self, cursor, conn, source_id: str, status: str):
        clears_failure = status in ('success', 'budget_exhausted', 'not_modified')
        cursor.execute(
            """
            INSERT INTO import_feed_state (feed_source, last_status, consecutive_failures)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
              last_status = VALUES(last_status),
              consecutive_failures = CASE
                WHEN VALUES(last_status) IN ('success', 'budget_exhausted', 'not_modified') THEN 0
                ELSE consecutive_failures + 1
              END,
              last_run_utc = CURRENT_TIMESTAMP
            """,
            (source_id, status, 0 if clears_failure else 1),
        )
        conn.commit()

    # ---------------- Budgets ----------------

    def _reserve_writes(self, cursor, conn, amount: int) -> bool:
        if amount <= 0:
            return True
        period = self._current_period()
        try:
            cursor.execute(
                "INSERT INTO import_usage_budget (period_ym, used_writes) VALUES (%s, 0) "
                "ON DUPLICATE KEY UPDATE period_ym = period_ym",
                (period,),
            )
            cursor.execute(
                "SELECT used_writes FROM import_usage_budget WHERE period_ym = %s FOR UPDATE",
                (period,),
            )
            used = int(cursor.fetchone()[0] or 0)
            if used + amount > self.monthly_write_limit:
                conn.rollback()
                logger.warning(
                    f"Write budget exceeded: period={period}, used={used:,}, "
                    f"request={amount:,}, limit={self.monthly_write_limit:,}"
                )
                return False
            cursor.execute(
                "UPDATE import_usage_budget SET used_writes = used_writes + %s WHERE period_ym = %s",
                (amount, period),
            )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise

    def _reserve_daily_rows(self, cursor, conn, amount: int, is_critical: bool = False) -> bool:
        if amount <= 0:
            return True
        day = self._current_day()
        try:
            cursor.execute(
                "INSERT INTO import_daily_row_budget (day_utc, used_rows) VALUES (%s, 0) "
                "ON DUPLICATE KEY UPDATE day_utc = day_utc",
                (day,),
            )
            cursor.execute(
                "SELECT used_rows, used_critical_rows FROM import_daily_row_budget WHERE day_utc = %s FOR UPDATE",
                (day,),
            )
            used_row = cursor.fetchone()
            used = int((used_row[0] if used_row else 0) or 0)
            used_critical = int((used_row[1] if used_row else 0) or 0)
            if used + amount > self.max_total_rows_per_day:
                if is_critical and (used_critical + amount) <= self.max_critical_rows_per_day:
                    cursor.execute(
                        "UPDATE import_daily_row_budget SET used_critical_rows = used_critical_rows + %s WHERE day_utc = %s",
                        (amount, day),
                    )
                    conn.commit()
                    logger.warning(
                        f"Critical feed uses overflow budget: day={day}, "
                        f"critical_used={used_critical + amount:,}/{self.max_critical_rows_per_day:,}"
                    )
                    return True
                conn.rollback()
                logger.warning(
                    f"Daily row budget exceeded: day={day}, used={used:,}, request={amount:,}, "
                    f"limit={self.max_total_rows_per_day:,}, critical={is_critical}"
                )
                return False
            cursor.execute(
                "UPDATE import_daily_row_budget SET used_rows = used_rows + %s WHERE day_utc = %s",
                (amount, day),
            )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise

    # ---------------- Sanity ----------------

    def _check_feed_sanity(self, cursor, source_id: str, total_seen: int) -> Tuple[bool, str]:
        if total_seen < self.min_feed_rows_absolute:
            return (
                False,
                f"feed has {total_seen:,} rows, below MIN_FEED_ROWS_ABSOLUTE={self.min_feed_rows_absolute:,}",
            )
        prev = self._get_last_total_seen(cursor, source_id)
        if prev is not None and prev > 0:
            min_allowed = int(prev * self.min_feed_size_ratio)
            if total_seen < min_allowed:
                return (
                    False,
                    f"feed shrunk from {prev:,} to {total_seen:,} "
                    f"(< {self.min_feed_size_ratio:.0%} of previous = {min_allowed:,})",
                )
        return True, "ok"

    # ---------------- Upsert / Delete ----------------

    def _fetch_existing_map(
        self, cursor, source_id: str, ids: List[int]
    ) -> Dict[int, Tuple[float, Optional[str]]]:
        if not ids:
            return {}
        placeholders = ",".join(["%s"] * len(ids))
        sql = (
            f"SELECT id, price, url FROM products "
            f"WHERE feed_source = %s AND id IN ({placeholders})"
        )
        cursor.execute(sql, (source_id, *ids))
        rows = cursor.fetchall()
        out: Dict[int, Tuple[float, Optional[str]]] = {}
        for row in rows:
            out[int(row[0])] = (float(row[1]), row[2])
        return out

    def _upsert_batch_changed_only(
        self,
        cursor,
        conn,
        source_id: str,
        batch_map: Dict[int, Tuple[float, Optional[str]]],
        run_id: int,
    ) -> Tuple[int, bool]:
        if not batch_map:
            return 0, False

        ids = list(batch_map.keys())
        existing = self._fetch_existing_map(cursor, source_id, ids)
        to_upsert: List[ProductRow] = []
        for item_id, (price, url) in batch_map.items():
            prev = existing.get(item_id)
            if prev is None:
                to_upsert.append((item_id, price, run_id, source_id, url))
                continue
            prev_price, prev_url = prev
            if float(prev_price) != float(price) or (prev_url or None) != (url or None):
                to_upsert.append((item_id, price, run_id, source_id, url))

        if not to_upsert:
            return 0, False

        if not self._reserve_writes(cursor, conn, len(to_upsert)):
            return 0, True

        upsert_sql = """
            INSERT INTO products (id, price, run_id, feed_source, url)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                price = VALUES(price),
                run_id = VALUES(run_id),
                url = VALUES(url)
        """
        cursor.executemany(upsert_sql, to_upsert)
        conn.commit()
        return len(to_upsert), False

    def _scan_feed(
        self,
        url: str,
        source_id: str,
        cursor,
        conn,
        run_id: int,
        is_critical: bool,
    ) -> Tuple[Set[int], int, int, bool, bool, Optional[str], Optional[str]]:
        current_ids: Set[int] = set()
        total_seen = 0
        total_upserted = 0
        invalid_rows = 0
        budget_exhausted = False
        batch_map: Dict[int, Tuple[float, Optional[str]]] = {}
        request_headers = self._get_feed_state_headers(cursor, source_id)
        reserved_rows_available = 0

        with requests.get(url, stream=True, timeout=self.http_timeout, headers=request_headers) as resp:
            if resp.status_code == 304:
                logger.info(f"[{source_id}] Feed not modified (HTTP 304). Skipping import.")
                return set(), 0, 0, False, True, None, None
            resp.raise_for_status()
            response_etag = resp.headers.get("ETag")
            response_last_modified = resp.headers.get("Last-Modified")
            lines = (line.decode('utf-8', errors='ignore') for line in resp.iter_lines(chunk_size=16384))
            reader = csv.reader(lines, delimiter=';')
            next(reader, None)

            for row in reader:
                if len(row) < 9:
                    invalid_rows += 1
                    continue
                try:
                    item_id = int(row[0])
                    price = float(row[8].strip().replace(',', '.').replace(' ', '') or 0)
                    product_url = row[2].strip() if len(row) > 2 and row[2] else None
                except (ValueError, TypeError, IndexError):
                    invalid_rows += 1
                    continue

                current_ids.add(item_id)
                total_seen += 1
                if total_seen > self.max_feed_rows:
                    raise RuntimeError(
                        f"Feed row limit exceeded for source={source_id}: "
                        f"{total_seen:,} > MAX_FEED_ROWS={self.max_feed_rows:,}"
                    )

                # After upsert budget is exhausted, keep building current_ids
                # (needed for stale cleanup) but skip all DB work.
                if budget_exhausted:
                    if total_seen % 250000 == 0:
                        logger.info(
                            f"[{source_id}] scanned={total_seen:,} (ids-only after exhausted), "
                            f"upserted={total_upserted:,}, invalid={invalid_rows:,}"
                        )
                    continue

                if reserved_rows_available <= 0:
                    reserve_amount = max(1, self.daily_row_reserve_chunk)
                    if not self._reserve_daily_rows(cursor, conn, reserve_amount, is_critical=is_critical):
                        raise RuntimeError(
                            f"Daily total row limit exceeded for source={source_id}. "
                            f"MAX_TOTAL_ROWS_PER_DAY={self.max_total_rows_per_day:,}"
                        )
                    reserved_rows_available = reserve_amount

                batch_map[item_id] = (price, product_url)
                reserved_rows_available -= 1

                if len(batch_map) >= self.batch_size:
                    written, exhausted = self._upsert_batch_changed_only(
                        cursor, conn, source_id, batch_map, run_id
                    )
                    total_upserted += written
                    if exhausted:
                        budget_exhausted = True
                        logger.warning(
                            f"[{source_id}] Monthly write budget exhausted at total_seen={total_seen:,}. "
                            f"Switching scan to ids-only mode."
                        )
                    batch_map = {}

                if total_seen % 250000 == 0:
                    logger.info(
                        f"[{source_id}] scanned={total_seen:,}, upserted={total_upserted:,}, "
                        f"unique_ids={len(current_ids):,}, invalid={invalid_rows:,}"
                    )

            if batch_map and not budget_exhausted:
                written, exhausted = self._upsert_batch_changed_only(
                    cursor, conn, source_id, batch_map, run_id
                )
                total_upserted += written
                if exhausted:
                    budget_exhausted = True

        return (
            current_ids, total_seen, total_upserted,
            budget_exhausted, False, response_etag, response_last_modified,
        )

    def _collect_stale_ids(self, cursor, source_id: str, current_ids: Set[int]) -> List[int]:
        stale_ids: List[int] = []
        last_id = 0
        while True:
            cursor.execute(
                """
                SELECT id FROM products
                WHERE feed_source = %s AND id > %s
                ORDER BY id
                LIMIT %s
                """,
                (source_id, last_id, self.feed_scan_batch_size),
            )
            rows = cursor.fetchall()
            if not rows:
                break
            for (db_id,) in rows:
                db_id_int = int(db_id)
                if db_id_int not in current_ids:
                    stale_ids.append(db_id_int)
                last_id = db_id_int
        return stale_ids

    def _delete_stale_ids(
        self, cursor, conn, source_id: str, stale_ids: List[int]
    ) -> Tuple[int, bool]:
        deleted_total = 0
        budget_exhausted = False
        for i in range(0, len(stale_ids), self.delete_batch_size):
            chunk = stale_ids[i:i + self.delete_batch_size]
            if not chunk:
                continue
            if not self._reserve_writes(cursor, conn, len(chunk)):
                budget_exhausted = True
                if self.strict_stale_delete:
                    logger.warning(
                        f"[{source_id}] STRICT_STALE_DELETE=1: deleting without budget reservation "
                        f"to keep DB fully aligned with feed"
                    )
                else:
                    logger.warning(
                        f"[{source_id}] STRICT_STALE_DELETE=0 and budget exhausted: "
                        f"halting stale cleanup at {deleted_total:,}/{len(stale_ids):,}"
                    )
                    break
            placeholders = ",".join(["%s"] * len(chunk))
            sql = f"DELETE FROM products WHERE feed_source = %s AND id IN ({placeholders})"
            cursor.execute(sql, (source_id, *chunk))
            conn.commit()
            deleted_total += cursor.rowcount
            if deleted_total and deleted_total % 100000 == 0:
                logger.info(f"[{source_id}] deleted stale rows: {deleted_total:,}")
        return deleted_total, budget_exhausted

    # ---------------- Main entrypoint ----------------

    def run_feed(self, url: str, source_id: str):
        run_id = int(time.time())
        logger.info(
            f"Start feed sync: source={source_id}, run_id={run_id}, "
            f"monthly_limit={self.monthly_write_limit:,}"
        )

        conn = None
        cursor = None
        lock_token: Optional[str] = None
        try:
            conn = self._connect()
            cursor = conn.cursor()
            self._ensure_budget_table(cursor, conn)
            self._ensure_feed_state_table(cursor, conn)
            self._ensure_daily_row_budget_table(cursor, conn)

            lock_token = self._acquire_lock(cursor, conn, source_id)
            if not lock_token:
                logger.warning(
                    f"[{source_id}] Another process holds the feed lock "
                    f"(TTL={self.lock_ttl_seconds}s). Skipping this run."
                )
                return

            last_status = self._get_feed_last_status(cursor, source_id)
            is_critical = last_status is not None and last_status not in (
                'success', 'budget_exhausted', 'not_modified'
            )
            if is_critical:
                logger.warning(
                    f"[{source_id}] Critical priority mode: previous run status={last_status}"
                )

            (
                current_ids,
                total_seen,
                total_upserted,
                budget_exhausted_upsert,
                not_modified,
                response_etag,
                response_last_modified,
            ) = self._scan_feed(
                url=url,
                source_id=source_id,
                cursor=cursor,
                conn=conn,
                run_id=run_id,
                is_critical=is_critical,
            )
            if not_modified:
                self._mark_feed_status(cursor, conn, source_id, 'not_modified')
                return
            logger.info(
                f"[{source_id}] Feed scan complete: seen={total_seen:,}, "
                f"upserted_changed={total_upserted:,}, unique_ids={len(current_ids):,}"
            )

            sane, reason = self._check_feed_sanity(cursor, source_id, total_seen)
            if not sane:
                if self.force_sync:
                    logger.warning(
                        f"[{source_id}] Feed sanity check FAILED: {reason}. "
                        f"FORCE_SYNC=1 — proceeding with stale cleanup anyway."
                    )
                else:
                    raise RuntimeError(
                        f"[{source_id}] Feed sanity check FAILED: {reason}. "
                        f"Stale cleanup skipped to prevent data loss. "
                        f"Set FORCE_SYNC=1 to override after manual review."
                    )

            stale_ids = self._collect_stale_ids(cursor, source_id, current_ids)
            logger.info(f"[{source_id}] stale candidates: {len(stale_ids):,}")
            deleted_total, budget_exhausted_delete = self._delete_stale_ids(
                cursor, conn, source_id, stale_ids
            )

            partial_cleanup = deleted_total < len(stale_ids)
            if partial_cleanup and self.strict_stale_delete:
                logger.error(
                    f"[{source_id}] stale cleanup incomplete in STRICT mode: "
                    f"deleted={deleted_total:,}, stale={len(stale_ids):,}"
                )
                raise RuntimeError(
                    f"[{source_id}] stale cleanup incomplete: "
                    f"deleted={deleted_total:,}, stale={len(stale_ids):,}"
                )

            if partial_cleanup:
                logger.warning(
                    f"[{source_id}] stale cleanup PARTIAL (non-strict): "
                    f"deleted={deleted_total:,}/{len(stale_ids):,}. "
                    f"ETag NOT persisted — next run will re-scan and retry cleanup."
                )
            else:
                self._save_feed_state_headers(
                    cursor, conn, source_id, response_etag, response_last_modified,
                )
                self._save_feed_total_seen(cursor, conn, source_id, total_seen)

            period = self._current_period()
            cursor.execute(
                "SELECT used_writes FROM import_usage_budget WHERE period_ym = %s",
                (period,),
            )
            used_writes = int((cursor.fetchone() or [0])[0])
            final_status = 'budget_exhausted' if (partial_cleanup or budget_exhausted_upsert) else 'success'
            logger.info(
                f"[{source_id}] DONE [{final_status}]: upserted={total_upserted:,}, "
                f"deleted={deleted_total:,}, "
                f"budget_used={used_writes:,}/{self.monthly_write_limit:,}, "
                f"budget_blocked_upsert={budget_exhausted_upsert}, "
                f"budget_blocked_delete={budget_exhausted_delete}"
            )
            self._mark_feed_status(cursor, conn, source_id, final_status)
        except Exception as e:
            logger.error(f"[{source_id}] Sync failed: {e}")
            try:
                if conn and cursor:
                    self._mark_feed_status(cursor, conn, source_id, 'failed')
            except Exception:
                pass
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                if lock_token and cursor and conn:
                    self._release_lock(cursor, conn, source_id, lock_token)
            except Exception:
                pass
            try:
                if cursor:
                    cursor.close()
            except Exception:
                pass
            try:
                if conn:
                    conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    creds = {
        'id': os.getenv('ADMITAD_USER_ID'),
        'token': os.getenv('ADMITAD_USER_TOKEN'),
        'code': os.getenv('ADMITAD_FID_CODE'),
        'id_feed': os.getenv('FID_ID'),
    }
    if not all(creds.values()):
        logger.error("Missing required env vars for Admitad feed import")
        raise SystemExit(1)

    url = (
        f"http://export.admitad.com/ru/webmaster/websites/{creds['id']}/products/export_adv_products/"
        f"?user={creds['token']}&code={creds['code']}&feed_id={creds['id_feed']}&format=csv&fcid=6115"
    )
    TiDBImporter().run_feed(url, creds['id_feed'])
