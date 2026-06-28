"""
Smoke-tests for v6_direct_db.py — no real DB, no real network.

mysql.connector and requests are replaced with in-memory fakes that record
SQL and serve scripted responses. Each test exercises one critical path:

  - sanity-check thresholds
  - HTTP 304 → 'not_modified' marker
  - CSV parsing + diff-upsert + change detection
  - early-exit to ids-only mode after budget exhaustion
  - lock acquisition contention and TTL takeover
  - non-strict stale delete swallows partial cleanup
  - strict stale delete raises on partial cleanup
  - FORCE_SYNC overrides sanity failure

Run:    python smoke_test.py
Exit:   0 on success, 1 on any failure.
"""

import io
import os
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Fake mysql.connector — minimal in-memory backend mimicking what the script needs.
# ---------------------------------------------------------------------------

class FakeRow:
    def __init__(self, values):
        self.values = values
    def __getitem__(self, i):
        return self.values[i]
    def __iter__(self):
        return iter(self.values)


class FakeCursor:
    """Records SQL, returns scripted responses based on a programmable handler."""

    def __init__(self, store):
        self.store = store
        self._last_rows = []
        self.rowcount = 0
        self.executed = []

    def execute(self, sql, params=()):
        self.executed.append((sql, params))
        self._last_rows = []
        self.rowcount = self.store.handle(sql, params)
        if self.store.pending_rows is not None:
            self._last_rows = self.store.pending_rows
            self.store.pending_rows = None

    def executemany(self, sql, seq):
        for params in seq:
            self.execute(sql, params)
        self.rowcount = len(seq)

    def fetchone(self):
        if not self._last_rows:
            return None
        return self._last_rows[0]

    def fetchall(self):
        rows = self._last_rows
        self._last_rows = []
        return rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self, store):
        self.store = store
        self.committed = 0
        self.rolled_back = 0

    def cursor(self):
        return FakeCursor(self.store)

    def commit(self):
        self.committed += 1
        self.store.on_commit()

    def rollback(self):
        self.rolled_back += 1

    def close(self):
        pass


class FakeStore:
    """Programmable state — tests configure rows, lock state, etc."""

    def __init__(self):
        self.products = {}   # (feed_source, id) -> (price, url)
        self.feed_state = {}  # feed_source -> dict
        self.budget = {}      # period_ym -> used_writes
        self.daily = {}       # day -> (used_rows, used_critical_rows)
        self.monthly_limit = 50_000_000
        self.pending_rows = None
        self.deletes = []

    def on_commit(self):
        pass

    def _set_rows(self, rows):
        self.pending_rows = [FakeRow(r) for r in rows]

    def handle(self, sql, params):
        s = " ".join(sql.split()).lower()

        if s.startswith("create table") or s.startswith("alter table"):
            return 0

        # ---- import_usage_budget ----
        if "insert into import_usage_budget" in s:
            period = params[0]
            self.budget.setdefault(period, 0)
            return 1
        if "select used_writes from import_usage_budget" in s:
            period = params[0]
            self._set_rows([(self.budget.get(period, 0),)])
            return 1
        if "update import_usage_budget set used_writes" in s:
            amount, period = params
            self.budget[period] = self.budget.get(period, 0) + amount
            return 1

        # ---- import_daily_row_budget ----
        if "insert into import_daily_row_budget" in s:
            day = params[0]
            self.daily.setdefault(day, (0, 0))
            return 1
        if "select used_rows, used_critical_rows from import_daily_row_budget" in s:
            day = params[0]
            used, crit = self.daily.get(day, (0, 0))
            self._set_rows([(used, crit)])
            return 1
        if "update import_daily_row_budget set used_rows" in s:
            amount, day = params
            used, crit = self.daily.get(day, (0, 0))
            self.daily[day] = (used + amount, crit)
            return 1
        if "update import_daily_row_budget set used_critical_rows" in s:
            amount, day = params
            used, crit = self.daily.get(day, (0, 0))
            self.daily[day] = (used, crit + amount)
            return 1

        # ---- import_feed_state ----
        if "insert into import_feed_state (feed_source) values" in s:
            fs = params[0]
            self.feed_state.setdefault(fs, {})
            return 1
        if "update import_feed_state set lock_token =" in s and "lock_token is null or lock_expires_at" in s:
            token, ttl, fs = params
            row = self.feed_state.setdefault(fs, {})
            now = datetime.now(timezone.utc)
            expires = row.get("lock_expires_at")
            if row.get("lock_token") is None or (expires and expires < now):
                row["lock_token"] = token
                row["lock_expires_at"] = now + timedelta(seconds=ttl)
                return 1
            return 0
        if "update import_feed_state set lock_token = null" in s:
            fs, token = params
            row = self.feed_state.get(fs, {})
            if row.get("lock_token") == token:
                row["lock_token"] = None
                row["lock_expires_at"] = None
                return 1
            return 0
        if "insert into import_feed_state (feed_source, etag, last_modified)" in s:
            fs, etag, lm = params
            row = self.feed_state.setdefault(fs, {})
            row["etag"] = etag
            row["last_modified"] = lm
            return 1
        if "insert into import_feed_state (feed_source, last_total_seen)" in s:
            fs, total = params
            row = self.feed_state.setdefault(fs, {})
            row["last_total_seen"] = total
            return 1
        if "insert into import_feed_state (feed_source, last_status, consecutive_failures)" in s:
            fs, status, failures = params
            row = self.feed_state.setdefault(fs, {})
            prev_status = row.get("last_status")
            if prev_status is None:
                row["last_status"] = status
                row["consecutive_failures"] = failures
            else:
                row["last_status"] = status
                if status in ("success", "budget_exhausted", "not_modified"):
                    row["consecutive_failures"] = 0
                else:
                    row["consecutive_failures"] = (row.get("consecutive_failures", 0) or 0) + 1
            return 1
        if "select etag, last_modified from import_feed_state" in s:
            fs = params[0]
            row = self.feed_state.get(fs, {})
            self._set_rows([(row.get("etag"), row.get("last_modified"))])
            return 1
        if "select last_status from import_feed_state" in s:
            fs = params[0]
            row = self.feed_state.get(fs, {})
            self._set_rows([(row.get("last_status"),)])
            return 1
        if "select last_total_seen from import_feed_state" in s:
            fs = params[0]
            row = self.feed_state.get(fs, {})
            self._set_rows([(row.get("last_total_seen"),)])
            return 1

        # ---- products ----
        if "select id, price, url from products" in s:
            fs, ids = params[0], params[1:]
            rows = []
            for pid in ids:
                key = (fs, pid)
                if key in self.products:
                    price, url = self.products[key]
                    rows.append((pid, price, url))
            self._set_rows(rows)
            return len(rows)
        if "insert into products" in s:
            pid, price, run_id, fs, url = params
            self.products[(fs, pid)] = (price, url)
            return 1
        if "select id from products where feed_source" in s:
            fs, last_id, limit = params
            ids = sorted(i for (s_, i) in self.products if s_ == fs and i > last_id)[:limit]
            self._set_rows([(i,) for i in ids])
            return len(ids)
        if "delete from products where feed_source" in s:
            fs, ids = params[0], params[1:]
            deleted = 0
            for pid in ids:
                if (fs, pid) in self.products:
                    del self.products[(fs, pid)]
                    deleted += 1
                    self.deletes.append((fs, pid))
            return deleted

        return 0


# ---------------------------------------------------------------------------
# Fake requests — yields scripted CSV bodies or 304 responses.
# ---------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status_code, body=b"", headers=None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_lines(self, chunk_size=8192):
        for line in self._body.split(b"\n"):
            yield line

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

CSV_HEADER = "id;name;url;col3;col4;col5;col6;col7;price"


def csv_body(rows):
    lines = [CSV_HEADER]
    for r in rows:
        lines.append(";".join(str(c) for c in r))
    return ("\n".join(lines) + "\n").encode("utf-8")


def import_module():
    """Import v6_direct_db with mysql.connector stubbed so import doesn't fail."""
    fake_mysql = types.ModuleType("mysql")
    fake_connector = types.ModuleType("mysql.connector")

    class _ClientFlag:
        SSL = 1
    fake_connector.ClientFlag = _ClientFlag
    fake_connector.connect = MagicMock()
    fake_mysql.connector = fake_connector
    sys.modules["mysql"] = fake_mysql
    sys.modules["mysql.connector"] = fake_connector

    sys.path.insert(0, os.path.dirname(__file__) or ".")
    if "v6_direct_db" in sys.modules:
        del sys.modules["v6_direct_db"]
    import v6_direct_db
    return v6_direct_db, fake_connector


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class SmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Force a clean, predictable env
        for k in (
            "TIDB_HOST", "TIDB_USER", "TIDB_PASSWORD", "TIDB_DB_NAME",
            "MIN_FEED_ROWS_ABSOLUTE", "MIN_FEED_SIZE_RATIO", "FORCE_SYNC",
            "MONTHLY_WRITE_LIMIT", "BATCH_SIZE", "STRICT_STALE_DELETE",
            "LOCK_TTL_SECONDS", "MAX_TOTAL_ROWS_PER_DAY",
        ):
            os.environ.pop(k, None)
        os.environ["TIDB_HOST"] = "fake"
        os.environ["TIDB_USER"] = "u"
        os.environ["TIDB_PASSWORD"] = "p"
        os.environ["TIDB_DB_NAME"] = "d"
        os.environ["MIN_FEED_ROWS_ABSOLUTE"] = "2"
        os.environ["MIN_FEED_SIZE_RATIO"] = "0.5"
        os.environ["BATCH_SIZE"] = "3"
        os.environ["DAILY_ROW_RESERVE_CHUNK"] = "10"

        cls.module, cls.fake_connector = import_module()

    def _wire(self, store):
        self.fake_connector.connect = MagicMock(return_value=FakeConnection(store))

    # ----- sanity-check -----

    def test_sanity_below_absolute_fails(self):
        store = FakeStore()
        self._wire(store)
        imp = self.module.TiDBImporter()
        conn = imp._connect()
        cur = conn.cursor()
        ok, reason = imp._check_feed_sanity(cur, "F1", total_seen=1)
        self.assertFalse(ok)
        self.assertIn("below MIN_FEED_ROWS_ABSOLUTE", reason)

    def test_sanity_below_ratio_fails(self):
        store = FakeStore()
        store.feed_state["F1"] = {"last_total_seen": 100}
        self._wire(store)
        imp = self.module.TiDBImporter()
        conn = imp._connect()
        cur = conn.cursor()
        ok, reason = imp._check_feed_sanity(cur, "F1", total_seen=40)
        self.assertFalse(ok)
        self.assertIn("shrunk", reason)

    def test_sanity_passes(self):
        store = FakeStore()
        store.feed_state["F1"] = {"last_total_seen": 100}
        self._wire(store)
        imp = self.module.TiDBImporter()
        conn = imp._connect()
        cur = conn.cursor()
        ok, reason = imp._check_feed_sanity(cur, "F1", total_seen=80)
        self.assertTrue(ok)

    # ----- lock -----

    def test_lock_acquire_and_release(self):
        store = FakeStore()
        self._wire(store)
        imp = self.module.TiDBImporter()
        conn = imp._connect()
        cur = conn.cursor()
        token = imp._acquire_lock(cur, conn, "F1")
        self.assertIsNotNone(token)
        self.assertEqual(store.feed_state["F1"]["lock_token"], token)
        imp._release_lock(cur, conn, "F1", token)
        self.assertIsNone(store.feed_state["F1"]["lock_token"])

    def test_lock_contention_second_caller_denied(self):
        store = FakeStore()
        self._wire(store)
        imp = self.module.TiDBImporter()
        conn = imp._connect()
        cur = conn.cursor()
        first = imp._acquire_lock(cur, conn, "F1")
        second = imp._acquire_lock(cur, conn, "F1")
        self.assertIsNotNone(first)
        self.assertIsNone(second)

    def test_lock_takeover_after_ttl(self):
        store = FakeStore()
        store.feed_state["F1"] = {
            "lock_token": "old",
            "lock_expires_at": datetime.now(timezone.utc) - timedelta(seconds=1),
        }
        self._wire(store)
        imp = self.module.TiDBImporter()
        conn = imp._connect()
        cur = conn.cursor()
        token = imp._acquire_lock(cur, conn, "F1")
        self.assertIsNotNone(token)
        self.assertNotEqual(token, "old")

    # ----- scan_feed -----

    def test_scan_304_returns_not_modified(self):
        store = FakeStore()
        self._wire(store)
        imp = self.module.TiDBImporter()
        conn = imp._connect()
        cur = conn.cursor()
        with patch.object(self.module.requests, "get",
                          return_value=FakeResponse(304)):
            current_ids, seen, ups, exh, nm, etag, lm = imp._scan_feed(
                "http://x", "F1", cur, conn, run_id=1, is_critical=False,
            )
        self.assertTrue(nm)
        self.assertEqual(seen, 0)
        self.assertEqual(current_ids, set())

    def test_scan_inserts_new_and_skips_unchanged(self):
        store = FakeStore()
        store.products[("F1", 2)] = (10.0, "u2")  # already up-to-date
        self._wire(store)
        imp = self.module.TiDBImporter()
        conn = imp._connect()
        cur = conn.cursor()
        body = csv_body([
            (1, "n1", "u1", "", "", "", "", "", "5.0"),
            (2, "n2", "u2", "", "", "", "", "", "10.0"),  # unchanged
            (3, "n3", "u3", "", "", "", "", "", "7.5"),
        ])
        with patch.object(self.module.requests, "get",
                          return_value=FakeResponse(200, body, {"ETag": "v1"})):
            current_ids, seen, ups, exh, nm, etag, lm = imp._scan_feed(
                "http://x", "F1", cur, conn, run_id=1, is_critical=False,
            )
        self.assertEqual(seen, 3)
        self.assertEqual(current_ids, {1, 2, 3})
        self.assertEqual(ups, 2)  # only 1 and 3 written
        self.assertEqual(etag, "v1")
        self.assertFalse(exh)
        self.assertIn(("F1", 1), store.products)
        self.assertIn(("F1", 3), store.products)

    def test_scan_switches_to_ids_only_on_budget_exhausted(self):
        store = FakeStore()
        # Budget that allows only ~3 writes
        store.budget["dummy"] = 0
        self._wire(store)
        os.environ["MONTHLY_WRITE_LIMIT"] = "3"
        imp = self.module.TiDBImporter()
        conn = imp._connect()
        cur = conn.cursor()
        body = csv_body([
            (i, "n", f"u{i}", "", "", "", "", "", "1.0") for i in range(1, 11)
        ])
        with patch.object(self.module.requests, "get",
                          return_value=FakeResponse(200, body, {"ETag": "v1"})):
            current_ids, seen, ups, exh, nm, etag, lm = imp._scan_feed(
                "http://x", "F1", cur, conn, run_id=1, is_critical=False,
            )
        self.assertEqual(seen, 10)
        self.assertEqual(current_ids, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
        self.assertTrue(exh)
        os.environ["MONTHLY_WRITE_LIMIT"] = "50000000"

    # ----- delete_stale -----

    def test_delete_stale_strict_proceeds_without_budget(self):
        store = FakeStore()
        for i in range(1, 6):
            store.products[("F1", i)] = (1.0, "u")
        self._wire(store)
        os.environ["MONTHLY_WRITE_LIMIT"] = "0"  # no budget for anything
        os.environ["STRICT_STALE_DELETE"] = "1"
        imp = self.module.TiDBImporter()
        conn = imp._connect()
        cur = conn.cursor()
        deleted, exh = imp._delete_stale_ids(cur, conn, "F1", [1, 2, 3])
        self.assertEqual(deleted, 3)  # strict deletes anyway
        self.assertTrue(exh)
        os.environ["MONTHLY_WRITE_LIMIT"] = "50000000"

    def test_delete_stale_non_strict_halts_on_budget(self):
        store = FakeStore()
        for i in range(1, 6):
            store.products[("F1", i)] = (1.0, "u")
        self._wire(store)
        os.environ["MONTHLY_WRITE_LIMIT"] = "0"
        os.environ["STRICT_STALE_DELETE"] = "0"
        imp = self.module.TiDBImporter()
        conn = imp._connect()
        cur = conn.cursor()
        deleted, exh = imp._delete_stale_ids(cur, conn, "F1", [1, 2, 3])
        self.assertEqual(deleted, 0)  # halted before any DELETE
        self.assertTrue(exh)
        os.environ["MONTHLY_WRITE_LIMIT"] = "50000000"
        os.environ["STRICT_STALE_DELETE"] = "1"

    # ----- run_feed integration -----

    def test_run_feed_full_success_persists_baseline_and_etag(self):
        store = FakeStore()
        self._wire(store)
        imp = self.module.TiDBImporter()
        body = csv_body([
            (i, "n", f"u{i}", "", "", "", "", "", "1.0") for i in range(1, 6)
        ])
        with patch.object(self.module.requests, "get",
                          return_value=FakeResponse(200, body, {"ETag": "v9"})):
            imp.run_feed("http://x", "F1")
        self.assertEqual(store.feed_state["F1"]["last_status"], "success")
        self.assertEqual(store.feed_state["F1"]["last_total_seen"], 5)
        self.assertEqual(store.feed_state["F1"]["etag"], "v9")
        self.assertIsNone(store.feed_state["F1"]["lock_token"])  # released

    def test_run_feed_304_marks_not_modified(self):
        store = FakeStore()
        self._wire(store)
        imp = self.module.TiDBImporter()
        with patch.object(self.module.requests, "get",
                          return_value=FakeResponse(304)):
            imp.run_feed("http://x", "F1")
        self.assertEqual(store.feed_state["F1"]["last_status"], "not_modified")
        self.assertIsNone(store.feed_state["F1"]["lock_token"])

    def test_run_feed_sanity_failure_aborts_and_preserves_data(self):
        store = FakeStore()
        # Existing baseline; today's feed shrinks below ratio
        store.feed_state["F1"] = {"last_total_seen": 100}
        for i in range(1, 101):
            store.products[("F1", i)] = (1.0, "u")
        self._wire(store)
        imp = self.module.TiDBImporter()
        body = csv_body([
            (i, "n", f"u{i}", "", "", "", "", "", "1.0") for i in range(1, 11)
        ])  # 10 rows vs baseline 100 → fails ratio 0.5
        with patch.object(self.module.requests, "get",
                          return_value=FakeResponse(200, body, {"ETag": "v2"})):
            with self.assertRaises(RuntimeError):
                imp.run_feed("http://x", "F1")
        self.assertEqual(store.feed_state["F1"]["last_status"], "failed")
        # CRITICAL: products untouched (no mass delete)
        self.assertEqual(len(store.products), 100)
        # ETag NOT saved — next run will re-fetch
        self.assertIsNone(store.feed_state["F1"].get("etag"))

    def test_run_feed_force_sync_bypasses_sanity(self):
        store = FakeStore()
        store.feed_state["F1"] = {"last_total_seen": 100}
        for i in range(1, 101):
            store.products[("F1", i)] = (1.0, "u")
        self._wire(store)
        os.environ["FORCE_SYNC"] = "1"
        try:
            imp = self.module.TiDBImporter()
            body = csv_body([
                (i, "n", f"u{i}", "", "", "", "", "", "1.0") for i in range(1, 11)
            ])
            with patch.object(self.module.requests, "get",
                              return_value=FakeResponse(200, body, {"ETag": "v3"})):
                imp.run_feed("http://x", "F1")
        finally:
            os.environ.pop("FORCE_SYNC")
        # Mass delete happened because operator opted in
        self.assertEqual(len(store.products), 10)
        self.assertEqual(store.feed_state["F1"]["last_status"], "success")

    def test_run_feed_skips_when_lock_held(self):
        store = FakeStore()
        store.feed_state["F1"] = {
            "lock_token": "held-by-other",
            "lock_expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
        }
        self._wire(store)
        imp = self.module.TiDBImporter()
        get_mock = MagicMock()
        with patch.object(self.module.requests, "get", get_mock):
            imp.run_feed("http://x", "F1")
        # No HTTP call attempted
        self.assertEqual(get_mock.call_count, 0)
        # Lock left intact
        self.assertEqual(store.feed_state["F1"]["lock_token"], "held-by-other")


if __name__ == "__main__":
    unittest.main(verbosity=2)
