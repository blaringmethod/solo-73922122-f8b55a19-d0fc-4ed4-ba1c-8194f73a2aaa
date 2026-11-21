"""SQLite 连接管理与建表。

写入统一走 :func:`write_transaction`：进程内互斥锁 + ``BEGIN IMMEDIATE``，
保证两个改单包并行提交时也被串行化，同一张旧单最多被替换一次。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

DEFAULT_DB_PATH = os.environ.get(
    "WAYBILL_DB_PATH", os.path.join(os.getcwd(), "data", "waybills.db")
)

# 进程内写锁：FastAPI 同步端点跑在线程池中，需要它把写事务完全串行化。
_write_lock = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS waybills (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    waybill_no         TEXT NOT NULL UNIQUE,
    status             TEXT NOT NULL CHECK (status IN ('active', 'replaced')),
    source_package_no  TEXT,
    created_at         TEXT NOT NULL,
    replaced_at        TEXT,
    replaced_by_pkg    TEXT
);

CREATE TABLE IF NOT EXISTS batches (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    waybill_id INTEGER NOT NULL REFERENCES waybills(id),
    batch_no   TEXT NOT NULL,
    weight_kg  INTEGER NOT NULL CHECK (weight_kg > 0),
    UNIQUE (waybill_id, batch_no)
);

CREATE TABLE IF NOT EXISTS packages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    package_no  TEXT NOT NULL UNIQUE,
    payload     TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('applied', 'suspended', 'rejected')),
    reason      TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
"""


def get_db_path() -> str:
    return os.environ.get("WAYBILL_DB_PATH", DEFAULT_DB_PATH)


def _connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or get_db_path()
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db(db_path: str | None = None) -> None:
    """建表（幂等）。"""
    conn = _connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


@contextmanager
def read_connection(db_path: str | None = None) -> Iterator[sqlite3.Connection]:
    conn = _connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def snapshot_connection(
    db_path: str | None = None,
) -> Iterator[sqlite3.Connection]:
    """固定一个 SQLite 读快照。

    WAL 模式下，读事务在第一次读取数据库时确定快照，之后同一事务内的所有
    查询都看到同一版本的数据；其他连接仍可继续提交写事务（不会被本次读
    阻塞），因此巡检期间可以正常提交改单，而本次报告不会混入新写入。

    ``PRAGMA query_only`` 保证巡检连接绝不会写库。退出时回滚只读事务。
    """
    conn = _connect(db_path)
    try:
        # 关闭 sqlite3 模块的隐式事务管理，显式开启只读事务。
        conn.isolation_level = None
        conn.execute("PRAGMA query_only = ON")
        # 读快照期间只持有共享锁，不阻塞其他连接的 WAL 写入。
        conn.execute("BEGIN")
        # 第一次 SELECT 才真正读取数据库并固定快照。
        conn.execute("SELECT COUNT(*) FROM waybills").fetchone()
        yield conn
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()


@contextmanager
def write_transaction(db_path: str | None = None) -> Iterator[sqlite3.Connection]:
    """串行化的写事务：先拿进程锁，再以 IMMEDIATE 立即获取写锁。"""
    with _write_lock:
        conn = _connect(db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
