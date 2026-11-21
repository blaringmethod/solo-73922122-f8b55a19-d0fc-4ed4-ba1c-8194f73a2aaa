"""质量巡检测试：快照隔离、重量守恒、重复归属、引用缺失、同名货批不误报。"""
from __future__ import annotations

import json
import sqlite3

from app import inspection, services
from app.schemas import BatchIn, NewWaybillIn, PackageIn, WaybillIn


def wb(no: str, batches: dict[str, int]) -> WaybillIn:
    return WaybillIn(
        waybill_no=no,
        batches=[BatchIn(batch_no=k, weight_kg=v) for k, v in batches.items()],
    )


def pkg(no: str, old: list[str], new: dict[str, dict[str, int]]) -> PackageIn:
    return PackageIn(
        package_no=no,
        old_waybill_nos=old,
        new_waybills=[
            NewWaybillIn(
                waybill_no=n,
                batches=[BatchIn(batch_no=k, weight_kg=v) for k, v in b.items()],
            )
            for n, b in new.items()
        ],
    )


def _db(temp_db: str) -> sqlite3.Connection:
    conn = sqlite3.connect(temp_db)
    conn.row_factory = sqlite3.Row
    return conn


def _inject_applied(
    temp_db: str,
    package_no: str,
    old: list[str],
    new: list[dict],
    *,
    actual: dict[str, dict[str, int]] | None = None,
) -> None:
    """绕过业务校验直接写“已生效包”，模拟历史脏数据 / 台账被篡改。"""
    payload = {
        "package_no": package_no,
        "old_waybill_nos": old,
        "new_waybills": new,
    }
    conn = _db(temp_db)
    try:
        conn.execute(
            "INSERT INTO packages (package_no, payload, content_hash, status, "
            "reason, created_at, updated_at) VALUES (?, ?, ?, 'applied', NULL, "
            "'2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')",
            (package_no, json.dumps(payload, ensure_ascii=False), "x" * 64),
        )
        conn.execute(
            "UPDATE waybills SET status='replaced', replaced_by_pkg=? "
            "WHERE waybill_no IN (%s)" % ",".join("?" * len(old)),
            [package_no, *old],
        )
        for spec in new:
            cur = conn.execute(
                "INSERT INTO waybills (waybill_no, status, source_package_no, "
                "created_at) VALUES (?, 'active', ?, '2026-01-01T00:00:00+00:00')",
                (spec["waybill_no"], package_no),
            )
            wid = cur.lastrowid
            batches = (
                actual[spec["waybill_no"]]
                if actual is not None
                else {b["batch_no"]: b["weight_kg"] for b in spec["batches"]}
            )
            conn.executemany(
                "INSERT INTO batches (waybill_id, batch_no, weight_kg) "
                "VALUES (?, ?, ?)",
                [(wid, k, v) for k, v in batches.items()],
            )
        conn.commit()
    finally:
        conn.close()


def _items(report: dict) -> list[dict]:
    return report["items"]


def _anomalies(report: dict, code: str | None = None) -> list[dict]:
    found = [it for it in report["items"] if it["status"] == "anomaly"]
    if code is not None:
        found = [it for it in found if code in it["codes"]]
    return found


# --------------------------------------------------------------------------- #
def test_clean_chain_has_no_anomalies(temp_db):
    services.create_waybill(wb("W1", {"B1": 100, "B2": 200}))
    services.submit_package(
        pkg("P1", ["W1"], {"N1": {"B1": 100}, "N2": {"B2": 200}})
    )
    services.submit_package(
        pkg("P2", ["N1", "N2"], {"N3": {"B1": 100, "B2": 200}})
    )
    report = inspection.run_inspection()
    assert report["read_snapshot"] is True
    assert report["packages_checked"] == 2
    assert report["summary"]["anomalies"] == 0
    assert {it["batch_no"] for it in _items(report)} == {"B1", "B2"}
    for it in _items(report):
        assert it["before_weight_kg"] == it["after_weight_kg"]
        assert it["status"] == "ok" and it["reason"] is None


def test_same_batch_name_under_different_roots_not_reported(temp_db):
    """不同原单下的同名货批（同号不同货）不得误报重复归属或重量异常。"""
    services.create_waybill(wb("W1", {"B1": 100}))
    services.create_waybill(wb("W2", {"B1": 200}))
    services.submit_package(pkg("P1", ["W1"], {"N1": {"B1": 100}}))
    services.submit_package(pkg("P2", ["W2"], {"N2": {"B1": 200}}))

    report = inspection.run_inspection()
    assert report["summary"]["anomalies"] == 0
    assert not _anomalies(report, inspection.DUPLICATE_ATTRIBUTION)
    origin = {
        it["package_no"]: it["origin_waybills"] for it in _items(report)
    }
    assert origin == {"P1": ["W1"], "P2": ["W2"]}  # 各自追溯到自己的根单


def test_only_applied_packages_are_checked(temp_db):
    services.create_waybill(wb("W1", {"B1": 10}))
    view, _ = services.submit_package(
        pkg("P1", ["W1", "W2"], {"N1": {"B1": 10, "B2": 5}})
    )
    assert view["status"] == "suspended"
    report = inspection.run_inspection()
    assert report["packages_checked"] == 0
    assert _items(report) == []


def test_weight_mismatch_reported_with_before_after(temp_db):
    """历史脏数据：新单实际重量少于旧单，报告改前/改后与原因。"""
    services.create_waybill(wb("W1", {"B1": 100}))
    _inject_applied(
        temp_db,
        "PX",
        ["W1"],
        [{"waybill_no": "NX", "batches": [{"batch_no": "B1", "weight_kg": 100}]}],
        actual={"NX": {"B1": 60}},
    )
    report = inspection.run_inspection()
    bad = _anomalies(report, inspection.WEIGHT_MISMATCH)
    assert len(bad) == 1
    item = bad[0]
    assert item["package_no"] == "PX"
    assert item["batch_no"] == "B1"
    assert (item["before_weight_kg"], item["after_weight_kg"]) == (100, 60)
    assert "100" in item["reason"] and "60" in item["reason"]


def test_declared_weight_mismatch_detected(temp_db):
    """包内申报 100，但新单实际记录 80：申报与实际不一致。"""
    services.create_waybill(wb("W1", {"B1": 100}))
    _inject_applied(
        temp_db,
        "PX",
        ["W1"],
        [{"waybill_no": "NX", "batches": [{"batch_no": "B1", "weight_kg": 100}]}],
        actual={"NX": {"B1": 80}},
    )
    report = inspection.run_inspection()
    assert _anomalies(report, inspection.DECLARED_MISMATCH)
    assert _anomalies(report, inspection.WEIGHT_MISMATCH)


def test_duplicate_attribution_within_package(temp_db):
    """同一原始货批被一个包拆到两张新单 -> 重复归属。"""
    services.create_waybill(wb("W1", {"B1": 100}))
    _inject_applied(
        temp_db,
        "PX",
        ["W1"],
        [
            {"waybill_no": "NX1", "batches": [{"batch_no": "B1", "weight_kg": 100}]},
            {"waybill_no": "NX2", "batches": [{"batch_no": "B1", "weight_kg": 100}]},
        ],
    )
    report = inspection.run_inspection()
    dup = _anomalies(report, inspection.DUPLICATE_ATTRIBUTION)
    assert len(dup) == 1
    item = dup[0]
    assert item["package_no"] == "PX"
    assert item["batch_no"] == "B1"
    assert item["origin_waybills"] == ["W1"]
    assert "NX1" in item["reason"] and "NX2" in item["reason"]


def test_duplicate_attribution_across_packages_in_ledger(temp_db):
    """两个已生效包把同一原始货批各自保留了一份 -> 台账终态重复归属。"""
    services.create_waybill(wb("W1", {"B1": 100}))
    services.submit_package(pkg("P1", ["W1"], {"N1": {"B1": 100}}))
    _inject_applied(
        temp_db,
        "P2",
        ["N1"],
        [{"waybill_no": "N2", "batches": [{"batch_no": "B1", "weight_kg": 100}]}],
    )
    # 人为把 N1 恢复为 active，使同一根单货批同时存在于 N1/N2
    conn = _db(temp_db)
    try:
        conn.execute("UPDATE waybills SET status='active' WHERE waybill_no='N1'")
        conn.commit()
    finally:
        conn.close()

    report = inspection.run_inspection()
    dup = _anomalies(report, inspection.DUPLICATE_ATTRIBUTION)
    ledger_dup = [it for it in dup if it["scope"] == "ledger"]
    assert ledger_dup, _items(report)
    item = ledger_dup[0]
    assert item["batch_no"] == "B1"
    assert item["origin_waybills"] == ["W1"]
    assert "N1" in item["reason"] and "N2" in item["reason"]


def test_merge_distinct_batches_no_anomaly(temp_db):
    """不同货批号的并单（不同原单）链路守恒，不得误报。"""
    services.create_waybill(wb("W1", {"B1": 100}))
    services.create_waybill(wb("W2", {"B2": 200}))
    services.submit_package(
        pkg("P1", ["W1", "W2"], {"N1": {"B1": 100, "B2": 200}})
    )
    report = inspection.run_inspection()
    assert report["summary"]["anomalies"] == 0
    # 两个货批都能追溯到各自的根单
    origins = {
        it["batch_no"]: it["origin_waybills"]
        for it in _items(report)
        if it["package_no"] == "P1"
    }
    assert origins == {"B1": ["W1"], "B2": ["W2"]}


def test_ambiguous_root_merge_not_reported_as_duplicate(temp_db):
    """两个原单都有同名货批时，来源不唯一，不判定重复归属（不误报）。"""
    services.create_waybill(wb("W1", {"B1": 100}))
    services.create_waybill(wb("W2", {"B1": 100}))
    _inject_applied(
        temp_db,
        "PX",
        ["W1", "W2"],
        [{"waybill_no": "NX", "batches": [{"batch_no": "B1", "weight_kg": 200}]}],
    )
    report = inspection.run_inspection()
    assert not _anomalies(report, inspection.DUPLICATE_ATTRIBUTION)
    assert report["summary"]["anomalies"] == 0


def test_missing_old_and_new_references(temp_db):
    services.create_waybill(wb("W1", {"B1": 100}))
    services.submit_package(pkg("P1", ["W1"], {"N1": {"B1": 100}}))
    conn = _db(temp_db)
    try:
        # 删除被引用的旧单 W1 与新单 N1（先删货批满足外键）
        for no in ("W1", "N1"):
            wid = conn.execute(
                "SELECT id FROM waybills WHERE waybill_no=?", (no,)
            ).fetchone()[0]
            conn.execute("DELETE FROM batches WHERE waybill_id=?", (wid,))
            conn.execute("DELETE FROM waybills WHERE id=?", (wid,))
        conn.commit()
    finally:
        conn.close()

    report = inspection.run_inspection()
    missing = _anomalies(report, inspection.MISSING_REFERENCE)
    assert missing
    assert any("W1" in it["reason"] for it in missing)
    # 新单已不存在，其包内申报的货批给出引用缺失明细
    assert any(
        it["batch_no"] == "B1" and "N1" in (it["reason"] or "")
        for it in missing
    )


def test_snapshot_isolates_concurrent_write(temp_db):
    """快照固定后写入的新数据不得进入本次报告，但写入本身不被阻塞。"""
    services.create_waybill(wb("W1", {"B1": 100}))
    services.submit_package(pkg("P1", ["W1"], {"N1": {"B1": 100}}))

    written: dict[str, str] = {}

    def write_after_snapshot() -> None:
        view = services.create_waybill(wb("W_LATE", {"B9": 999}))
        written["no"] = view["waybill_no"]

    report = inspection.run_inspection(on_snapshot_ready=write_after_snapshot)

    assert written["no"] == "W_LATE"
    # 报告基于旧快照
    assert report["snapshot_counts"]["waybills"] == 2  # W1 + N1
    assert report["snapshot_counts"]["batches"] == 2
    assert all(it["package_no"] != "W_LATE" for it in _items(report))
    # 巡检结束后新写入已落库
    assert services.get_waybill("W_LATE")["status"] == "active"
    assert len(services.list_waybills()) == 3


def test_inspection_endpoint_and_page(client, temp_db):
    services.create_waybill(wb("W1", {"B1": 100}))
    services.submit_package(pkg("P1", ["W1"], {"N1": {"B1": 100}}))
    r = client.get("/api/inspections/quality")
    assert r.status_code == 200
    body = r.json()
    assert body["packages_checked"] == 1
    assert body["summary"]["anomalies"] == 0
    assert body["items"][0]["package_no"] == "P1"

    page = client.get("/")
    assert "质量巡检" in page.text
    assert "/api/inspections/quality" in page.text
