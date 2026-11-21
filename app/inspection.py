"""改单台账质量巡检。

巡检只读取**已生效**（``status='applied'``）的改单包，并在一个 SQLite 只读
事务内完成全部查询。WAL 模式下，该事务在第一次读取时固定快照；巡检期间其他
连接仍可提交原单/改单包，但本报告不会看到这些新写入。

核对口径
--------
* 引用完整性：包内引用的旧单、新单必须真实存在于运单台账。
* 重量守恒：沿每条原单链路，逐个货批比较改单包的改前（旧单）与改后（新单）
  重量；批次结束后再核对原单货批与有效台账总量。
* 重复归属：同一“原单链路的原始货批”（根单号 + 货批号）不得同时归属到
  多张有效运单。货批身份带根单号，因此不同原单下的同名货批不会被误报。
* 无法唯一追溯来源（例如并单时多个根单都有同名货批）的批次只做总量核对，
  不做重复归属判定，宁可漏报也不误报。
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from .database import snapshot_connection

# 异常码
WEIGHT_MISMATCH = "weight_mismatch"
DUPLICATE_ATTRIBUTION = "duplicate_attribution"
MISSING_REFERENCE = "missing_reference"
DECLARED_MISMATCH = "declared_mismatch"
INVALID_PAYLOAD = "invalid_payload"

CODE_LABELS = {
    WEIGHT_MISMATCH: "重量不守恒",
    DUPLICATE_ATTRIBUTION: "重复归属",
    MISSING_REFERENCE: "引用缺失",
    DECLARED_MISMATCH: "申报与实际不一致",
    INVALID_PAYLOAD: "包内容不可解析",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class _BatchOccurrence:
    waybill_no: str
    weight_kg: int
    roots: frozenset[str] | None  # None 表示无法唯一追溯


@dataclass
class _Waybill:
    id: int
    waybill_no: str
    status: str
    source_package_no: str | None
    replaced_by_pkg: str | None
    batches: list[tuple[str, int]] = field(default_factory=list)

    def batch_map(self) -> dict[str, int]:
        return {no: weight for no, weight in self.batches}


@dataclass
class _State:
    waybills: dict[str, _Waybill]
    applied: dict[str, dict[str, Any]]
    active: list[_Waybill]
    root_waybills: list[_Waybill]
    counts: dict[str, int]


def _load_snapshot(conn) -> _State:
    """全部数据都从同一个只读事务（同一快照）读取。"""
    waybills: dict[str, _Waybill] = {}
    by_id: dict[int, _Waybill] = {}
    for row in conn.execute(
        "SELECT id, waybill_no, status, source_package_no, replaced_by_pkg "
        "FROM waybills ORDER BY id"
    ):
        wb = _Waybill(
            id=row["id"],
            waybill_no=row["waybill_no"],
            status=row["status"],
            source_package_no=row["source_package_no"],
            replaced_by_pkg=row["replaced_by_pkg"],
        )
        waybills[wb.waybill_no] = wb
        by_id[wb.id] = wb
    for row in conn.execute(
        "SELECT waybill_id, batch_no, weight_kg FROM batches ORDER BY id"
    ):
        wb = by_id.get(row["waybill_id"])
        if wb is not None:
            wb.batches.append((row["batch_no"], row["weight_kg"]))

    applied: dict[str, dict[str, Any]] = {}
    package_count = 0
    for row in conn.execute(
        "SELECT package_no, payload FROM packages WHERE status = 'applied' "
        "ORDER BY id"
    ):
        package_count += 1
        try:
            payload = json.loads(row["payload"])
        except (TypeError, ValueError):
            payload = None
        if isinstance(payload, dict):
            payload["_valid"] = True
            applied[row["package_no"]] = payload
        else:
            applied[row["package_no"]] = {"_valid": False}

    active = [w for w in waybills.values() if w.status == "active"]
    roots = [w for w in waybills.values() if w.source_package_no is None]
    return _State(
        waybills=waybills,
        applied=applied,
        active=active,
        root_waybills=roots,
        counts={
            "packages": package_count,
            "waybills": len(waybills),
            "batches": sum(len(w.batches) for w in waybills.values()),
        },
    )


class _Lineage:
    """沿 source_package_no 反向追溯某货批的原始根单。"""

    def __init__(self, state: _State):
        self.state = state
        self._cache: dict[tuple[str, str], frozenset[str] | None] = {}
        self._visiting: set[tuple[str, str]] = set()

    def roots_for(self, waybill_no: str, batch_no: str) -> frozenset[str] | None:
        key = (waybill_no, batch_no)
        if key in self._cache:
            return self._cache[key]
        if key in self._visiting:  # 数据异常成环
            return None
        wb = self.state.waybills.get(waybill_no)
        if wb is None or batch_no not in wb.batch_map():
            return None
        if wb.source_package_no is None:
            result: frozenset[str] | None = frozenset({waybill_no})
        else:
            payload = self.state.applied.get(wb.source_package_no)
            if not payload or not payload.get("_valid"):
                # 来源包不是已生效包，无法安全追溯，按“无法唯一判定”处理。
                result = None
            else:
                parents = [
                    no
                    for no in payload.get("old_waybill_nos", [])
                    if isinstance(no, str)
                    and no in self.state.waybills
                    and batch_no in self.state.waybills[no].batch_map()
                ]
                if not parents:
                    result = None
                else:
                    self._visiting.add(key)
                    roots: set[str] = set()
                    for parent in parents:
                        parent_roots = self.roots_for(parent, batch_no)
                        if parent_roots is None:
                            roots = set()
                            result = None
                            break
                        roots |= parent_roots
                    else:
                        result = frozenset(roots)
                    self._visiting.discard(key)
        self._cache[key] = result
        return result


def _item(
    *,
    package_no: str,
    batch_no: str,
    old_waybills: list[str],
    new_waybills: list[str],
    origin_waybills: list[str],
    before: int | None,
    after: int | None,
    codes: list[str],
    reason: str | None,
    scope: str,
) -> dict[str, Any]:
    return {
        "package_no": package_no,
        "batch_no": batch_no,
        "old_waybills": sorted(set(old_waybills)),
        "new_waybills": sorted(set(new_waybills)),
        "origin_waybills": sorted(set(origin_waybills)),
        "before_weight_kg": before,
        "after_weight_kg": after,
        "status": "anomaly" if codes else "ok",
        "codes": codes,
        "reason": reason,
        "scope": scope,
    }


def _audit_package(
    package_no: str,
    payload: dict[str, Any],
    state: _State,
    lineage: _Lineage,
    items: list[dict[str, Any]],
    covered_weight_keys: set[tuple[str, str]],
    covered_duplicate_keys: set[tuple[str, str]],
) -> None:
    if not payload.get("_valid"):
        items.append(
            _item(
                package_no=package_no,
                batch_no="*",
                old_waybills=[],
                new_waybills=[],
                origin_waybills=[],
                before=None,
                after=None,
                codes=[INVALID_PAYLOAD],
                reason="已生效改单包的 payload 不是合法 JSON 对象，无法巡检",
                scope="package",
            )
        )
        return

    old_raw = payload.get("old_waybill_nos")
    new_raw = payload.get("new_waybills")
    old_nos = (
        [no for no in old_raw if isinstance(no, str)]
        if isinstance(old_raw, list)
        else []
    )
    new_specs = (
        [w for w in new_raw if isinstance(w, dict)]
        if isinstance(new_raw, list)
        else []
    )
    if not old_nos or not new_specs:
        # payload 能解析但结构不完整，仍属于“不可巡检”。
        items.append(
            _item(
                package_no=package_no,
                batch_no="*",
                old_waybills=old_nos,
                new_waybills=[],
                origin_waybills=[],
                before=None,
                after=None,
                codes=[INVALID_PAYLOAD],
                reason="已生效改单包缺少有效的旧单或新单列表，无法巡检",
                scope="package",
            )
        )
        return
    new_nos = [
        w.get("waybill_no")
        for w in new_specs
        if isinstance(w.get("waybill_no"), str)
    ]

    missing_old = [no for no in old_nos if no not in state.waybills]
    missing_new = [no for no in new_nos if no not in state.waybills]

    if missing_old or missing_new:
        for no in missing_old:
            items.append(
                _item(
                    package_no=package_no,
                    batch_no="*",
                    old_waybills=[no],
                    new_waybills=new_nos,
                    origin_waybills=[],
                    before=None,
                    after=None,
                    codes=[MISSING_REFERENCE],
                    reason=f"改单包引用的旧单不存在: {no}",
                    scope="package",
                )
            )
        for spec in new_specs:
            no = spec.get("waybill_no")
            if not isinstance(no, str) or no in state.waybills:
                continue
            spec_batches = spec.get("batches")
            if not isinstance(spec_batches, list):
                spec_batches = []
            spec_batches = [
                b for b in spec_batches if isinstance(b, dict)
            ]
            if not spec_batches:
                items.append(
                    _item(
                        package_no=package_no,
                        batch_no="*",
                        old_waybills=old_nos,
                        new_waybills=[no],
                        origin_waybills=[],
                        before=None,
                        after=None,
                        codes=[MISSING_REFERENCE],
                        reason=f"改单包引用的新单不存在: {no}",
                        scope="package",
                    )
                )
                continue
            for batch in spec_batches:
                batch_no = batch.get("batch_no")
                declared = batch.get("weight_kg")
                if not isinstance(batch_no, str):
                    continue
                before = 0
                old_present = True
                for old_no in old_nos:
                    old_wb = state.waybills.get(old_no)
                    if old_wb is None:
                        old_present = False
                        break
                    before += old_wb.batch_map().get(batch_no, 0)
                items.append(
                    _item(
                        package_no=package_no,
                        batch_no=batch_no,
                        old_waybills=old_nos,
                        new_waybills=[no],
                        origin_waybills=[],
                        before=before if old_present else None,
                        after=None,
                        codes=[MISSING_REFERENCE],
                        reason=(
                            f"改单包引用的新单不存在: {no}"
                            f"（包内申报货批 {batch_no} = {declared}kg）"
                        ),
                        scope="package",
                    )
                )
        # 引用不完整时不继续重量/重复归属判定，避免制造二次误报。
        return

    # 改前：旧单在快照中的实际货批；改后：新单在快照中的实际货批。
    old_by_batch: dict[str, list[_BatchOccurrence]] = {}
    new_by_batch: dict[str, list[_BatchOccurrence]] = {}
    for no in old_nos:
        wb = state.waybills[no]
        for batch_no, weight in wb.batches:
            old_by_batch.setdefault(batch_no, []).append(
                _BatchOccurrence(no, weight, lineage.roots_for(no, batch_no))
            )
    for no in new_nos:
        wb = state.waybills[no]
        for batch_no, weight in wb.batches:
            new_by_batch.setdefault(batch_no, []).append(
                _BatchOccurrence(no, weight, lineage.roots_for(no, batch_no))
            )

    declared_by_batch: dict[str, int] = {}
    for spec in new_specs:
        spec_batches = spec.get("batches")
        if not isinstance(spec_batches, list):
            continue
        for batch in spec_batches:
            if not isinstance(batch, dict):
                continue
            batch_no = batch.get("batch_no")
            weight = batch.get("weight_kg")
            if isinstance(batch_no, str) and isinstance(weight, int):
                declared_by_batch[batch_no] = (
                    declared_by_batch.get(batch_no, 0) + weight
                )

    for batch_no in sorted(set(old_by_batch) | set(new_by_batch)):
        old_occ = old_by_batch.get(batch_no, [])
        new_occ = new_by_batch.get(batch_no, [])
        before = sum(o.weight_kg for o in old_occ)
        after = sum(o.weight_kg for o in new_occ)
        old_ways = [o.waybill_no for o in old_occ]
        new_ways = [o.waybill_no for o in new_occ]

        root_sets = [o.roots for o in old_occ]
        unambiguous_roots: frozenset[str] | None
        if old_occ and all(r is not None and len(r) == 1 for r in root_sets):
            merged: set[str] = set()
            for roots in root_sets:
                assert roots is not None
                merged |= roots
            unambiguous_roots = frozenset(merged)
        else:
            unambiguous_roots = None
        origin_key: tuple[str, str] | None = None
        if unambiguous_roots is not None and len(unambiguous_roots) == 1:
            origin_key = (next(iter(unambiguous_roots)), batch_no)

        codes: list[str] = []
        reasons: list[str] = []

        if before != after:
            codes.append(WEIGHT_MISMATCH)
            reasons.append(
                f"货批 {batch_no} 重量不守恒：改前 {before}kg，改后 {after}kg"
                f"（差 {after - before:+d}kg）"
            )
            if origin_key is not None:
                covered_weight_keys.add(origin_key)

        declared = declared_by_batch.get(batch_no)
        if declared is not None and declared != after:
            codes.append(DECLARED_MISMATCH)
            reasons.append(
                f"货批 {batch_no} 包内申报重量与实际新单台账不一致："
                f"申报 {declared}kg，实际 {after}kg"
            )

        # 同一个原始货批被本包拆到多张新单：仅当来源根单唯一时才判定，
        # 不同原单下的同名货批（来源不唯一）不会被误报。
        if origin_key is not None and len(set(new_ways)) > 1:
            codes.append(DUPLICATE_ATTRIBUTION)
            covered_duplicate_keys.add(origin_key)
            reasons.append(
                f"原始货批（原单 {origin_key[0]} / 货批 {batch_no}）"
                f"重复归属到 {len(set(new_ways))} 张新单："
                f"{', '.join(sorted(set(new_ways)))}"
            )

        items.append(
            _item(
                package_no=package_no,
                batch_no=batch_no,
                old_waybills=old_ways,
                new_waybills=new_ways,
                origin_waybills=(
                    sorted(unambiguous_roots)
                    if unambiguous_roots is not None
                    else []
                ),
                before=before,
                after=after,
                codes=codes,
                reason="；".join(reasons) if reasons else None,
                scope="package",
            )
        )


def _audit_ledger(
    state: _State,
    lineage: _Lineage,
    items: list[dict[str, Any]],
    covered_weight_keys: set[tuple[str, str]],
    covered_duplicate_keys: set[tuple[str, str]],
) -> None:
    """有效台账终态核对：按原始根单 + 货批汇总，识别跨包重复与链路失衡。"""
    occurrences: dict[tuple[str, str], list[_BatchOccurrence]] = {}
    unsafe_keys: set[tuple[str, str]] = set()
    unknown_batches: set[str] = set()

    for wb in state.active:
        for batch_no, weight in wb.batches:
            roots = lineage.roots_for(wb.waybill_no, batch_no)
            if roots is not None and len(roots) == 1:
                key = (next(iter(roots)), batch_no)
                occurrences.setdefault(key, []).append(
                    _BatchOccurrence(wb.waybill_no, weight, roots)
                )
            elif roots is not None:
                for root in roots:
                    unsafe_keys.add((root, batch_no))
            else:
                unknown_batches.add(batch_no)

    for root_wb in state.root_waybills:
        root_batches = root_wb.batch_map()
        for batch_no, root_weight in sorted(root_batches.items()):
            key = (root_wb.waybill_no, batch_no)
            if key in unsafe_keys or batch_no in unknown_batches:
                # 来源无法唯一追溯：宁可不判，避免同名货批误报。
                continue
            occ = occurrences.get(key, [])
            if len(occ) > 1:
                if key not in covered_duplicate_keys:
                    all_ways = sorted(o.waybill_no for o in occ)
                    for occurrence in occ[1:]:
                        producer = state.waybills[occurrence.waybill_no]
                        items.append(
                            _item(
                                package_no=(
                                    producer.source_package_no or "（原单）"
                                ),
                                batch_no=batch_no,
                                old_waybills=[root_wb.waybill_no],
                                new_waybills=[occurrence.waybill_no],
                                origin_waybills=[root_wb.waybill_no],
                                before=root_weight,
                                after=occurrence.weight_kg,
                                codes=[DUPLICATE_ATTRIBUTION],
                                reason=(
                                    f"原始货批（原单 {root_wb.waybill_no} / "
                                    f"货批 {batch_no}）在有效台账中重复归属到 "
                                    f"{len(all_ways)} 张运单："
                                    f"{', '.join(all_ways)}"
                                ),
                                scope="ledger",
                            )
                        )
                    covered_duplicate_keys.add(key)
                continue

            total = sum(o.weight_kg for o in occ)
            if total != root_weight and key not in covered_weight_keys:
                new_nos = [o.waybill_no for o in occ]
                items.append(
                    _item(
                        package_no=(
                            state.waybills[occ[0].waybill_no].source_package_no
                            or "（原单）"
                        )
                        if occ
                        else (root_wb.replaced_by_pkg or "（原单）"),
                        batch_no=batch_no,
                        old_waybills=[root_wb.waybill_no],
                        new_waybills=new_nos,
                        origin_waybills=[root_wb.waybill_no],
                        before=root_weight,
                        after=total,
                        codes=[WEIGHT_MISMATCH],
                        reason=(
                            f"原单链路重量不守恒：原单 {root_wb.waybill_no} "
                            f"货批 {batch_no} 原重 {root_weight}kg → "
                            f"有效台账 {total}kg（差 {total - root_weight:+d}kg）"
                        ),
                        scope="ledger",
                    )
                )
                covered_weight_keys.add(key)


def run_inspection(
    on_snapshot_ready: Callable[[], None] | None = None,
) -> dict[str, Any]:
    """固定一个读快照并生成质量巡检报告。

    ``on_snapshot_ready`` 仅供测试/诊断：快照固定后、读取业务表前调用，
    用于验证巡检期间的新写入不会进入本报告。
    """
    with snapshot_connection() as conn:
        # snapshot_connection 在进入时已固定快照，用该时刻作为报告快照时间。
        snapshot_at = _now()
        if on_snapshot_ready is not None:
            on_snapshot_ready()
        state = _load_snapshot(conn)

    lineage = _Lineage(state)
    items: list[dict[str, Any]] = []
    covered_weight_keys: set[tuple[str, str]] = set()
    covered_duplicate_keys: set[tuple[str, str]] = set()

    for package_no in sorted(state.applied):
        _audit_package(
            package_no,
            state.applied[package_no],
            state,
            lineage,
            items,
            covered_weight_keys,
            covered_duplicate_keys,
        )

    _audit_ledger(
        state, lineage, items, covered_weight_keys, covered_duplicate_keys
    )

    # 稳定的展示顺序：异常在前，其次包号/货批。
    items.sort(
        key=lambda it: (
            0 if it["status"] == "anomaly" else 1,
            it["package_no"],
            it["batch_no"],
            it["scope"],
        )
    )
    by_code = {code: 0 for code in CODE_LABELS}
    for it in items:
        for code in it["codes"]:
            by_code[code] = by_code.get(code, 0) + 1
    anomaly_count = sum(1 for it in items if it["status"] == "anomaly")
    return {
        "inspection_id": uuid.uuid4().hex,
        "generated_at": _now(),
        "snapshot_at": snapshot_at,
        "read_snapshot": True,
        "packages_checked": len(state.applied),
        "snapshot_counts": state.counts,
        "summary": {
            "items": len(items),
            "anomalies": anomaly_count,
            "by_code": by_code,
        },
        "items": items,
    }
