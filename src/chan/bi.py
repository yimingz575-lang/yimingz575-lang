from __future__ import annotations

from collections.abc import Sequence
import math
from pathlib import Path
from typing import Any

import pandas as pd

from src.chan.fractal import (
    FRACTAL_COLUMNS,
    TYPE_BOTTOM,
    TYPE_TOP,
    build_virtual_klines,
    detect_candidate_fractals,
)
from src.chan.inclusion import process_inclusions

MIN_CENTER_GAP_FOR_BI = 4
MIN_CENTER_TO_CENTER_BARS_FOR_BI = 5
MIN_BI_KLINE_COUNT = MIN_CENTER_TO_CENTER_BARS_FOR_BI
DIRECTION_UP = "up"
DIRECTION_DOWN = "down"
DEFAULT_TAIL_RAW_START = 4375
DEFAULT_TAIL_RAW_END = 5064
DEFAULT_MAX_BI_ROLLBACK = 15
MAX_BI_ROLLBACK = DEFAULT_MAX_BI_ROLLBACK
STUCK_CANDIDATE_THRESHOLD = 20
FALLBACK_AFFECTED_BI_THRESHOLD = 3
FALLBACK_BI_COLOR = "yellow"
FALLBACK_BI_REASON = "affected_confirmed_bi_count >= 3"

EFFECTIVE_FRACTAL_COLUMNS = [*FRACTAL_COLUMNS, "is_effective"]
BI_COLUMNS = [
    "direction",
    "start_type",
    "end_type",
    "start_virtual_index",
    "end_virtual_index",
    "start_center_index",
    "end_center_index",
    "start_original_index",
    "end_original_index",
    "start_x",
    "end_x",
    "start_date",
    "end_date",
    "start_price",
    "end_price",
    "start_source_indices",
    "end_source_indices",
    "start_fractal",
    "end_fractal",
    "kline_count",
    "is_valid",
    "is_temporary",
    "is_fallback_bi",
    "fallback_reason",
    "color",
    "affected_confirmed_bi_count",
    "fallback_level",
]
TAIL_REGION_DEBUG_COLUMNS = [
    "candidate_order",
    "virtual_index",
    "raw_start_index",
    "raw_end_index",
    "date",
    "type",
    "high",
    "low",
    "anchor_virtual_index",
    "candidate_virtual_index",
    "attempt_result",
    "reject_reason",
    "center_gap",
    "shared_bar",
    "neutral_bar_count",
    "extreme_ok",
    "would_form_direction",
]
ROLLBACK_DEBUG_COLUMNS = [
    "stuck_candidate_index",
    "stuck_virtual_index",
    "stuck_raw_index",
    "rollback_count",
    "kept_bis_count",
    "tail_bis_count",
    "old_last_raw_index",
    "new_last_raw_index",
    "old_bis_count",
    "new_bis_count",
    "accepted",
    "reason",
]


def generate_bi_result(df: pd.DataFrame, debug: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Confirm effective fractals and connect them into valid bi records."""
    inclusion_result = process_inclusions(df)
    virtual_bars = inclusion_result.standard_bars
    candidates = detect_candidate_fractals(virtual_bars)
    if candidates.empty:
        return _empty_effective_fractals(), _empty_bis()

    candidates = candidates.sort_values(["center_index", "index"]).reset_index(drop=True)
    effective_fractals, bis = _confirm_fractals_and_bis(candidates, virtual_bars, debug=debug)
    return effective_fractals, bis


def confirm_effective_fractals(df: pd.DataFrame, debug: bool = False) -> pd.DataFrame:
    """Return only fractals confirmed by at least one valid bi."""
    effective_fractals, _ = generate_bi_result(df, debug=debug)
    return effective_fractals


def generate_bis(df: pd.DataFrame, debug: bool = False) -> pd.DataFrame:
    """Generate valid Chan bi records from the original K-line DataFrame."""
    _, bis = generate_bi_result(df, debug=debug)
    return bis


def write_bi_debug_report(
    df: pd.DataFrame,
    output_dir: str | Path = "output",
    manual_expected_bis: Sequence[tuple[int, int]] | None = None,
) -> dict[str, Any]:
    """Generate bi diagnostics files for the given K-line DataFrame."""
    bars = build_virtual_klines(df)
    raw_fractals = detect_candidate_fractals(bars)
    cleaned_fractals = raw_fractals.sort_values(["center_index", "index"]).reset_index(drop=True)
    attempts: list[dict] = []
    _, confirmed_bis = build_bis_incremental(
        bars,
        cleaned_fractals,
        attempt_records=attempts,
    )
    return debug_bi_generation(
        bars=bars,
        raw_fractals=raw_fractals,
        cleaned_fractals=cleaned_fractals,
        confirmed_bis=confirmed_bis,
        original_kline_count=len(df),
        output_dir=output_dir,
        manual_expected_bis=manual_expected_bis,
        attempt_records=attempts,
    )


def debug_bi_generation(
    bars: Sequence[Any],
    raw_fractals: pd.DataFrame,
    cleaned_fractals: pd.DataFrame,
    confirmed_bis: pd.DataFrame | None = None,
    original_kline_count: int | None = None,
    output_dir: str | Path = "output",
    manual_expected_bis: Sequence[tuple[int, int]] | None = None,
    attempt_records: list[dict] | None = None,
) -> dict[str, Any]:
    """Write detailed diagnostics for missed/accepted bi generation decisions."""
    cleaned_fractals = cleaned_fractals.sort_values(["center_index", "index"]).reset_index(drop=True)
    if attempt_records is None or confirmed_bis is None:
        attempt_records = []
        _, confirmed_bis = build_bis_incremental(
            bars,
            cleaned_fractals,
            attempt_records=attempt_records,
        )

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    fractals_debug = _make_fractals_debug(raw_fractals, cleaned_fractals)
    attempts_debug = pd.DataFrame(
        attempt_records,
        columns=[
            "anchor_index",
            "anchor_virtual_index",
            "anchor_source_start_index",
            "anchor_source_end_index",
            "anchor_date",
            "anchor_type",
            "candidate_index",
            "candidate_virtual_index",
            "candidate_source_start_index",
            "candidate_source_end_index",
            "candidate_date",
            "candidate_type",
            "bar_count",
            "center_gap",
            "neutral_bar_count",
            "extreme_ok",
            "result",
            "reason",
            "would_form_direction",
            "same_type",
            "shared_kline",
            "has_neutral_bar_between_fractals",
            "kline_count_ok",
        ],
    )
    manual_debug = _make_manual_expected_debug(
        manual_expected_bis or [],
        raw_fractals,
        cleaned_fractals,
        confirmed_bis,
        bars,
    )
    suspected_missing = _make_suspected_missing_bis(raw_fractals, confirmed_bis)
    tail_region_debug, tail_region_stats = _make_tail_region_debug(
        bars=bars,
        cleaned_fractals=cleaned_fractals,
        attempts_debug=attempts_debug,
        confirmed_bis=confirmed_bis,
        target_raw_start=DEFAULT_TAIL_RAW_START,
        target_raw_end=DEFAULT_TAIL_RAW_END,
    )
    rollback_debug = pd.DataFrame(
        confirmed_bis.attrs.get("rollback_debug_records", []),
        columns=ROLLBACK_DEBUG_COLUMNS,
    )

    fractals_path = output_path / "fractals_debug.csv"
    attempts_path = output_path / "bi_attempts_debug.csv"
    suspected_missing_path = output_path / "suspected_missing_bis.csv"
    tail_region_path = output_path / "bi_tail_region_debug.csv"
    rollback_path = output_path / "bi_rollback_debug.csv"
    report_path = output_path / "bi_debug_report.txt"
    fractals_debug.to_csv(fractals_path, index=False, encoding="utf-8-sig")
    attempts_debug.to_csv(attempts_path, index=False, encoding="utf-8-sig")
    suspected_missing.to_csv(suspected_missing_path, index=False, encoding="utf-8-sig")
    tail_region_debug.to_csv(tail_region_path, index=False, encoding="utf-8-sig")
    rollback_debug.to_csv(rollback_path, index=False, encoding="utf-8-sig")

    active_bi_endpoint_extensions_count = int(
        confirmed_bis.attrs.get(
            "active_bi_endpoint_extensions_count",
            _count_attempt_reason(attempts_debug, "extend_active_bi_endpoint"),
        )
    )
    reverse_reject_extreme_check_failed_count = int(
        confirmed_bis.attrs.get(
            "reverse_reject_extreme_check_failed_count",
            _count_attempt_reason(attempts_debug, "reject_reverse_extreme_check_failed")
            + _count_attempt_reason(attempts_debug, "reject_extreme_check_failed"),
        )
    )
    reverse_reject_not_enough_bars_count = int(
        confirmed_bis.attrs.get(
            "reverse_reject_not_enough_bars_count",
            _count_attempt_reason(attempts_debug, "reject_reverse_not_enough_bars")
            + _count_attempt_reason(attempts_debug, "reject_reverse_not_enough_center_gap")
            + _count_attempt_reason(attempts_debug, "reject_not_enough_center_gap"),
        )
    )

    stats = {
        "original_kline_count": original_kline_count,
        "virtual_kline_count": len(bars),
        "standard_bars_count": len(bars),
        "raw_fractals_count": len(raw_fractals),
        "candidate_fractals_count": len(raw_fractals),
        "cleaned_fractals_count": len(cleaned_fractals),
        "confirmed_bis_count": len(confirmed_bis),
        "locked_bis_count": int(confirmed_bis.attrs.get("locked_bis_count", max(len(confirmed_bis) - 1, 0))),
        "pending_bi_count": int(confirmed_bis.attrs.get("pending_bi_count", 0)),
        "active_bi_count": int(confirmed_bis.attrs.get("active_bi_count", 1 if len(confirmed_bis) else 0)),
        "final_confirmed_bis_count": int(
            confirmed_bis.attrs.get("final_confirmed_bis_count", len(confirmed_bis))
        ),
        "active_bi_endpoint_extensions_count": active_bi_endpoint_extensions_count,
        "reverse_reject_extreme_check_failed_count": reverse_reject_extreme_check_failed_count,
        "reverse_reject_not_enough_bars_count": reverse_reject_not_enough_bars_count,
        "suspected_missing_bis_count": int(suspected_missing["suspected_missing"].sum())
        if not suspected_missing.empty
        else 0,
        "continuity_ok": validate_bi_sequence_continuity(confirmed_bis),
        "all_extreme_ok": _all_bis_pass_extreme(bars, confirmed_bis),
        "report_path": str(report_path),
        "fractals_csv_path": str(fractals_path),
        "attempts_csv_path": str(attempts_path),
        "suspected_missing_csv_path": str(suspected_missing_path),
        "tail_region_csv_path": str(tail_region_path),
        "rollback_enabled": bool(confirmed_bis.attrs.get("rollback_enabled", True)),
        "max_bi_rollback": int(confirmed_bis.attrs.get("max_bi_rollback", MAX_BI_ROLLBACK)),
        "stuck_candidate_threshold": int(
            confirmed_bis.attrs.get("stuck_candidate_threshold", STUCK_CANDIDATE_THRESHOLD)
        ),
        "rollback_trigger_count": int(confirmed_bis.attrs.get("rollback_trigger_count", 0)),
        "rollback_success_count": int(confirmed_bis.attrs.get("rollback_success_count", 0)),
        "rollback_failed_count": int(confirmed_bis.attrs.get("rollback_failed_count", 0)),
        "accepted_rollback_count": confirmed_bis.attrs.get("accepted_rollback_count", None),
        "fallback_trigger_count": int(confirmed_bis.attrs.get("fallback_trigger_count", 0)),
        "fallback_bi_count": int(confirmed_bis.attrs.get("fallback_bi_count", 0)),
        "affected_confirmed_bi_count": int(confirmed_bis.attrs.get("affected_confirmed_bi_count", 0)),
        "fallback_reason": confirmed_bis.attrs.get("fallback_reason", None),
        "rollback_csv_path": str(rollback_path),
        **tail_region_stats,
    }

    report_lines = _make_debug_report_lines(
        stats=stats,
        raw_fractals=raw_fractals,
        cleaned_fractals=cleaned_fractals,
        confirmed_bis=confirmed_bis,
        attempts_debug=attempts_debug,
        manual_debug=manual_debug,
    )
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    return {
        **stats,
        "fractals_debug": fractals_debug,
        "attempts_debug": attempts_debug,
        "suspected_missing_bis": suspected_missing,
        "manual_expected_debug": manual_debug,
        "tail_region_debug": tail_region_debug,
        "rollback_debug": rollback_debug,
    }


def can_form_bi(start_fractal: Any, end_fractal: Any) -> bool:
    """Check whether two opposite fractals can form one valid bi."""
    start_type = _get_value(start_fractal, "type")
    end_type = _get_value(end_fractal, "type")
    if start_type == end_type:
        return False
    if {start_type, end_type} != {TYPE_TOP, TYPE_BOTTOM}:
        return False

    if _shares_kline(start_fractal, end_fractal):
        return False

    center_gap = _calculate_center_gap(start_fractal, end_fractal)
    if center_gap < MIN_CENTER_GAP_FOR_BI:
        return False

    if not _has_neutral_bar_between_fractals(start_fractal, end_fractal):
        return False

    return is_price_range_separated(start_fractal, end_fractal)


def is_price_range_separated(start_fractal: Any, end_fractal: Any) -> bool:
    """Validate endpoint price order; historical name kept for existing callers."""
    start_type = _get_value(start_fractal, "type")
    end_type = _get_value(end_fractal, "type")
    start_price = float(_get_value(start_fractal, "price"))
    end_price = float(_get_value(end_fractal, "price"))

    if start_type == TYPE_TOP and end_type == TYPE_BOTTOM:
        return start_price > end_price
    if start_type == TYPE_BOTTOM and end_type == TYPE_TOP:
        return start_price < end_price
    return False


def validate_bi_extreme(
    bars: Sequence[Any],
    start_fractal: Any,
    end_fractal: Any,
) -> bool:
    """Require bi endpoints to be the high/low extremes of their virtual-bar interval."""
    start_type = _get_value(start_fractal, "type")
    end_type = _get_value(end_fractal, "type")
    if {start_type, end_type} != {TYPE_TOP, TYPE_BOTTOM}:
        return False

    start_center_index = _get_center_index(start_fractal)
    end_center_index = _get_center_index(end_fractal)
    left_index, right_index = sorted([start_center_index, end_center_index])
    if left_index < 0 or right_index >= len(bars):
        return False

    interval_bars = bars[left_index : right_index + 1]
    if not interval_bars:
        return False

    interval_high = max(float(_get_bar_value(bar, "high")) for bar in interval_bars)
    interval_low = min(float(_get_bar_value(bar, "low")) for bar in interval_bars)
    start_high = float(_get_value(start_fractal, "high"))
    start_low = float(_get_value(start_fractal, "low"))
    end_high = float(_get_value(end_fractal, "high"))
    end_low = float(_get_value(end_fractal, "low"))

    if start_type == TYPE_TOP and end_type == TYPE_BOTTOM:
        return _prices_equal(start_high, interval_high) and _prices_equal(end_low, interval_low)
    if start_type == TYPE_BOTTOM and end_type == TYPE_TOP:
        return _prices_equal(start_low, interval_low) and _prices_equal(end_high, interval_high)
    return False


def validate_bi_sequence_continuity(bis: pd.DataFrame, debug: bool = False) -> bool:
    """Validate that confirmed bis form one continuous alternating sequence."""
    if bis.empty:
        return True

    bis = _standard_bis_for_strict_validation(bis)
    if bis.empty:
        return True

    is_valid = True
    for position in range(len(bis) - 1):
        current = bis.iloc[position]
        following = bis.iloc[position + 1]
        same_endpoint = (
            current["end_fractal"] == following["start_fractal"]
            if "end_fractal" in bis.columns and "start_fractal" in bis.columns
            else (
                current["end_type"] == following["start_type"]
                and current["end_virtual_index"] == following["start_virtual_index"]
                and current["end_original_index"] == following["start_original_index"]
            )
        )
        alternating_direction = current["direction"] != following["direction"]
        if same_endpoint and alternating_direction:
            continue

        is_valid = False
        if debug:
            print(
                {
                    "action": "continuity_break",
                    "between": [position, position + 1],
                    "current_end": {
                        "type": current["end_type"],
                        "virtual_index": int(current["end_virtual_index"]),
                        "original_index": current["end_original_index"],
                        "direction": current["direction"],
                    },
                    "next_start": {
                        "type": following["start_type"],
                        "virtual_index": int(following["start_virtual_index"]),
                        "original_index": following["start_original_index"],
                        "direction": following["direction"],
                    },
                    "same_endpoint": same_endpoint,
                    "alternating_direction": alternating_direction,
                }
            )

    return is_valid


def _standard_bis_for_strict_validation(bis: pd.DataFrame) -> pd.DataFrame:
    if "is_fallback_bi" not in bis.columns:
        return bis.reset_index(drop=True)
    return bis[bis["is_fallback_bi"] != True].reset_index(drop=True)


def _confirm_fractals_and_bis(
    candidates: pd.DataFrame,
    virtual_bars: Sequence[Any],
    debug: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return build_bis_incremental(virtual_bars, candidates, debug=debug)


def build_bis_incremental(
    bars: Sequence[Any],
    fractals: pd.DataFrame,
    debug: bool = False,
    attempt_records: list[dict] | None = None,
    rollback_enabled: bool = True,
    max_rollback: int = MAX_BI_ROLLBACK,
    stuck_candidate_threshold: int = STUCK_CANDIDATE_THRESHOLD,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build valid bi records with locked history, active bi, and bounded rollback."""
    _validate_standard_bars_for_bi(bars)
    if fractals.empty:
        return _empty_effective_fractals(), _empty_bis()

    fractals = _normalize_fractals_for_bi(fractals)
    candidates = fractals.sort_values(["center_index", "index"]).reset_index(drop=True)
    anchor: pd.Series | None = None
    locked_bis: list[tuple[pd.Series, pd.Series]] = []
    pending_bi: tuple[pd.Series, pd.Series] | None = None
    active_bi: tuple[pd.Series, pd.Series] | None = None
    active_bi_endpoint_extensions_count = 0
    reverse_reject_extreme_check_failed_count = 0
    reverse_reject_not_enough_bars_count = 0
    if debug:
        print(
            {
                "action": "bi_build_start",
                "raw_fractals_count": len(fractals),
                "cleaned_fractals_count": len(candidates),
            }
        )

    for _, candidate in candidates.iterrows():
        if active_bi is not None:
            active_start, active_end = active_bi
            anchor = active_end

            if candidate["type"] == active_end["type"]:
                stronger = _pick_stronger_same_type(active_end, candidate)
                if _same_fractal(stronger, active_end):
                    _record_bi_attempt(
                        attempt_records,
                        bars,
                        active_end,
                        candidate,
                        result="reject",
                        reason="reject_extension_not_more_extreme",
                    )
                    _debug_bi_event(
                        debug,
                        "reject_extension_not_more_extreme",
                        anchor=active_end,
                        candidate=candidate,
                        same_direction=True,
                        replaced_anchor=False,
                        reason="extension_endpoint_not_more_extreme",
                    )
                    continue

                extension_reject_reason = _get_bi_reject_reason(active_start, candidate)
                extension_extreme_ok = (
                    extension_reject_reason is None and validate_bi_extreme(bars, active_start, candidate)
                )
                if extension_reject_reason is None and extension_extreme_ok:
                    active_bi = (active_start, candidate)
                    active_bi_endpoint_extensions_count += 1
                    _record_bi_attempt(
                        attempt_records,
                        bars,
                        active_end,
                        candidate,
                        result="extend_active_bi_endpoint",
                        reason="extend_active_bi_endpoint",
                        extreme_ok=True,
                    )
                    _debug_bi_event(
                        debug,
                        "extend_active_bi_endpoint",
                        anchor=active_end,
                        candidate=candidate,
                        same_direction=True,
                        replaced_anchor=True,
                        active_start=_format_fractal_for_debug(active_start),
                    )
                    continue

                extension_reason = _debug_reject_action(extension_reject_reason or "extreme_validation_failed")
                _record_bi_attempt(
                    attempt_records,
                    bars,
                    active_end,
                    candidate,
                    result="reject",
                    reason=extension_reason,
                    extreme_ok=extension_extreme_ok,
                )
                _debug_bi_event(
                    debug,
                    extension_reason,
                    anchor=active_end,
                    candidate=candidate,
                    same_direction=True,
                    replaced_anchor=False,
                    reason=extension_reject_reason or "extreme_validation_failed",
                    active_start=_format_fractal_for_debug(active_start),
                )
                continue

            reverse_reject_reason = _get_bi_reject_reason(active_end, candidate)
            reverse_extreme_ok = (
                reverse_reject_reason is None and validate_bi_extreme(bars, active_end, candidate)
            )
            if reverse_reject_reason is None and reverse_extreme_ok:
                if pending_bi is not None:
                    locked_bis.append(pending_bi)
                pending_bi = active_bi
                active_bi = (active_end, candidate)
                _record_bi_attempt(
                    attempt_records,
                    bars,
                    active_end,
                    candidate,
                    result="lock_previous_and_start_new_active_bi",
                    reason="lock_previous_and_start_new_active_bi",
                    extreme_ok=True,
                )
                _debug_bi_event(
                    debug,
                    "lock_previous_and_start_new_active_bi",
                    anchor=active_end,
                    candidate=candidate,
                    same_direction=False,
                    kline_count_ok=True,
                    extreme_ok=True,
                    generated_bi=True,
                    locked_bis_count=len(locked_bis),
                )
                continue

            reopened_active = _try_reopen_active_window_with_reverse_candidate(
                bars=bars,
                candidate=candidate,
                pending_bi=pending_bi,
                active_bi=active_bi,
                debug=debug,
            )
            if reopened_active is not None:
                pending_bi = None
                active_bi = reopened_active
                _record_bi_attempt(
                    attempt_records,
                    bars,
                    active_end,
                    candidate,
                    result="reopen_active_window_with_more_extreme_reverse",
                    reason="reopen_active_window_with_more_extreme_reverse",
                    extreme_ok=reverse_extreme_ok,
                )
                anchor = candidate
                continue

            reverse_reason = _debug_reverse_reject_action(reverse_reject_reason or "extreme_validation_failed")
            if reverse_reason == "reject_extreme_check_failed":
                reverse_reject_extreme_check_failed_count += 1
            if reverse_reason in {"reject_not_enough_bars", "reject_not_enough_center_gap"}:
                reverse_reject_not_enough_bars_count += 1
            _record_bi_attempt(
                attempt_records,
                bars,
                active_end,
                candidate,
                result="reject",
                reason=reverse_reason,
                extreme_ok=reverse_extreme_ok,
            )
            _debug_bi_event(
                debug,
                reverse_reason,
                anchor=active_end,
                candidate=candidate,
                same_direction=False,
                kline_count_ok=_has_enough_kline_count(active_end, candidate),
                extreme_ok=reverse_extreme_ok,
                generated_bi=False,
                reason=reverse_reject_reason or "extreme_validation_failed",
            )
            continue

        if anchor is None:
            anchor = candidate
            _debug_bi_event(debug, "set_anchor", anchor=anchor, candidate=candidate)
            continue

        if candidate["type"] == anchor["type"]:
            stronger = _pick_stronger_same_type(anchor, candidate)
            if _same_fractal(stronger, anchor):
                _record_bi_attempt(
                    attempt_records,
                    bars,
                    anchor,
                    candidate,
                    result="reject",
                    reason="reject_same_type_not_more_extreme",
                )
                _debug_bi_event(
                    debug,
                    "reject_same_type_not_more_extreme",
                    anchor=anchor,
                    candidate=candidate,
                    same_direction=True,
                    replaced_anchor=False,
                    reason="same_type_not_more_extreme",
                )
                continue

            action = _same_type_replace_action(candidate)
            _record_bi_attempt(
                attempt_records,
                bars,
                anchor,
                candidate,
                result=action,
                reason="replace_anchor_same_type_more_extreme",
            )
            _debug_bi_event(debug, action, anchor=anchor, candidate=candidate, same_direction=True)
            anchor = stronger
            continue

        reject_reason = _get_bi_reject_reason(anchor, candidate)
        extreme_ok = reject_reason is None and validate_bi_extreme(bars, anchor, candidate)
        if reject_reason is None and extreme_ok:
            _record_bi_attempt(
                attempt_records,
                bars,
                anchor,
                candidate,
                result="start_first_active_bi",
                reason="start_first_active_bi",
                extreme_ok=True,
            )
            active_bi = (anchor, candidate)
            _debug_bi_event(
                debug,
                "start_first_active_bi",
                anchor=anchor,
                candidate=candidate,
                same_direction=False,
                kline_count_ok=True,
                extreme_ok=True,
                generated_bi=True,
            )
            anchor = candidate
            continue

        reason = reject_reason or "extreme_validation_failed"
        debug_reason = _debug_reject_action(reason)
        _record_bi_attempt(
            attempt_records,
            bars,
            anchor,
            candidate,
            result="reject",
            reason=debug_reason,
            extreme_ok=extreme_ok,
        )
        _debug_bi_event(
            debug,
            debug_reason,
            anchor=anchor,
            candidate=candidate,
            same_direction=False,
            kline_count_ok=_has_enough_kline_count(anchor, candidate),
            extreme_ok=extreme_ok,
            generated_bi=False,
            reason=reason,
        )

    bi_pairs = [*locked_bis]
    if pending_bi is not None:
        bi_pairs.append(pending_bi)
    if active_bi is not None:
        bi_pairs.append(active_bi)

    fallback_metadata: list[dict[str, Any]] = []
    pre_rollback_bi_pairs = [*bi_pairs]
    rollback_stats = _empty_rollback_stats()
    rollback_records: list[dict[str, Any]] = []
    if rollback_enabled:
        rollback_bi_pairs, rollback_stats, rollback_records = try_rollback_and_rebuild_tail(
            standard_bars=bars,
            candidate_fractals=candidates,
            confirmed_bis=bi_pairs,
            max_rollback=max_rollback,
            stuck_candidate_threshold=stuck_candidate_threshold,
        )
        bi_pairs, fallback_metadata = apply_rollback_or_fallback_bi(
            standard_bars=bars,
            candidate_fractals=candidates,
            original_bis=pre_rollback_bi_pairs,
            rollback_bis=rollback_bi_pairs,
            rollback_stats=rollback_stats,
        )

    fallback_metadata_by_key = {
        _bi_pair_key(metadata["pair"]): metadata for metadata in fallback_metadata
    }
    standard_bi_pairs = [
        pair for pair in bi_pairs if _bi_pair_key(pair) not in fallback_metadata_by_key
    ]
    effective_points = _make_effective_points_from_bi_pairs(standard_bi_pairs)
    bi_records = [
        _make_bi_record(start, end, fallback_metadata=fallback_metadata_by_key.get(_bi_pair_key((start, end))))
        for start, end in bi_pairs
    ]
    effective_records = [_make_effective_fractal_record(fractal) for fractal in effective_points]
    effective_fractals = pd.DataFrame(effective_records, columns=EFFECTIVE_FRACTAL_COLUMNS)
    bis = pd.DataFrame(bi_records, columns=BI_COLUMNS)
    fallback_applied = bool(fallback_metadata)
    rollback_accepted = rollback_stats["rollback_success_count"] > 0 and not fallback_applied
    bis.attrs["locked_bis_count"] = max(len(bi_pairs) - 1, 0) if rollback_accepted else len(locked_bis)
    bis.attrs["pending_bi_count"] = 0 if rollback_accepted else 1 if pending_bi is not None else 0
    bis.attrs["active_bi_count"] = 1 if len(bi_pairs) else 0
    bis.attrs["final_confirmed_bis_count"] = len(bis)
    bis.attrs["active_bi_endpoint_extensions_count"] = active_bi_endpoint_extensions_count
    bis.attrs["reverse_reject_extreme_check_failed_count"] = reverse_reject_extreme_check_failed_count
    bis.attrs["reverse_reject_not_enough_bars_count"] = reverse_reject_not_enough_bars_count
    bis.attrs["rollback_enabled"] = rollback_enabled
    bis.attrs["max_bi_rollback"] = max_rollback
    bis.attrs["stuck_candidate_threshold"] = stuck_candidate_threshold
    bis.attrs["rollback_trigger_count"] = rollback_stats["rollback_trigger_count"]
    bis.attrs["rollback_success_count"] = rollback_stats["rollback_success_count"]
    bis.attrs["rollback_failed_count"] = rollback_stats["rollback_failed_count"]
    bis.attrs["accepted_rollback_count"] = rollback_stats["accepted_rollback_count"]
    bis.attrs["rollback_debug_records"] = rollback_records
    bis.attrs["fallback_trigger_count"] = 1 if fallback_applied else 0
    bis.attrs["fallback_bi_count"] = len(fallback_metadata)
    bis.attrs["affected_confirmed_bi_count"] = (
        int(fallback_metadata[0]["affected_confirmed_bi_count"]) if fallback_applied else 0
    )
    bis.attrs["fallback_reason"] = fallback_metadata[0]["fallback_reason"] if fallback_applied else None
    if debug:
        print(
            {
                "action": "bi_build_end",
                "locked_bis_count": len(locked_bis),
                "pending_bi_count": 1 if pending_bi is not None else 0,
                "active_bi_count": 1 if active_bi is not None else 0,
                "confirmed_bis_count": len(bis),
                "active_bi_endpoint_extensions_count": active_bi_endpoint_extensions_count,
                "rollback_trigger_count": rollback_stats["rollback_trigger_count"],
                "rollback_success_count": rollback_stats["rollback_success_count"],
                "rollback_failed_count": rollback_stats["rollback_failed_count"],
                "accepted_rollback_count": rollback_stats["accepted_rollback_count"],
                "fallback_bi_count": len(fallback_metadata),
                "sequence_continuity_ok": validate_bi_sequence_continuity(bis, debug=True),
            }
        )
        for position, bi in bis.iterrows():
            print(
                {
                    "action": "confirmed_bi",
                    "position": int(position),
                    "direction": bi["direction"],
                    "start_virtual_index": int(bi["start_virtual_index"]),
                    "end_virtual_index": int(bi["end_virtual_index"]),
                    "start_price": float(bi["start_price"]),
                    "end_price": float(bi["end_price"]),
                }
            )
    return effective_fractals, bis


def try_rollback_and_rebuild_tail(
    standard_bars: Sequence[Any],
    candidate_fractals: pd.DataFrame,
    confirmed_bis: Sequence[tuple[pd.Series, pd.Series]],
    stuck_candidate_index: int | None = None,
    max_rollback: int = MAX_BI_ROLLBACK,
    stuck_candidate_threshold: int = STUCK_CANDIDATE_THRESHOLD,
) -> tuple[list[tuple[pd.Series, pd.Series]], dict[str, int | None], list[dict[str, Any]]]:
    """Try bounded rollback of recent bis when the tail has enough candidates but no progress."""
    bi_pairs = list(confirmed_bis)
    stats = _empty_rollback_stats()
    records: list[dict[str, Any]] = []
    if not bi_pairs:
        return bi_pairs, stats, records

    candidates = _normalize_fractals_for_bi(candidate_fractals).sort_values(
        ["center_index", "index"]
    ).reset_index(drop=True)
    trigger = _find_rollback_trigger(candidates, bi_pairs, stuck_candidate_index, stuck_candidate_threshold)
    if trigger is None:
        return bi_pairs, stats, records

    stats["rollback_trigger_count"] = 1
    stuck_position, stuck_fractal = trigger
    old_last_raw_index = _get_bi_pairs_last_raw_index(bi_pairs)
    old_bis_count = len(bi_pairs)

    max_rollback_count = min(max_rollback, max(len(bi_pairs) - 1, 0))
    for rollback_count in range(1, max_rollback_count + 1):
        kept_bis = bi_pairs[:-rollback_count]
        if not kept_bis:
            records.append(
                _make_rollback_record(
                    stuck_position=stuck_position,
                    stuck_fractal=stuck_fractal,
                    rollback_count=rollback_count,
                    kept_bis_count=0,
                    tail_bis_count=0,
                    old_last_raw_index=old_last_raw_index,
                    new_last_raw_index=None,
                    old_bis_count=old_bis_count,
                    new_bis_count=0,
                    accepted=False,
                    reason="reject_would_remove_all_history",
                )
            )
            continue

        anchor = kept_bis[-1][1]
        anchor_position = _find_fractal_position(candidates, anchor)
        if anchor_position is None:
            records.append(
                _make_rollback_record(
                    stuck_position=stuck_position,
                    stuck_fractal=stuck_fractal,
                    rollback_count=rollback_count,
                    kept_bis_count=len(kept_bis),
                    tail_bis_count=0,
                    old_last_raw_index=old_last_raw_index,
                    new_last_raw_index=None,
                    old_bis_count=old_bis_count,
                    new_bis_count=len(kept_bis),
                    accepted=False,
                    reason="reject_anchor_not_found_in_candidates",
                )
            )
            continue

        tail_candidates = candidates.iloc[anchor_position:].reset_index(drop=True)
        rebuilt_tail_bis = _rebuild_tail_bi_pairs_by_search(standard_bars, tail_candidates)
        new_bis = [*kept_bis, *rebuilt_tail_bis]
        new_last_raw_index = _get_bi_pairs_last_raw_index(new_bis)
        accepted, reject_reason = _validate_rollback_candidate(
            standard_bars=standard_bars,
            new_bis=new_bis,
            old_last_raw_index=old_last_raw_index,
            old_bis_count=old_bis_count,
        )
        records.append(
            _make_rollback_record(
                stuck_position=stuck_position,
                stuck_fractal=stuck_fractal,
                rollback_count=rollback_count,
                kept_bis_count=len(kept_bis),
                tail_bis_count=len(rebuilt_tail_bis),
                old_last_raw_index=old_last_raw_index,
                new_last_raw_index=new_last_raw_index,
                old_bis_count=old_bis_count,
                new_bis_count=len(new_bis),
                accepted=accepted,
                reason="accepted" if accepted else reject_reason,
            )
        )
        if accepted:
            stats["rollback_success_count"] = 1
            stats["accepted_rollback_count"] = rollback_count
            return new_bis, stats, records

    stats["rollback_failed_count"] = 1
    return bi_pairs, stats, records


def count_affected_confirmed_bis(rollback_stats: dict[str, int | None] | int | None) -> int:
    """Count confirmed historical bis a rollback would rewrite."""
    if rollback_stats is None:
        return 0
    if isinstance(rollback_stats, int):
        return max(rollback_stats, 0)
    accepted_rollback_count = rollback_stats.get("accepted_rollback_count")
    if accepted_rollback_count is None:
        return 0
    return max(int(accepted_rollback_count), 0)


def should_use_fallback_bi(
    affected_confirmed_bi_count: int,
    threshold: int = FALLBACK_AFFECTED_BI_THRESHOLD,
) -> bool:
    return int(affected_confirmed_bi_count) >= int(threshold)


def apply_rollback_or_fallback_bi(
    standard_bars: Sequence[Any],
    candidate_fractals: pd.DataFrame,
    original_bis: Sequence[tuple[pd.Series, pd.Series]],
    rollback_bis: Sequence[tuple[pd.Series, pd.Series]],
    rollback_stats: dict[str, int | None],
) -> tuple[list[tuple[pd.Series, pd.Series]], list[dict[str, Any]]]:
    affected_count = count_affected_confirmed_bis(rollback_stats)
    rollback_succeeded = int(rollback_stats.get("rollback_success_count") or 0) > 0
    if not rollback_succeeded or not should_use_fallback_bi(affected_count):
        return list(rollback_bis), []

    fallback_bi = build_temporary_fallback_bi(
        standard_bars=standard_bars,
        candidate_fractals=candidate_fractals,
        confirmed_bis=original_bis,
        affected_confirmed_bi_count=affected_count,
    )
    if fallback_bi is None:
        return list(original_bis), []
    return append_fallback_bi_without_rewriting_history(original_bis, fallback_bi), [fallback_bi]


def append_fallback_bi_without_rewriting_history(
    confirmed_bis: Sequence[tuple[pd.Series, pd.Series]],
    fallback_bi: dict[str, Any],
) -> list[tuple[pd.Series, pd.Series]]:
    return [*list(confirmed_bis), fallback_bi["pair"]]


def build_temporary_fallback_bi(
    standard_bars: Sequence[Any],
    candidate_fractals: pd.DataFrame,
    confirmed_bis: Sequence[tuple[pd.Series, pd.Series]],
    affected_confirmed_bi_count: int,
) -> dict[str, Any] | None:
    """Build one non-standard fallback bi after locked history without rewriting it."""
    _validate_standard_bars_for_bi(standard_bars)
    if not confirmed_bis or candidate_fractals.empty:
        return None

    start = confirmed_bis[-1][1]
    candidates = _normalize_fractals_for_bi(candidate_fractals).sort_values(
        ["center_index", "index"]
    ).reset_index(drop=True)
    tail_candidates = _tail_candidates_after_fractal(candidates, start)
    if tail_candidates.empty:
        return None

    fallback_end = _pick_fallback_level_one_endpoint(start, tail_candidates)
    fallback_level = 1
    if fallback_end is None:
        fallback_end = _pick_fallback_level_two_endpoint(start, tail_candidates)
        fallback_level = 2
    if fallback_end is None:
        fallback_end = _pick_fallback_level_three_endpoint(start, tail_candidates)
        fallback_level = 3
    if fallback_end is None:
        return None

    if _get_center_index(fallback_end) <= _get_center_index(start):
        return None
    if not _fallback_price_has_direction(start, fallback_end):
        return None

    return {
        "pair": (start, fallback_end),
        "is_temporary": True,
        "is_fallback_bi": True,
        "fallback_reason": FALLBACK_BI_REASON,
        "color": FALLBACK_BI_COLOR,
        "affected_confirmed_bi_count": int(affected_confirmed_bi_count),
        "fallback_level": fallback_level,
    }


def _tail_candidates_after_fractal(candidates: pd.DataFrame, start: pd.Series) -> pd.DataFrame:
    start_center_index = _get_center_index(start)
    start_raw_index = _get_fractal_raw_end(start)
    tail_candidates = candidates[
        (candidates["center_index"].astype(int) > start_center_index)
        & (candidates["source_end_index"].astype(int) > start_raw_index)
    ]
    return tail_candidates.reset_index(drop=True)


def _pick_fallback_level_one_endpoint(start: pd.Series, tail_candidates: pd.DataFrame) -> pd.Series | None:
    opposite = tail_candidates[tail_candidates["type"] != start["type"]]
    return _pick_latest_directional_candidate(start, opposite)


def _pick_fallback_level_two_endpoint(start: pd.Series, tail_candidates: pd.DataFrame) -> pd.Series | None:
    return _pick_latest_directional_candidate(start, tail_candidates)


def _pick_fallback_level_three_endpoint(start: pd.Series, tail_candidates: pd.DataFrame) -> pd.Series | None:
    if tail_candidates.empty:
        return None
    latest = tail_candidates.sort_values(["source_end_index", "center_index"]).iloc[-1]
    return latest if _fallback_price_has_direction(start, latest) else None


def _pick_latest_directional_candidate(start: pd.Series, candidates: pd.DataFrame) -> pd.Series | None:
    if candidates.empty:
        return None
    directional = [
        candidate for _, candidate in candidates.iterrows() if _fallback_price_has_direction(start, candidate)
    ]
    if not directional:
        return None
    return sorted(
        directional,
        key=lambda candidate: (_get_fractal_raw_end(candidate), _get_center_index(candidate)),
    )[-1]


def _fallback_price_has_direction(start: Any, end: Any) -> bool:
    start_price = float(_get_value(start, "price"))
    end_price = float(_get_value(end, "price"))
    return not _prices_equal(start_price, end_price)


def _empty_rollback_stats() -> dict[str, int | None]:
    return {
        "rollback_trigger_count": 0,
        "rollback_success_count": 0,
        "rollback_failed_count": 0,
        "accepted_rollback_count": None,
    }


def _find_rollback_trigger(
    candidates: pd.DataFrame,
    bi_pairs: Sequence[tuple[pd.Series, pd.Series]],
    stuck_candidate_index: int | None,
    stuck_candidate_threshold: int,
) -> tuple[int, pd.Series] | None:
    if not bi_pairs or candidates.empty:
        return None

    last_end = bi_pairs[-1][1]
    last_end_position = _find_fractal_position(candidates, last_end)
    if last_end_position is None:
        return None

    if stuck_candidate_index is not None:
        tail_start_position = max(int(stuck_candidate_index), last_end_position + 1)
    else:
        tail_start_position = last_end_position + 1
    tail_candidates = candidates.iloc[tail_start_position:]
    if len(tail_candidates) < stuck_candidate_threshold:
        return None
    if not _tail_has_top_and_bottom(tail_candidates):
        return None
    if _get_fractal_raw_end(tail_candidates.iloc[-1]) <= _get_fractal_raw_end(last_end):
        return None
    return tail_start_position, tail_candidates.iloc[0]


def _tail_has_top_and_bottom(tail_candidates: pd.DataFrame) -> bool:
    return bool((tail_candidates["type"] == TYPE_TOP).any() and (tail_candidates["type"] == TYPE_BOTTOM).any())


def _rebuild_tail_bi_pairs_by_search(
    standard_bars: Sequence[Any],
    tail_candidates: pd.DataFrame,
) -> list[tuple[pd.Series, pd.Series]]:
    if len(tail_candidates) < 2:
        return []

    candidate_rows = [row for _, row in tail_candidates.iterrows()]
    best_paths: dict[int, list[int]] = {}
    edge_cache: dict[tuple[int, int], bool] = {}

    def edge_ok(left_position: int, right_position: int) -> bool:
        key = (left_position, right_position)
        if key not in edge_cache:
            edge_cache[key] = _can_confirm_bi(
                standard_bars,
                candidate_rows[left_position],
                candidate_rows[right_position],
            )
        return edge_cache[key]

    def best_from(position: int) -> list[int]:
        if position in best_paths:
            return best_paths[position]
        best_path = [position]
        for next_position in range(position + 1, len(candidate_rows)):
            if not edge_ok(position, next_position):
                continue
            candidate_path = [position, *best_from(next_position)]
            if _is_better_tail_path(candidate_path, best_path, candidate_rows):
                best_path = candidate_path
        best_paths[position] = best_path
        return best_path

    best_path = best_from(0)
    if len(best_path) < 2:
        return []
    return [
        (candidate_rows[best_path[index]], candidate_rows[best_path[index + 1]])
        for index in range(len(best_path) - 1)
    ]


def _is_better_tail_path(candidate_path: list[int], current_path: list[int], candidate_rows: Sequence[pd.Series]) -> bool:
    candidate_last = candidate_rows[candidate_path[-1]]
    current_last = candidate_rows[current_path[-1]]
    candidate_score = (
        _get_fractal_raw_end(candidate_last),
        _get_center_index(candidate_last),
        len(candidate_path),
    )
    current_score = (
        _get_fractal_raw_end(current_last),
        _get_center_index(current_last),
        len(current_path),
    )
    return candidate_score > current_score


def _validate_rollback_candidate(
    standard_bars: Sequence[Any],
    new_bis: Sequence[tuple[pd.Series, pd.Series]],
    old_last_raw_index: int | None,
    old_bis_count: int,
) -> tuple[bool, str]:
    if not new_bis:
        return False, "reject_empty_rebuilt_sequence"
    if len(new_bis) < old_bis_count:
        return False, "reject_new_bis_count_less_than_old"
    new_last_raw_index = _get_bi_pairs_last_raw_index(new_bis)
    if old_last_raw_index is not None and (new_last_raw_index is None or new_last_raw_index <= old_last_raw_index):
        return False, "reject_no_later_coverage"
    if not _validate_bi_pairs_strict(standard_bars, new_bis):
        return False, "reject_rebuilt_sequence_invalid"
    return True, "accepted"


def _validate_bi_pairs_strict(
    standard_bars: Sequence[Any],
    bi_pairs: Sequence[tuple[pd.Series, pd.Series]],
) -> bool:
    for position, (start, end) in enumerate(bi_pairs):
        if _get_bi_reject_reason(start, end) is not None:
            return False
        if not validate_bi_extreme(standard_bars, start, end):
            return False
        if position == 0:
            continue
        previous_start, previous_end = bi_pairs[position - 1]
        if not _same_fractal(previous_end, start):
            return False
        if _direction_from_types(previous_start["type"], previous_end["type"]) == _direction_from_types(
            start["type"], end["type"]
        ):
            return False
    return True


def _make_rollback_record(
    stuck_position: int,
    stuck_fractal: pd.Series,
    rollback_count: int,
    kept_bis_count: int,
    tail_bis_count: int,
    old_last_raw_index: int | None,
    new_last_raw_index: int | None,
    old_bis_count: int,
    new_bis_count: int,
    accepted: bool,
    reason: str,
) -> dict[str, Any]:
    return {
        "stuck_candidate_index": stuck_position,
        "stuck_virtual_index": _get_center_index(stuck_fractal),
        "stuck_raw_index": _get_fractal_raw_end(stuck_fractal),
        "rollback_count": rollback_count,
        "kept_bis_count": kept_bis_count,
        "tail_bis_count": tail_bis_count,
        "old_last_raw_index": old_last_raw_index,
        "new_last_raw_index": new_last_raw_index,
        "old_bis_count": old_bis_count,
        "new_bis_count": new_bis_count,
        "accepted": accepted,
        "reason": reason,
    }


def _find_fractal_position(candidates: pd.DataFrame, fractal: Any) -> int | None:
    target_key = _fractal_key(fractal)
    for position, (_, candidate) in enumerate(candidates.iterrows()):
        if _fractal_key(candidate) == target_key:
            return position
    return None


def _get_bi_pairs_last_raw_index(bi_pairs: Sequence[tuple[pd.Series, pd.Series]]) -> int | None:
    if not bi_pairs:
        return None
    return _get_fractal_raw_end(bi_pairs[-1][1])


def _get_fractal_raw_end(fractal: Any) -> int:
    if _has_key(fractal, "source_end_index"):
        return int(_get_value(fractal, "source_end_index"))
    if _has_key(fractal, "original_index"):
        return int(_get_value(fractal, "original_index"))
    return _get_center_index(fractal)


def _pick_stronger_same_type(left: pd.Series, right: pd.Series) -> pd.Series:
    if left["type"] == TYPE_TOP:
        return right if float(right["high"]) > float(left["high"]) else left
    if left["type"] == TYPE_BOTTOM:
        return right if float(right["low"]) < float(left["low"]) else left
    return left


def _can_confirm_bi(virtual_bars: Sequence[Any], start: Any, end: Any) -> bool:
    return can_form_bi(start, end) and validate_bi_extreme(virtual_bars, start, end)


def _get_bi_reject_reason(start_fractal: Any, end_fractal: Any) -> str | None:
    start_type = _get_value(start_fractal, "type")
    end_type = _get_value(end_fractal, "type")
    if start_type == end_type:
        return "same_type"
    if {start_type, end_type} != {TYPE_TOP, TYPE_BOTTOM}:
        return "invalid_type_pair"

    if _shares_kline(start_fractal, end_fractal):
        return "shared_kline"

    if _calculate_center_gap(start_fractal, end_fractal) < MIN_CENTER_GAP_FOR_BI:
        return "center_gap_not_enough"

    if not _has_neutral_bar_between_fractals(start_fractal, end_fractal):
        return "no_neutral_bar_between_fractals"

    if not is_price_range_separated(start_fractal, end_fractal):
        return "price_order_invalid"

    return None


def _has_enough_kline_count(start_fractal: Any, end_fractal: Any) -> bool:
    return _calculate_center_gap(start_fractal, end_fractal) >= MIN_CENTER_GAP_FOR_BI


def _try_replace_previous_same_type_endpoint(
    bars: Sequence[Any],
    candidate: pd.Series,
    effective_points: list[pd.Series],
    bi_records: list[dict],
    debug: bool,
) -> pd.Series | None:
    if len(effective_points) < 3 or len(bi_records) < 2:
        return None

    previous_same_type = effective_points[-2]
    if candidate["type"] != previous_same_type["type"]:
        return None

    stronger = _pick_stronger_same_type(previous_same_type, candidate)
    if not _same_fractal(stronger, candidate):
        return None

    previous_start = effective_points[-3]
    if not _can_confirm_bi(bars, previous_start, candidate):
        return None

    removed_endpoint = effective_points.pop()
    removed_bi = bi_records.pop()
    effective_points[-1] = candidate
    bi_records[-1] = _make_bi_record(previous_start, candidate)
    _debug_bi_event(
        debug,
        "same_type_replace_previous_endpoint",
        anchor=removed_endpoint,
        candidate=candidate,
        same_direction=False,
        replaced_anchor=True,
        removed_bi={
            "start_virtual_index": int(removed_bi["start_virtual_index"]),
            "end_virtual_index": int(removed_bi["end_virtual_index"]),
        },
    )
    return candidate


def _try_reopen_active_window_with_reverse_candidate(
    bars: Sequence[Any],
    candidate: pd.Series,
    pending_bi: tuple[pd.Series, pd.Series] | None,
    active_bi: tuple[pd.Series, pd.Series],
    debug: bool,
) -> tuple[pd.Series, pd.Series] | None:
    if pending_bi is None:
        return None

    active_start, active_end = active_bi
    if candidate["type"] != active_start["type"]:
        return None

    stronger = _pick_stronger_same_type(active_start, candidate)
    if not _same_fractal(stronger, candidate):
        return None

    previous_start, previous_end = pending_bi
    if not _same_fractal(previous_end, active_start):
        return None

    if not _can_confirm_bi(bars, previous_start, candidate):
        return None

    reopened_active_bi = (previous_start, candidate)
    _debug_bi_event(
        debug,
        "reopen_active_window_with_more_extreme_reverse",
        anchor=active_end,
        candidate=candidate,
        same_direction=False,
        replaced_anchor=True,
        previous_active_start=_format_fractal_for_debug(active_start),
        previous_pending_start=_format_fractal_for_debug(previous_start),
    )
    return reopened_active_bi


def _record_bi_attempt(
    attempt_records: list[dict] | None,
    bars: Sequence[Any],
    anchor: Any,
    candidate: Any,
    result: str,
    reason: str,
    extreme_ok: bool | None = None,
) -> None:
    if attempt_records is None:
        return

    same_type = _get_value(anchor, "type") == _get_value(candidate, "type")
    bar_count = _calculate_kline_count(anchor, candidate)
    center_gap = _calculate_center_gap(anchor, candidate)
    shared_kline = _shares_kline(anchor, candidate)
    has_neutral_bar = _has_neutral_bar_between_fractals(anchor, candidate)
    neutral_bar_count = _calculate_neutral_bar_count(anchor, candidate)
    kline_count_ok = center_gap >= MIN_CENTER_GAP_FOR_BI
    if extreme_ok is None and not same_type and not shared_kline and kline_count_ok and has_neutral_bar:
        extreme_ok = validate_bi_extreme(bars, anchor, candidate)

    attempt_records.append(
        {
            "anchor_index": _get_value(anchor, "index"),
            "anchor_virtual_index": _get_center_index(anchor),
            "anchor_source_start_index": _get_value(anchor, "source_start_index"),
            "anchor_source_end_index": _get_value(anchor, "source_end_index"),
            "anchor_date": _format_date_for_debug(_get_value(anchor, "date")),
            "anchor_type": _get_value(anchor, "type"),
            "candidate_index": _get_value(candidate, "index"),
            "candidate_virtual_index": _get_center_index(candidate),
            "candidate_source_start_index": _get_value(candidate, "source_start_index"),
            "candidate_source_end_index": _get_value(candidate, "source_end_index"),
            "candidate_date": _format_date_for_debug(_get_value(candidate, "date")),
            "candidate_type": _get_value(candidate, "type"),
            "bar_count": bar_count,
            "center_gap": center_gap,
            "neutral_bar_count": neutral_bar_count,
            "extreme_ok": bool(extreme_ok) if extreme_ok is not None else False,
            "result": result,
            "reason": reason,
            "would_form_direction": _would_form_direction(anchor, candidate),
            "same_type": same_type,
            "shared_kline": shared_kline,
            "has_neutral_bar_between_fractals": has_neutral_bar,
            "kline_count_ok": kline_count_ok,
        }
    )


def _is_last_effective_endpoint(anchor: pd.Series, effective_points: list[pd.Series]) -> bool:
    return bool(effective_points) and _same_fractal(anchor, effective_points[-1])


def _same_type_replace_action(candidate: Any) -> str:
    if _get_value(candidate, "type") == TYPE_TOP:
        return "same_type_replace_top"
    return "same_type_replace_bottom"


def _debug_reject_action(reason: str) -> str:
    if reason in {"shared_kline", "same_virtual_kline", "same_original_kline", "source_indices_overlap"}:
        return "reject_shared_kline"
    if reason in {"kline_count_not_enough", "center_gap_not_enough"}:
        return "reject_not_enough_center_gap"
    if reason == "no_neutral_bar_between_fractals":
        return "reject_no_neutral_bar_between_fractals"
    if reason == "extreme_validation_failed":
        return "reject_extreme_check_failed"
    if reason == "price_order_invalid":
        return "reject_direction_error"
    return f"reject_{reason}"


def _debug_reverse_reject_action(reason: str) -> str:
    if reason in {"kline_count_not_enough", "center_gap_not_enough"}:
        return "reject_not_enough_center_gap"
    if reason == "no_neutral_bar_between_fractals":
        return "reject_no_neutral_bar_between_fractals"
    if reason == "extreme_validation_failed":
        return "reject_extreme_check_failed"
    return _debug_reject_action(reason)


def _make_effective_points_from_bi_pairs(
    bi_pairs: Sequence[tuple[pd.Series, pd.Series]],
) -> list[pd.Series]:
    if not bi_pairs:
        return []

    effective_points = [bi_pairs[0][0]]
    for _, end in bi_pairs:
        effective_points.append(end)
    return effective_points


def _make_fractals_debug(raw_fractals: pd.DataFrame, cleaned_fractals: pd.DataFrame) -> pd.DataFrame:
    cleaned_keys = {
        _fractal_key(fractal)
        for _, fractal in cleaned_fractals.iterrows()
    }
    records = []
    for _, fractal in raw_fractals.iterrows():
        in_cleaned = _fractal_key(fractal) in cleaned_keys
        records.append(
            {
                "index": fractal["index"],
                "center_index": fractal.get("center_index", fractal["virtual_index"]),
                "span_start": fractal.get("span_start", int(fractal["virtual_index"]) - 1),
                "span_end": fractal.get("span_end", int(fractal["virtual_index"]) + 1),
                "date": _format_date_for_debug(fractal["date"]),
                "type": fractal["type"],
                "high": float(fractal["high"]),
                "low": float(fractal["low"]),
                "source_bar_index": fractal["source_index"],
                "in_cleaned": in_cleaned,
                "removed_reason": "" if in_cleaned else "not_in_cleaned_fractals",
            }
        )
    return pd.DataFrame(
        records,
        columns=[
            "index",
            "center_index",
            "span_start",
            "span_end",
            "date",
            "type",
            "high",
            "low",
            "source_bar_index",
            "in_cleaned",
            "removed_reason",
        ],
    )


def _make_manual_expected_debug(
    manual_expected_bis: Sequence[tuple[int, int]],
    raw_fractals: pd.DataFrame,
    cleaned_fractals: pd.DataFrame,
    confirmed_bis: pd.DataFrame,
    bars: Sequence[Any],
) -> pd.DataFrame:
    records = []
    for start_index, end_index in manual_expected_bis:
        raw_start = _find_fractal_by_manual_index(raw_fractals, start_index)
        raw_end = _find_fractal_by_manual_index(raw_fractals, end_index)
        clean_start = _find_fractal_by_manual_index(cleaned_fractals, start_index)
        clean_end = _find_fractal_by_manual_index(cleaned_fractals, end_index)
        reason = ""
        bar_count = None
        kline_count_ok = False
        extreme_ok = False
        shared_kline = False
        confirmed = False

        if raw_start is None or raw_end is None:
            reason = "raw_fractal_missing"
        elif clean_start is None or clean_end is None:
            reason = "reject_candidate_not_in_cleaned_fractals"
        else:
            bar_count = _calculate_kline_count(clean_start, clean_end)
            kline_count_ok = _calculate_center_gap(clean_start, clean_end) >= MIN_CENTER_GAP_FOR_BI
            shared_kline = _shares_kline(clean_start, clean_end)
            reject_reason = _get_bi_reject_reason(clean_start, clean_end)
            extreme_ok = reject_reason is None and validate_bi_extreme(bars, clean_start, clean_end)
            confirmed = _confirmed_contains_pair(confirmed_bis, clean_start, clean_end)
            if confirmed:
                reason = "accept_new_bi"
            elif reject_reason is not None:
                reason = _debug_reject_action(reject_reason)
            elif not extreme_ok:
                reason = "reject_extreme_check_failed"
            else:
                reason = "passes_rules_but_not_confirmed_anchor_path"

        records.append(
            {
                "start_index": start_index,
                "end_index": end_index,
                "start_in_raw": raw_start is not None,
                "end_in_raw": raw_end is not None,
                "start_in_cleaned": clean_start is not None,
                "end_in_cleaned": clean_end is not None,
                "bar_count": bar_count,
                "kline_count_ok": kline_count_ok,
                "shared_kline": shared_kline,
                "extreme_ok": extreme_ok,
                "confirmed": confirmed,
                "reason": reason,
            }
        )
    return pd.DataFrame(records)


def _make_suspected_missing_bis(raw_fractals: pd.DataFrame, confirmed_bis: pd.DataFrame) -> pd.DataFrame:
    records = []
    sorted_fractals = raw_fractals.sort_values(["center_index", "index"]).reset_index(drop=True)
    for _, bi in confirmed_bis.iterrows():
        start_index = int(bi["start_center_index"]) if "start_center_index" in bi else int(bi["start_virtual_index"])
        end_index = int(bi["end_center_index"]) if "end_center_index" in bi else int(bi["end_virtual_index"])
        left_index, right_index = sorted([start_index, end_index])
        internal_fractals = sorted_fractals[
            (sorted_fractals["center_index"] > left_index)
            & (sorted_fractals["center_index"] < right_index)
        ]
        internal_alternating_count = _count_alternating_fractals(internal_fractals)
        virtual_bar_count = abs(end_index - start_index) + 1
        suspected_missing = virtual_bar_count >= MIN_BI_KLINE_COUNT * 2 and internal_alternating_count >= 3
        records.append(
            {
                "start_index": start_index,
                "end_index": end_index,
                "start_date": _format_date_for_debug(bi["start_date"]),
                "end_date": _format_date_for_debug(bi["end_date"]),
                "virtual_bar_count": virtual_bar_count,
                "internal_raw_fractals_count": len(internal_fractals),
                "internal_alternating_fractals_count": internal_alternating_count,
                "suspected_missing": suspected_missing,
            }
        )

    return pd.DataFrame(
        records,
        columns=[
            "start_index",
            "end_index",
            "start_date",
            "end_date",
            "virtual_bar_count",
            "internal_raw_fractals_count",
            "internal_alternating_fractals_count",
            "suspected_missing",
        ],
    )


def _make_tail_region_debug(
    bars: Sequence[Any],
    cleaned_fractals: pd.DataFrame,
    attempts_debug: pd.DataFrame,
    confirmed_bis: pd.DataFrame,
    target_raw_start: int,
    target_raw_end: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    tail_virtual_indices = [
        int(_get_bar_value(bar, "virtual_index"))
        for bar in bars
        if _bar_overlaps_raw_range(bar, target_raw_start, target_raw_end)
    ]
    tail_virtual_start = min(tail_virtual_indices) if tail_virtual_indices else None
    tail_virtual_end = max(tail_virtual_indices) if tail_virtual_indices else None

    tail_candidates = cleaned_fractals[
        (cleaned_fractals["source_end_index"] >= target_raw_start)
        & (cleaned_fractals["source_start_index"] <= target_raw_end)
    ].copy()
    tail_attempts = _filter_attempts_for_tail_region(attempts_debug, target_raw_start, target_raw_end)
    tail_attempt_by_candidate = {
        int(attempt["candidate_virtual_index"]): attempt
        for _, attempt in tail_attempts.iterrows()
        if pd.notna(attempt.get("candidate_virtual_index"))
    }

    rows: list[dict[str, Any]] = []
    for candidate_order, fractal in tail_candidates.iterrows():
        virtual_index = int(fractal["center_index"])
        attempt = tail_attempt_by_candidate.get(virtual_index)
        rows.append(
            {
                "candidate_order": int(candidate_order),
                "virtual_index": virtual_index,
                "raw_start_index": int(fractal["source_start_index"]),
                "raw_end_index": int(fractal["source_end_index"]),
                "date": _format_date_for_debug(fractal["date"]),
                "type": fractal["type"],
                "high": float(fractal["high"]),
                "low": float(fractal["low"]),
                "anchor_virtual_index": _tail_attempt_value(attempt, "anchor_virtual_index"),
                "candidate_virtual_index": _tail_attempt_value(attempt, "candidate_virtual_index"),
                "attempt_result": _tail_attempt_value(attempt, "result", "no_attempt"),
                "reject_reason": _tail_attempt_value(attempt, "reason", "no_attempt"),
                "center_gap": _tail_attempt_value(attempt, "center_gap"),
                "shared_bar": _tail_attempt_value(attempt, "shared_kline"),
                "neutral_bar_count": _tail_attempt_value(attempt, "neutral_bar_count"),
                "extreme_ok": _tail_attempt_value(attempt, "extreme_ok"),
                "would_form_direction": _tail_attempt_value(attempt, "would_form_direction"),
            }
        )

    reason_counts = tail_attempts["reason"].value_counts().to_dict() if not tail_attempts.empty else {}
    last_bi = confirmed_bis.iloc[-1] if not confirmed_bis.empty else None
    stats = {
        "tail_target_raw_start": target_raw_start,
        "tail_target_raw_end": target_raw_end,
        "tail_standard_virtual_start": tail_virtual_start,
        "tail_standard_virtual_end": tail_virtual_end,
        "tail_standard_bars_count": len(tail_virtual_indices),
        "tail_candidate_fractals_count": len(tail_candidates),
        "tail_top_fractals_count": int((tail_candidates["type"] == TYPE_TOP).sum()) if not tail_candidates.empty else 0,
        "tail_bottom_fractals_count": int((tail_candidates["type"] == TYPE_BOTTOM).sum())
        if not tail_candidates.empty
        else 0,
        "tail_bi_attempt_count": len(tail_attempts),
        "tail_attempt_reject_reason_counts": reason_counts,
        "tail_anchor_stuck_before_region": _tail_anchor_stuck_before_region(tail_attempts, target_raw_start),
        "last_confirmed_bi_start_virtual_index": int(last_bi["start_center_index"]) if last_bi is not None else None,
        "last_confirmed_bi_end_virtual_index": int(last_bi["end_center_index"]) if last_bi is not None else None,
        "last_confirmed_bi_start_raw_index": int(last_bi["start_original_index"]) if last_bi is not None else None,
        "last_confirmed_bi_end_raw_index": int(last_bi["end_original_index"]) if last_bi is not None else None,
        "last_confirmed_bi_end_date": _format_date_for_debug(last_bi["end_date"]) if last_bi is not None else None,
    }
    return pd.DataFrame(rows, columns=TAIL_REGION_DEBUG_COLUMNS), stats


def _filter_attempts_for_tail_region(
    attempts_debug: pd.DataFrame,
    target_raw_start: int,
    target_raw_end: int,
) -> pd.DataFrame:
    if attempts_debug.empty:
        return attempts_debug.copy()
    return attempts_debug[
        (attempts_debug["candidate_source_end_index"] >= target_raw_start)
        & (attempts_debug["candidate_source_start_index"] <= target_raw_end)
    ].copy()


def _bar_overlaps_raw_range(bar: Any, target_raw_start: int, target_raw_end: int) -> bool:
    return (
        int(_get_bar_value(bar, "source_end_index")) >= target_raw_start
        and int(_get_bar_value(bar, "source_start_index")) <= target_raw_end
    )


def _tail_attempt_value(attempt: pd.Series | None, key: str, default: Any = None) -> Any:
    if attempt is None or key not in attempt.index or pd.isna(attempt[key]):
        return default
    value = attempt[key]
    if isinstance(value, (pd.Timestamp,)):
        return _format_date_for_debug(value)
    return value


def _tail_anchor_stuck_before_region(tail_attempts: pd.DataFrame, target_raw_start: int) -> bool:
    if tail_attempts.empty or "anchor_source_end_index" not in tail_attempts.columns:
        return False
    return bool((tail_attempts["anchor_source_end_index"] < target_raw_start).all())


def _count_alternating_fractals(fractals: pd.DataFrame) -> int:
    count = 0
    last_type = None
    for _, fractal in fractals.iterrows():
        fractal_type = fractal["type"]
        if fractal_type == last_type:
            continue
        count += 1
        last_type = fractal_type
    return count


def _count_attempt_reason(attempts_debug: pd.DataFrame, reason: str) -> int:
    if attempts_debug.empty or "reason" not in attempts_debug:
        return 0
    return int((attempts_debug["reason"] == reason).sum())


def _all_bis_pass_extreme(bars: Sequence[Any], bis: pd.DataFrame) -> bool:
    bis = _standard_bis_for_strict_validation(bis)
    for _, bi in bis.iterrows():
        start = _make_fractal_from_bi_endpoint(bi, "start", bars)
        end = _make_fractal_from_bi_endpoint(bi, "end", bars)
        if not validate_bi_extreme(bars, start, end):
            return False
    return True


def _make_debug_report_lines(
    stats: dict[str, Any],
    raw_fractals: pd.DataFrame,
    cleaned_fractals: pd.DataFrame,
    confirmed_bis: pd.DataFrame,
    attempts_debug: pd.DataFrame,
    manual_debug: pd.DataFrame,
) -> list[str]:
    lines = [
        "# Bi Debug Report",
        "",
        "## Counts",
        f"original_kline_count={stats['original_kline_count']}",
        f"virtual_kline_count={stats['virtual_kline_count']}",
        f"standard_bars_count={stats['standard_bars_count']}",
        f"raw_fractals_count={stats['raw_fractals_count']}",
        f"candidate_fractals_count={stats['candidate_fractals_count']}",
        f"cleaned_fractals_count={stats['cleaned_fractals_count']}",
        f"confirmed_bis_count={stats['confirmed_bis_count']}",
        f"locked_bis_count={stats['locked_bis_count']}",
        f"pending_bi_count={stats['pending_bi_count']}",
        f"active_bi_count={stats['active_bi_count']}",
        f"final_confirmed_bis_count={stats['final_confirmed_bis_count']}",
        f"active_bi_endpoint_extensions_count={stats['active_bi_endpoint_extensions_count']}",
        f"reverse_reject_extreme_check_failed_count={stats['reverse_reject_extreme_check_failed_count']}",
        f"reverse_reject_not_enough_bars_count={stats['reverse_reject_not_enough_bars_count']}",
        f"rollback_enabled={stats['rollback_enabled']}",
        f"max_bi_rollback={stats['max_bi_rollback']}",
        f"stuck_candidate_threshold={stats['stuck_candidate_threshold']}",
        f"rollback_trigger_count={stats['rollback_trigger_count']}",
        f"rollback_success_count={stats['rollback_success_count']}",
        f"rollback_failed_count={stats['rollback_failed_count']}",
        f"accepted_rollback_count={stats['accepted_rollback_count']}",
        f"fallback_trigger_count={stats['fallback_trigger_count']}",
        f"fallback_bi_count={stats['fallback_bi_count']}",
        f"affected_confirmed_bi_count={stats['affected_confirmed_bi_count']}",
        f"fallback_reason={stats['fallback_reason']}",
        f"suspected_missing_bis_count={stats['suspected_missing_bis_count']}",
        f"continuity_ok={stats['continuity_ok']}",
        f"all_extreme_ok={stats['all_extreme_ok']}",
        "",
        "## Cleaned Fractals",
    ]
    for position, fractal in cleaned_fractals.iterrows():
        lines.append(
            f"{position}: index={fractal['index']}, center_index={fractal.get('center_index', fractal['virtual_index'])}, "
            f"span=[{fractal.get('span_start', int(fractal['virtual_index']) - 1)}, "
            f"{fractal.get('span_end', int(fractal['virtual_index']) + 1)}], "
            f"date={_format_date_for_debug(fractal['date'])}, "
            f"type={fractal['type']}, high={float(fractal['high'])}, low={float(fractal['low'])}"
        )

    lines.extend(["", "## Confirmed Bis"])
    for position, bi in confirmed_bis.iterrows():
        lines.append(
            f"bi[{position}]: {_format_date_for_debug(bi['start_date'])}, {bi['start_type']}, "
            f"{float(bi['start_price'])} -> {_format_date_for_debug(bi['end_date'])}, "
            f"{bi['end_type']}, {float(bi['end_price'])}, direction={bi['direction']}"
        )

    lines.extend(["", "## Attempt Summary"])
    if attempts_debug.empty:
        lines.append("no attempts")
    else:
        reason_counts = attempts_debug["reason"].value_counts().to_dict()
        for reason, count in reason_counts.items():
            lines.append(f"{reason}: {count}")

    lines.extend(["", "## Tail Region Diagnostics"])
    lines.append(f"target_raw_start={stats['tail_target_raw_start']}")
    lines.append(f"target_raw_end={stats['tail_target_raw_end']}")
    lines.append(f"tail_standard_virtual_start={stats['tail_standard_virtual_start']}")
    lines.append(f"tail_standard_virtual_end={stats['tail_standard_virtual_end']}")
    lines.append(f"tail_standard_bars_count={stats['tail_standard_bars_count']}")
    lines.append(f"tail_candidate_fractals_count={stats['tail_candidate_fractals_count']}")
    lines.append(f"tail_top_fractals_count={stats['tail_top_fractals_count']}")
    lines.append(f"tail_bottom_fractals_count={stats['tail_bottom_fractals_count']}")
    lines.append(f"tail_bi_attempt_count={stats['tail_bi_attempt_count']}")
    lines.append(f"tail_attempt_reject_reason_counts={stats['tail_attempt_reject_reason_counts']}")
    lines.append(f"tail_anchor_stuck_before_region={stats['tail_anchor_stuck_before_region']}")
    lines.append(f"final_confirmed_bis_count={stats['final_confirmed_bis_count']}")
    lines.append(f"last_confirmed_bi_start_virtual_index={stats['last_confirmed_bi_start_virtual_index']}")
    lines.append(f"last_confirmed_bi_end_virtual_index={stats['last_confirmed_bi_end_virtual_index']}")
    lines.append(f"last_confirmed_bi_start_raw_index={stats['last_confirmed_bi_start_raw_index']}")
    lines.append(f"last_confirmed_bi_end_raw_index={stats['last_confirmed_bi_end_raw_index']}")
    lines.append(f"last_confirmed_bi_end_date={stats['last_confirmed_bi_end_date']}")

    lines.extend(["", "## Manual Expected Bis"])
    if manual_debug.empty:
        lines.append("manual_expected_bis not provided")
    else:
        for _, record in manual_debug.iterrows():
            lines.append(", ".join(f"{key}={value}" for key, value in record.to_dict().items()))

    lines.extend(["", "## CSV Files"])
    lines.append(f"fractals_debug_csv={stats['fractals_csv_path']}")
    lines.append(f"bi_attempts_debug_csv={stats['attempts_csv_path']}")
    lines.append(f"suspected_missing_bis_csv={stats['suspected_missing_csv_path']}")
    lines.append(f"bi_tail_region_debug_csv={stats['tail_region_csv_path']}")
    lines.append(f"bi_rollback_debug_csv={stats['rollback_csv_path']}")
    return lines


def _make_fractal_from_bi_endpoint(bi: pd.Series, endpoint: str, bars: Sequence[Any]) -> dict:
    virtual_index = int(bi[f"{endpoint}_virtual_index"])
    bar = bars[virtual_index]
    return {
        "type": bi[f"{endpoint}_type"],
        "virtual_index": virtual_index,
        "center_index": int(bi[f"{endpoint}_center_index"])
        if f"{endpoint}_center_index" in bi
        else virtual_index,
        "span_start": int(bi[f"{endpoint}_center_index"]) - 1
        if f"{endpoint}_center_index" in bi
        else virtual_index - 1,
        "span_end": int(bi[f"{endpoint}_center_index"]) + 1
        if f"{endpoint}_center_index" in bi
        else virtual_index + 1,
        "original_index": bi[f"{endpoint}_original_index"],
        "price": float(bi[f"{endpoint}_price"]),
        "high": float(_get_bar_value(bar, "high")),
        "low": float(_get_bar_value(bar, "low")),
        "source_indices": list(bi[f"{endpoint}_source_indices"]),
    }


def _find_fractal_by_manual_index(fractals: pd.DataFrame, index: int) -> pd.Series | None:
    if fractals.empty:
        return None
    for column in ["index", "original_index", "source_index", "virtual_index", "center_index"]:
        matches = fractals[fractals[column] == index]
        if not matches.empty:
            return matches.iloc[0]
    return None


def _confirmed_contains_pair(confirmed_bis: pd.DataFrame, start: Any, end: Any) -> bool:
    if confirmed_bis.empty:
        return False
    start_center_index = _get_center_index(start)
    end_center_index = _get_center_index(end)
    matches = confirmed_bis[
        (confirmed_bis["start_center_index"] == start_center_index)
        & (confirmed_bis["end_center_index"] == end_center_index)
    ]
    return not matches.empty


def _fractal_key(fractal: Any) -> tuple:
    return (
        _get_value(fractal, "type"),
        _get_center_index(fractal),
        _get_span_start(fractal),
        _get_span_end(fractal),
    )


def _bi_pair_key(pair: tuple[pd.Series, pd.Series]) -> tuple[tuple, tuple]:
    return (_fractal_key(pair[0]), _fractal_key(pair[1]))


def _calculate_kline_count(start_fractal: Any, end_fractal: Any) -> int:
    return _calculate_center_gap(start_fractal, end_fractal) + 1


def _calculate_center_gap(start_fractal: Any, end_fractal: Any) -> int:
    return abs(_get_center_index(end_fractal) - _get_center_index(start_fractal))


def _shares_kline(start_fractal: Any, end_fractal: Any) -> bool:
    start_span = set(range(_get_span_start(start_fractal), _get_span_end(start_fractal) + 1))
    end_span = set(range(_get_span_start(end_fractal), _get_span_end(end_fractal) + 1))
    return bool(start_span & end_span)


def _has_neutral_bar_between_fractals(start_fractal: Any, end_fractal: Any) -> bool:
    return _calculate_neutral_bar_count(start_fractal, end_fractal) >= 1


def _calculate_neutral_bar_count(start_fractal: Any, end_fractal: Any) -> int:
    if _get_center_index(start_fractal) <= _get_center_index(end_fractal):
        earlier = start_fractal
        later = end_fractal
    else:
        earlier = end_fractal
        later = start_fractal
    return max(_get_span_start(later) - _get_span_end(earlier) - 1, 0)


def _would_form_direction(start_fractal: Any, end_fractal: Any) -> str:
    start_type = _get_value(start_fractal, "type")
    end_type = _get_value(end_fractal, "type")
    if start_type == TYPE_TOP and end_type == TYPE_BOTTOM:
        return DIRECTION_DOWN
    if start_type == TYPE_BOTTOM and end_type == TYPE_TOP:
        return DIRECTION_UP
    return "same_type"


def _format_date_for_debug(value: Any) -> str:
    date_value = pd.to_datetime(value, errors="coerce")
    if pd.isna(date_value):
        return str(value)
    return date_value.strftime("%Y-%m-%d %H:%M:%S")


def _debug_bi_event(debug: bool, action: str, **fields: Any) -> None:
    if not debug:
        return

    anchor = fields.pop("anchor", None)
    candidate = fields.pop("candidate", None)
    detail = {
        "action": action,
        "anchor": _format_fractal_for_debug(anchor),
        "candidate": _format_fractal_for_debug(candidate),
        **fields,
    }
    print(detail)


def _format_fractal_for_debug(fractal: Any | None) -> dict | None:
    if fractal is None:
        return None
    return {
        "type": _get_value(fractal, "type"),
        "virtual_index": int(_get_value(fractal, "virtual_index")),
        "center_index": _get_center_index(fractal),
        "span_start": _get_span_start(fractal),
        "span_end": _get_span_end(fractal),
        "price": float(_get_value(fractal, "price")),
        "high": float(_get_value(fractal, "high")),
        "low": float(_get_value(fractal, "low")),
    }


def _make_bi_record(
    start: pd.Series,
    end: pd.Series,
    fallback_metadata: dict[str, Any] | None = None,
) -> dict:
    is_fallback_bi = fallback_metadata is not None
    direction = (
        _fallback_direction_from_prices(start, end)
        if is_fallback_bi
        else _direction_from_types(start["type"], end["type"])
    )
    return {
        "direction": direction,
        "start_type": start["type"],
        "end_type": end["type"],
        "start_virtual_index": int(start["virtual_index"]),
        "end_virtual_index": int(end["virtual_index"]),
        "start_center_index": _get_center_index(start),
        "end_center_index": _get_center_index(end),
        "start_original_index": start["original_index"],
        "end_original_index": end["original_index"],
        "start_x": start["x"],
        "end_x": end["x"],
        "start_date": start["date"],
        "end_date": end["date"],
        "start_price": float(start["price"]),
        "end_price": float(end["price"]),
        "start_source_indices": list(start["source_indices"]),
        "end_source_indices": list(end["source_indices"]),
        "start_fractal": _fractal_key(start),
        "end_fractal": _fractal_key(end),
        "kline_count": _calculate_kline_count(start, end),
        "is_valid": not is_fallback_bi,
        "is_temporary": bool(is_fallback_bi),
        "is_fallback_bi": bool(is_fallback_bi),
        "fallback_reason": fallback_metadata["fallback_reason"] if fallback_metadata else None,
        "color": fallback_metadata["color"] if fallback_metadata else None,
        "affected_confirmed_bi_count": fallback_metadata["affected_confirmed_bi_count"]
        if fallback_metadata
        else 0,
        "fallback_level": fallback_metadata["fallback_level"] if fallback_metadata else None,
    }


def _make_effective_fractal_record(fractal: pd.Series) -> dict:
    record = {}
    for column in FRACTAL_COLUMNS:
        if column in fractal:
            record[column] = fractal[column]
        elif column == "center_index":
            record[column] = _get_center_index(fractal)
        elif column == "span_start":
            record[column] = _get_span_start(fractal)
        elif column == "span_end":
            record[column] = _get_span_end(fractal)
        elif column == "source_start_index":
            record[column] = fractal.get("original_index", fractal.get("index"))
        elif column == "source_end_index":
            record[column] = fractal.get("original_index", fractal.get("index"))
        else:
            record[column] = None
    record["is_effective"] = True
    return record


def _direction_from_types(start_type: str, end_type: str) -> str:
    if start_type == TYPE_TOP and end_type == TYPE_BOTTOM:
        return DIRECTION_DOWN
    if start_type == TYPE_BOTTOM and end_type == TYPE_TOP:
        return DIRECTION_UP
    raise ValueError(f"不能用同类型分型生成笔：{start_type} -> {end_type}")


def _fallback_direction_from_prices(start: Any, end: Any) -> str:
    start_price = float(_get_value(start, "price"))
    end_price = float(_get_value(end, "price"))
    if end_price > start_price:
        return DIRECTION_UP
    if end_price < start_price:
        return DIRECTION_DOWN
    return _direction_from_types(_get_value(start, "type"), _get_value(end, "type"))


def _source_indices_overlap(start_fractal: Any, end_fractal: Any) -> bool:
    start_indices = set(_as_index_sequence(_get_value(start_fractal, "source_indices")))
    end_indices = set(_as_index_sequence(_get_value(end_fractal, "source_indices")))
    return bool(start_indices & end_indices)


def _as_index_sequence(value: Any) -> Sequence:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set, pd.Index)):
        return list(value)
    return [value]


def _same_fractal(left: pd.Series, right: pd.Series) -> bool:
    return (
        left["type"] == right["type"]
        and _get_center_index(left) == _get_center_index(right)
        and _get_span_start(left) == _get_span_start(right)
        and _get_span_end(left) == _get_span_end(right)
    )


def _get_value(fractal: Any, key: str) -> Any:
    if isinstance(fractal, dict):
        return fractal[key]
    return fractal[key]


def _get_center_index(fractal: Any) -> int:
    if _has_key(fractal, "center_index"):
        return int(_get_value(fractal, "center_index"))
    return int(_get_value(fractal, "virtual_index"))


def _get_span_start(fractal: Any) -> int:
    if _has_key(fractal, "span_start"):
        return int(_get_value(fractal, "span_start"))
    return _get_center_index(fractal) - 1


def _get_span_end(fractal: Any) -> int:
    if _has_key(fractal, "span_end"):
        return int(_get_value(fractal, "span_end"))
    return _get_center_index(fractal) + 1


def _has_key(fractal: Any, key: str) -> bool:
    if isinstance(fractal, dict):
        return key in fractal
    return key in fractal.index


def _get_bar_value(bar: Any, key: str) -> Any:
    if isinstance(bar, dict):
        return bar[key]
    return getattr(bar, key)


def _validate_standard_bars_for_bi(bars: Sequence[Any]) -> None:
    if isinstance(bars, pd.DataFrame):
        raise TypeError("build_bis_incremental() 必须接收 standard_bars，不能直接接收 raw_bars/raw_df")
    for bar in bars:
        if not hasattr(bar, "high") or not hasattr(bar, "low"):
            raise TypeError("build_bis_incremental() 的 bars 参数必须是 standard_bars 序列")


def _normalize_fractals_for_bi(fractals: pd.DataFrame) -> pd.DataFrame:
    normalized = fractals.copy()
    if "virtual_index" not in normalized.columns and "index" in normalized.columns:
        normalized["virtual_index"] = normalized["index"]
    if "index" not in normalized.columns and "virtual_index" in normalized.columns:
        normalized["index"] = normalized["virtual_index"]
    if "center_index" not in normalized.columns:
        normalized["center_index"] = normalized["virtual_index"]
    else:
        normalized["center_index"] = normalized["center_index"].where(
            normalized["center_index"].notna(), normalized["virtual_index"]
        )
    if "span_start" not in normalized.columns:
        normalized["span_start"] = normalized["center_index"].astype(int) - 1
    if "span_end" not in normalized.columns:
        normalized["span_end"] = normalized["center_index"].astype(int) + 1
    if "source_indices" not in normalized.columns:
        normalized["source_indices"] = normalized["virtual_index"].map(lambda value: [int(value)])
    if "source_index" not in normalized.columns:
        normalized["source_index"] = normalized["source_indices"].map(lambda values: list(values)[0])
    if "original_index" not in normalized.columns:
        normalized["original_index"] = normalized["source_index"]
    if "x" not in normalized.columns:
        normalized["x"] = normalized["source_index"]
    if "source_start_index" not in normalized.columns:
        normalized["source_start_index"] = normalized["source_indices"].map(lambda values: list(values)[0])
    if "source_end_index" not in normalized.columns:
        normalized["source_end_index"] = normalized["source_indices"].map(lambda values: list(values)[-1])
    if "source_date" not in normalized.columns and "date" in normalized.columns:
        normalized["source_date"] = normalized["date"]
    return normalized


def _prices_equal(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=0.0, abs_tol=1e-9)


def _empty_effective_fractals() -> pd.DataFrame:
    return pd.DataFrame(columns=EFFECTIVE_FRACTAL_COLUMNS)


def _empty_bis() -> pd.DataFrame:
    return pd.DataFrame(columns=BI_COLUMNS)
