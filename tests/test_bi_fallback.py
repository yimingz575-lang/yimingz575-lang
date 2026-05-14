from __future__ import annotations

from src.chan.bi import (
    FALLBACK_BI_COLOR,
    FALLBACK_BI_REASON,
    apply_rollback_or_fallback_bi,
    build_bis_incremental,
    should_use_fallback_bi,
    validate_bi_sequence_continuity,
)
from tests.test_bi_rollback import _pair_indexes as _rollback_pair_indexes
from tests.test_bi_rollback import _rollback_two_fixture
from tests.test_bi_rollback_depth import _build_depth_fixture


def test_affected_count_below_three_still_uses_standard_rollback_result() -> None:
    bars, fractals, old_pairs = _rollback_two_fixture()
    standard_rebuilt_pairs = old_pairs[:1]
    rollback_stats = {
        "rollback_trigger_count": 1,
        "rollback_success_count": 1,
        "rollback_failed_count": 0,
        "accepted_rollback_count": 2,
    }

    selected_pairs, fallback_metadata = apply_rollback_or_fallback_bi(
        standard_bars=bars,
        candidate_fractals=fractals,
        original_bis=old_pairs,
        rollback_bis=standard_rebuilt_pairs,
        rollback_stats=rollback_stats,
    )

    assert should_use_fallback_bi(2) is False
    assert fallback_metadata == []
    assert selected_pairs == standard_rebuilt_pairs


def test_large_rollback_appends_yellow_temporary_bi_without_rewriting_history() -> None:
    bars, fractals, old_pairs = _build_depth_fixture()

    _, bis = build_bis_incremental(
        bars,
        fractals,
        stuck_candidate_threshold=2,
    )

    standard_bis = bis[bis["is_fallback_bi"] == False]
    temporary_bis = bis[bis["is_fallback_bi"] == True]

    assert bis.attrs["fallback_bi_count"] == 1
    assert bis.attrs["fallback_trigger_count"] == 1
    assert bis.attrs["affected_confirmed_bi_count"] == 10
    assert should_use_fallback_bi(10) is True
    assert standard_bis[["start_center_index", "end_center_index"]].values.tolist() == [
        [int(start["center_index"]), int(end["center_index"])]
        for start, end in old_pairs
    ]
    assert len(temporary_bis) == 1

    temporary_bi = temporary_bis.iloc[0]
    assert bool(temporary_bi["is_temporary"]) is True
    assert bool(temporary_bi["is_fallback_bi"]) is True
    assert temporary_bi["color"] == FALLBACK_BI_COLOR
    assert temporary_bi["fallback_reason"] == FALLBACK_BI_REASON
    assert int(temporary_bi["affected_confirmed_bi_count"]) == 10
    assert int(temporary_bi["start_center_index"]) == 44
    assert int(temporary_bi["end_center_index"]) == 124
    assert temporary_bi["direction"] == "up"

    assert _rollback_pair_indexes(old_pairs) == [
        (int(row["start_center_index"]), int(row["end_center_index"]))
        for _, row in standard_bis.iterrows()
    ]
    assert validate_bi_sequence_continuity(bis)


def test_standard_bis_have_explicit_non_temporary_fields() -> None:
    bars, fractals, _ = _build_depth_fixture()

    _, bis = build_bis_incremental(
        bars,
        fractals,
        rollback_enabled=False,
        stuck_candidate_threshold=2,
    )

    assert not bis.empty
    assert (bis["is_temporary"] == False).all()
    assert (bis["is_fallback_bi"] == False).all()
    assert bis["fallback_reason"].isna().all()
    assert bis["color"].isna().all()
