from __future__ import annotations

import pandas as pd

from src.chan.bi import (
    MAX_BI_ROLLBACK,
    can_form_bi,
    try_rollback_and_rebuild_tail,
    validate_bi_extreme,
)
from src.chan.fractal import TYPE_BOTTOM, TYPE_TOP
from src.chan.inclusion import StandardKLine


def _bar(index: int, high: float, low: float) -> StandardKLine:
    date = pd.Timestamp("2024-01-01") + pd.Timedelta(days=index)
    return StandardKLine(
        virtual_index=index,
        source_start_index=index,
        source_end_index=index,
        source_indices=[index],
        source_positions=[index],
        date_start=date,
        date_end=date,
        open=low + 1,
        high=high,
        low=low,
        close=high - 1,
        volume=1000 + index,
    )


def _bars(highs: list[float], lows: list[float]) -> list[StandardKLine]:
    return [_bar(index, high, low) for index, (high, low) in enumerate(zip(highs, lows))]


def _fractal(fractal_type: str, center_index: int, high: float, low: float) -> dict:
    date = pd.Timestamp("2024-01-01") + pd.Timedelta(days=center_index)
    return {
        "index": center_index,
        "x": center_index,
        "date": date,
        "type": fractal_type,
        "price": high if fractal_type == TYPE_TOP else low,
        "source_index": center_index,
        "source_date": date,
        "virtual_index": center_index,
        "center_index": center_index,
        "span_start": center_index - 1,
        "span_end": center_index + 1,
        "original_index": center_index,
        "high": high,
        "low": low,
        "source_start_index": center_index,
        "source_end_index": center_index,
        "source_indices": [center_index],
    }


def _fractals(records: list[tuple[str, int, float, float]]) -> pd.DataFrame:
    return pd.DataFrame([_fractal(*record) for record in records])


def _pairs(fractals: pd.DataFrame, indexes: list[tuple[int, int]]) -> list[tuple[pd.Series, pd.Series]]:
    rows = {int(row["center_index"]): row for _, row in fractals.iterrows()}
    return [(rows[start], rows[end]) for start, end in indexes]


def _pair_indexes(pairs: list[tuple[pd.Series, pd.Series]]) -> list[tuple[int, int]]:
    return [(int(start["center_index"]), int(end["center_index"])) for start, end in pairs]


def _assert_strict_bi_pairs(bars: list[StandardKLine], pairs: list[tuple[pd.Series, pd.Series]]) -> None:
    for position, (start, end) in enumerate(pairs):
        assert can_form_bi(start, end)
        assert validate_bi_extreme(bars, start, end)
        assert abs(int(end["center_index"]) - int(start["center_index"])) >= 4
        if position > 0:
            previous_end = pairs[position - 1][1]
            assert (
                previous_end["type"],
                int(previous_end["center_index"]),
            ) == (
                start["type"],
                int(start["center_index"]),
            )


def _rollback_one_fixture() -> tuple[list[StandardKLine], pd.DataFrame, list[tuple[pd.Series, pd.Series]]]:
    highs = [
        100,
        96,
        90,
        84,
        80,
        82,
        88,
        96,
        105,
        108,
        110,
        92,
        70,
        90,
        104,
        114,
        120,
        112,
        96,
        82,
        75,
    ]
    lows = [
        90,
        82,
        72,
        60,
        50,
        58,
        66,
        78,
        90,
        88,
        84,
        70,
        55,
        68,
        82,
        94,
        100,
        88,
        66,
        50,
        40,
    ]
    bars = _bars(highs, lows)
    fractals = _fractals(
        [
            (TYPE_TOP, 0, 100, 90),
            (TYPE_BOTTOM, 4, 80, 50),
            (TYPE_TOP, 8, 105, 90),
            (TYPE_BOTTOM, 12, 70, 55),
            (TYPE_TOP, 16, 120, 100),
            (TYPE_BOTTOM, 20, 75, 40),
        ]
    )
    old_pairs = _pairs(fractals, [(0, 4), (4, 8)])
    return bars, fractals, old_pairs


def _rollback_two_fixture() -> tuple[list[StandardKLine], pd.DataFrame, list[tuple[pd.Series, pd.Series]]]:
    highs = [
        150,
        132,
        112,
        92,
        80,
        82,
        88,
        95,
        100,
        96,
        88,
        78,
        70,
        82,
        92,
        108,
        120,
        112,
        94,
        82,
        75,
    ]
    lows = [
        130,
        106,
        84,
        64,
        50,
        58,
        66,
        78,
        90,
        80,
        70,
        65,
        60,
        55,
        58,
        84,
        96,
        88,
        66,
        52,
        45,
    ]
    bars = _bars(highs, lows)
    fractals = _fractals(
        [
            (TYPE_TOP, 0, 150, 130),
            (TYPE_BOTTOM, 4, 80, 50),
            (TYPE_TOP, 8, 100, 90),
            (TYPE_BOTTOM, 12, 70, 60),
            (TYPE_TOP, 16, 120, 96),
            (TYPE_BOTTOM, 20, 75, 45),
        ]
    )
    old_pairs = _pairs(fractals, [(0, 4), (4, 8), (8, 12)])
    return bars, fractals, old_pairs


def test_rollback_one_bi_rebuilds_later_valid_tail() -> None:
    bars, fractals, old_pairs = _rollback_one_fixture()

    new_pairs, stats, records = try_rollback_and_rebuild_tail(
        bars,
        fractals,
        old_pairs,
        max_rollback=MAX_BI_ROLLBACK,
        stuck_candidate_threshold=2,
    )

    assert stats["rollback_trigger_count"] == 1
    assert stats["rollback_success_count"] == 1
    assert stats["accepted_rollback_count"] == 1
    assert stats["fallback_bi_count"] == 0
    assert records[-1]["accepted"] is True
    assert _pair_indexes(new_pairs) == [
        (0, 4),
        (4, 16),
        (16, 20),
    ]


def test_rollback_two_bis_when_one_is_not_enough() -> None:
    bars, fractals, old_pairs = _rollback_two_fixture()

    new_pairs, stats, records = try_rollback_and_rebuild_tail(
        bars,
        fractals,
        old_pairs,
        max_rollback=MAX_BI_ROLLBACK,
        stuck_candidate_threshold=2,
    )

    assert stats["rollback_success_count"] == 1
    assert stats["accepted_rollback_count"] == 2
    assert stats["fallback_bi_count"] == 0
    assert [record["rollback_count"] for record in records] == [1, 2]
    assert records[0]["accepted"] is False
    assert records[1]["accepted"] is True
    assert _pair_indexes(new_pairs) == [
        (0, 4),
        (4, 16),
        (16, 20),
    ]


def test_rollback_uses_smallest_successful_count() -> None:
    bars, fractals, old_pairs = _rollback_one_fixture()

    _, stats, records = try_rollback_and_rebuild_tail(
        bars,
        fractals,
        old_pairs,
        max_rollback=MAX_BI_ROLLBACK,
        stuck_candidate_threshold=2,
    )

    assert stats["accepted_rollback_count"] == 1
    assert [record["rollback_count"] for record in records] == [1]


def test_rollback_result_keeps_all_hard_bi_constraints() -> None:
    bars, fractals, old_pairs = _rollback_two_fixture()

    new_pairs, _, _ = try_rollback_and_rebuild_tail(
        bars,
        fractals,
        old_pairs,
        max_rollback=MAX_BI_ROLLBACK,
        stuck_candidate_threshold=2,
    )

    _assert_strict_bi_pairs(bars, new_pairs)


def test_rollback_not_triggered_without_enough_tail_candidates() -> None:
    bars, fractals, old_pairs = _rollback_one_fixture()

    new_pairs, stats, records = try_rollback_and_rebuild_tail(
        bars,
        fractals,
        old_pairs,
        max_rollback=MAX_BI_ROLLBACK,
        stuck_candidate_threshold=20,
    )

    assert _pair_indexes(new_pairs) == _pair_indexes(old_pairs)
    assert stats["rollback_trigger_count"] == 0
    assert records == []


def test_failed_rollback_keeps_original_bis() -> None:
    bars, fractals, old_pairs = _rollback_one_fixture()
    broken_bars = [
        _bar(index, bar.high, 30 if 9 <= index <= 16 else bar.low)
        for index, bar in enumerate(bars)
    ]

    new_pairs, stats, records = try_rollback_and_rebuild_tail(
        broken_bars,
        fractals,
        old_pairs,
        max_rollback=1,
        stuck_candidate_threshold=2,
    )

    assert _pair_indexes(new_pairs) == _pair_indexes(old_pairs)
    assert stats["rollback_trigger_count"] == 1
    assert stats["rollback_failed_count"] == 1
    assert records[-1]["accepted"] is False
