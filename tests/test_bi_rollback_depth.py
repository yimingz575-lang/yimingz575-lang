from __future__ import annotations

import pandas as pd

from src.chan.bi import try_rollback_and_rebuild_tail, validate_bi_extreme
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


def _build_depth_fixture() -> tuple[list[StandardKLine], pd.DataFrame, list[tuple[pd.Series, pd.Series]]]:
    highs = [50.0] * 125
    lows = [20.0] * 125
    points = [
        (TYPE_TOP, 0, 100.0, 90.0),
        (TYPE_BOTTOM, 4, 80.0, 0.0),
        (TYPE_TOP, 8, 120.0, 92.0),
        (TYPE_BOTTOM, 12, 78.0, 15.0),
        (TYPE_TOP, 16, 130.0, 94.0),
        (TYPE_BOTTOM, 20, 76.0, 16.0),
        (TYPE_TOP, 24, 140.0, 96.0),
        (TYPE_BOTTOM, 28, 74.0, 17.0),
        (TYPE_TOP, 32, 150.0, 98.0),
        (TYPE_BOTTOM, 36, 72.0, 18.0),
        (TYPE_TOP, 40, 160.0, 100.0),
        (TYPE_BOTTOM, 44, 70.0, 5.0),
        (TYPE_TOP, 84, 1000.0, 120.0),
        (TYPE_BOTTOM, 88, 60.0, -10.0),
        (TYPE_TOP, 92, 1010.0, 130.0),
        (TYPE_BOTTOM, 96, 58.0, -20.0),
        (TYPE_TOP, 100, 1020.0, 140.0),
        (TYPE_BOTTOM, 104, 56.0, -30.0),
        (TYPE_TOP, 108, 1030.0, 150.0),
        (TYPE_BOTTOM, 112, 54.0, -40.0),
        (TYPE_TOP, 116, 1040.0, 160.0),
        (TYPE_BOTTOM, 120, 52.0, -50.0),
        (TYPE_TOP, 124, 1050.0, 170.0),
    ]
    lows[60] = 3.0
    for fractal_type, center_index, high, low in points:
        highs[center_index] = high
        lows[center_index] = low

    bars = [_bar(index, high, low) for index, (high, low) in enumerate(zip(highs, lows))]
    fractals = pd.DataFrame([_fractal(*point) for point in points])
    rows = {int(row["center_index"]): row for _, row in fractals.iterrows()}
    old_pairs = [
        (rows[0], rows[4]),
        (rows[4], rows[8]),
        (rows[8], rows[12]),
        (rows[12], rows[16]),
        (rows[16], rows[20]),
        (rows[20], rows[24]),
        (rows[24], rows[28]),
        (rows[28], rows[32]),
        (rows[32], rows[36]),
        (rows[36], rows[40]),
        (rows[40], rows[44]),
    ]
    return bars, fractals, old_pairs


def _pair_indexes(pairs: list[tuple[pd.Series, pd.Series]]) -> list[tuple[int, int]]:
    return [(int(start["center_index"]), int(end["center_index"])) for start, end in pairs]


def test_rollback_depth_can_find_first_success_at_ten_without_global_rebuild() -> None:
    bars, fractals, old_pairs = _build_depth_fixture()

    new_pairs, stats, records = try_rollback_and_rebuild_tail(
        bars,
        fractals,
        old_pairs,
        max_rollback=15,
        stuck_candidate_threshold=2,
    )

    assert stats["rollback_success_count"] == 1
    assert stats["accepted_rollback_count"] == 10
    assert [record["rollback_count"] for record in records] == list(range(1, 11))
    assert all(record["accepted"] is False for record in records[:9])
    assert records[9]["accepted"] is True
    assert records[9]["kept_bis_count"] == 1
    assert records[9]["old_last_raw_index"] == 44
    assert records[9]["new_last_raw_index"] == 124
    assert _pair_indexes(new_pairs) == [
        (0, 4),
        (4, 84),
        (84, 88),
        (88, 92),
        (92, 96),
        (96, 100),
        (100, 104),
        (104, 108),
        (108, 112),
        (112, 116),
        (116, 120),
        (120, 124),
    ]

    for position, (start, end) in enumerate(new_pairs):
        assert start["type"] != end["type"]
        assert abs(int(end["center_index"]) - int(start["center_index"])) >= 4
        assert validate_bi_extreme(bars, start, end)
        if position > 0:
            previous_end = new_pairs[position - 1][1]
            assert int(previous_end["center_index"]) == int(start["center_index"])
