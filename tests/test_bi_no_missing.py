from __future__ import annotations

import pandas as pd

from src.chan.bi import (
    MIN_BI_KLINE_COUNT,
    build_bis_incremental,
    validate_bi_extreme,
    validate_bi_sequence_continuity,
)
from src.chan.fractal import TYPE_BOTTOM, TYPE_TOP, VirtualKLine


def _make_long_alternating_case() -> tuple[list[VirtualKLine], pd.DataFrame]:
    points = [
        (TYPE_TOP, 0, 100, 90),
        (TYPE_BOTTOM, 4, 85, 50),
        (TYPE_TOP, 8, 110, 70),
        (TYPE_BOTTOM, 12, 90, 45),
        (TYPE_TOP, 16, 115, 80),
        (TYPE_BOTTOM, 20, 88, 40),
        (TYPE_TOP, 24, 120, 78),
        (TYPE_BOTTOM, 28, 86, 35),
        (TYPE_TOP, 32, 125, 82),
        (TYPE_BOTTOM, 36, 84, 30),
        (TYPE_TOP, 40, 130, 85),
    ]
    highs = [0.0] * 41
    lows = [0.0] * 41

    for left, right in zip(points, points[1:]):
        left_type, left_index, left_high, left_low = left
        right_type, right_index, right_high, right_low = right
        for index in range(left_index, right_index + 1):
            ratio = (index - left_index) / (right_index - left_index)
            highs[index] = left_high + (right_high - left_high) * ratio
            lows[index] = left_low + (right_low - left_low) * ratio

        if left_type == TYPE_TOP:
            highs[left_index] = left_high
            lows[right_index] = right_low
        else:
            lows[left_index] = left_low
            highs[right_index] = right_high

    bars = [
        VirtualKLine(high=high, low=low, source_positions=[index])
        for index, (high, low) in enumerate(zip(highs, lows))
    ]

    records = []
    for fractal_type, virtual_index, high, low in points:
        date = pd.Timestamp("2024-01-01") + pd.Timedelta(days=virtual_index)
        records.append(
            {
                "index": virtual_index,
                "x": virtual_index,
                "date": date,
                "type": fractal_type,
                "price": high if fractal_type == TYPE_TOP else low,
                "source_index": virtual_index,
                "source_date": date,
                "virtual_index": virtual_index,
                "original_index": virtual_index,
                "high": high,
                "low": low,
                "source_indices": [virtual_index],
            }
        )

    return bars, pd.DataFrame(records)


def _fractal_from_bi_endpoint(bi: pd.Series, endpoint: str, bars: list[VirtualKLine]) -> dict:
    virtual_index = int(bi[f"{endpoint}_virtual_index"])
    bar = bars[virtual_index]
    fractal_type = bi[f"{endpoint}_type"]
    return {
        "type": fractal_type,
        "virtual_index": virtual_index,
        "original_index": bi[f"{endpoint}_original_index"],
        "price": float(bi[f"{endpoint}_price"]),
        "high": bar.high,
        "low": bar.low,
        "source_indices": list(bi[f"{endpoint}_source_indices"]),
    }


def test_long_alternating_case_generates_more_than_ten_valid_continuous_bis() -> None:
    bars, fractals = _make_long_alternating_case()

    _, bis = build_bis_incremental(bars, fractals)

    assert len(bis) == 10
    assert validate_bi_sequence_continuity(bis)
    for _, bi in bis.iterrows():
        start = _fractal_from_bi_endpoint(bi, "start", bars)
        end = _fractal_from_bi_endpoint(bi, "end", bars)
        assert bi["start_type"] != bi["end_type"]
        assert bi["start_original_index"] != bi["end_original_index"]
        assert bi["kline_count"] >= MIN_BI_KLINE_COUNT
        assert validate_bi_extreme(bars, start, end)
