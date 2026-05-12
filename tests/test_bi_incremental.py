from __future__ import annotations

import pandas as pd

from src.chan.bi import build_bis_incremental
from src.chan.fractal import TYPE_BOTTOM, TYPE_TOP, VirtualKLine


def _bars(highs: list[float], lows: list[float]) -> list[VirtualKLine]:
    return [
        VirtualKLine(high=high, low=low, source_positions=[index])
        for index, (high, low) in enumerate(zip(highs, lows))
    ]


def _fractal(fractal_type: str, virtual_index: int, high: float, low: float) -> dict:
    date = pd.Timestamp("2024-01-01") + pd.Timedelta(days=virtual_index)
    price = high if fractal_type == TYPE_TOP else low
    return {
        "index": virtual_index,
        "x": virtual_index,
        "date": date,
        "type": fractal_type,
        "price": price,
        "source_index": virtual_index,
        "source_date": date,
        "virtual_index": virtual_index,
        "original_index": virtual_index,
        "high": high,
        "low": low,
        "source_indices": [virtual_index],
    }


def _fractals(records: list[tuple[str, int, float, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [_fractal(fractal_type, virtual_index, high, low) for fractal_type, virtual_index, high, low in records]
    )


def test_top_bottom_top_generates_two_incremental_bis() -> None:
    bars = _bars(
        highs=[20, 18, 16, 14, 12, 13, 15, 17, 22],
        lows=[15, 14, 12, 10, 5, 6, 7, 9, 11],
    )
    fractals = _fractals(
        [
            (TYPE_TOP, 0, 20, 15),
            (TYPE_BOTTOM, 4, 12, 5),
            (TYPE_TOP, 8, 22, 11),
        ]
    )

    _, bis = build_bis_incremental(bars, fractals)

    assert len(bis) == 2
    assert bis[["start_virtual_index", "end_virtual_index"]].values.tolist() == [[0, 4], [4, 8]]


def test_five_alternating_fractals_generate_four_incremental_bis() -> None:
    bars = _bars(
        highs=[30, 28, 25, 20, 18, 19, 23, 27, 32, 30, 25, 20, 17, 18, 22, 26, 34],
        lows=[25, 22, 18, 12, 5, 6, 8, 12, 20, 17, 10, 6, 4, 5, 8, 12, 22],
    )
    fractals = _fractals(
        [
            (TYPE_TOP, 0, 30, 25),
            (TYPE_BOTTOM, 4, 18, 5),
            (TYPE_TOP, 8, 32, 20),
            (TYPE_BOTTOM, 12, 17, 4),
            (TYPE_TOP, 16, 34, 22),
        ]
    )

    _, bis = build_bis_incremental(bars, fractals)

    assert len(bis) == 4
    assert bis[["start_virtual_index", "end_virtual_index"]].values.tolist() == [
        [0, 4],
        [4, 8],
        [8, 12],
        [12, 16],
    ]


def test_waits_past_reverse_fractal_with_insufficient_kline_count() -> None:
    bars = _bars(
        highs=[20, 18, 13, 12, 10],
        lows=[15, 12, 9, 7, 5],
    )
    fractals = _fractals(
        [
            (TYPE_TOP, 0, 20, 15),
            (TYPE_BOTTOM, 2, 13, 9),
            (TYPE_BOTTOM, 4, 10, 5),
        ]
    )

    _, bis = build_bis_incremental(bars, fractals)

    assert len(bis) == 1
    assert bis.loc[0, "end_virtual_index"] == 4


def test_replaces_same_direction_anchor_before_valid_reverse_exists() -> None:
    bars = _bars(
        highs=[20, 18, 13, 25, 21, 18, 14, 10],
        lows=[15, 12, 9, 16, 12, 9, 6, 4],
    )
    fractals = _fractals(
        [
            (TYPE_TOP, 0, 20, 15),
            (TYPE_BOTTOM, 2, 13, 9),
            (TYPE_TOP, 3, 25, 16),
            (TYPE_BOTTOM, 7, 10, 4),
        ]
    )

    _, bis = build_bis_incremental(bars, fractals)

    assert len(bis) == 1
    assert bis.loc[0, "start_virtual_index"] == 3
    assert bis.loc[0, "start_price"] == 25


def test_later_more_extreme_same_type_fractal_starts_next_bi_without_swallowing_confirmed_bi() -> None:
    bars = _bars(
        highs=[30, 28, 25, 20, 18, 17, 15, 16, 20, 24, 29],
        lows=[25, 22, 18, 12, 5, 4, 2, 5, 8, 11, 14],
    )
    fractals = _fractals(
        [
            (TYPE_TOP, 0, 30, 25),
            (TYPE_BOTTOM, 4, 18, 5),
            (TYPE_BOTTOM, 6, 15, 2),
            (TYPE_TOP, 10, 29, 14),
        ]
    )

    _, bis = build_bis_incremental(bars, fractals)

    assert len(bis) == 2
    assert bis.loc[0, "start_virtual_index"] == 0
    assert bis.loc[0, "end_virtual_index"] == 6
    assert bis.loc[0, "end_price"] == 2
    assert bis.loc[1, "start_virtual_index"] == 6
    assert bis.loc[1, "end_virtual_index"] == 10
    assert bis.loc[1, "start_price"] == 2
