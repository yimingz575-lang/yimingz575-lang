from __future__ import annotations

import pandas as pd

from src.chan.bi import (
    MIN_BI_KLINE_COUNT,
    build_bis_incremental,
    validate_bi_extreme,
    validate_bi_sequence_continuity,
)
from src.chan.fractal import TYPE_BOTTOM, TYPE_TOP, VirtualKLine


def _bars(highs: list[float], lows: list[float]) -> list[VirtualKLine]:
    return [
        VirtualKLine(high=high, low=low, source_positions=[index])
        for index, (high, low) in enumerate(zip(highs, lows))
    ]


def _fractal(fractal_type: str, virtual_index: int, high: float, low: float) -> dict:
    date = pd.Timestamp("2024-01-01") + pd.Timedelta(days=virtual_index)
    return {
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


def _fractals(records: list[tuple[str, int, float, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [_fractal(fractal_type, virtual_index, high, low) for fractal_type, virtual_index, high, low in records]
    )


def _fractal_from_bi_endpoint(bi: pd.Series, endpoint: str, bars: list[VirtualKLine]) -> dict:
    virtual_index = int(bi[f"{endpoint}_virtual_index"])
    bar = bars[virtual_index]
    return {
        "type": bi[f"{endpoint}_type"],
        "virtual_index": virtual_index,
        "original_index": bi[f"{endpoint}_original_index"],
        "price": float(bi[f"{endpoint}_price"]),
        "high": float(bar.high),
        "low": float(bar.low),
        "source_indices": list(bi[f"{endpoint}_source_indices"]),
    }


def _active_extension_bars() -> list[VirtualKLine]:
    return _bars(
        highs=[30, 28, 25, 21, 18, 20, 24, 18, 15, 18, 23, 29, 35, 31, 24, 16, 12],
        lows=[25, 22, 18, 10, 5, 8, 10, 6, 2, 5, 8, 12, 15, 10, 6, 3, 1],
    )


def test_active_bi_endpoint_extends_before_reverse_bi_is_confirmed() -> None:
    bars = _active_extension_bars()
    fractals = _fractals(
        [
            (TYPE_TOP, 0, 30, 25),
            (TYPE_BOTTOM, 4, 18, 5),
            (TYPE_TOP, 6, 24, 10),
            (TYPE_BOTTOM, 8, 15, 2),
        ]
    )
    attempts: list[dict] = []

    _, bis = build_bis_incremental(bars, fractals, attempt_records=attempts)

    assert len(bis) == 1
    assert bis.loc[0, "start_virtual_index"] == 0
    assert bis.loc[0, "end_virtual_index"] == 8
    assert bis.loc[0, "end_price"] == 2
    assert any(attempt["reason"] == "extend_active_bi_endpoint" for attempt in attempts)


def test_next_bi_starts_from_extended_active_endpoint() -> None:
    bars = _active_extension_bars()
    fractals = _fractals(
        [
            (TYPE_TOP, 0, 30, 25),
            (TYPE_BOTTOM, 4, 18, 5),
            (TYPE_TOP, 6, 24, 10),
            (TYPE_BOTTOM, 8, 15, 2),
            (TYPE_TOP, 12, 35, 15),
        ]
    )

    _, bis = build_bis_incremental(bars, fractals)

    assert bis[["start_virtual_index", "end_virtual_index"]].values.tolist() == [[0, 8], [8, 12]]
    assert validate_bi_sequence_continuity(bis)


def test_locked_previous_bi_is_not_rewritten_by_later_lower_bottom() -> None:
    bars = _active_extension_bars()
    fractals = _fractals(
        [
            (TYPE_TOP, 0, 30, 25),
            (TYPE_BOTTOM, 4, 18, 5),
            (TYPE_TOP, 6, 24, 10),
            (TYPE_BOTTOM, 8, 15, 2),
            (TYPE_TOP, 12, 35, 15),
            (TYPE_BOTTOM, 16, 12, 1),
        ]
    )

    _, bis = build_bis_incremental(bars, fractals)

    assert bis[["start_virtual_index", "end_virtual_index"]].values.tolist() == [[0, 8], [8, 12], [12, 16]]
    assert bis.loc[0, "end_virtual_index"] == 8
    assert bis.loc[0, "end_price"] == 2
    assert validate_bi_sequence_continuity(bis)


def test_alternating_structure_still_generates_multiple_bis() -> None:
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


def test_final_active_model_bis_keep_all_hard_constraints() -> None:
    bars = _active_extension_bars()
    fractals = _fractals(
        [
            (TYPE_TOP, 0, 30, 25),
            (TYPE_BOTTOM, 4, 18, 5),
            (TYPE_TOP, 6, 24, 10),
            (TYPE_BOTTOM, 8, 15, 2),
            (TYPE_TOP, 12, 35, 15),
            (TYPE_BOTTOM, 16, 12, 1),
        ]
    )

    _, bis = build_bis_incremental(bars, fractals)

    assert validate_bi_sequence_continuity(bis)
    assert bis.loc[0, "end_virtual_index"] == 8
    for _, bi in bis.iterrows():
        start = _fractal_from_bi_endpoint(bi, "start", bars)
        end = _fractal_from_bi_endpoint(bi, "end", bars)
        assert bi["start_type"] != bi["end_type"]
        assert bi["kline_count"] >= MIN_BI_KLINE_COUNT
        assert validate_bi_extreme(bars, start, end)
