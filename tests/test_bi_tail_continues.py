from __future__ import annotations

import pandas as pd

from src.chan.bi import build_bis_incremental, validate_bi_sequence_continuity
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
        "center_index": virtual_index,
        "span_start": virtual_index - 1,
        "span_end": virtual_index + 1,
        "original_index": virtual_index,
        "high": high,
        "low": low,
        "source_start_index": virtual_index,
        "source_end_index": virtual_index,
        "source_indices": [virtual_index],
    }


def _fractals(records: list[tuple[str, int, float, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [_fractal(fractal_type, virtual_index, high, low) for fractal_type, virtual_index, high, low in records]
    )


def test_active_bi_reject_does_not_stop_tail_generation() -> None:
    bars = _bars(
        highs=[30, 28, 25, 22, 20, 23, 28, 30, 35, 32, 28, 24, 22, 25, 30, 35, 40, 35, 28, 22, 18],
        lows=[25, 22, 18, 14, 10, 12, 18, 20, 24, 20, 15, 9, 5, 8, 15, 22, 30, 25, 15, 8, 2],
    )
    fractals = _fractals(
        [
            (TYPE_TOP, 0, 30, 25),
            (TYPE_BOTTOM, 4, 20, 10),
            (TYPE_TOP, 7, 30, 20),  # reverse candidate is too close and must be rejected
            (TYPE_TOP, 8, 35, 24),
            (TYPE_BOTTOM, 12, 22, 5),
            (TYPE_TOP, 16, 40, 30),
            (TYPE_BOTTOM, 20, 18, 2),
        ]
    )
    attempts: list[dict] = []

    _, bis = build_bis_incremental(bars, fractals, attempt_records=attempts)

    assert any(attempt["reason"] == "reject_not_enough_center_gap" for attempt in attempts)
    assert bis[["start_virtual_index", "end_virtual_index"]].values.tolist() == [
        [0, 4],
        [4, 8],
        [8, 12],
        [12, 16],
        [16, 20],
    ]
    assert int(bis.iloc[-1]["end_virtual_index"]) == 20
    assert validate_bi_sequence_continuity(bis)
