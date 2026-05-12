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
        "original_index": virtual_index,
        "high": high,
        "low": low,
        "source_indices": [virtual_index],
    }


def _fractals(records: list[tuple[str, int, float, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [_fractal(fractal_type, virtual_index, high, low) for fractal_type, virtual_index, high, low in records]
    )


def test_alternating_five_fractals_generate_four_continuous_bis() -> None:
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
    assert validate_bi_sequence_continuity(bis)


def test_each_bi_starts_from_previous_bi_end() -> None:
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
    assert bis.loc[0, "end_virtual_index"] == bis.loc[1, "start_virtual_index"]
    assert bis.loc[0, "end_type"] == bis.loc[1, "start_type"]
    assert validate_bi_sequence_continuity(bis)


def test_next_bi_uses_previous_confirmed_endpoint_when_no_more_extreme_same_type_appears() -> None:
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

    assert bis.loc[1, "start_virtual_index"] == 4
    assert bis.loc[1, "start_price"] == 5


def test_rejected_candidate_does_not_reset_anchor_or_break_continuity() -> None:
    bars = _bars(
        highs=[20, 18, 13, 12, 10, 11, 14, 17, 22],
        lows=[15, 12, 9, 7, 5, 6, 8, 10, 12],
    )
    fractals = _fractals(
        [
            (TYPE_TOP, 0, 20, 15),
            (TYPE_BOTTOM, 2, 13, 9),
            (TYPE_BOTTOM, 4, 10, 5),
            (TYPE_TOP, 8, 22, 12),
        ]
    )

    _, bis = build_bis_incremental(bars, fractals)

    assert len(bis) == 2
    assert bis.loc[0, "end_virtual_index"] == 4
    assert bis.loc[1, "start_virtual_index"] == 4
    assert validate_bi_sequence_continuity(bis)


def test_more_extreme_same_type_endpoint_updates_previous_bi_without_creating_gap() -> None:
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
    assert bis.loc[0, "end_virtual_index"] == 6
    assert bis.loc[1, "start_virtual_index"] == 6
    assert validate_bi_sequence_continuity(bis)


def test_failed_reverse_can_reopen_unlocked_active_window() -> None:
    bars = _bars(
        highs=[12, 14, 16, 18, 20, 18, 16, 14, 12, 10, 14, 18, 22, 20, 17, 13, 9],
        lows=[5, 6, 7, 8, 10, 9, 8, 7, 6.5, 6, 7, 9, 11, 8, 5, 2, 0],
    )
    fractals = _fractals(
        [
            (TYPE_BOTTOM, 0, 12, 5),
            (TYPE_TOP, 4, 20, 10),
            (TYPE_BOTTOM, 9, 10, 6),
            (TYPE_TOP, 12, 22, 11),
            (TYPE_BOTTOM, 16, 9, 0),
        ]
    )
    attempts: list[dict] = []

    _, bis = build_bis_incremental(bars, fractals, attempt_records=attempts)

    assert len(bis) == 2
    assert bis.loc[0, "start_virtual_index"] == 0
    assert bis.loc[0, "end_virtual_index"] == 12
    assert bis.loc[1, "start_virtual_index"] == 12
    assert bis.loc[1, "end_virtual_index"] == 16
    assert bis.attrs["locked_bis_count"] == 0
    assert any(attempt["reason"] == "reopen_active_window_with_more_extreme_reverse" for attempt in attempts)
    assert validate_bi_sequence_continuity(bis)


def test_reopening_active_window_does_not_rewrite_locked_bis() -> None:
    bars = _bars(
        highs=[12, 14, 17, 21, 25, 20, 16, 12, 10, 13, 17, 20, 22, 18, 12, 9, 13, 18, 23, 28],
        lows=[5, 6, 8, 12, 15, 12, 8, 5, 4, 6, 8, 10, 12, 9, 5, 2, 5, 9, 13, 16],
    )
    fractals = _fractals(
        [
            (TYPE_BOTTOM, 0, 12, 5),
            (TYPE_TOP, 4, 25, 15),
            (TYPE_BOTTOM, 8, 10, 4),
            (TYPE_TOP, 12, 22, 12),
            (TYPE_BOTTOM, 15, 9, 2),
            (TYPE_TOP, 19, 28, 16),
        ]
    )
    attempts: list[dict] = []

    _, bis = build_bis_incremental(bars, fractals, attempt_records=attempts)

    assert bis[["start_virtual_index", "end_virtual_index"]].values.tolist() == [
        [0, 4],
        [4, 15],
        [15, 19],
    ]
    assert bis.attrs["locked_bis_count"] == 1
    assert bis.attrs["pending_bi_count"] == 1
    assert bis.attrs["active_bi_count"] == 1
    assert any(attempt["reason"] == "reopen_active_window_with_more_extreme_reverse" for attempt in attempts)
    assert validate_bi_sequence_continuity(bis)
