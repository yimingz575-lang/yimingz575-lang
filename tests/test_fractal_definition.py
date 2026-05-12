from __future__ import annotations

import pandas as pd
import pytest

from src.chan.fractal import TYPE_BOTTOM, TYPE_TOP, detect_candidate_fractals
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


def test_top_fractal_requires_strictly_higher_high_and_low() -> None:
    fractals = detect_candidate_fractals([
        _bar(0, 10, 5),
        _bar(1, 13, 8),
        _bar(2, 11, 6),
    ])

    assert len(fractals) == 1
    assert fractals.loc[0, "type"] == TYPE_TOP
    assert fractals.loc[0, "center_index"] == 1
    assert fractals.loc[0, "span_start"] == 0
    assert fractals.loc[0, "span_end"] == 2


def test_bottom_fractal_requires_strictly_lower_low_and_high() -> None:
    fractals = detect_candidate_fractals([
        _bar(0, 13, 6),
        _bar(1, 10, 3),
        _bar(2, 12, 5),
    ])

    assert len(fractals) == 1
    assert fractals.loc[0, "type"] == TYPE_BOTTOM
    assert fractals.loc[0, "center_index"] == 1
    assert fractals.loc[0, "span_start"] == 0
    assert fractals.loc[0, "span_end"] == 2


def test_equal_high_or_low_does_not_create_fractal() -> None:
    equal_high = detect_candidate_fractals([
        _bar(0, 13, 5),
        _bar(1, 13, 8),
        _bar(2, 11, 6),
    ])
    equal_low = detect_candidate_fractals([
        _bar(0, 13, 3),
        _bar(1, 10, 3),
        _bar(2, 12, 5),
    ])

    assert equal_high.empty
    assert equal_low.empty


def test_first_and_last_standard_bars_are_never_fractal_centers() -> None:
    fractals = detect_candidate_fractals([
        _bar(0, 20, 15),
        _bar(1, 18, 12),
    ])

    assert fractals.empty


def test_detect_candidate_fractals_rejects_raw_dataframe_input() -> None:
    raw_df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=3),
            "open": [6, 9, 7],
            "high": [10, 13, 11],
            "low": [5, 8, 6],
            "close": [9, 12, 10],
            "volume": [1000, 1001, 1002],
            "x": [0, 1, 2],
        }
    )

    with pytest.raises(TypeError):
        detect_candidate_fractals(raw_df)
