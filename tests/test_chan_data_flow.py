from __future__ import annotations

import pandas as pd
import pytest

from src.chan.bi import build_bis_incremental
from src.chan.engine import analyze_chan_marks
from src.chan.fractal import detect_candidate_fractals


def _make_df(highs: list[float], lows: list[float]) -> pd.DataFrame:
    rows = len(highs)
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=rows),
            "open": [low + 1 for low in lows],
            "high": highs,
            "low": lows,
            "close": [high - 1 for high in highs],
            "volume": [1000 + index for index in range(rows)],
            "x": list(range(rows)),
        }
    )


def test_raw_bars_are_not_valid_detect_candidate_fractals_input() -> None:
    raw_bars = _make_df([10, 13, 11], [5, 8, 6])

    with pytest.raises(TypeError):
        detect_candidate_fractals(raw_bars)


def test_raw_bars_are_not_valid_build_bis_incremental_input() -> None:
    raw_bars = _make_df([10, 13, 11], [5, 8, 6])
    empty_fractals = pd.DataFrame()

    with pytest.raises(TypeError):
        build_bis_incremental(raw_bars, empty_fractals)


def test_analyze_chan_marks_generates_standard_bars_and_inclusion_groups_first() -> None:
    raw_bars = _make_df([10, 13, 11, 9, 8, 7, 9], [5, 8, 6, 4, 3, 1, 3])

    result = analyze_chan_marks(raw_bars)

    assert "raw_bars" in result
    assert "standard_bars" in result
    assert "inclusion_groups" in result
    assert "candidate_fractals" in result
    assert "confirmed_bis" in result
    assert len(result["standard_bars"]) == len(result["inclusion_groups"])


def test_fractals_and_bis_use_standard_virtual_indices() -> None:
    raw_bars = _make_df([10, 13, 11, 9, 8, 7, 9], [5, 8, 6, 4, 3, 1, 3])

    result = analyze_chan_marks(raw_bars)
    standard_indices = {bar.virtual_index for bar in result["standard_bars"]}
    candidate_fractals = result["candidate_fractals"]
    confirmed_bis = result["confirmed_bis"]

    if not candidate_fractals.empty:
        assert set(candidate_fractals["center_index"]).issubset(standard_indices)
        assert (candidate_fractals["virtual_index"] == candidate_fractals["center_index"]).all()
    if not confirmed_bis.empty:
        assert set(confirmed_bis["start_center_index"]).issubset(standard_indices)
        assert set(confirmed_bis["end_center_index"]).issubset(standard_indices)


def test_chart_mapping_information_is_available_through_inclusion_groups() -> None:
    raw_bars = _make_df([10, 13, 11, 9, 8, 7, 9], [5, 8, 6, 4, 3, 1, 3])

    result = analyze_chan_marks(raw_bars)
    inclusion_groups = result["inclusion_groups"]

    assert {
        "virtual_index",
        "source_start_index",
        "source_end_index",
        "source_indices",
    }.issubset(inclusion_groups.columns)
