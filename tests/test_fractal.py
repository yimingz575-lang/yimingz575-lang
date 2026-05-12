from __future__ import annotations

import pandas as pd
import pytest

from src.chan.fractal import TYPE_BOTTOM, TYPE_TOP, detect_fractals
from src.chan.inclusion import build_standard_bars


def _make_df(highs: list[float], lows: list[float]) -> pd.DataFrame:
    rows = len(highs)
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=rows, freq="D"),
            "open": [10.0 + index for index in range(rows)],
            "high": highs,
            "low": lows,
            "close": [10.5 + index for index in range(rows)],
            "volume": [1000 + index for index in range(rows)],
            "x": list(range(rows)),
        }
    )


def test_detects_top_fractal() -> None:
    df = _make_df(highs=[10, 13, 11], lows=[5, 8, 6])

    fractals = detect_fractals(build_standard_bars(df))

    assert len(fractals) == 1
    assert fractals.loc[0, "type"] == TYPE_TOP
    assert fractals.loc[0, "index"] == 1
    assert fractals.loc[0, "x"] == 1
    assert fractals.loc[0, "price"] == 13


def test_detects_bottom_fractal() -> None:
    df = _make_df(highs=[13, 10, 12], lows=[6, 3, 5])

    fractals = detect_fractals(build_standard_bars(df))

    assert len(fractals) == 1
    assert fractals.loc[0, "type"] == TYPE_BOTTOM
    assert fractals.loc[0, "index"] == 1
    assert fractals.loc[0, "x"] == 1
    assert fractals.loc[0, "price"] == 3


def test_first_and_last_rows_are_not_fractals() -> None:
    df = _make_df(highs=[15, 16], lows=[8, 7])

    fractals = detect_fractals(build_standard_bars(df))

    assert fractals.empty


def test_same_kline_is_not_both_top_and_bottom() -> None:
    df = _make_df(highs=[10, 13, 11, 13, 10], lows=[5, 8, 6, 3, 5])

    fractals = detect_fractals(build_standard_bars(df))

    assert not fractals.empty
    assert fractals.groupby("index")["type"].nunique().max() == 1


def test_fractal_result_contains_required_fields() -> None:
    df = _make_df(highs=[10, 13, 11], lows=[5, 8, 6])

    fractals = detect_fractals(build_standard_bars(df))

    for column in ["index", "x", "date", "type", "price", "center_index", "span_start", "span_end"]:
        assert column in fractals.columns


def test_original_kline_count_and_data_are_unchanged() -> None:
    df = _make_df(highs=[10, 13, 11], lows=[5, 8, 6])
    original_df = df.copy(deep=True)

    _ = detect_fractals(build_standard_bars(df))

    assert len(df) == len(original_df)
    pd.testing.assert_frame_equal(df, original_df)


def test_fractal_detection_requires_standard_bars_not_raw_klines() -> None:
    df = _make_df(highs=[10, 13, 11], lows=[5, 8, 6])

    with pytest.raises(TypeError, match="standard_bars"):
        detect_fractals(df)

    fractals = detect_fractals(build_standard_bars(df))
    assert len(fractals) == 1
