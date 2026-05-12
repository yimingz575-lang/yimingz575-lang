from __future__ import annotations

import pandas as pd

from src.chan.inclusion import (
    TYPE_CURRENT_CONTAINS_PREVIOUS,
    TYPE_CURRENT_INSIDE_PREVIOUS,
    TYPE_NONE,
    build_standard_bars,
    detect_inclusion_marks,
    process_inclusions,
)


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


def test_no_inclusion_marks_all_false() -> None:
    df = _make_df(highs=[10, 11, 12], lows=[8, 9, 10])

    marks = detect_inclusion_marks(df)
    _ = build_standard_bars(df)
    standard_bars = build_standard_bars(df)

    assert marks["has_inclusion"].tolist() == [False, False, False]
    assert marks["inclusion_with_prev"].tolist() == [False, False, False]
    assert marks["inclusion_type"].tolist() == [TYPE_NONE, TYPE_NONE, TYPE_NONE]
    assert len(standard_bars) == len(df)


def test_current_inside_previous_marks_pair() -> None:
    df = _make_df(highs=[12, 11, 13], lows=[8, 9, 7])

    marks = detect_inclusion_marks(df)

    assert bool(marks.loc[0, "has_inclusion"]) is True
    assert bool(marks.loc[1, "has_inclusion"]) is True
    assert bool(marks.loc[1, "inclusion_with_prev"]) is True
    assert marks.loc[1, "inclusion_type"] == TYPE_CURRENT_INSIDE_PREVIOUS


def test_current_contains_previous_marks_pair() -> None:
    df = _make_df(highs=[11, 12, 13], lows=[9, 8, 10])

    marks = detect_inclusion_marks(df)

    assert bool(marks.loc[0, "has_inclusion"]) is True
    assert bool(marks.loc[1, "has_inclusion"]) is True
    assert bool(marks.loc[1, "inclusion_with_prev"]) is True
    assert marks.loc[1, "inclusion_type"] == TYPE_CURRENT_CONTAINS_PREVIOUS


def test_continuous_inclusions_mark_all_related_bars() -> None:
    df = _make_df(highs=[12, 11, 13, 12], lows=[8, 9, 7, 8])

    marks = detect_inclusion_marks(df)

    assert marks["has_inclusion"].tolist() == [True, True, True, True]
    assert marks.loc[1, "inclusion_type"] == TYPE_CURRENT_INSIDE_PREVIOUS
    assert marks.loc[2, "inclusion_type"] == TYPE_CURRENT_CONTAINS_PREVIOUS
    assert marks.loc[3, "inclusion_type"] == TYPE_CURRENT_INSIDE_PREVIOUS


def test_original_rows_and_ohlc_are_unchanged() -> None:
    df = _make_df(highs=[12, 11, 13], lows=[8, 9, 7])
    original_ohlc = df[["open", "high", "low", "close"]].copy(deep=True)

    marks = detect_inclusion_marks(df)

    assert len(marks) == len(df)
    pd.testing.assert_frame_equal(df[["open", "high", "low", "close"]], original_ohlc)


def test_upward_inclusion_standard_bar_uses_higher_high_and_higher_low() -> None:
    df = _make_df(highs=[10, 12, 11], lows=[5, 7, 8])

    standard_bars = build_standard_bars(df)

    assert len(standard_bars) == 2
    assert standard_bars[1].source_indices == [1, 2]
    assert standard_bars[1].high == 12
    assert standard_bars[1].low == 8


def test_downward_inclusion_standard_bar_uses_lower_high_and_lower_low() -> None:
    df = _make_df(highs=[12, 10, 9], lows=[8, 5, 6])

    standard_bars = build_standard_bars(df)

    assert len(standard_bars) == 2
    assert standard_bars[1].source_indices == [1, 2]
    assert standard_bars[1].high == 9
    assert standard_bars[1].low == 5


def test_three_or_more_continuous_inclusions_merge_recursively() -> None:
    df = _make_df(highs=[10, 12, 11, 11.5], lows=[5, 7, 8, 8.5])

    standard_bars = build_standard_bars(df)

    assert len(standard_bars) == 2
    assert standard_bars[1].source_indices == [1, 2, 3]
    assert standard_bars[1].high == 12
    assert standard_bars[1].low == 8.5
    assert standard_bars[1].close == df.loc[3, "close"]
    assert standard_bars[1].volume == df.loc[1:3, "volume"].sum()


def test_standard_bar_records_source_range_and_indices() -> None:
    df = _make_df(highs=[10, 12, 11], lows=[5, 7, 8])

    result = process_inclusions(df)
    standard_bar = result.standard_bars[1]
    group = result.inclusion_groups.iloc[1]

    assert standard_bar.source_start_index == 1
    assert standard_bar.source_end_index == 2
    assert standard_bar.source_indices == [1, 2]
    assert group["source_start_index"] == 1
    assert group["source_end_index"] == 2
    assert group["source_indices"] == [1, 2]


def test_starting_inclusion_waits_for_first_clear_direction() -> None:
    df = _make_df(highs=[12, 11, 13], lows=[8, 9, 10])

    standard_bars = build_standard_bars(df)

    assert len(standard_bars) == 2
    assert standard_bars[0].source_indices == [0, 1]
    assert standard_bars[0].high == 12
    assert standard_bars[0].low == 9
