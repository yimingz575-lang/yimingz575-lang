from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

import pandas as pd

REQUIRED_COLUMNS = ["date", "high", "low", "x"]
STANDARD_REQUIRED_COLUMNS = ["date", "open", "high", "low", "close", "volume"]
TYPE_NONE = "none"
TYPE_CURRENT_INSIDE_PREVIOUS = "current_inside_previous"
TYPE_CURRENT_CONTAINS_PREVIOUS = "current_contains_previous"
DIRECTION_UPWARD = "upward"
DIRECTION_DOWNWARD = "downward"


@dataclass(frozen=True)
class StandardKLine:
    virtual_index: int
    source_start_index: Any
    source_end_index: Any
    source_indices: list[Any]
    source_positions: list[int]
    date_start: Any
    date_end: Any
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class InclusionResult:
    standard_bars: list[StandardKLine]
    inclusion_groups: pd.DataFrame


def detect_inclusion_marks(df: pd.DataFrame) -> pd.DataFrame:
    """Detect adjacent K-line inclusion relationships without changing K-line data."""
    _validate_required_columns(df)

    marks = pd.DataFrame(
        {
            "index": df.index,
            "x": df["x"].to_numpy(),
            "date": df["date"].to_numpy(),
            "has_inclusion": False,
            "inclusion_with_prev": False,
            "inclusion_type": TYPE_NONE,
            "reason": "",
        }
    )

    if len(df) < 2:
        return marks

    highs = pd.to_numeric(df["high"], errors="coerce").to_numpy()
    lows = pd.to_numeric(df["low"], errors="coerce").to_numpy()

    for position in range(1, len(df)):
        prev_high = highs[position - 1]
        prev_low = lows[position - 1]
        curr_high = highs[position]
        curr_low = lows[position]

        inclusion_type = _classify_pair(prev_high, prev_low, curr_high, curr_low)
        if inclusion_type == TYPE_NONE:
            continue

        prev_position = position - 1
        marks.loc[prev_position, "has_inclusion"] = True
        if marks.loc[prev_position, "inclusion_type"] == TYPE_NONE:
            marks.loc[prev_position, "inclusion_type"] = inclusion_type
        if not marks.loc[prev_position, "reason"]:
            marks.loc[prev_position, "reason"] = "与后一根K线存在包含关系"

        marks.loc[position, "has_inclusion"] = True
        marks.loc[position, "inclusion_with_prev"] = True
        marks.loc[position, "inclusion_type"] = inclusion_type
        marks.loc[position, "reason"] = "与前一根K线存在包含关系"

    return marks


def process_inclusions(df: pd.DataFrame) -> InclusionResult:
    """Build standard bars and their source groups without mutating original K-lines."""
    standard_bars = build_standard_bars(df)
    return InclusionResult(
        standard_bars=standard_bars,
        inclusion_groups=_make_inclusion_groups(standard_bars),
    )


def build_standard_bars(df: pd.DataFrame) -> list[StandardKLine]:
    """Create the internal de-included standard K-line sequence from left to right."""
    _validate_standard_required_columns(df)
    raw_bars = _make_raw_standard_bars(df)
    if len(raw_bars) <= 1:
        return _assign_virtual_indices(raw_bars)

    direction = _find_initial_direction(raw_bars)
    if direction is None:
        return []

    standard_bars: list[StandardKLine] = []
    current = raw_bars[0]
    for next_bar in raw_bars[1:]:
        if _has_inclusion(current, next_bar):
            current = _merge_standard_bars(current, next_bar, direction)
            continue

        standard_bars.append(current)
        new_direction = _detect_direction(current, next_bar)
        if new_direction is not None:
            direction = new_direction
        current = next_bar

    standard_bars.append(current)
    return _assign_virtual_indices(standard_bars)


def build_inclusion_groups(df: pd.DataFrame) -> pd.DataFrame:
    """Return source index groups for each standard K-line."""
    return process_inclusions(df).inclusion_groups


def _validate_required_columns(df: pd.DataFrame) -> None:
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"K线数据缺少包含关系检测所需字段：{missing}")


def _validate_standard_required_columns(df: pd.DataFrame) -> None:
    missing_columns = [column for column in STANDARD_REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"K线数据缺少标准K线生成所需字段：{missing}")


def _make_raw_standard_bars(df: pd.DataFrame) -> list[StandardKLine]:
    bars: list[StandardKLine] = []
    for position, (source_index, row) in enumerate(df.iterrows()):
        high = pd.to_numeric(row["high"], errors="coerce")
        low = pd.to_numeric(row["low"], errors="coerce")
        if pd.isna(high) or pd.isna(low):
            continue

        bars.append(
            StandardKLine(
                virtual_index=-1,
                source_start_index=source_index,
                source_end_index=source_index,
                source_indices=[source_index],
                source_positions=[int(position)],
                date_start=row["date"],
                date_end=row["date"],
                open=float(pd.to_numeric(row["open"], errors="coerce")),
                high=float(high),
                low=float(low),
                close=float(pd.to_numeric(row["close"], errors="coerce")),
                volume=_to_float(row["volume"], default=0.0),
            )
        )
    return bars


def _find_initial_direction(bars: list[StandardKLine]) -> str | None:
    for left, right in zip(bars, bars[1:]):
        if _has_inclusion(left, right):
            continue
        return _detect_direction(left, right)
    return None


def _detect_direction(prev: StandardKLine, curr: StandardKLine) -> str | None:
    if curr.high > prev.high and curr.low > prev.low:
        return DIRECTION_UPWARD
    if curr.high < prev.high and curr.low < prev.low:
        return DIRECTION_DOWNWARD
    return None


def _has_inclusion(left: StandardKLine, right: StandardKLine) -> bool:
    return _has_inclusion_values(left.high, left.low, right.high, right.low)


def _has_inclusion_values(
    left_high: float,
    left_low: float,
    right_high: float,
    right_low: float,
) -> bool:
    left_inside_right = left_high <= right_high and left_low >= right_low
    right_inside_left = right_high <= left_high and right_low >= left_low
    return left_inside_right or right_inside_left


def _merge_standard_bars(
    current: StandardKLine,
    next_bar: StandardKLine,
    direction: str,
) -> StandardKLine:
    if direction == DIRECTION_UPWARD:
        high = max(current.high, next_bar.high)
        low = max(current.low, next_bar.low)
    elif direction == DIRECTION_DOWNWARD:
        high = min(current.high, next_bar.high)
        low = min(current.low, next_bar.low)
    else:
        raise ValueError(f"包含关系处理方向不明确：{direction}")

    return StandardKLine(
        virtual_index=-1,
        source_start_index=current.source_start_index,
        source_end_index=next_bar.source_end_index,
        source_indices=[*current.source_indices, *next_bar.source_indices],
        source_positions=[*current.source_positions, *next_bar.source_positions],
        date_start=current.date_start,
        date_end=next_bar.date_end,
        open=current.open,
        high=float(high),
        low=float(low),
        close=next_bar.close,
        volume=current.volume + next_bar.volume,
    )


def _assign_virtual_indices(bars: list[StandardKLine]) -> list[StandardKLine]:
    return [replace(bar, virtual_index=index) for index, bar in enumerate(bars)]


def _make_inclusion_groups(standard_bars: list[StandardKLine]) -> pd.DataFrame:
    records = [
        {
            "virtual_index": bar.virtual_index,
            "source_start_index": bar.source_start_index,
            "source_end_index": bar.source_end_index,
            "source_indices": list(bar.source_indices),
            "date_start": bar.date_start,
            "date_end": bar.date_end,
            "source_count": len(bar.source_indices),
        }
        for bar in standard_bars
    ]
    return pd.DataFrame(
        records,
        columns=[
            "virtual_index",
            "source_start_index",
            "source_end_index",
            "source_indices",
            "date_start",
            "date_end",
            "source_count",
        ],
    )


def _to_float(value: Any, default: float) -> float:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return default
    return float(number)


def _classify_pair(
    prev_high: float,
    prev_low: float,
    curr_high: float,
    curr_low: float,
) -> str:
    if pd.isna(prev_high) or pd.isna(prev_low) or pd.isna(curr_high) or pd.isna(curr_low):
        return TYPE_NONE

    if not _has_inclusion_values(prev_high, prev_low, curr_high, curr_low):
        return TYPE_NONE

    if curr_high <= prev_high and curr_low >= prev_low:
        return TYPE_CURRENT_INSIDE_PREVIOUS

    if curr_high >= prev_high and curr_low <= prev_low:
        return TYPE_CURRENT_CONTAINS_PREVIOUS

    return TYPE_NONE
