from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.chan.inclusion import StandardKLine, build_standard_bars

FRACTAL_COLUMNS = [
    "index",
    "x",
    "date",
    "type",
    "price",
    "source_index",
    "source_date",
    "virtual_index",
    "center_index",
    "span_start",
    "span_end",
    "original_index",
    "high",
    "low",
    "source_start_index",
    "source_end_index",
    "source_indices",
]
REQUIRED_COLUMNS = ["date", "open", "high", "low", "close", "volume", "x"]
TYPE_TOP = "top"
TYPE_BOTTOM = "bottom"


@dataclass
class VirtualKLine:
    high: float
    low: float
    source_positions: list[int]


def detect_fractals(standard_bars: list[StandardKLine]) -> pd.DataFrame:
    """Detect candidate top/bottom fractals on the standard K-line sequence."""
    return detect_candidate_fractals(standard_bars)


def detect_candidate_fractals(standard_bars: list[StandardKLine]) -> pd.DataFrame:
    """Detect candidate fractals from standard_bars only."""
    _validate_standard_bars_input(standard_bars)

    if len(standard_bars) < 3:
        return pd.DataFrame(columns=FRACTAL_COLUMNS)

    records: list[dict] = []
    for position in range(1, len(standard_bars) - 1):
        left = standard_bars[position - 1]
        middle = standard_bars[position]
        right = standard_bars[position + 1]

        if _is_top_fractal(left, middle, right):
            records.append(_make_fractal_record(TYPE_TOP, middle, position, middle.high))
        elif _is_bottom_fractal(left, middle, right):
            records.append(_make_fractal_record(TYPE_BOTTOM, middle, position, middle.low))

    return pd.DataFrame(records, columns=FRACTAL_COLUMNS)


def detect_candidate_fractals_from_raw(df: pd.DataFrame) -> pd.DataFrame:
    """Compatibility wrapper: convert raw bars to standard_bars before fractal analysis."""
    _validate_required_columns(df)
    return detect_candidate_fractals(build_standard_bars(df))


def build_virtual_klines(df: pd.DataFrame) -> list[StandardKLine]:
    """Build the internal standard K-line sequence without modifying original K-lines."""
    _validate_required_columns(df)
    return build_standard_bars(df)


def _is_top_fractal(left: Any, middle: Any, right: Any) -> bool:
    return (
        middle.high > left.high
        and middle.high > right.high
        and middle.low > left.low
        and middle.low > right.low
    )


def _is_bottom_fractal(left: Any, middle: Any, right: Any) -> bool:
    return (
        middle.low < left.low
        and middle.low < right.low
        and middle.high < left.high
        and middle.high < right.high
    )


def _make_fractal_record(
    fractal_type: str,
    virtual_bar: StandardKLine,
    virtual_index: int,
    price: float,
) -> dict:
    source_index = _select_endpoint_source_index(virtual_bar, fractal_type)
    return {
        "index": source_index,
        "x": source_index,
        "date": virtual_bar.date_end,
        "type": fractal_type,
        "price": float(price),
        "source_index": source_index,
        "source_date": virtual_bar.date_end,
        "virtual_index": virtual_index,
        "center_index": virtual_index,
        "span_start": virtual_index - 1,
        "span_end": virtual_index + 1,
        "original_index": source_index,
        "high": float(virtual_bar.high),
        "low": float(virtual_bar.low),
        "source_start_index": virtual_bar.source_start_index,
        "source_end_index": virtual_bar.source_end_index,
        "source_indices": list(virtual_bar.source_indices),
    }


def _select_endpoint_source_index(virtual_bar: StandardKLine, fractal_type: str) -> Any:
    if fractal_type == TYPE_TOP:
        return virtual_bar.source_end_index
    if fractal_type == TYPE_BOTTOM:
        return virtual_bar.source_end_index
    return virtual_bar.source_end_index


def _validate_required_columns(df: pd.DataFrame) -> None:
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"K线数据缺少分型识别所需字段：{missing}")


def _validate_standard_bars_input(standard_bars: Any) -> None:
    if isinstance(standard_bars, pd.DataFrame):
        raise TypeError("detect_candidate_fractals() 必须接收 standard_bars，不能直接接收 raw_bars/raw_df")
    if not isinstance(standard_bars, list):
        raise TypeError("detect_candidate_fractals() 必须接收 standard_bars 列表")
    for bar in standard_bars:
        if not hasattr(bar, "virtual_index") or not hasattr(bar, "high") or not hasattr(bar, "low"):
            raise TypeError("standard_bars 中的元素必须包含 virtual_index/high/low")
