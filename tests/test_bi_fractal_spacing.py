from __future__ import annotations

import pandas as pd

from src.chan.bi import MIN_CENTER_GAP_FOR_BI, can_form_bi
from src.chan.fractal import TYPE_BOTTOM, TYPE_TOP


def _fractal(fractal_type: str, center_index: int, high: float, low: float) -> dict:
    date = pd.Timestamp("2024-01-01") + pd.Timedelta(days=center_index)
    return {
        "index": center_index,
        "x": center_index,
        "date": date,
        "type": fractal_type,
        "price": high if fractal_type == TYPE_TOP else low,
        "source_index": center_index,
        "source_date": date,
        "virtual_index": center_index,
        "center_index": center_index,
        "span_start": center_index - 1,
        "span_end": center_index + 1,
        "original_index": center_index,
        "high": high,
        "low": low,
        "source_start_index": center_index,
        "source_end_index": center_index,
        "source_indices": [center_index],
    }


def test_effective_bi_endpoints_cannot_share_standard_bar() -> None:
    start = _fractal(TYPE_TOP, 4, 20, 15)
    end = _fractal(TYPE_BOTTOM, 6, 12, 8)

    assert not can_form_bi(start, end)


def test_effective_bi_endpoints_need_one_neutral_standard_bar_between_spans() -> None:
    start = _fractal(TYPE_TOP, 4, 20, 15)
    end = _fractal(TYPE_BOTTOM, 7, 12, 8)

    assert not can_form_bi(start, end)


def test_center_gap_below_four_cannot_form_bi() -> None:
    start = _fractal(TYPE_TOP, 4, 20, 15)
    end = _fractal(TYPE_BOTTOM, 7, 12, 8)
    center_gap = abs(end["center_index"] - start["center_index"])

    assert center_gap < MIN_CENTER_GAP_FOR_BI
    assert not can_form_bi(start, end)


def test_center_gap_at_least_four_can_enter_later_bi_checks() -> None:
    start = _fractal(TYPE_TOP, 4, 20, 15)
    end = _fractal(TYPE_BOTTOM, 8, 12, 8)
    center_gap = abs(end["center_index"] - start["center_index"])

    assert center_gap >= MIN_CENTER_GAP_FOR_BI
    assert can_form_bi(start, end)
