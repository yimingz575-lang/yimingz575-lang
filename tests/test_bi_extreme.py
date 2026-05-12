from __future__ import annotations

import pandas as pd

from src.chan.bi import DIRECTION_DOWN, generate_bis, validate_bi_extreme
from src.chan.fractal import TYPE_BOTTOM, TYPE_TOP, VirtualKLine, build_virtual_klines


def _make_df(highs: list[float], lows: list[float]) -> pd.DataFrame:
    rows = len(highs)
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=rows, freq="D"),
            "open": [low + 1 for low in lows],
            "high": highs,
            "low": lows,
            "close": [high - 1 for high in highs],
            "volume": [1000 + index for index in range(rows)],
            "x": list(range(rows)),
        }
    )


def _fractal(fractal_type: str, virtual_index: int, high: float, low: float) -> dict:
    return {
        "type": fractal_type,
        "virtual_index": virtual_index,
        "original_index": virtual_index,
        "price": high if fractal_type == TYPE_TOP else low,
        "high": high,
        "low": low,
        "source_indices": [virtual_index],
    }


def test_down_bi_replaces_top_with_higher_top_before_valid_bottom() -> None:
    df = _make_df(
        highs=[10, 13, 11, 15, 12, 11, 10, 7, 8],
        lows=[5, 8, 6, 9, 7, 6, 4, 1, 3],
    )

    bis = generate_bis(df)

    assert len(bis) == 1
    assert bis.loc[0, "direction"] == DIRECTION_DOWN
    assert bis.loc[0, "start_virtual_index"] == 3
    assert bis.loc[0, "start_price"] == 15


def test_up_bi_replaces_bottom_with_lower_bottom_before_valid_top() -> None:
    df = _make_df(
        highs=[13, 10, 11, 9, 11, 12, 13, 15, 13],
        lows=[6, 3, 5, 2, 4, 5, 6, 11, 7],
    )

    bis = generate_bis(df)

    assert len(bis) == 1
    assert bis.loc[0, "direction"] == "up"
    assert bis.loc[0, "start_virtual_index"] == 3
    assert bis.loc[0, "start_price"] == 2


def test_down_bi_extreme_validation_rejects_start_top_below_interval_high() -> None:
    bars = [
        VirtualKLine(high=10, low=5, source_positions=[0]),
        VirtualKLine(high=13, low=8, source_positions=[1]),
        VirtualKLine(high=11, low=6, source_positions=[2]),
        VirtualKLine(high=15, low=4, source_positions=[3]),
        VirtualKLine(high=9, low=3, source_positions=[4]),
        VirtualKLine(high=7, low=1, source_positions=[5]),
    ]
    start = _fractal(TYPE_TOP, virtual_index=1, high=13, low=8)
    end = _fractal(TYPE_BOTTOM, virtual_index=5, high=7, low=1)

    assert not validate_bi_extreme(bars, start, end)


def test_up_bi_extreme_validation_rejects_start_bottom_above_interval_low() -> None:
    bars = [
        VirtualKLine(high=13, low=6, source_positions=[0]),
        VirtualKLine(high=10, low=3, source_positions=[1]),
        VirtualKLine(high=17, low=5, source_positions=[2]),
        VirtualKLine(high=16, low=1, source_positions=[3]),
        VirtualKLine(high=13, low=4, source_positions=[4]),
        VirtualKLine(high=15, low=8, source_positions=[5]),
    ]
    start = _fractal(TYPE_BOTTOM, virtual_index=1, high=10, low=3)
    end = _fractal(TYPE_TOP, virtual_index=5, high=15, low=8)

    assert not validate_bi_extreme(bars, start, end)


def test_generated_bis_always_use_interval_extremes_on_virtual_klines() -> None:
    df = _make_df(
        highs=[10, 13, 11, 15, 9, 7, 8, 12, 10, 14, 11, 10, 9, 7, 8],
        lows=[5, 8, 6, 4, 3, 1, 2, 6, 5, 9, 6, 5, 3, 0, 2],
    )
    bars = build_virtual_klines(df)

    bis = generate_bis(df)

    assert not bis.empty
    for _, bi in bis.iterrows():
        interval = bars[int(bi["start_virtual_index"]) : int(bi["end_virtual_index"]) + 1]
        interval_high = max(bar.high for bar in interval)
        interval_low = min(bar.low for bar in interval)
        if bi["direction"] == "up":
            assert bi["start_price"] == interval_low
            assert bi["end_price"] == interval_high
        else:
            assert bi["start_price"] == interval_high
            assert bi["end_price"] == interval_low
