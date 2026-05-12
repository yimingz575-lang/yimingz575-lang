from __future__ import annotations

import pandas as pd

from src.chan.bi import (
    DIRECTION_DOWN,
    DIRECTION_UP,
    can_form_bi,
    confirm_effective_fractals,
    generate_bis,
)


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


def _fractal(
    fractal_type: str,
    virtual_index: int,
    original_index: int,
    price: float,
    high: float,
    low: float,
):
    return {
        "type": fractal_type,
        "virtual_index": virtual_index,
        "original_index": original_index,
        "price": price,
        "high": high,
        "low": low,
        "source_indices": [original_index],
    }


def test_shared_kline_cannot_form_bi() -> None:
    start = _fractal("top", 3, 3, 20, 20, 15)
    end = _fractal("bottom", 3, 3, 8, 10, 8)

    assert not can_form_bi(start, end)


def test_less_than_five_virtual_klines_cannot_form_bi() -> None:
    start = _fractal("top", 1, 1, 20, 20, 15)
    end = _fractal("bottom", 4, 4, 8, 10, 8)

    assert not can_form_bi(start, end)


def test_five_klines_and_correct_down_direction_can_form_bi() -> None:
    df = _make_df(
        highs=[10, 13, 11, 10, 9, 7, 8],
        lows=[5, 8, 6, 5, 4, 1, 3],
    )

    bis = generate_bis(df)

    assert len(bis) == 1
    assert bis.loc[0, "direction"] == DIRECTION_DOWN
    assert bis.loc[0, "start_type"] == "top"
    assert bis.loc[0, "end_type"] == "bottom"
    assert bis.loc[0, "kline_count"] >= 5


def test_overlapping_endpoint_ranges_can_still_form_bi_when_prices_are_valid() -> None:
    df = _make_df(
        highs=[10, 13, 11, 12, 11, 10, 12],
        lows=[5, 8, 6, 7, 4, 1, 3],
    )

    bis = generate_bis(df)

    assert len(bis) == 1
    assert bis.loc[0, "direction"] == DIRECTION_DOWN
    assert bis.loc[0, "start_price"] == 13
    assert bis.loc[0, "end_price"] == 1
    assert bis.loc[0, "kline_count"] >= 5


def test_bis_are_always_built_from_alternating_fractals() -> None:
    df = _make_df(
        highs=[10, 13, 11, 15, 12, 11, 10, 7, 8, 11, 10, 12, 11],
        lows=[5, 8, 6, 9, 7, 6, 4, 1, 3, 6, 5, 8, 6],
    )

    bis = generate_bis(df)

    assert not bis.empty
    assert (bis["start_type"] != bis["end_type"]).all()


def test_multiple_top_candidates_keep_the_higher_top() -> None:
    df = _make_df(
        highs=[10, 13, 11, 15, 12, 11, 10, 7, 8],
        lows=[5, 8, 6, 9, 7, 6, 4, 1, 3],
    )

    bis = generate_bis(df)

    assert len(bis) == 1
    assert bis.loc[0, "start_original_index"] == 3
    assert bis.loc[0, "start_price"] == 15


def test_multiple_bottom_candidates_keep_the_lower_bottom() -> None:
    df = _make_df(
        highs=[13, 10, 11, 9, 11, 12, 13, 15, 13],
        lows=[6, 3, 5, 2, 4, 5, 6, 11, 7],
    )

    bis = generate_bis(df)

    assert len(bis) == 1
    assert bis.loc[0, "start_original_index"] == 3
    assert bis.loc[0, "start_price"] == 2


def test_down_bi_must_be_top_to_bottom() -> None:
    df = _make_df(
        highs=[10, 13, 11, 10, 9, 7, 8],
        lows=[5, 8, 6, 5, 4, 1, 3],
    )

    bi = generate_bis(df).iloc[0]

    assert bi["direction"] == DIRECTION_DOWN
    assert bi["start_type"] == "top"
    assert bi["end_type"] == "bottom"


def test_up_bi_must_be_bottom_to_top() -> None:
    df = _make_df(
        highs=[13, 10, 11, 12, 13, 15, 13],
        lows=[6, 3, 5, 6, 7, 11, 7],
    )

    bi = generate_bis(df).iloc[0]

    assert bi["direction"] == DIRECTION_UP
    assert bi["start_type"] == "bottom"
    assert bi["end_type"] == "top"


def test_original_kline_count_is_not_modified() -> None:
    df = _make_df(
        highs=[10, 13, 11, 10, 9, 7, 8],
        lows=[5, 8, 6, 5, 4, 1, 3],
    )
    original_count = len(df)

    _ = generate_bis(df)

    assert len(df) == original_count


def test_original_ohlc_is_not_modified() -> None:
    df = _make_df(
        highs=[10, 13, 11, 10, 9, 7, 8],
        lows=[5, 8, 6, 5, 4, 1, 3],
    )
    original_ohlc = df[["open", "high", "low", "close"]].copy(deep=True)

    _ = generate_bis(df)

    pd.testing.assert_frame_equal(df[["open", "high", "low", "close"]], original_ohlc)


def test_generated_bi_contains_chart_coordinates_and_prices() -> None:
    df = _make_df(
        highs=[10, 13, 11, 10, 9, 7, 8],
        lows=[5, 8, 6, 5, 4, 1, 3],
    )

    bis = generate_bis(df)

    for column in ["start_x", "end_x", "start_price", "end_price"]:
        assert column in bis.columns
        assert pd.notna(bis.loc[0, column])


def test_candidate_without_valid_reverse_bi_is_not_confirmed() -> None:
    df = _make_df(
        highs=[10, 13, 11, 12, 13],
        lows=[5, 8, 6, 7, 8],
    )

    effective_fractals = confirm_effective_fractals(df)
    bis = generate_bis(df)

    assert effective_fractals.empty
    assert bis.empty
