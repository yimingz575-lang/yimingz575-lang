from __future__ import annotations

import pandas as pd

from src.chan.bi import build_bis_incremental, validate_bi_extreme, validate_bi_sequence_continuity
from src.chan.fractal import TYPE_BOTTOM, TYPE_TOP
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


def _bars(highs: list[float], lows: list[float]) -> list[StandardKLine]:
    return [_bar(index, high, low) for index, (high, low) in enumerate(zip(highs, lows))]


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


def _fractals(records: list[tuple[str, int, float, float]]) -> pd.DataFrame:
    return pd.DataFrame([_fractal(*record) for record in records])


def test_top_to_bottom_can_form_down_bi_with_extreme_and_spacing_checks() -> None:
    bars = _bars(
        highs=[10, 14, 20, 16, 13, 11, 9],
        lows=[5, 9, 15, 11, 8, 4, 6],
    )
    fractals = _fractals([
        (TYPE_TOP, 2, 20, 15),
        (TYPE_BOTTOM, 6, 9, 4),
    ])

    _, bis = build_bis_incremental(bars, fractals)

    assert len(bis) == 1
    assert bis.loc[0, "direction"] == "down"
    assert bis.loc[0, "start_center_index"] == 2
    assert bis.loc[0, "end_center_index"] == 6


def test_bottom_to_top_can_form_up_bi_with_extreme_and_spacing_checks() -> None:
    bars = _bars(
        highs=[20, 16, 9, 11, 14, 18, 23],
        lows=[15, 10, 4, 7, 9, 13, 17],
    )
    fractals = _fractals([
        (TYPE_BOTTOM, 2, 9, 4),
        (TYPE_TOP, 6, 23, 17),
    ])

    _, bis = build_bis_incremental(bars, fractals)

    assert len(bis) == 1
    assert bis.loc[0, "direction"] == "up"
    assert bis.loc[0, "start_center_index"] == 2
    assert bis.loc[0, "end_center_index"] == 6


def test_same_type_fractals_do_not_form_bi() -> None:
    bars = _bars([10, 12, 20, 18, 15, 21, 17], [5, 7, 15, 12, 9, 16, 11])
    top_top = _fractals([
        (TYPE_TOP, 2, 20, 15),
        (TYPE_TOP, 6, 21, 16),
    ])
    bottom_bottom = _fractals([
        (TYPE_BOTTOM, 2, 10, 4),
        (TYPE_BOTTOM, 6, 9, 3),
    ])

    _, top_bis = build_bis_incremental(bars, top_top)
    _, bottom_bis = build_bis_incremental(bars, bottom_bottom)

    assert top_bis.empty
    assert bottom_bis.empty


def test_confirmed_bis_are_alternating_and_continuous() -> None:
    bars = _bars(
        highs=[10, 14, 20, 16, 13, 11, 9, 12, 15, 19, 24],
        lows=[5, 9, 15, 11, 8, 4, 3, 6, 9, 13, 18],
    )
    fractals = _fractals([
        (TYPE_TOP, 2, 20, 15),
        (TYPE_BOTTOM, 6, 9, 3),
        (TYPE_TOP, 10, 24, 18),
    ])

    _, bis = build_bis_incremental(bars, fractals)

    assert bis["direction"].tolist() == ["down", "up"]
    assert validate_bi_sequence_continuity(bis)
    assert bis.loc[0, "end_center_index"] == bis.loc[1, "start_center_index"]


def test_bi_extreme_checks_use_only_current_candidate_interval() -> None:
    down_bars = _bars(
        highs=[10, 14, 20, 16, 18, 11, 9],
        lows=[5, 9, 15, 11, 8, 6, 4],
    )
    down_start = _fractal(TYPE_TOP, 2, 20, 15)
    down_end = _fractal(TYPE_BOTTOM, 6, 9, 4)
    bad_down_start = _fractal(TYPE_TOP, 3, 16, 11)

    up_bars = _bars(
        highs=[20, 16, 9, 11, 14, 18, 23],
        lows=[15, 10, 4, 7, 9, 8, 17],
    )
    up_start = _fractal(TYPE_BOTTOM, 2, 9, 4)
    up_end = _fractal(TYPE_TOP, 6, 23, 17)
    bad_up_start = _fractal(TYPE_BOTTOM, 4, 14, 9)

    assert validate_bi_extreme(down_bars, down_start, down_end)
    assert not validate_bi_extreme(down_bars, bad_down_start, down_end)
    assert validate_bi_extreme(up_bars, up_start, up_end)
    assert not validate_bi_extreme(up_bars, bad_up_start, up_end)
