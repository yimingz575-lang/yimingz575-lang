from __future__ import annotations

import pandas as pd

from src.ui import chart
from src.ui.app import ANALYSIS_VERSION, _make_cache_key


def _make_raw_df(raw_start: int, rows: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=rows, freq="D"),
            "open": [10 + index for index in range(rows)],
            "high": [11 + index for index in range(rows)],
            "low": [9 + index for index in range(rows)],
            "close": [10.5 + index for index in range(rows)],
            "volume": [1000 + index for index in range(rows)],
            "raw_index": list(range(raw_start, raw_start + rows)),
            "x": list(range(raw_start, raw_start + rows)),
        }
    )


def _bi_record(start_virtual: int, end_virtual: int) -> dict:
    return {
        "direction": "down",
        "start_type": "top",
        "end_type": "bottom",
        "start_virtual_index": start_virtual,
        "end_virtual_index": end_virtual,
        "start_center_index": start_virtual,
        "end_center_index": end_virtual,
        "start_date": pd.Timestamp("2024-01-03"),
        "end_date": pd.Timestamp("2024-01-07"),
        "start_price": 20.0,
        "end_price": 12.0,
        "kline_count": 5,
    }


def _fake_marks(
    virtual_to_raw: dict[int, int],
    bi_pairs: list[tuple[int, int]],
) -> dict[str, object]:
    inclusion_groups = pd.DataFrame(
        [
            {
                "virtual_index": virtual_index,
                "source_start_index": raw_index,
                "source_end_index": raw_index,
                "source_indices": [raw_index],
            }
            for virtual_index, raw_index in virtual_to_raw.items()
        ]
    )
    confirmed_bis = pd.DataFrame([_bi_record(start, end) for start, end in bi_pairs])
    return {
        "raw_bars": pd.DataFrame(),
        "standard_bars": [object()] * (max(virtual_to_raw) + 1),
        "inclusion_groups": inclusion_groups,
        "candidate_fractals": pd.DataFrame(),
        "confirmed_bis": confirmed_bis,
        "bis": pd.DataFrame(),
        "fractals": pd.DataFrame(),
    }


def _bi_traces(fig):
    return [
        trace
        for trace in fig.data
        if getattr(trace, "line", None) is not None and getattr(trace.line, "color", None) == "#00a8ff"
    ]


def test_candles_and_bis_use_same_chart_x_range(monkeypatch) -> None:
    monkeypatch.setattr(
        chart,
        "analyze_chan_marks",
        lambda _: _fake_marks({20: 102, 30: 106}, [(20, 30)]),
    )

    fig = chart.create_kline_figure(
        _make_raw_df(raw_start=100, rows=10),
        display_options=["bi"],
        visible_count=None,
    )

    candle_x = list(fig.data[0].x)
    bi_x = list(_bi_traces(fig)[0].x)
    assert candle_x == list(range(10))
    assert bi_x == [2, 6]
    assert all(0 <= x <= len(candle_x) - 1 for x in bi_x)


def test_confirmed_bi_virtual_index_is_not_used_directly_as_chart_x(monkeypatch) -> None:
    monkeypatch.setattr(
        chart,
        "analyze_chan_marks",
        lambda _: _fake_marks({20: 102, 30: 106}, [(20, 30)]),
    )

    fig = chart.create_kline_figure(
        _make_raw_df(raw_start=100, rows=10),
        display_options=["bi"],
        visible_count=None,
    )

    assert list(_bi_traces(fig)[0].x) == [2, 6]
    assert list(_bi_traces(fig)[0].x) != [20, 30]


def test_inclusion_groups_map_virtual_index_to_source_end_index() -> None:
    inclusion_groups = pd.DataFrame(
        [
            {"virtual_index": 20, "source_start_index": 101, "source_end_index": 102},
            {"virtual_index": 30, "source_start_index": 105, "source_end_index": 106},
        ]
    )

    assert chart._build_virtual_to_raw_index_lookup(inclusion_groups) == {20: 102, 30: 106}
    assert chart._build_virtual_to_source_range_lookup(inclusion_groups) == {
        20: {"source_start_index": 101, "source_end_index": 102},
        30: {"source_start_index": 105, "source_end_index": 106},
    }


def test_only_bis_with_both_endpoints_in_displayed_window_are_drawn(monkeypatch) -> None:
    monkeypatch.setattr(
        chart,
        "analyze_chan_marks",
        lambda _: _fake_marks(
            {20: 106, 30: 108, 40: 101, 50: 103},
            [(20, 30), (40, 50)],
        ),
    )

    fig = chart.create_kline_figure(
        _make_raw_df(raw_start=105, rows=5),
        display_options=["bi"],
        visible_count=None,
    )

    traces = _bi_traces(fig)
    assert len(traces) == 1
    assert list(traces[0].x) == [1, 3]


def test_demo_and_real_chart_use_analyze_chan_marks_confirmed_bis(monkeypatch) -> None:
    calls: list[int] = []

    def fake_analyze(df: pd.DataFrame) -> dict[str, object]:
        calls.append(len(df))
        return _fake_marks({20: 102, 30: 106}, [(20, 30)])

    monkeypatch.setattr(chart, "analyze_chan_marks", fake_analyze)

    for symbol in ["DEMO", "600497"]:
        fig = chart.create_kline_figure(
            _make_raw_df(raw_start=100, rows=10),
            stock_code=symbol,
            display_options=["bi"],
            visible_count=None,
        )
        assert len(_bi_traces(fig)) == 1

    assert len(calls) == 2
    assert _make_cache_key("600497", "daily").startswith(ANALYSIS_VERSION)


def test_yaxis_range_uses_display_chart_x_not_cached_raw_index() -> None:
    df = _make_raw_df(raw_start=1000, rows=10)

    price_range, macd_range = chart.calculate_visible_yaxis_ranges(df, [0, 4])

    assert price_range is not None
    assert macd_range is not None
    assert price_range[0] < 9
    assert price_range[1] > 15
    assert price_range[1] < 20


def test_default_xaxis_range_stays_on_latest_kline_window(monkeypatch) -> None:
    monkeypatch.setattr(
        chart,
        "analyze_chan_marks",
        lambda _: _fake_marks({1: 1001, 7: 1007}, [(1, 7)]),
    )

    fig = chart.create_kline_figure(
        _make_raw_df(raw_start=1000, rows=1000),
        display_options=["bi"],
        visible_count=300,
    )

    assert list(fig.layout.xaxis.range) == [700, 999]
