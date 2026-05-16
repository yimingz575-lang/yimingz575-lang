from __future__ import annotations

import pandas as pd
import pytest

from src.ui import chart
from src.ui.app import ANALYSIS_VERSION, DEFAULT_DISPLAY_OPTIONS, DISPLAY_OPTIONS, _index_string, _make_cache_key


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


def _bi_record(
    start_virtual: int,
    end_virtual: int,
    direction: str | None = "down",
    start_price: float = 20.0,
    end_price: float = 12.0,
) -> dict:
    record = {
        "start_type": "top",
        "end_type": "bottom",
        "start_virtual_index": start_virtual,
        "end_virtual_index": end_virtual,
        "start_center_index": start_virtual,
        "end_center_index": end_virtual,
        "start_date": pd.Timestamp("2024-01-03"),
        "end_date": pd.Timestamp("2024-01-07"),
        "start_price": start_price,
        "end_price": end_price,
        "kline_count": 5,
    }
    if direction is not None:
        record["direction"] = direction
    return record


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
        if getattr(trace, "mode", None) == "lines+markers"
    ]


def test_toolbar_removes_inclusion_and_fractal_options_and_adds_ma() -> None:
    option_values = {option["value"] for option in DISPLAY_OPTIONS}

    assert "inclusion" not in option_values
    assert "fractal" not in option_values
    assert "ma" in option_values
    assert "ma" not in DEFAULT_DISPLAY_OPTIONS


def test_candlestick_uses_white_for_all_ohlc_colors() -> None:
    fig = chart.create_kline_figure(
        _make_raw_df(raw_start=0, rows=10),
        display_options=[],
        visible_count=None,
    )

    candle = fig.data[0]
    assert candle.increasing.line.color == "white"
    assert candle.increasing.fillcolor == "white"
    assert candle.decreasing.line.color == "white"
    assert candle.decreasing.fillcolor == "white"


def test_chart_spike_lines_are_enabled_for_cursor_tracking() -> None:
    fig = chart.create_kline_figure(
        _make_raw_df(raw_start=0, rows=10),
        display_options=[],
        visible_count=None,
    )

    assert fig.layout.hovermode == "closest"
    assert fig.layout.spikedistance == -1
    assert fig.layout.xaxis.showspikes is True
    assert fig.layout.xaxis.spikemode == "across"
    assert fig.layout.xaxis.spikesnap == "cursor"
    assert fig.layout.yaxis.showspikes is True
    assert fig.layout.yaxis.spikemode == "across"
    assert fig.layout.yaxis.spikesnap == "cursor"
    assert fig.layout.xaxis2.showspikes is True
    assert fig.layout.yaxis2.showspikes is True


def test_candlestick_hover_keeps_ohlc_and_volume_data() -> None:
    fig = chart.create_kline_figure(
        _make_raw_df(raw_start=0, rows=3),
        display_options=[],
        visible_count=None,
    )

    candle = fig.data[0]
    assert candle.customdata[0][0] == "2024-01-01 00:00"
    assert candle.customdata[0][1] == 1000
    assert "%{open:.2f}" in candle.hovertemplate
    assert "%{high:.2f}" in candle.hovertemplate
    assert "%{low:.2f}" in candle.hovertemplate
    assert "%{close:.2f}" in candle.hovertemplate
    assert "%{customdata[1]}" in candle.hovertemplate


def test_dash_index_injects_mouse_info_panel_script() -> None:
    index_html = _index_string()

    assert "mouse-info-panel" in index_html
    assert "mousemove" in index_html
    assert "plotly_hover" in index_html
    assert "Plotly 原生 hover 只会在数据 trace 上触发" in index_html


def test_moving_averages_are_hidden_by_default_and_shown_by_option() -> None:
    default_fig = chart.create_kline_figure(
        _make_raw_df(raw_start=0, rows=30),
        display_options=[],
        visible_count=None,
    )
    default_trace_names = {trace.name for trace in default_fig.data}

    ma_fig = chart.create_kline_figure(
        _make_raw_df(raw_start=0, rows=30),
        display_options=["ma"],
        visible_count=None,
    )
    ma_traces = [trace for trace in ma_fig.data if trace.name in {"MA5", "MA10", "MA20", "MA60"}]

    assert {"MA5", "MA10", "MA20"}.isdisjoint(default_trace_names)
    assert [trace.name for trace in ma_traces] == ["MA5", "MA10", "MA20"]
    assert "MA60" not in {trace.name for trace in ma_fig.data}
    assert pd.isna(ma_traces[0].y[0])
    assert ma_traces[0].y[4] == 12.5


def test_bi_line_color_uses_direction_and_price_fallback(monkeypatch) -> None:
    def fake_marks(_: pd.DataFrame) -> dict[str, object]:
        marks = _fake_marks(
            {20: 101, 30: 102, 40: 103, 50: 104, 60: 105, 70: 106},
            [],
        )
        marks["confirmed_bis"] = pd.DataFrame(
            [
                _bi_record(20, 30, direction="up", start_price=10.0, end_price=15.0),
                _bi_record(40, 50, direction="down", start_price=18.0, end_price=12.0),
                _bi_record(60, 70, direction=None, start_price=8.0, end_price=16.0),
            ]
        )
        return marks

    monkeypatch.setattr(chart, "analyze_chan_marks", fake_marks)

    fig = chart.create_kline_figure(
        _make_raw_df(raw_start=100, rows=10),
        display_options=["bi"],
        visible_count=None,
    )

    assert [trace.line.color for trace in _bi_traces(fig)] == [
        chart.BI_UP_COLOR,
        chart.BI_DOWN_COLOR,
        chart.BI_UP_COLOR,
    ]


def test_temporary_fallback_bi_is_drawn_yellow_without_hover(monkeypatch) -> None:
    def fake_marks(_: pd.DataFrame) -> dict[str, object]:
        marks = _fake_marks(
            {20: 101, 30: 102},
            [],
        )
        fallback = _bi_record(20, 30, direction="up", start_price=10.0, end_price=18.0)
        fallback.update(
            {
                "is_temporary": True,
                "is_fallback_bi": True,
                "color": "yellow",
                "fallback_reason": "affected_confirmed_bi_count >= 3",
            }
        )
        marks["confirmed_bis"] = pd.DataFrame([fallback])
        return marks

    monkeypatch.setattr(chart, "analyze_chan_marks", fake_marks)

    fig = chart.create_kline_figure(
        _make_raw_df(raw_start=100, rows=10),
        display_options=["bi"],
        visible_count=None,
    )

    trace = _bi_traces(fig)[0]
    assert trace.line.color == "yellow"
    assert trace.marker.color == "yellow"
    assert trace.customdata[0][6] == "临时笔 / fallback bi"
    assert trace.customdata[0][7] == "affected_confirmed_bi_count >= 3"
    assert trace.hoverinfo == "skip"
    assert trace.hovertemplate is None


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


def test_zone_option_draws_bi_zhongshu_from_confirmed_bis(monkeypatch) -> None:
    def fake_marks(_: pd.DataFrame) -> dict[str, object]:
        marks = _fake_marks(
            {20: 101, 30: 102, 40: 103, 50: 104},
            [],
        )
        marks["confirmed_bis"] = pd.DataFrame(
            [
                _bi_record(20, 30, direction="up", start_price=10.0, end_price=20.0),
                _bi_record(30, 40, direction="down", start_price=18.0, end_price=12.0),
                _bi_record(40, 50, direction="up", start_price=14.0, end_price=22.0),
            ]
        )
        return marks

    monkeypatch.setattr(chart, "analyze_chan_marks", fake_marks)

    fig = chart.create_kline_figure(
        _make_raw_df(raw_start=100, rows=10),
        display_options=["zone"],
        visible_count=None,
    )

    zhongshu_traces = [trace for trace in fig.data if trace.name == "笔中枢"]
    assert len(zhongshu_traces) == 1
    trace = zhongshu_traces[0]
    assert list(trace.x) == [1.0, 4.0, 4.0, 1.0, 1.0]
    assert list(trace.y) == [14.0, 14.0, 18.0, 18.0, 14.0]
    assert trace.line.dash == "dot"
    assert trace.hoverinfo == "skip"
    assert trace.hovertemplate is None
    assert not _bi_traces(fig)


def test_bi_zhongshu_rectangles_do_not_include_breakout_bi(monkeypatch) -> None:
    def fake_marks(_: pd.DataFrame) -> dict[str, object]:
        marks = _fake_marks(
            {virtual_index: 100 + virtual_index for virtual_index in range(1, 9)},
            [],
        )
        marks["confirmed_bis"] = pd.DataFrame(
            [
                _bi_record(1, 2, direction="up", start_price=10.0, end_price=20.0),
                _bi_record(2, 3, direction="down", start_price=18.0, end_price=12.0),
                _bi_record(3, 4, direction="up", start_price=14.0, end_price=22.0),
                _bi_record(4, 5, direction="up", start_price=16.0, end_price=24.0),
                _bi_record(5, 6, direction="down", start_price=23.0, end_price=19.0),
                _bi_record(6, 7, direction="up", start_price=20.0, end_price=26.0),
                _bi_record(7, 8, direction="down", start_price=25.0, end_price=21.0),
            ]
        )
        return marks

    monkeypatch.setattr(chart, "analyze_chan_marks", fake_marks)

    fig = chart.create_kline_figure(
        _make_raw_df(raw_start=100, rows=12),
        display_options=["zone"],
        visible_count=None,
    )

    zhongshu_traces = [trace for trace in fig.data if trace.name == "笔中枢"]
    assert len(zhongshu_traces) == 2
    assert list(zhongshu_traces[0].x) == [1.0, 4.0, 4.0, 1.0, 1.0]
    assert list(zhongshu_traces[1].x) == [5.0, 8.0, 8.0, 5.0, 5.0]


def test_bi_zhongshu_rectangle_uses_participating_bi_endpoints() -> None:
    mapped_bis = pd.DataFrame(
        [
            {"start_x": 10, "end_x": 20},
            {"start_x": 20, "end_x": 30},
            {"start_x": 30, "end_x": 40},
        ]
    )
    bi_zhongshu = pd.DataFrame(
        [
            {
                "start_bi_index": 0,
                "end_bi_index": 2,
                "start_x": -100,
                "end_x": 999,
                "zd": 14.0,
                "zg": 18.0,
            }
        ]
    )
    fig = chart.make_subplots(rows=1, cols=1)

    trace_count = chart._add_bi_zhongshu_traces(fig, mapped_bis, bi_zhongshu)

    zhongshu_traces = [trace for trace in fig.data if trace.name == "笔中枢"]
    assert trace_count == 1
    assert len(zhongshu_traces) == 1
    assert list(zhongshu_traces[0].x) == [10.0, 40.0, 40.0, 10.0, 10.0]


def test_bi_zhongshu_rectangles_use_explicit_bi_indices_and_skip_connector(capsys) -> None:
    mapped_bis = pd.DataFrame(
        [{"start_x": index, "end_x": index + 1} for index in range(7)]
    )
    bi_zhongshu = pd.DataFrame(
        [
            {
                "center_id": 0,
                "bi_indices": [0, 1, 2],
                "start_bi_index": 0,
                "end_bi_index": 3,
                "zd": 14.0,
                "zg": 18.0,
            },
            {
                "center_id": 1,
                "bi_indices": [4, 5, 6],
                "start_bi_index": 3,
                "end_bi_index": 6,
                "zd": 21.0,
                "zg": 23.0,
            },
        ]
    )
    fig = chart.make_subplots(rows=1, cols=1)

    trace_count = chart._add_bi_zhongshu_traces(fig, mapped_bis, bi_zhongshu)

    assert trace_count == 2
    assert list(fig.data[0].x) == [0.0, 3.0, 3.0, 0.0, 0.0]
    assert list(fig.data[1].x) == [4.0, 7.0, 7.0, 4.0, 4.0]
    output = capsys.readouterr().out
    assert "zs_id=0" in output
    assert "bi_indices=[0, 1, 2]" in output
    assert "expected_x0=0.0" in output
    assert "actual_x1=3.0" in output
    assert "zs_id=1" in output
    assert "bi_indices=[4, 5, 6]" in output
    assert "WARNING" not in output


def test_bi_zhongshu_rectangles_filter_second_center_without_connector(capsys) -> None:
    mapped_bis = pd.DataFrame(
        [{"start_x": index, "end_x": index + 1} for index in range(6)]
    )
    bi_zhongshu = pd.DataFrame(
        [
            {
                "center_id": 0,
                "bi_indices": [0, 1, 2],
                "start_bi_index": 0,
                "end_bi_index": 2,
                "zd": 14.0,
                "zg": 18.0,
            },
            {
                "center_id": 1,
                "bi_indices": [3, 4, 5],
                "start_bi_index": 3,
                "end_bi_index": 5,
                "zd": 21.0,
                "zg": 23.0,
            },
        ]
    )
    fig = chart.make_subplots(rows=1, cols=1)

    trace_count = chart._add_bi_zhongshu_traces(fig, mapped_bis, bi_zhongshu)

    assert trace_count == 1
    assert list(fig.data[0].x) == [0.0, 3.0, 3.0, 0.0, 0.0]
    output = capsys.readouterr().out
    assert "两个中枢之间缺少至少一根连接笔，后一个中枢已从绘图结果中过滤" in output


def test_filter_independent_zhongshu_filters_overlapping_second_center(capsys) -> None:
    zs_list = [
        {"center_id": 0, "bi_indices": [0, 1, 2], "zd": 14.0, "zg": 18.0},
        {"center_id": 1, "bi_indices": [2, 3, 4], "zd": 16.0, "zg": 20.0},
    ]

    valid_zhongshu_list = chart.filter_independent_zhongshu_with_connector(zs_list)

    assert len(valid_zhongshu_list) == 1
    assert valid_zhongshu_list[0]["center_id"] == 0
    output = capsys.readouterr().out
    assert "两个中枢之间缺少至少一根连接笔，后一个中枢已从绘图结果中过滤" in output


def test_bi_zhongshu_rect_boundary_assertion_prints_warning(capsys) -> None:
    with pytest.raises(AssertionError):
        chart._assert_bi_zhongshu_rect_boundary(
            expected_x0=0.0,
            expected_x1=3.0,
            actual_x0=0.0,
            actual_x1=4.0,
        )

    output = capsys.readouterr().out
    assert "中枢矩形绘图边界与中枢 bi_indices 不一致" in output


def test_segment_option_does_not_add_segment_trace(monkeypatch) -> None:
    monkeypatch.setattr(
        chart,
        "analyze_chan_marks",
        lambda _: _fake_marks({20: 102, 30: 106}, [(20, 30)]),
    )

    fig = chart.create_kline_figure(
        _make_raw_df(raw_start=100, rows=10),
        display_options=["segment"],
        visible_count=None,
    )

    trace_names = {trace.name for trace in fig.data}
    assert "线段" not in trace_names
    assert "线段(预留)" not in trace_names


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
