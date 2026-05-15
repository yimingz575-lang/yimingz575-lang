from __future__ import annotations

from collections.abc import Iterable
import inspect
import math
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.chan.bi_zhongshu import build_bi_zhongshu
from src.chan.engine import analyze_chan_marks
from src.chan.inclusion import detect_inclusion_marks
from src.indicators.macd import append_macd

MA_CONFIG = {
    "ma5": {"window": 5, "name": "MA5", "color": "#ffd400"},
    "ma10": {"window": 10, "name": "MA10", "color": "#ff9f1c"},
    "ma20": {"window": 20, "name": "MA20", "color": "#d96cff"},
}

CHAN_PLACEHOLDER_TRACES = {
    "fractal": {"name": "分型(预留)", "color": "#6cb6ff", "mode": "markers"},
    "bi": {"name": "笔(预留)", "color": "#ff9f1c", "mode": "lines"},
    "segment": {"name": "线段(预留)", "color": "#00e5ff", "mode": "lines"},
    "zone": {"name": "中枢(预留)", "color": "#ff66c4", "mode": "lines"},
    "signal": {"name": "买卖点(预留)", "color": "#f8f32b", "mode": "markers"},
}

BACKGROUND_COLOR = "#000000"
GRID_COLOR = "rgba(255, 0, 0, 0.38)"
KLINE_COLOR = "white"
UP_COLOR = "#ff2b2b"
DOWN_COLOR = "#00d6a3"
BI_UP_COLOR = "#ff3030"
BI_DOWN_COLOR = "#33d17a"
BI_TEMPORARY_COLOR = "yellow"
BI_ZHONGSHU_LINE_COLOR = "rgba(255, 214, 0, 0.95)"
BI_ZHONGSHU_FILL_COLOR = "rgba(255, 214, 0, 0.16)"
CHART_DEBUG_DIR = Path("output")
CHART_BI_MAPPING_DEBUG_PATH = CHART_DEBUG_DIR / "chart_bi_mapping_debug.csv"
CHART_BI_COVERAGE_DEBUG_PATH = CHART_DEBUG_DIR / "chart_bi_coverage_debug.csv"
BI_MAPPING_DEBUG_COLUMNS = [
    "bi_index",
    "start_virtual_index",
    "end_virtual_index",
    "start_raw_index",
    "end_raw_index",
    "start_in_plot_window",
    "end_in_plot_window",
    "start_x",
    "end_x",
    "drawn",
    "skip_reason",
]
BI_COVERAGE_DEBUG_COLUMNS = [
    "gap_start_x",
    "gap_end_x",
    "gap_start_raw_index",
    "gap_end_raw_index",
    "gap_bar_count",
    "reason_guess",
]
PREPARED_COLUMNS = [
    "x",
    "chart_x",
    "raw_index",
    "date_label",
    "ma5",
    "ma10",
    "ma20",
    "dif",
    "dea",
    "macd",
]


def create_kline_figure(
    df: pd.DataFrame,
    stock_code: str = "DEMO",
    period_label: str = "日线",
    display_options: Iterable[str] | None = None,
    analysis_df: pd.DataFrame | None = None,
    visible_count: int | None = 120,
    xaxis_range: Iterable[float] | None = None,
    performance_mode: bool = True,
) -> go.Figure:
    """Create a Tongdaxin-style K-line figure with MA and MACD panels."""
    chart_df = _prepare_chart_data(df)
    selected_options = set(display_options or [])
    show_ma = "ma" in selected_options

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.7, 0.3],
        vertical_spacing=0.03,
        specs=[[{"type": "candlestick"}], [{"type": "bar"}]],
    )

    customdata = chart_df[["date_label"]]
    hovertemplate = (
        "时间：%{customdata[0]}<br>"
        "开盘价：%{open:.2f}<br>"
        "收盘价：%{close:.2f}<br>"
        "最高价：%{high:.2f}<br>"
        "最低价：%{low:.2f}"
        "<extra></extra>"
    )

    fig.add_trace(
        go.Candlestick(
            x=chart_df["x"],
            open=chart_df["open"],
            high=chart_df["high"],
            low=chart_df["low"],
            close=chart_df["close"],
            name="K线",
            increasing_line_color=KLINE_COLOR,
            increasing_fillcolor=KLINE_COLOR,
            decreasing_line_color=KLINE_COLOR,
            decreasing_fillcolor=KLINE_COLOR,
            customdata=customdata,
            hovertemplate=hovertemplate,
        ),
        row=1,
        col=1,
    )

    if show_ma:
        for column, config in MA_CONFIG.items():
            fig.add_trace(
                go.Scatter(
                    x=chart_df["x"],
                    y=chart_df[column],
                    mode="lines",
                    name=config["name"],
                    line={"color": config["color"], "width": 1.2},
                    hoverinfo="skip",
                ),
                row=1,
                col=1,
            )

    _add_inclusion_marker_trace(fig, chart_df, display_options)
    chan_trace_info = _add_chan_algorithm_traces(
        fig,
        chart_df,
        display_options,
        analysis_df,
        symbol=stock_code,
        timeframe=period_label,
    )
    _add_chan_placeholder_traces(fig, display_options)

    macd_colors = [UP_COLOR if value >= 0 else DOWN_COLOR for value in chart_df["macd"]]
    fig.add_trace(
        go.Bar(
            x=chart_df["x"],
            y=chart_df["macd"],
            name="MACD",
            marker={"color": macd_colors},
            hoverinfo="skip",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=chart_df["x"],
            y=chart_df["dif"],
            mode="lines",
            name="DIF",
            line={"color": "#ffffff", "width": 1.1},
            hoverinfo="skip",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=chart_df["x"],
            y=chart_df["dea"],
            mode="lines",
            name="DEA",
            line={"color": "#ffd400", "width": 1.1},
            hoverinfo="skip",
        ),
        row=2,
        col=1,
    )

    _style_figure(
        fig,
        chart_df=chart_df,
        stock_code=stock_code,
        period_label=period_label,
        visible_count=visible_count,
        xaxis_range=xaxis_range,
        chan_trace_info=chan_trace_info,
    )
    return fig


def prepare_chart_data(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw K-line data and calculate MA/MACD once for chart reuse."""
    chart_df = df.copy()
    chart_df["date"] = pd.to_datetime(chart_df["date"], errors="coerce")
    chart_df = chart_df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    chart_df["raw_index"] = range(len(chart_df))
    chart_df["chart_x"] = range(len(chart_df))
    chart_df["x"] = chart_df["chart_x"]
    chart_df["date_label"] = chart_df["date"].dt.strftime("%Y-%m-%d %H:%M")

    for column in ["open", "high", "low", "close", "volume"]:
        chart_df[column] = pd.to_numeric(chart_df[column], errors="coerce")

    for column, config in MA_CONFIG.items():
        chart_df[column] = chart_df["close"].rolling(config["window"]).mean()

    return append_macd(chart_df)


def calculate_visible_yaxis_ranges(
    chart_df: pd.DataFrame,
    xaxis_range: Iterable[float] | None,
) -> tuple[list[float] | None, list[float] | None]:
    """Calculate price and MACD y-axis ranges for the current visible x-axis window."""
    if chart_df.empty or xaxis_range is None:
        return None, None

    chart_df = _prepare_chart_data(chart_df)
    visible_df = _slice_visible_df(chart_df, xaxis_range)
    if visible_df.empty:
        return None, None

    price_range = _make_padded_range(visible_df["low"].min(), visible_df["high"].max())
    macd_columns = visible_df[["dif", "dea", "macd"]]
    macd_range = _make_padded_range(
        macd_columns.min().min(),
        macd_columns.max().max(),
        include_zero=True,
    )
    return price_range, macd_range


def _prepare_chart_data(df: pd.DataFrame) -> pd.DataFrame:
    if not all(column in df.columns for column in ["date", "open", "high", "low", "close", "volume"]):
        return prepare_chart_data(df)

    chart_df = df.copy()
    chart_df["date"] = pd.to_datetime(chart_df["date"], errors="coerce")
    chart_df = chart_df.dropna(subset=["date"]).copy()

    if "raw_index" in chart_df.columns:
        chart_df["raw_index"] = pd.to_numeric(chart_df["raw_index"], errors="coerce")
    elif "x" in chart_df.columns:
        chart_df["raw_index"] = pd.to_numeric(chart_df["x"], errors="coerce")
    else:
        chart_df = chart_df.sort_values("date").reset_index(drop=True)
        chart_df["raw_index"] = range(len(chart_df))

    chart_df = chart_df.dropna(subset=["raw_index"]).sort_values("raw_index").reset_index(drop=True)
    chart_df["raw_index"] = chart_df["raw_index"].astype(int)
    chart_df["chart_x"] = range(len(chart_df))
    chart_df["x"] = chart_df["chart_x"]
    chart_df["date_label"] = chart_df["date"].dt.strftime("%Y-%m-%d %H:%M")

    for column in ["open", "high", "low", "close", "volume"]:
        chart_df[column] = pd.to_numeric(chart_df[column], errors="coerce")

    missing_indicators = [column for column in PREPARED_COLUMNS[4:] if column not in chart_df.columns]
    if missing_indicators:
        for column, config in MA_CONFIG.items():
            chart_df[column] = chart_df["close"].rolling(config["window"]).mean()
        chart_df = append_macd(chart_df)
    else:
        for column in PREPARED_COLUMNS[4:]:
            chart_df[column] = pd.to_numeric(chart_df[column], errors="coerce")

    return chart_df


def _add_chan_placeholder_traces(
    fig: go.Figure,
    display_options: Iterable[str] | None,
) -> None:
    selected_options = set(display_options or [])
    for option, config in CHAN_PLACEHOLDER_TRACES.items():
        if option in {"fractal", "bi", "segment", "zone"}:
            continue
        if option not in selected_options:
            continue
        fig.add_trace(
            go.Scatter(
                x=[],
                y=[],
                mode=config["mode"],
                name=config["name"],
                line={"color": config["color"], "width": 1.4},
                marker={"color": config["color"], "size": 8},
                hoverinfo="skip",
            ),
            row=1,
            col=1,
        )


def _add_inclusion_marker_trace(
    fig: go.Figure,
    chart_df: pd.DataFrame,
    display_options: Iterable[str] | None,
) -> None:
    if "inclusion" not in set(display_options or []) or chart_df.empty:
        return

    marks = detect_inclusion_marks(chart_df)
    marked = marks[marks["has_inclusion"]].copy()
    if marked.empty:
        return

    high_by_position = pd.to_numeric(chart_df["high"], errors="coerce").reset_index(drop=True)
    price_span = chart_df["high"].max() - chart_df["low"].min()
    marker_offset = price_span * 0.018 if price_span else max(abs(chart_df["high"].max()) * 0.01, 1.0)
    marked["marker_y"] = high_by_position.loc[marked.index].to_numpy() + marker_offset
    marked["date_label"] = pd.to_datetime(marked["date"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")

    fig.add_trace(
        go.Scatter(
            x=marked["x"],
            y=marked["marker_y"],
            mode="markers",
            name="包含关系",
            marker={
                "color": "#ffd400",
                "size": 9,
                "symbol": "triangle-up",
                "line": {"color": "#fff3a3", "width": 1},
            },
            customdata=marked[["date_label", "index", "inclusion_type", "reason"]],
            hoverinfo="skip",
            hovertemplate=None,
        ),
        row=1,
        col=1,
    )


def _add_chan_algorithm_traces(
    fig: go.Figure,
    chart_df: pd.DataFrame,
    display_options: Iterable[str] | None,
    analysis_df: pd.DataFrame | None = None,
    symbol: str = "",
    timeframe: str = "",
) -> dict[str, object]:
    trace_info: dict[str, object] = {
        "confirmed_bis_count": 0,
        "bi_trace_count": 0,
        "bi_zhongshu_count": 0,
        "bi_segments": [],
    }
    selected_options = set(display_options or [])
    show_bi = "bi" in selected_options
    show_bi_zhongshu = "zone" in selected_options
    candle_x_min = int(chart_df["chart_x"].min()) if not chart_df.empty else None
    candle_x_max = int(chart_df["chart_x"].max()) if not chart_df.empty else None
    print("[chart] symbol =", symbol)
    print("[chart] timeframe =", timeframe)
    print("[chart] show_bi =", show_bi)
    print("[chart] show_bi_zhongshu =", show_bi_zhongshu)
    if chart_df.empty or not ({"fractal", "bi", "zone"} & selected_options):
        if show_bi:
            print("[chart] marks keys =", [])
            print("[chart] raw_bars_total =", 0)
            print("[chart] raw_bars_plot_window =", len(chart_df))
            print("[chart] raw_bars_displayed =", len(chart_df))
            print("[chart] plot_window_raw_index_min =", None)
            print("[chart] plot_window_raw_index_max =", None)
            print("[chart] standard_bars =", 0)
            print("[chart] inclusion_groups =", 0)
            print("[chart] confirmed_bis =", 0)
            print("[chart] candle_x_min =", candle_x_min)
            print("[chart] candle_x_max =", candle_x_max)
            print("[chart] bi_trace_count =", 0)
            print("[chart] skipped_bi_mapping_failed =", 0)
            print("[chart] skipped_bi_endpoint_outside_plot_window =", 0)
        return trace_info

    analysis_chart_df = _prepare_chart_data(analysis_df) if analysis_df is not None else chart_df
    chan_marks = _analyze_chan_marks_for_chart(analysis_chart_df, symbol)
    print("[chart] marks keys =", chan_marks.keys())
    inclusion_groups = _get_mark_dataframe(chan_marks, "inclusion_groups")
    confirmed_bis = _get_mark_dataframe(chan_marks, "confirmed_bis")
    print("[chart] using field = confirmed_bis")
    print("[chart] uses inclusion_groups mapping =", True)
    standard_bars = chan_marks.get("standard_bars") or []
    raw_index_to_chart_x = _build_raw_index_to_chart_x_lookup(chart_df)
    virtual_source_ranges = _build_virtual_to_source_range_lookup(inclusion_groups)
    (
        mapped_bis,
        skipped_mapping_failed,
        skipped_endpoint_outside,
        x_warning_count,
        debug_bis,
        mapping_debug_df,
    ) = _map_bis_to_chart_x(
        confirmed_bis=confirmed_bis,
        virtual_source_ranges=virtual_source_ranges,
        raw_index_to_chart_x=raw_index_to_chart_x,
        displayed_count=len(chart_df),
    )
    coverage_debug_df = _build_chart_bi_coverage_debug(chart_df, mapping_debug_df)
    _write_chart_bi_debug_files(mapping_debug_df, coverage_debug_df)
    drawn_debug_count = _count_drawn_debug_rows(mapping_debug_df)
    skipped_debug_count = _count_skipped_debug_rows(mapping_debug_df)
    trace_info["confirmed_bis_count"] = len(confirmed_bis)
    trace_info["bi_segments"] = _collect_bi_segments(mapped_bis)
    trace_info["bi_zhongshu_count"] = 0
    plot_window_raw_index_min = int(chart_df["raw_index"].min()) if not chart_df.empty else None
    plot_window_raw_index_max = int(chart_df["raw_index"].max()) if not chart_df.empty else None
    print("[chart] raw_bars_total =", len(analysis_chart_df))
    print("[chart] raw_bars_plot_window =", len(chart_df))
    print("[chart] raw_bars_plot_window_len =", len(chart_df))
    print("[chart] raw_bars_displayed =", len(chart_df))
    print("[chart] plot_window_raw_index_min =", plot_window_raw_index_min)
    print("[chart] plot_window_raw_index_max =", plot_window_raw_index_max)
    print("[chart] standard_bars =", len(standard_bars))
    print("[chart] inclusion_groups =", len(inclusion_groups))
    print("[chart] confirmed_bis =", len(confirmed_bis))
    print("[chart] confirmed_bis_total =", len(confirmed_bis))
    print("[chart] drawn_bis =", drawn_debug_count)
    print("[chart] skipped_bis =", skipped_debug_count)
    print("[chart] candle_x_min =", candle_x_min)
    print("[chart] candle_x_max =", candle_x_max)
    for debug_record in debug_bis:
        _print_bi_mapping_debug(debug_record)
    if "fractal" in selected_options:
        fractals = _get_mark_dataframe(chan_marks, "valid_fractals_for_bi", "fractals")
        fractals = _map_fractals_to_chart_x(
            fractals=fractals,
            virtual_to_raw_index=_build_virtual_to_raw_index_lookup(inclusion_groups),
            raw_index_to_chart_x=raw_index_to_chart_x,
        )
        _add_fractal_marker_traces(fig, chart_df, fractals)
    if show_bi_zhongshu:
        bi_zhongshu = build_bi_zhongshu(mapped_bis)
        trace_info["bi_zhongshu_count"] = len(bi_zhongshu)
        bi_zhongshu_trace_count = _add_bi_zhongshu_traces(fig, mapped_bis, bi_zhongshu)
        print("[chart] bi_zhongshu_count =", len(bi_zhongshu))
        print("[chart] bi_zhongshu_trace_count =", bi_zhongshu_trace_count)
    if "bi" in selected_options:
        bi_trace_count = _add_bi_line_traces(fig, mapped_bis)
        trace_info["bi_trace_count"] = bi_trace_count
        print("[chart] bi_trace_count =", bi_trace_count)
        print("[chart] skipped_bi_mapping_failed =", skipped_mapping_failed)
        print("[chart] skipped_bi_endpoint_outside_plot_window =", skipped_endpoint_outside)
        print("[chart] skipped_bi_not_in_visible_window =", skipped_endpoint_outside)
        if x_warning_count:
            print("[chart] warning_bi_x_out_of_range =", x_warning_count)
        print("[chart] bi traces added =", bi_trace_count)
    return trace_info


def _add_fractal_marker_traces(
    fig: go.Figure,
    chart_df: pd.DataFrame,
    fractals: pd.DataFrame,
) -> None:
    if fractals.empty:
        return

    price_span = chart_df["high"].max() - chart_df["low"].min()
    marker_offset = price_span * 0.026 if price_span else max(abs(chart_df["high"].max()) * 0.01, 1.0)

    top_fractals = fractals[fractals["type"] == "top"].copy()
    bottom_fractals = fractals[fractals["type"] == "bottom"].copy()

    if not top_fractals.empty:
        top_fractals["marker_y"] = pd.to_numeric(top_fractals["price"], errors="coerce") + marker_offset
        _add_single_fractal_trace(
            fig=fig,
            fractals=top_fractals,
            name="顶分型",
            marker_color="#ff3030",
            marker_symbol="triangle-down",
            type_label="顶分型",
        )

    if not bottom_fractals.empty:
        bottom_fractals["marker_y"] = pd.to_numeric(bottom_fractals["price"], errors="coerce") - marker_offset
        _add_single_fractal_trace(
            fig=fig,
            fractals=bottom_fractals,
            name="底分型",
            marker_color="#33d17a",
            marker_symbol="triangle-up",
            type_label="底分型",
        )


def _add_bi_zhongshu_traces(
    fig: go.Figure,
    mapped_bis: pd.DataFrame,
    bi_zhongshu: pd.DataFrame,
) -> int:
    if mapped_bis.empty or bi_zhongshu.empty:
        return 0

    trace_count = 0
    for position, (_, zs) in enumerate(bi_zhongshu.iterrows()):
        start_bi_index = _coerce_int(zs.get("start_bi_index"))
        end_bi_index = _coerce_int(zs.get("end_bi_index"))
        if start_bi_index is None or end_bi_index is None:
            continue
        if start_bi_index < 0 or end_bi_index >= len(mapped_bis):
            continue

        start_x = pd.to_numeric(mapped_bis.iloc[start_bi_index].get("start_x"), errors="coerce")
        end_x = pd.to_numeric(mapped_bis.iloc[end_bi_index].get("end_x"), errors="coerce")
        zd = pd.to_numeric(zs.get("zd"), errors="coerce")
        zg = pd.to_numeric(zs.get("zg"), errors="coerce")
        if pd.isna(start_x) or pd.isna(end_x) or pd.isna(zd) or pd.isna(zg):
            continue

        x0 = float(min(start_x, end_x))
        x1 = float(max(start_x, end_x))
        y0 = float(min(zd, zg))
        y1 = float(max(zd, zg))
        fig.add_trace(
            go.Scatter(
                x=[x0, x1, x1, x0, x0],
                y=[y0, y0, y1, y1, y0],
                mode="lines",
                name="笔中枢",
                showlegend=position == 0,
                line={"color": BI_ZHONGSHU_LINE_COLOR, "width": 1.4, "dash": "dot"},
                fill="toself",
                fillcolor=BI_ZHONGSHU_FILL_COLOR,
                hoverinfo="skip",
                hovertemplate=None,
            ),
            row=1,
            col=1,
        )
        trace_count += 1
    return trace_count


def _add_bi_line_traces(fig: go.Figure, bis: pd.DataFrame) -> int:
    if bis.empty:
        return 0

    bi_trace_count = 0
    for position, (_, bi) in enumerate(bis.iterrows()):
        direction = _resolve_bi_direction(bi)
        direction_label = _format_bi_direction_label(direction)
        line_color = _get_bi_line_color(direction, bi)
        bi_state_label = _format_bi_state_label(bi)
        fallback_reason = _format_fallback_reason(bi)
        start_date = _format_hover_date(bi["start_date"])
        end_date = _format_hover_date(bi["end_date"])
        customdata = [
            [
                direction_label,
                start_date,
                end_date,
                float(bi["start_price"]),
                float(bi["end_price"]),
                int(bi["kline_count"]),
                bi_state_label,
                fallback_reason,
            ],
            [
                direction_label,
                start_date,
                end_date,
                float(bi["start_price"]),
                float(bi["end_price"]),
                int(bi["kline_count"]),
                bi_state_label,
                fallback_reason,
            ],
        ]
        fig.add_trace(
            go.Scatter(
                x=[bi["start_x"], bi["end_x"]],
                y=[bi["start_price"], bi["end_price"]],
                mode="lines+markers",
                name="笔",
                showlegend=position == 0,
                line={"color": line_color, "width": 2.2},
                marker={"color": line_color, "size": 5},
                customdata=customdata,
                hoverinfo="skip",
                hovertemplate=None,
            ),
            row=1,
            col=1,
        )
        bi_trace_count += 1
    return bi_trace_count


def _resolve_bi_direction(bi: pd.Series) -> str:
    direction = _row_value(bi, "direction")
    if isinstance(direction, str):
        normalized = direction.strip().lower()
        if normalized in {"up", "down"}:
            return normalized

    start_price = pd.to_numeric(_row_value(bi, "start_price"), errors="coerce")
    end_price = pd.to_numeric(_row_value(bi, "end_price"), errors="coerce")
    if not pd.isna(start_price) and not pd.isna(end_price):
        if float(end_price) > float(start_price):
            return "up"
        if float(end_price) < float(start_price):
            return "down"
    return "unknown"


def _get_bi_line_color(direction: str, bi: pd.Series | None = None) -> str:
    if bi is not None and (_is_temporary_bi(bi) or _is_fallback_bi(bi)):
        row_color = _row_value(bi, "color")
        if isinstance(row_color, str) and row_color.strip():
            return row_color
        return BI_TEMPORARY_COLOR
    if direction == "up":
        return BI_UP_COLOR
    if direction == "down":
        return BI_DOWN_COLOR
    return "#f8f32b"


def _format_bi_direction_label(direction: str) -> str:
    if direction == "up":
        return "上升笔"
    if direction == "down":
        return "下降笔"
    return "方向未知"


def _format_bi_state_label(bi: pd.Series) -> str:
    if _is_temporary_bi(bi) or _is_fallback_bi(bi):
        return "临时笔 / fallback bi"
    return "标准笔"


def _format_fallback_reason(bi: pd.Series) -> str:
    reason = _row_value(bi, "fallback_reason", "")
    if isinstance(reason, str) and reason.strip():
        return reason
    return "-"


def _is_temporary_bi(bi: pd.Series) -> bool:
    return _row_bool(bi, "is_temporary")


def _is_fallback_bi(bi: pd.Series) -> bool:
    return _row_bool(bi, "is_fallback_bi")


def _row_bool(row: pd.Series, key: str) -> bool:
    value = _row_value(row, key, False)
    if pd.isna(value):
        return False
    return _coerce_bool(value)


def _collect_bi_segments(bis: pd.DataFrame) -> list[tuple[float, float]]:
    if bis.empty or not {"start_x", "end_x"}.issubset(bis.columns):
        return []

    segments: list[tuple[float, float]] = []
    for _, bi in bis.iterrows():
        start_x = pd.to_numeric(bi["start_x"], errors="coerce")
        end_x = pd.to_numeric(bi["end_x"], errors="coerce")
        if pd.isna(start_x) or pd.isna(end_x):
            continue
        segments.append((float(start_x), float(end_x)))
    return segments


def _get_mark_dataframe(marks: dict, preferred_key: str, fallback_key: str | None = None) -> pd.DataFrame:
    value = marks.get(preferred_key)
    if isinstance(value, pd.DataFrame):
        return value
    if fallback_key is not None:
        fallback = marks.get(fallback_key)
        if isinstance(fallback, pd.DataFrame):
            return fallback
    return pd.DataFrame()


def _analyze_chan_marks_for_chart(df: pd.DataFrame, symbol: str) -> dict[str, object]:
    parameters = inspect.signature(analyze_chan_marks).parameters
    if "symbol" in parameters:
        return analyze_chan_marks(df, symbol=symbol)
    return analyze_chan_marks(df)


def _map_fractals_to_chart_x(
    fractals: pd.DataFrame,
    virtual_to_raw_index: dict[int, int],
    raw_index_to_chart_x: dict[int, int],
) -> pd.DataFrame:
    if fractals.empty:
        return fractals

    records: list[pd.Series] = []
    for _, fractal in fractals.iterrows():
        virtual_index = _coerce_int(_row_value(fractal, "center_index", _row_value(fractal, "virtual_index")))
        raw_index = virtual_to_raw_index.get(virtual_index) if virtual_index is not None else None
        if raw_index is None or raw_index not in raw_index_to_chart_x:
            continue
        mapped = fractal.copy()
        mapped["draw_raw_index"] = raw_index
        mapped["x"] = raw_index_to_chart_x[raw_index]
        records.append(mapped)
    if not records:
        return pd.DataFrame(columns=[*fractals.columns, "draw_raw_index"])
    return pd.DataFrame(records).reset_index(drop=True)


def _map_bis_to_chart_x(
    confirmed_bis: pd.DataFrame,
    virtual_source_ranges: dict[int, dict[str, int]],
    raw_index_to_chart_x: dict[int, int],
    displayed_count: int,
) -> tuple[pd.DataFrame, int, int, int, list[dict], pd.DataFrame]:
    if confirmed_bis.empty:
        mapping_debug_df = pd.DataFrame(
            [
                {
                    "bi_index": None,
                    "start_virtual_index": None,
                    "end_virtual_index": None,
                    "start_raw_index": None,
                    "end_raw_index": None,
                    "start_in_plot_window": False,
                    "end_in_plot_window": False,
                    "start_x": None,
                    "end_x": None,
                    "drawn": False,
                    "skip_reason": "confirmed_bis_empty",
                }
            ],
            columns=BI_MAPPING_DEBUG_COLUMNS,
        )
        return confirmed_bis.copy(), 0, 0, 0, [], mapping_debug_df

    records: list[pd.Series] = []
    skipped_mapping_failed = 0
    skipped_endpoint_outside = 0
    x_warning_count = 0
    debug_records: list[dict] = []
    mapping_records: list[dict] = []
    for position, (_, bi) in enumerate(confirmed_bis.iterrows()):
        start_virtual_index = _coerce_int(
            _row_value(bi, "start_center_index", _row_value(bi, "start_virtual_index"))
        )
        end_virtual_index = _coerce_int(
            _row_value(bi, "end_center_index", _row_value(bi, "end_virtual_index"))
        )
        start_range = virtual_source_ranges.get(start_virtual_index) if start_virtual_index is not None else None
        end_range = virtual_source_ranges.get(end_virtual_index) if end_virtual_index is not None else None
        start_raw_index = start_range.get("source_end_index") if start_range else None
        end_raw_index = end_range.get("source_end_index") if end_range else None
        start_x = raw_index_to_chart_x.get(start_raw_index) if start_raw_index is not None else None
        end_x = raw_index_to_chart_x.get(end_raw_index) if end_raw_index is not None else None
        start_in_plot_window = start_raw_index in raw_index_to_chart_x if start_raw_index is not None else False
        end_in_plot_window = end_raw_index in raw_index_to_chart_x if end_raw_index is not None else False
        skip_reason = "drawn"
        mapped_ok = False

        debug_record = {
            "position": position,
            "start_virtual_index": start_virtual_index,
            "end_virtual_index": end_virtual_index,
            "start_source_start_index": start_range.get("source_start_index") if start_range else None,
            "start_source_end_index": start_range.get("source_end_index") if start_range else None,
            "end_source_start_index": end_range.get("source_start_index") if end_range else None,
            "end_source_end_index": end_range.get("source_end_index") if end_range else None,
            "start_raw_index": start_raw_index,
            "end_raw_index": end_raw_index,
            "start_x": start_x,
            "end_x": end_x,
            "start_price": _row_value(bi, "start_price"),
            "end_price": _row_value(bi, "end_price"),
            "mapped_ok": mapped_ok,
            "skip_reason": skip_reason,
        }
        mapping_record = {
            "bi_index": position,
            "start_virtual_index": start_virtual_index,
            "end_virtual_index": end_virtual_index,
            "start_raw_index": start_raw_index,
            "end_raw_index": end_raw_index,
            "start_in_plot_window": start_in_plot_window,
            "end_in_plot_window": end_in_plot_window,
            "start_x": start_x,
            "end_x": end_x,
            "drawn": False,
            "skip_reason": skip_reason,
        }

        if start_virtual_index is None or end_virtual_index is None:
            skipped_mapping_failed += 1
            debug_record["mapped_ok"] = False
            debug_record["skip_reason"] = "field_missing"
            mapping_record["skip_reason"] = "field_missing"
            mapping_records.append(mapping_record)
            if position < 20:
                debug_records.append(debug_record)
            continue
        if start_range is None or end_range is None or start_raw_index is None or end_raw_index is None:
            skipped_mapping_failed += 1
            debug_record["mapped_ok"] = False
            debug_record["skip_reason"] = "mapping_failed"
            mapping_record["skip_reason"] = "mapping_failed"
            mapping_records.append(mapping_record)
            if position < 20:
                debug_records.append(debug_record)
            continue
        if not start_in_plot_window or not end_in_plot_window:
            skipped_endpoint_outside += 1
            debug_record["mapped_ok"] = False
            if not start_in_plot_window and not end_in_plot_window:
                skip_reason = "both_outside_plot_window"
            elif not start_in_plot_window:
                skip_reason = "start_outside_plot_window"
            else:
                skip_reason = "end_outside_plot_window"
            debug_record["skip_reason"] = skip_reason
            mapping_record["skip_reason"] = skip_reason
            mapping_records.append(mapping_record)
            if position < 20:
                debug_records.append(debug_record)
            continue
        if start_x is None or end_x is None:
            skipped_mapping_failed += 1
            debug_record["mapped_ok"] = False
            debug_record["skip_reason"] = "mapping_failed"
            mapping_record["skip_reason"] = "mapping_failed"
            mapping_records.append(mapping_record)
            if position < 20:
                debug_records.append(debug_record)
            continue
        if not _is_chart_x_in_range(start_x, displayed_count) or not _is_chart_x_in_range(end_x, displayed_count):
            x_warning_count += 1
            skipped_mapping_failed += 1
            debug_record["mapped_ok"] = False
            debug_record["skip_reason"] = "mapping_failed"
            mapping_record["skip_reason"] = "mapping_failed"
            mapping_records.append(mapping_record)
            if position < 20:
                debug_records.append(debug_record)
            continue

        mapped = bi.copy()
        mapped["start_raw_index"] = start_raw_index
        mapped["end_raw_index"] = end_raw_index
        mapped["start_x"] = start_x
        mapped["end_x"] = end_x
        records.append(mapped)
        debug_record["mapped_ok"] = True
        debug_record["skip_reason"] = "drawn"
        mapping_record["drawn"] = True
        mapping_record["skip_reason"] = "drawn"
        mapping_records.append(mapping_record)
        if position < 20:
            debug_records.append(debug_record)

    mapping_debug_df = pd.DataFrame(mapping_records, columns=BI_MAPPING_DEBUG_COLUMNS)
    if not records:
        return (
            pd.DataFrame(columns=[*confirmed_bis.columns, "start_raw_index", "end_raw_index"]),
            skipped_mapping_failed,
            skipped_endpoint_outside,
            x_warning_count,
            debug_records,
            mapping_debug_df,
        )
    return (
        pd.DataFrame(records).reset_index(drop=True),
        skipped_mapping_failed,
        skipped_endpoint_outside,
        x_warning_count,
        debug_records,
        mapping_debug_df,
    )


def _build_chart_bi_coverage_debug(chart_df: pd.DataFrame, mapping_debug_df: pd.DataFrame) -> pd.DataFrame:
    if chart_df.empty:
        return pd.DataFrame(columns=BI_COVERAGE_DEBUG_COLUMNS)

    displayed_count = len(chart_df)
    drawn_covered = [False] * displayed_count
    confirmed_covered = [False] * displayed_count
    raw_index_to_chart_x = _build_raw_index_to_chart_x_lookup(chart_df)
    plot_raw_min = int(chart_df["raw_index"].min())
    plot_raw_max = int(chart_df["raw_index"].max())

    for _, record in mapping_debug_df.iterrows():
        start_raw_index = _coerce_int(record.get("start_raw_index"))
        end_raw_index = _coerce_int(record.get("end_raw_index"))
        if start_raw_index is None or end_raw_index is None:
            continue

        confirmed_interval = _raw_interval_to_chart_x_interval(
            start_raw_index=start_raw_index,
            end_raw_index=end_raw_index,
            plot_raw_min=plot_raw_min,
            plot_raw_max=plot_raw_max,
            raw_index_to_chart_x=raw_index_to_chart_x,
        )
        if confirmed_interval is not None:
            _mark_covered_range(confirmed_covered, confirmed_interval[0], confirmed_interval[1])

        if _coerce_bool(record.get("drawn")):
            start_x = _coerce_int(record.get("start_x"))
            end_x = _coerce_int(record.get("end_x"))
            if start_x is not None and end_x is not None:
                _mark_covered_range(drawn_covered, start_x, end_x)

    gap_records: list[dict[str, object]] = []
    position = 0
    while position < displayed_count:
        if drawn_covered[position]:
            position += 1
            continue
        gap_start_x = position
        while position + 1 < displayed_count and not drawn_covered[position + 1]:
            position += 1
        gap_end_x = position
        reason_guess = (
            "chart_mapping_or_filtering_problem"
            if any(confirmed_covered[gap_start_x : gap_end_x + 1])
            else "no_confirmed_bis_cover_this_region"
        )
        gap_records.append(
            {
                "gap_start_x": gap_start_x,
                "gap_end_x": gap_end_x,
                "gap_start_raw_index": int(chart_df.iloc[gap_start_x]["raw_index"]),
                "gap_end_raw_index": int(chart_df.iloc[gap_end_x]["raw_index"]),
                "gap_bar_count": gap_end_x - gap_start_x + 1,
                "reason_guess": reason_guess,
            }
        )
        position += 1

    return pd.DataFrame(gap_records, columns=BI_COVERAGE_DEBUG_COLUMNS)


def _raw_interval_to_chart_x_interval(
    start_raw_index: int,
    end_raw_index: int,
    plot_raw_min: int,
    plot_raw_max: int,
    raw_index_to_chart_x: dict[int, int],
) -> tuple[int, int] | None:
    raw_start, raw_end = sorted([int(start_raw_index), int(end_raw_index)])
    if raw_end < plot_raw_min or raw_start > plot_raw_max:
        return None

    visible_raw_start = max(raw_start, plot_raw_min)
    visible_raw_end = min(raw_end, plot_raw_max)
    start_x = raw_index_to_chart_x.get(visible_raw_start)
    end_x = raw_index_to_chart_x.get(visible_raw_end)
    if start_x is None or end_x is None:
        return None
    return sorted([int(start_x), int(end_x)])


def _mark_covered_range(covered: list[bool], start_x: int, end_x: int) -> None:
    start, end = sorted([int(start_x), int(end_x)])
    start = max(start, 0)
    end = min(end, len(covered) - 1)
    for position in range(start, end + 1):
        covered[position] = True


def _write_chart_bi_debug_files(mapping_debug_df: pd.DataFrame, coverage_debug_df: pd.DataFrame) -> None:
    CHART_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    mapping_debug_df.to_csv(CHART_BI_MAPPING_DEBUG_PATH, index=False, encoding="utf-8-sig")
    coverage_debug_df.to_csv(CHART_BI_COVERAGE_DEBUG_PATH, index=False, encoding="utf-8-sig")
    print("[chart] chart_bi_mapping_debug_csv =", CHART_BI_MAPPING_DEBUG_PATH.as_posix())
    print("[chart] chart_bi_coverage_debug_csv =", CHART_BI_COVERAGE_DEBUG_PATH.as_posix())


def _count_drawn_debug_rows(mapping_debug_df: pd.DataFrame) -> int:
    if mapping_debug_df.empty or "drawn" not in mapping_debug_df.columns:
        return 0
    return sum(1 for value in mapping_debug_df["drawn"] if _coerce_bool(value))


def _count_skipped_debug_rows(mapping_debug_df: pd.DataFrame) -> int:
    if mapping_debug_df.empty or "skip_reason" not in mapping_debug_df.columns:
        return 0
    skipped = mapping_debug_df[mapping_debug_df["skip_reason"] != "confirmed_bis_empty"]
    return len(skipped) - _count_drawn_debug_rows(skipped)


def _build_virtual_to_source_range_lookup(inclusion_groups: pd.DataFrame) -> dict[int, dict[str, int]]:
    if inclusion_groups.empty:
        return {}

    lookup: dict[int, dict[str, int]] = {}
    for _, group in inclusion_groups.iterrows():
        virtual_index = _coerce_int(group.get("virtual_index"))
        source_start_index = _coerce_int(group.get("source_start_index"))
        source_end_index = _coerce_int(group.get("source_end_index"))
        if virtual_index is None or source_end_index is None:
            continue
        if source_start_index is None:
            source_start_index = source_end_index
        lookup[virtual_index] = {
            "source_start_index": source_start_index,
            "source_end_index": source_end_index,
        }
    return lookup


def _build_virtual_to_raw_index_lookup(inclusion_groups: pd.DataFrame) -> dict[int, int]:
    return {
        virtual_index: source_range["source_end_index"]
        for virtual_index, source_range in _build_virtual_to_source_range_lookup(inclusion_groups).items()
    }


def _build_raw_index_to_chart_x_lookup(chart_df: pd.DataFrame) -> dict[int, int]:
    if chart_df.empty or not {"raw_index", "chart_x"}.issubset(chart_df.columns):
        return {}

    lookup: dict[int, int] = {}
    for _, row in chart_df.iterrows():
        raw_index = _coerce_int(row.get("raw_index"))
        chart_x = _coerce_int(row.get("chart_x"))
        if raw_index is None or chart_x is None:
            continue
        lookup[raw_index] = chart_x
    return lookup


def _is_chart_x_in_range(chart_x: int, displayed_count: int) -> bool:
    return 0 <= int(chart_x) <= max(displayed_count - 1, 0)


def _print_bi_mapping_debug(record: dict) -> None:
    position = record["position"]
    print(
        f"[chart] bi[{position}] start_virtual_index={record['start_virtual_index']}, "
        f"end_virtual_index={record['end_virtual_index']}"
    )
    print(
        f"[chart] bi[{position}] start_source_start_index={record['start_source_start_index']}, "
        f"start_source_end_index={record['start_source_end_index']}"
    )
    print(
        f"[chart] bi[{position}] end_source_start_index={record['end_source_start_index']}, "
        f"end_source_end_index={record['end_source_end_index']}"
    )
    print(
        f"[chart] bi[{position}] start_raw_index={record['start_raw_index']}, "
        f"end_raw_index={record['end_raw_index']}"
    )
    print(f"[chart] bi[{position}] start_x={record['start_x']}, end_x={record['end_x']}")
    print(
        f"[chart] bi[{position}] start_price={record['start_price']}, "
        f"end_price={record['end_price']}"
    )
    print(f"[chart] bi[{position}] mapped_ok={record['mapped_ok']}, skip_reason={record['skip_reason']}")


def _row_value(row: pd.Series, key: str, default=None):
    return row[key] if key in row.index else default


def _coerce_int(value) -> int | None:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return None
    return int(number)


def _coerce_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return bool(value)


def _print_confirmed_bis_summary(bis: pd.DataFrame) -> None:
    print(f"confirmed_bis count = {len(bis)}")
    for position, (_, bi) in enumerate(bis.iterrows()):
        print(
            "bi[{position}]: {start_date}, {start_type}, {start_price} -> "
            "{end_date}, {end_type}, {end_price}, direction={direction}".format(
                position=position,
                start_date=_format_hover_date(bi["start_date"]),
                start_type=bi["start_type"],
                start_price=float(bi["start_price"]),
                end_date=_format_hover_date(bi["end_date"]),
                end_type=bi["end_type"],
                end_price=float(bi["end_price"]),
                direction=bi["direction"],
            )
        )


def _add_single_fractal_trace(
    fig: go.Figure,
    fractals: pd.DataFrame,
    name: str,
    marker_color: str,
    marker_symbol: str,
    type_label: str,
) -> None:
    chart_marks = fractals.copy()
    chart_marks["date_label"] = pd.to_datetime(chart_marks["date"], errors="coerce").dt.strftime(
        "%Y-%m-%d %H:%M"
    )
    chart_marks["type_label"] = type_label

    fig.add_trace(
        go.Scatter(
            x=chart_marks["x"],
            y=chart_marks["marker_y"],
            mode="markers",
            name=name,
            marker={
                "color": marker_color,
                "size": 10,
                "symbol": marker_symbol,
                "line": {"color": "#ffffff", "width": 1},
            },
            customdata=chart_marks[["date_label", "type_label", "price", "index"]],
            hoverinfo="skip",
            hovertemplate=None,
        ),
        row=1,
        col=1,
    )


def _format_hover_date(value) -> str:
    date_value = pd.to_datetime(value, errors="coerce")
    if pd.isna(date_value):
        return str(value)
    return date_value.strftime("%Y-%m-%d %H:%M")


def _style_figure(
    fig: go.Figure,
    chart_df: pd.DataFrame,
    stock_code: str,
    period_label: str,
    visible_count: int | None,
    xaxis_range: Iterable[float] | None,
    chan_trace_info: dict[str, object] | None = None,
) -> None:
    grid_settings = {
        "showgrid": True,
        "gridcolor": GRID_COLOR,
        "gridwidth": 1,
        "griddash": "dot",
        "zeroline": False,
        "showline": True,
        "linecolor": "rgba(255, 0, 0, 0.45)",
        "tickfont": {"color": "#d8d8d8"},
        "showspikes": True,
        "spikecolor": "rgba(255, 255, 255, 0.72)",
        "spikethickness": 1,
        "spikedash": "dot",
        "spikemode": "across",
    }

    fig.update_layout(
        title={
            "text": f"{stock_code}  {period_label}",
            "x": 0.01,
            "xanchor": "left",
            "font": {"size": 18, "color": "#f0f0f0"},
        },
        height=900,
        margin={"l": 62, "r": 28, "t": 48, "b": 42},
        paper_bgcolor=BACKGROUND_COLOR,
        plot_bgcolor=BACKGROUND_COLOR,
        font={"color": "#eeeeee", "family": "Microsoft YaHei, SimHei, Arial, sans-serif"},
        dragmode="pan",
        hovermode="closest",
        hoverdistance=80,
        spikedistance=80,
        bargap=0,
        legend={
            "orientation": "h",
            "x": 0.01,
            "y": 1.04,
            "xanchor": "left",
            "yanchor": "bottom",
            "bgcolor": "rgba(0, 0, 0, 0)",
            "font": {"color": "#f0f0f0"},
        },
        hoverlabel={
            "bgcolor": "rgba(0, 0, 0, 0.92)",
            "bordercolor": "rgba(255, 0, 0, 0.58)",
            "font": {"color": "#ffffff", "size": 12},
        },
        xaxis_rangeslider_visible=False,
    )

    tick_values, tick_text = _make_trade_day_ticks(chart_df)
    visible_x_range = _normalize_xaxis_range(chart_df, xaxis_range)
    if visible_x_range is None:
        visible_x_range = _make_default_xaxis_range(chart_df, visible_count, chan_trace_info)
    fig.update_xaxes(
        **grid_settings,
        type="linear",
        tickmode="array",
        tickvals=tick_values,
        ticktext=tick_text,
        range=visible_x_range,
        rangeslider_visible=False,
    )
    x_spike_settings = {
        "showspikes": False,
        "spikecolor": "rgba(255, 255, 255, 0.72)",
        "spikethickness": 1,
        "spikedash": "dot",
        "spikemode": "across",
        "spikesnap": "cursor",
    }
    fig.update_xaxes(**x_spike_settings, row=1, col=1)
    fig.update_xaxes(**x_spike_settings, row=2, col=1)
    fig.update_yaxes(**grid_settings)
    fig.update_yaxes(title_text="价格", row=1, col=1)
    fig.update_yaxes(title_text="MACD", row=2, col=1)
    _apply_visible_yaxis_ranges(fig, chart_df, visible_x_range)


def _make_trade_day_ticks(chart_df: pd.DataFrame) -> tuple[list[int], list[str]]:
    if chart_df.empty:
        return [], []

    target_tick_count = 10
    step = max(len(chart_df) // target_tick_count, 1)
    tick_positions = list(range(0, len(chart_df), step))
    last_position = len(chart_df) - 1
    if tick_positions[-1] != last_position:
        tick_positions.append(last_position)

    tick_values = chart_df.iloc[tick_positions]["x"].astype(int).tolist()
    tick_text = chart_df.iloc[tick_positions]["date"].dt.strftime("%Y-%m-%d").tolist()
    return tick_values, tick_text


def _make_default_xaxis_range(
    chart_df: pd.DataFrame,
    visible_count: int | None,
    chan_trace_info: dict[str, object] | None = None,
) -> list[int] | None:
    if chart_df.empty:
        return None

    min_x = int(chart_df["x"].iloc[0])
    end_x = int(chart_df["x"].iloc[-1])
    if visible_count is None:
        start_x = min_x
    else:
        safe_count = max(int(visible_count), 1)
        start_x = max(end_x - safe_count + 1, min_x)

    return [start_x, end_x]


def _normalize_xaxis_range(
    chart_df: pd.DataFrame,
    xaxis_range: Iterable[float] | None,
) -> list[float] | None:
    if chart_df.empty or xaxis_range is None:
        return None

    values = list(xaxis_range)
    if len(values) != 2:
        return None

    try:
        start_x = float(values[0])
        end_x = float(values[1])
    except (TypeError, ValueError):
        return None

    if not math.isfinite(start_x) or not math.isfinite(end_x):
        return None

    min_x = float(chart_df["x"].iloc[0])
    max_x = float(chart_df["x"].iloc[-1])
    start_x, end_x = sorted([start_x, end_x])
    start_x = max(start_x, min_x)
    end_x = min(end_x, max_x)

    if start_x >= end_x:
        return None

    return [start_x, end_x]


def _apply_visible_yaxis_ranges(
    fig: go.Figure,
    chart_df: pd.DataFrame,
    xaxis_range: Iterable[float] | None,
) -> None:
    price_range, macd_range = calculate_visible_yaxis_ranges(chart_df, xaxis_range)

    if price_range is not None:
        fig.update_yaxes(range=price_range, row=1, col=1)
    if macd_range is not None:
        fig.update_yaxes(range=macd_range, row=2, col=1)


def _slice_visible_df(chart_df: pd.DataFrame, xaxis_range: Iterable[float]) -> pd.DataFrame:
    values = list(xaxis_range)
    first_x = int(chart_df["x"].iloc[0])
    last_x = int(chart_df["x"].iloc[-1])
    start_x = max(math.floor(float(values[0])), first_x)
    end_x = min(math.ceil(float(values[1])), last_x)
    if start_x > end_x:
        return chart_df.iloc[0:0]

    start_pos = max(start_x - first_x, 0)
    end_pos = min(end_x - first_x, len(chart_df) - 1)
    return chart_df.iloc[start_pos : end_pos + 1]


def _make_padded_range(
    min_value: float,
    max_value: float,
    include_zero: bool = False,
) -> list[float] | None:
    if pd.isna(min_value) or pd.isna(max_value):
        return None

    low = float(min_value)
    high = float(max_value)
    if include_zero:
        low = min(low, 0.0)
        high = max(high, 0.0)

    span = high - low
    padding = abs(high) * 0.05 if span == 0 else span * 0.05
    if padding == 0:
        padding = 1.0

    return [low - padding, high + padding]
