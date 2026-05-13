from __future__ import annotations

from pathlib import Path

from dash import Dash, Input, Output, Patch, State, dcc, html, no_update

from src.data_source.csv_loader import load_kline_data
from src.ui.chart import calculate_visible_yaxis_ranges, create_kline_figure, prepare_chart_data

DEFAULT_STOCK_CODE = "DEMO"
DEFAULT_PERIOD = "daily"
DEFAULT_VISIBLE_COUNT = 300
DEFAULT_DISPLAY_OPTIONS = ["bi"]
ANALYSIS_VERSION = "standard_bars_v4_visual_options"
DRAW_DATA_LIMIT = 1000
DATA_CACHE: dict[str, dict] = {}
LAST_X_RANGE_CACHE: dict[str, tuple[float, float]] = {}

PERIOD_OPTIONS = [
    {"label": "1分钟", "value": "1m"},
    {"label": "5分钟", "value": "5m"},
    {"label": "30分钟", "value": "30m"},
    {"label": "60分钟", "value": "60m"},
    {"label": "日线", "value": "daily"},
    {"label": "周线", "value": "weekly"},
    {"label": "月线", "value": "monthly"},
]
PERIOD_LABELS = {option["value"]: option["label"] for option in PERIOD_OPTIONS}

DISPLAY_OPTIONS = [
    {"label": "显示均线", "value": "ma"},
    {"label": "显示笔", "value": "bi"},
    {"label": "显示线段", "value": "segment"},
    {"label": "显示中枢", "value": "zone"},
    {"label": "显示买卖点", "value": "signal"},
]

VISIBLE_COUNT_OPTIONS = [
    {"label": "80", "value": "80"},
    {"label": "120", "value": "120"},
    {"label": "200", "value": "200"},
    {"label": "300", "value": "300"},
    {"label": "500", "value": "500"},
    {"label": "1000", "value": "1000"},
    {"label": "全部", "value": "all"},
]

PERFORMANCE_MODE_OPTIONS = [{"label": "性能模式", "value": "on"}]


def create_app(project_root: Path | None = None) -> Dash:
    """Build the Dash app for the first-stage K-line UI skeleton."""
    root = Path(project_root or Path.cwd())
    initial_cache = _get_cached_chart_data(root, DEFAULT_STOCK_CODE, DEFAULT_PERIOD)
    initial_data = initial_cache["result"]
    initial_figure = create_kline_figure(
        _select_plot_df(initial_cache, str(DEFAULT_VISIBLE_COUNT)),
        stock_code=initial_data.display_stock_code,
        period_label="日线",
        display_options=DEFAULT_DISPLAY_OPTIONS,
        analysis_df=initial_cache["full_df"],
        visible_count=DEFAULT_VISIBLE_COUNT,
        performance_mode=True,
    )
    app = Dash(__name__, title="通达信风格缠论K线界面骨架")
    app.index_string = _index_string()

    app.layout = html.Div(
        className="app-shell",
        children=[
            html.Div(
                className="top-toolbar",
                children=[
                    html.Div("缠论K线", className="brand"),
                    html.Div(
                        className="field stock-field",
                        children=[
                            html.Label("股票代码", htmlFor="stock-code-input"),
                            dcc.Input(
                                id="stock-code-input",
                                type="text",
                                value=DEFAULT_STOCK_CODE,
                                debounce=True,
                                className="stock-input",
                            ),
                        ],
                    ),
                    html.Div(
                        className="field period-field",
                        children=[
                            html.Label("周期", htmlFor="period-select"),
                            dcc.Dropdown(
                                id="period-select",
                                options=PERIOD_OPTIONS,
                                value=DEFAULT_PERIOD,
                                clearable=False,
                                searchable=False,
                                className="period-select",
                            ),
                        ],
                    ),
                    html.Div(
                        className="field visible-count-field",
                        children=[
                            html.Label("显示K线数量", htmlFor="visible-count-select"),
                            dcc.Dropdown(
                                id="visible-count-select",
                                options=VISIBLE_COUNT_OPTIONS,
                                value=str(DEFAULT_VISIBLE_COUNT),
                                clearable=False,
                                searchable=False,
                                className="visible-count-select",
                            ),
                        ],
                    ),
                    html.Div(
                        className="display-options",
                        children=dcc.Checklist(
                            id="display-options",
                            options=DISPLAY_OPTIONS,
                            value=DEFAULT_DISPLAY_OPTIONS,
                            inline=True,
                            inputClassName="option-input",
                            labelClassName="option-label",
                        ),
                    ),
                    html.Div(
                        className="performance-mode",
                        children=dcc.Checklist(
                            id="performance-mode",
                            options=PERFORMANCE_MODE_OPTIONS,
                            value=["on"],
                            inline=True,
                            inputClassName="option-input",
                            labelClassName="option-label",
                        ),
                    ),
                ],
            ),
            html.Div(
                id="data-source-hint",
                className=f"data-source-hint source-{initial_data.source_kind}",
                children=_format_data_message(initial_data.message, str(DEFAULT_VISIBLE_COUNT)),
            ),
            dcc.Graph(
                id="kline-chart",
                className="kline-chart",
                figure=initial_figure,
                config={
                    "scrollZoom": True,
                    "displayModeBar": True,
                },
            ),
        ],
    )

    @app.callback(
        Output("kline-chart", "figure"),
        Output("data-source-hint", "children"),
        Output("data-source-hint", "className"),
        Input("stock-code-input", "value"),
        Input("period-select", "value"),
        Input("visible-count-select", "value"),
        Input("display-options", "value"),
        Input("performance-mode", "value"),
    )
    def update_chart(
        stock_code: str | None,
        period: str,
        visible_count_value: str,
        display_options: list[str] | None,
        performance_mode_value: list[str] | None,
    ):
        clean_stock_code = (stock_code or DEFAULT_STOCK_CODE).strip() or DEFAULT_STOCK_CODE
        clean_period = period or DEFAULT_PERIOD
        visible_count = _parse_visible_count(visible_count_value)
        cached_data = _get_cached_chart_data(root, clean_stock_code, clean_period)
        data_result = cached_data["result"]
        plot_df = _select_plot_df(cached_data, visible_count_value)
        period_label = PERIOD_LABELS.get(clean_period, "日线")
        figure = create_kline_figure(
            plot_df,
            stock_code=data_result.display_stock_code,
            period_label=period_label,
            display_options=display_options,
            analysis_df=cached_data["full_df"],
            visible_count=visible_count,
            performance_mode=_is_performance_mode(performance_mode_value),
        )
        return (
            figure,
            _format_data_message(data_result.message, visible_count_value),
            f"data-source-hint source-{data_result.source_kind}",
        )

    @app.callback(
        Output("kline-chart", "figure", allow_duplicate=True),
        Input("kline-chart", "relayoutData"),
        State("stock-code-input", "value"),
        State("period-select", "value"),
        State("visible-count-select", "value"),
        prevent_initial_call=True,
    )
    def update_visible_yaxis(
        relayout_data: dict | None,
        stock_code: str | None,
        period: str,
        visible_count_value: str,
    ):
        xaxis_range = _extract_relayout_xaxis_range(relayout_data)
        if xaxis_range is None:
            return no_update

        cache_key = _make_cache_key(stock_code, period)
        cached_data = DATA_CACHE.get(cache_key)
        if cached_data is None:
            return no_update
        normalized_range = _normalize_range_tuple(xaxis_range)
        if normalized_range is None:
            return no_update

        range_cache_key = f"{cache_key}_{visible_count_value or DEFAULT_VISIBLE_COUNT}"
        if _ranges_close(LAST_X_RANGE_CACHE.get(range_cache_key), normalized_range):
            return no_update
        LAST_X_RANGE_CACHE[range_cache_key] = normalized_range

        plot_df = _select_plot_df(cached_data, visible_count_value)

        price_range, macd_range = calculate_visible_yaxis_ranges(
            plot_df,
            normalized_range,
        )
        if price_range is None or macd_range is None:
            return no_update

        figure_patch = Patch()
        figure_patch["layout"]["yaxis"]["range"] = price_range
        figure_patch["layout"]["yaxis2"]["range"] = macd_range
        return figure_patch

    return app


def _parse_visible_count(value: str | None) -> int | None:
    if value == "all":
        return None
    try:
        return int(value or DEFAULT_VISIBLE_COUNT)
    except ValueError:
        return DEFAULT_VISIBLE_COUNT


def _get_cached_chart_data(root: Path, stock_code: str | None, period: str | None) -> dict:
    clean_stock_code = _normalize_stock_code(stock_code)
    clean_period = period or DEFAULT_PERIOD
    cache_key = _make_cache_key(clean_stock_code, clean_period)

    if cache_key not in DATA_CACHE:
        data_result = load_kline_data(root, clean_stock_code, clean_period)
        full_df = prepare_chart_data(data_result.df)
        plot_df = _limit_plot_df(full_df, clean_period)
        DATA_CACHE[cache_key] = {
            "result": data_result,
            "full_df": full_df,
            "plot_df": plot_df,
        }

    return DATA_CACHE[cache_key]


def _limit_plot_df(full_df, period: str):
    if period in {"daily", "weekly", "monthly"} and len(full_df) > DRAW_DATA_LIMIT:
        return full_df.tail(DRAW_DATA_LIMIT).copy().reset_index(drop=True)
    return full_df.copy().reset_index(drop=True)


def _select_plot_df(cached_data: dict, visible_count_value: str | None):
    if visible_count_value == "all":
        return cached_data["full_df"]
    return cached_data["plot_df"]


def _format_data_message(message: str, visible_count_value: str | None) -> str:
    if visible_count_value == "all":
        return f"{message} 全部数据模式可能影响流畅度。"
    return message


def _is_performance_mode(value: list[str] | None) -> bool:
    return value is None or "on" in value


def _normalize_stock_code(stock_code: str | None) -> str:
    return (stock_code or DEFAULT_STOCK_CODE).strip().upper() or DEFAULT_STOCK_CODE


def _make_cache_key(stock_code: str | None, period: str | None) -> str:
    return f"{ANALYSIS_VERSION}_{_normalize_stock_code(stock_code)}_{period or DEFAULT_PERIOD}"


def _extract_relayout_xaxis_range(relayout_data: dict | None) -> list[float] | None:
    if not relayout_data:
        return None

    if "xaxis.range[0]" in relayout_data and "xaxis.range[1]" in relayout_data:
        return [relayout_data["xaxis.range[0]"], relayout_data["xaxis.range[1]"]]

    if "xaxis.range" in relayout_data:
        values = relayout_data["xaxis.range"]
        if isinstance(values, list) and len(values) == 2:
            return values

    if "xaxis2.range[0]" in relayout_data and "xaxis2.range[1]" in relayout_data:
        return [relayout_data["xaxis2.range[0]"], relayout_data["xaxis2.range[1]"]]

    if "xaxis2.range" in relayout_data:
        values = relayout_data["xaxis2.range"]
        if isinstance(values, list) and len(values) == 2:
            return values

    return None


def _normalize_range_tuple(xaxis_range: list[float]) -> tuple[float, float] | None:
    try:
        start_x = float(xaxis_range[0])
        end_x = float(xaxis_range[1])
    except (TypeError, ValueError, IndexError):
        return None

    if start_x > end_x:
        start_x, end_x = end_x, start_x
    return (round(start_x, 4), round(end_x, 4))


def _ranges_close(
    previous_range: tuple[float, float] | None,
    current_range: tuple[float, float],
) -> bool:
    if previous_range is None:
        return False
    return (
        abs(previous_range[0] - current_range[0]) < 0.0001
        and abs(previous_range[1] - current_range[1]) < 0.0001
    )


def _index_string() -> str:
    return """
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            * { box-sizing: border-box; }
            html, body { margin: 0; min-height: 100%; background: #000; }
            body { font-family: "Microsoft YaHei", SimHei, Arial, sans-serif; color: #f1f1f1; }
            .app-shell { min-height: 100vh; background: #000; }
            .top-toolbar {
                min-height: 64px;
                display: flex;
                align-items: center;
                gap: 16px;
                padding: 10px 16px;
                border-bottom: 1px solid rgba(255, 0, 0, 0.55);
                background: #080808;
                flex-wrap: wrap;
            }
            .brand { color: #ff3030; font-size: 20px; font-weight: 700; margin-right: 2px; }
            .field { display: flex; align-items: center; gap: 8px; }
            .field label { color: #ddd; font-size: 14px; white-space: nowrap; }
            .stock-input {
                width: 112px;
                height: 34px;
                color: #fff;
                background: #101010;
                border: 1px solid rgba(255, 0, 0, 0.7);
                border-radius: 3px;
                padding: 0 9px;
                outline: none;
            }
            .period-field { min-width: 164px; }
            .visible-count-field { min-width: 184px; }
            .period-select { width: 104px; color: #fff; }
            .visible-count-select { width: 92px; color: #fff; }
            .period-select .Select-control,
            .period-select .Select-menu-outer,
            .period-select .Select-value,
            .period-select .Select-placeholder,
            .visible-count-select .Select-control,
            .visible-count-select .Select-menu-outer,
            .visible-count-select .Select-value,
            .visible-count-select .Select-placeholder {
                background: #101010;
                color: #fff;
                border-color: rgba(255, 0, 0, 0.7);
            }
            .period-select .Select-value-label,
            .period-select .Select-option,
            .visible-count-select .Select-value-label,
            .visible-count-select .Select-option { color: #fff !important; }
            .period-select .Select-option,
            .visible-count-select .Select-option { background: #101010; }
            .period-select .Select-option.is-focused,
            .visible-count-select .Select-option.is-focused { background: #2a0000; }
            .period-select .Select-arrow,
            .visible-count-select .Select-arrow { border-color: #ff3030 transparent transparent; }
            .display-options { display: flex; align-items: center; min-height: 34px; }
            .performance-mode { display: flex; align-items: center; min-height: 34px; }
            .option-label {
                color: #e8e8e8;
                font-size: 14px;
                margin-right: 14px;
                white-space: nowrap;
            }
            .option-input { margin-right: 5px; accent-color: #ff3030; }
            .data-source-hint {
                min-height: 32px;
                padding: 7px 16px;
                border-bottom: 1px solid rgba(255, 0, 0, 0.38);
                background: #050505;
                font-size: 14px;
                line-height: 18px;
            }
            .source-real { color: #33d17a; }
            .source-demo { color: #ffd400; }
            .source-missing { color: #ff6b6b; }
            .source-error { color: #ff6b6b; }
            .kline-chart { height: calc(100vh - 96px); min-height: 900px; }
            @media (max-width: 760px) {
                .top-toolbar { align-items: flex-start; gap: 10px 12px; }
                .brand { width: 100%; }
                .display-options { width: 100%; }
                .option-label { margin-bottom: 8px; }
                .kline-chart { height: 900px; }
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
"""
