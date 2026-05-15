from __future__ import annotations

from pathlib import Path

from dash import Dash, Input, Output, Patch, State, ctx, dcc, html, no_update

from src.data.market_data_center import (
    DEFAULT_MAX_BARS_BY_PERIOD,
    PERIOD_LABELS,
    SUPPORTED_PERIODS,
    add_stock,
    list_local_stocks,
    remove_stock,
    update_kline,
)
from src.data_source.csv_loader import load_kline_data
from src.ui.chart import calculate_visible_yaxis_ranges, create_kline_figure, prepare_chart_data

DEFAULT_STOCK_CODE = ""
DEFAULT_PERIOD = "daily"
DEFAULT_VISIBLE_COUNT_VALUE = "default"
DEFAULT_DISPLAY_OPTIONS = ["bi"]
ANALYSIS_VERSION = "market_data_center_v1_standard_bars_v4_visual_options"
DATA_CACHE: dict[str, dict] = {}
LAST_X_RANGE_CACHE: dict[str, tuple[float, float]] = {}

PERIOD_OPTIONS = [{"label": PERIOD_LABELS[period], "value": period} for period in SUPPORTED_PERIODS]

DISPLAY_OPTIONS = [
    {"label": "显示均线", "value": "ma"},
    {"label": "显示笔", "value": "bi"},
    {"label": "显示线段", "value": "segment"},
    {"label": "显示中枢", "value": "zone"},
    {"label": "显示买卖点", "value": "signal"},
]

VISIBLE_COUNT_OPTIONS = [
    {"label": "默认", "value": "default"},
    {"label": "1000", "value": "1000"},
    {"label": "3000", "value": "3000"},
    {"label": "5000", "value": "5000"},
    {"label": "8000", "value": "8000"},
    {"label": "12000", "value": "12000"},
    {"label": "20000", "value": "20000"},
    {"label": "30000", "value": "30000"},
    {"label": "50000", "value": "50000"},
]

PERFORMANCE_MODE_OPTIONS = [{"label": "性能模式", "value": "on"}]


def create_app(project_root: Path | None = None) -> Dash:
    """Build the Dash app with a dynamic local stock data center toolbar."""
    root = Path(project_root or Path.cwd())
    initial_stock_options = _make_stock_options(root)
    initial_stock_code = _pick_stock_value(initial_stock_options)
    initial_visible_count = _parse_visible_count(DEFAULT_VISIBLE_COUNT_VALUE, DEFAULT_PERIOD)
    initial_cache = _get_cached_chart_data(root, initial_stock_code, DEFAULT_PERIOD, initial_visible_count)
    initial_data = initial_cache["result"]
    initial_figure = create_kline_figure(
        _select_plot_df(initial_cache),
        stock_code=initial_data.display_stock_code,
        period_label=PERIOD_LABELS.get(DEFAULT_PERIOD, "日线"),
        display_options=DEFAULT_DISPLAY_OPTIONS,
        analysis_df=initial_cache["full_df"],
        visible_count=initial_visible_count,
        performance_mode=True,
    )

    app = Dash(__name__, title="缠论K线本地数据中心")
    app.index_string = _index_string()

    app.layout = html.Div(
        className="app-shell",
        children=[
            html.Div(
                className="top-toolbar",
                children=[
                    html.Div("缠论K线", className="brand"),
                    html.Div(
                        className="field stock-select-field",
                        children=[
                            html.Label("股票", htmlFor="stock-select"),
                            dcc.Dropdown(
                                id="stock-select",
                                options=initial_stock_options,
                                value=initial_stock_code,
                                clearable=False,
                                searchable=True,
                                placeholder="暂无股票",
                                className="stock-select",
                            ),
                        ],
                    ),
                    html.Div(
                        className="field new-stock-field",
                        children=[
                            html.Label("代码", htmlFor="new-stock-symbol-input"),
                            dcc.Input(
                                id="new-stock-symbol-input",
                                type="text",
                                placeholder="600497",
                                debounce=True,
                                className="stock-input",
                            ),
                        ],
                    ),
                    html.Div(
                        className="field stock-name-field",
                        children=[
                            html.Label("名称", htmlFor="new-stock-name-input"),
                            dcc.Input(
                                id="new-stock-name-input",
                                type="text",
                                placeholder="可选",
                                debounce=True,
                                className="stock-name-input",
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
                            html.Label("K线数量", htmlFor="visible-count-select"),
                            dcc.Dropdown(
                                id="visible-count-select",
                                options=VISIBLE_COUNT_OPTIONS,
                                value=DEFAULT_VISIBLE_COUNT_VALUE,
                                clearable=False,
                                searchable=False,
                                className="visible-count-select",
                            ),
                        ],
                    ),
                    html.Div(
                        className="action-group",
                        children=[
                            html.Button("添加股票", id="add-stock-button", n_clicks=0, className="toolbar-button"),
                            html.Button(
                                "下载/更新当前周期",
                                id="download-current-button",
                                n_clicks=0,
                                className="toolbar-button primary",
                            ),
                            html.Button(
                                "下载/更新全部周期",
                                id="download-all-button",
                                n_clicks=0,
                                className="toolbar-button",
                            ),
                            html.Button("删除股票", id="delete-stock-button", n_clicks=0, className="toolbar-button danger"),
                            html.Button("刷新图表", id="refresh-chart-button", n_clicks=0, className="toolbar-button"),
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
                id="toolbar-action-status",
                className="toolbar-action-status",
                children="股票池来自 config/watchlist.json；添加股票后可按周期下载或读取本地CSV。",
            ),
            html.Div(
                id="data-source-hint",
                className=f"data-source-hint source-{initial_data.source_kind}",
                children=_format_data_message(initial_data.message, DEFAULT_VISIBLE_COUNT_VALUE, initial_visible_count),
            ),
            dcc.Graph(
                id="kline-chart",
                className="kline-chart",
                figure=initial_figure,
                clear_on_unhover=True,
                config={
                    "scrollZoom": True,
                    "displayModeBar": True,
                },
            ),
        ],
    )

    @app.callback(
        Output("stock-select", "options"),
        Output("stock-select", "value"),
        Output("toolbar-action-status", "children"),
        Input("add-stock-button", "n_clicks"),
        Input("delete-stock-button", "n_clicks"),
        State("new-stock-symbol-input", "value"),
        State("new-stock-name-input", "value"),
        State("stock-select", "value"),
        prevent_initial_call=True,
    )
    def update_stock_pool(
        add_clicks: int,
        delete_clicks: int,
        new_stock_symbol: str | None,
        new_stock_name: str | None,
        selected_stock: str | None,
    ):
        triggered_id = ctx.triggered_id
        if triggered_id == "add-stock-button":
            try:
                record = add_stock(new_stock_symbol or "", name=new_stock_name, project_root=root)
            except Exception as exc:
                return no_update, no_update, f"添加失败：{str(exc) or exc.__class__.__name__}"
            options = _make_stock_options(root)
            _clear_cache_for_stock(record["symbol"])
            return options, record["symbol"], f"已添加股票 {record['symbol']}。"

        if triggered_id == "delete-stock-button":
            clean_selected = _normalize_stock_code(selected_stock)
            if not clean_selected:
                return no_update, no_update, "请先选择要删除的股票。"
            try:
                remove_stock(clean_selected, project_root=root)
            except Exception as exc:
                return no_update, no_update, f"删除失败：{str(exc) or exc.__class__.__name__}"
            options = _make_stock_options(root)
            _clear_cache_for_stock(clean_selected)
            return options, _pick_stock_value(options), f"已从股票池删除 {clean_selected}。本地CSV文件未删除。"

        return no_update, no_update, no_update

    @app.callback(
        Output("kline-chart", "figure"),
        Output("data-source-hint", "children"),
        Output("data-source-hint", "className"),
        Output("toolbar-action-status", "children", allow_duplicate=True),
        Input("stock-select", "value"),
        Input("period-select", "value"),
        Input("visible-count-select", "value"),
        Input("display-options", "value"),
        Input("performance-mode", "value"),
        Input("refresh-chart-button", "n_clicks"),
        Input("download-current-button", "n_clicks"),
        Input("download-all-button", "n_clicks"),
        prevent_initial_call=True,
    )
    def update_chart(
        stock_code: str | None,
        period: str,
        visible_count_value: str,
        display_options: list[str] | None,
        performance_mode_value: list[str] | None,
        refresh_clicks: int,
        download_current_clicks: int,
        download_all_clicks: int,
    ):
        clean_stock_code = _normalize_stock_code(stock_code)
        clean_period = period or DEFAULT_PERIOD
        visible_count = _parse_visible_count(visible_count_value, clean_period)
        action_status = no_update
        triggered_id = ctx.triggered_id

        if triggered_id == "download-current-button":
            if not clean_stock_code:
                action_status = "请先选择股票，或输入代码后点击“添加股票”。"
            else:
                result = update_kline(clean_stock_code, clean_period, project_root=root)
                _clear_cache_for_stock_period(clean_stock_code, clean_period)
                action_status = _format_update_result(root, result)

        if triggered_id == "download-all-button":
            if not clean_stock_code:
                action_status = "请先选择股票，或输入代码后点击“添加股票”。"
            else:
                results = [update_kline(clean_stock_code, candidate_period, project_root=root) for candidate_period in SUPPORTED_PERIODS]
                _clear_cache_for_stock(clean_stock_code)
                action_status = _format_batch_update_result(results)

        if triggered_id == "refresh-chart-button":
            _clear_cache_for_stock_period(clean_stock_code, clean_period)
            action_status = "已从本地CSV刷新图表。"

        cached_data = _get_cached_chart_data(root, clean_stock_code, clean_period, visible_count)
        data_result = cached_data["result"]
        period_label = PERIOD_LABELS.get(clean_period, "日线")
        figure = create_kline_figure(
            _select_plot_df(cached_data),
            stock_code=data_result.display_stock_code,
            period_label=period_label,
            display_options=display_options,
            analysis_df=cached_data["full_df"],
            visible_count=visible_count,
            performance_mode=_is_performance_mode(performance_mode_value),
        )
        return (
            figure,
            _format_data_message(data_result.message, visible_count_value, visible_count),
            f"data-source-hint source-{data_result.source_kind}",
            action_status,
        )

    @app.callback(
        Output("kline-chart", "figure", allow_duplicate=True),
        Input("kline-chart", "relayoutData"),
        State("stock-select", "value"),
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

        clean_period = period or DEFAULT_PERIOD
        visible_count = _parse_visible_count(visible_count_value, clean_period)
        cache_key = _make_cache_key(stock_code, clean_period, visible_count)
        cached_data = DATA_CACHE.get(cache_key)
        if cached_data is None:
            return no_update
        normalized_range = _normalize_range_tuple(xaxis_range)
        if normalized_range is None:
            return no_update

        range_cache_key = f"{cache_key}_{visible_count_value or DEFAULT_VISIBLE_COUNT_VALUE}"
        if _ranges_close(LAST_X_RANGE_CACHE.get(range_cache_key), normalized_range):
            return no_update
        LAST_X_RANGE_CACHE[range_cache_key] = normalized_range

        plot_df = _select_plot_df(cached_data)
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

    @app.callback(
        Output("kline-chart", "figure", allow_duplicate=True),
        Input("kline-chart", "hoverData"),
        prevent_initial_call=True,
    )
    def update_vertical_cursor_line(hover_data: dict | None):
        figure_patch = Patch()
        hover_x = _extract_hover_x_value(hover_data)
        figure_patch["layout"]["shapes"] = (
            [_make_vertical_cursor_line_shape(hover_x)] if hover_x is not None else []
        )
        return figure_patch

    return app


def _make_stock_options(root: Path) -> list[dict[str, str]]:
    options = []
    for stock in list_local_stocks(root):
        symbol = stock["symbol"]
        name = (stock.get("name") or "").strip()
        label = f"{symbol} {name}" if name else symbol
        options.append({"label": label, "value": symbol})
    return options


def _pick_stock_value(options: list[dict[str, str]]) -> str | None:
    if not options:
        return None
    return options[0]["value"]


def _parse_visible_count(value: str | None, period: str | None = None) -> int:
    clean_period = period or DEFAULT_PERIOD
    default_count = DEFAULT_MAX_BARS_BY_PERIOD.get(clean_period, 1000)
    if value in {None, "", "default"}:
        return default_count
    try:
        return int(value)
    except ValueError:
        return default_count


def _get_cached_chart_data(root: Path, stock_code: str | None, period: str | None, max_bars: int) -> dict:
    clean_stock_code = _normalize_stock_code(stock_code)
    clean_period = period or DEFAULT_PERIOD
    cache_key = _make_cache_key(clean_stock_code, clean_period, max_bars)

    if cache_key not in DATA_CACHE:
        data_result = load_kline_data(root, clean_stock_code, clean_period, max_bars=max_bars)
        full_df = prepare_chart_data(data_result.df)
        DATA_CACHE[cache_key] = {
            "result": data_result,
            "full_df": full_df,
        }

    return DATA_CACHE[cache_key]


def _select_plot_df(cached_data: dict):
    return cached_data["full_df"]


def _format_data_message(message: str, visible_count_value: str | None, visible_count: int) -> str:
    if visible_count_value == "default":
        return f"{message} 当前使用周期默认K线数量：{visible_count}。"
    return f"{message} 当前选择K线数量：{visible_count}。"


def _format_update_result(root: Path, result) -> str:
    path_text = _relative_text(root, result.csv_path)
    if not result.success:
        return result.message
    return f"{result.message} 保存到 {path_text}，数据范围 {result.start_datetime} 至 {result.end_datetime}。"


def _format_batch_update_result(results: list) -> str:
    success_periods = [result.period for result in results if result.success]
    failed_periods = [f"{result.period}({result.message})" for result in results if not result.success]
    success_text = "、".join(success_periods) if success_periods else "无"
    failed_text = "；".join(failed_periods) if failed_periods else "无"
    return f"全部周期更新完成。成功：{success_text}。失败：{failed_text}。"


def _is_performance_mode(value: list[str] | None) -> bool:
    return value is None or "on" in value


def _normalize_stock_code(stock_code: str | None) -> str:
    return (stock_code or "").strip().upper()


def _make_cache_key(stock_code: str | None, period: str | None, max_bars: int | None = None) -> str:
    symbol_key = _normalize_stock_code(stock_code) or "NO_STOCK"
    bars_key = max_bars if max_bars is not None else "default"
    return f"{ANALYSIS_VERSION}_{symbol_key}_{period or DEFAULT_PERIOD}_{bars_key}"


def _clear_cache_for_stock(stock_code: str | None) -> None:
    clean_stock_code = _normalize_stock_code(stock_code)
    if not clean_stock_code:
        DATA_CACHE.clear()
        LAST_X_RANGE_CACHE.clear()
        return
    prefix = f"{ANALYSIS_VERSION}_{clean_stock_code}_"
    for key in list(DATA_CACHE):
        if key.startswith(prefix):
            DATA_CACHE.pop(key, None)
    for key in list(LAST_X_RANGE_CACHE):
        if key.startswith(prefix):
            LAST_X_RANGE_CACHE.pop(key, None)


def _clear_cache_for_stock_period(stock_code: str | None, period: str | None) -> None:
    clean_stock_code = _normalize_stock_code(stock_code)
    clean_period = period or DEFAULT_PERIOD
    if not clean_stock_code:
        DATA_CACHE.clear()
        LAST_X_RANGE_CACHE.clear()
        return
    prefix = f"{ANALYSIS_VERSION}_{clean_stock_code}_{clean_period}_"
    for key in list(DATA_CACHE):
        if key.startswith(prefix):
            DATA_CACHE.pop(key, None)
    for key in list(LAST_X_RANGE_CACHE):
        if key.startswith(prefix):
            LAST_X_RANGE_CACHE.pop(key, None)


def _extract_hover_x_value(hover_data: dict | None):
    if not hover_data:
        return None
    points = hover_data.get("points")
    if not points:
        return None
    return points[0].get("x")


def _make_vertical_cursor_line_shape(x_value) -> dict:
    return {
        "type": "line",
        "xref": "x",
        "yref": "paper",
        "x0": x_value,
        "x1": x_value,
        "y0": 0,
        "y1": 1,
        "line": {
            "color": "rgba(255, 255, 255, 0.8)",
            "width": 1,
            "dash": "dot",
        },
    }


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


def _relative_text(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path)


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
                min-height: 86px;
                display: flex;
                align-items: center;
                gap: 10px 12px;
                padding: 10px 16px;
                border-bottom: 1px solid rgba(255, 0, 0, 0.55);
                background: #080808;
                flex-wrap: wrap;
            }
            .brand { color: #ff3030; font-size: 20px; font-weight: 700; margin-right: 2px; }
            .field { display: flex; align-items: center; gap: 8px; min-height: 34px; }
            .field label { color: #ddd; font-size: 14px; white-space: nowrap; }
            .stock-select-field { min-width: 214px; }
            .stock-select { width: 150px; color: #fff; }
            .stock-input {
                width: 104px;
                height: 34px;
                color: #fff;
                background: #101010;
                border: 1px solid rgba(255, 0, 0, 0.7);
                border-radius: 3px;
                padding: 0 9px;
                outline: none;
            }
            .stock-name-input {
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
            .visible-count-field { min-width: 188px; }
            .period-select { width: 104px; color: #fff; }
            .visible-count-select { width: 98px; color: #fff; }
            .stock-select .Select-control,
            .stock-select .Select-menu-outer,
            .stock-select .Select-value,
            .stock-select .Select-placeholder,
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
            .stock-select .Select-value-label,
            .stock-select .Select-option,
            .period-select .Select-value-label,
            .period-select .Select-option,
            .visible-count-select .Select-value-label,
            .visible-count-select .Select-option { color: #fff !important; }
            .stock-select .Select-option,
            .period-select .Select-option,
            .visible-count-select .Select-option { background: #101010; }
            .stock-select .Select-option.is-focused,
            .period-select .Select-option.is-focused,
            .visible-count-select .Select-option.is-focused { background: #2a0000; }
            .stock-select .Select-arrow,
            .period-select .Select-arrow,
            .visible-count-select .Select-arrow { border-color: #ff3030 transparent transparent; }
            .action-group { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
            .toolbar-button {
                height: 34px;
                padding: 0 11px;
                border: 1px solid rgba(255, 0, 0, 0.7);
                border-radius: 3px;
                background: #141414;
                color: #f4f4f4;
                cursor: pointer;
                font-size: 14px;
            }
            .toolbar-button:hover { background: #260000; }
            .toolbar-button.primary { background: #5a0000; }
            .toolbar-button.danger { border-color: rgba(255, 91, 91, 0.8); color: #ff9b9b; }
            .display-options { display: flex; align-items: center; min-height: 34px; }
            .performance-mode { display: flex; align-items: center; min-height: 34px; }
            .option-label {
                color: #e8e8e8;
                font-size: 14px;
                margin-right: 14px;
                white-space: nowrap;
            }
            .option-input { margin-right: 5px; accent-color: #ff3030; }
            .toolbar-action-status {
                min-height: 30px;
                padding: 6px 16px;
                border-bottom: 1px solid rgba(255, 0, 0, 0.28);
                background: #050505;
                color: #d7d7d7;
                font-size: 13px;
                line-height: 18px;
            }
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
            .kline-chart { height: calc(100vh - 148px); min-height: 860px; }
            @media (max-width: 900px) {
                .top-toolbar { align-items: flex-start; gap: 10px 12px; }
                .brand { width: 100%; }
                .action-group { width: 100%; }
                .display-options { width: 100%; }
                .option-label { margin-bottom: 8px; }
                .kline-chart { height: 860px; }
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
