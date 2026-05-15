from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.data.providers.akshare_provider import AkShareProvider

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WATCHLIST_RELATIVE_PATH = Path("config") / "watchlist.json"
MARKET_DATA_RELATIVE_DIR = Path("data") / "market"
LEGACY_REAL_RELATIVE_DIR = Path("data") / "real"
CHAN_CACHE_RELATIVE_DIR = Path("data") / "cache"

SUPPORTED_PERIODS = ("1m", "5m", "15m", "30m", "60m", "daily", "weekly", "monthly")
PERIOD_LABELS = {
    "1m": "1分钟",
    "5m": "5分钟",
    "15m": "15分钟",
    "30m": "30分钟",
    "60m": "60分钟",
    "daily": "日线",
    "weekly": "周线",
    "monthly": "月线",
}
DEFAULT_MAX_BARS_BY_PERIOD = {
    "1m": 20000,
    "5m": 12000,
    "15m": 8000,
    "30m": 5000,
    "60m": 4000,
    "daily": 1000,
    "weekly": 500,
    "monthly": 300,
}
STANDARD_KLINE_COLUMNS = ["datetime", "open", "high", "low", "close", "volume", "amount"]
CHART_KLINE_COLUMNS = ["date", "open", "high", "low", "close", "volume"]


@dataclass(frozen=True)
class UpdateKlineResult:
    symbol: str
    period: str
    success: bool
    message: str
    csv_path: Path
    old_count: int = 0
    fetched_count: int = 0
    saved_count: int = 0
    added_count: int = 0
    duplicate_count: int = 0
    start_datetime: str | None = None
    end_datetime: str | None = None


def load_watchlist(project_root: str | Path | None = None) -> list[dict[str, Any]]:
    """Read config/watchlist.json, creating an empty stock pool when missing."""
    root = _resolve_root(project_root)
    path = _watchlist_path(root)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        _write_watchlist_payload(path, {"stocks": []})
        print(f"[market_data_center] created watchlist = {_relative_text(root, path)}")
        return []

    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    stocks = payload.get("stocks", [])
    if not isinstance(stocks, list):
        raise ValueError(f"watchlist 格式错误: {_relative_text(root, path)}")
    return [_normalize_stock_record(stock) for stock in stocks if _normalize_symbol_value(stock.get("symbol"))]


def save_watchlist(stocks: list[dict[str, Any]], project_root: str | Path | None = None) -> None:
    """Save the local stock pool to config/watchlist.json."""
    root = _resolve_root(project_root)
    path = _watchlist_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)

    normalized_by_symbol: dict[str, dict[str, Any]] = {}
    for stock in stocks:
        record = _normalize_stock_record(stock)
        normalized_by_symbol[record["symbol"]] = record
    _write_watchlist_payload(path, {"stocks": list(normalized_by_symbol.values())})
    print(f"[market_data_center] saved watchlist = {_relative_text(root, path)}")


def add_stock(
    symbol: str,
    name: str | None = None,
    market: str = "A股",
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Add or re-enable a stock in the local stock pool."""
    clean_symbol = _normalize_symbol(symbol)
    root = _resolve_root(project_root)
    stocks = load_watchlist(root)
    today = date.today().isoformat()
    clean_name = (name or "").strip()

    for stock in stocks:
        if stock["symbol"] == clean_symbol:
            if clean_name:
                stock["name"] = clean_name
            stock["market"] = market or stock.get("market") or "A股"
            stock["enabled"] = True
            stock["updated_at"] = today
            save_watchlist(stocks, root)
            return stock

    record = {
        "symbol": clean_symbol,
        "name": clean_name,
        "market": market or "A股",
        "enabled": True,
        "created_at": today,
        "updated_at": today,
    }
    stocks.append(record)
    save_watchlist(stocks, root)
    print(f"[market_data_center] added stock = {clean_symbol}")
    return record


def remove_stock(symbol: str, project_root: str | Path | None = None) -> None:
    """Remove a stock from config/watchlist.json without deleting local CSV files."""
    clean_symbol = _normalize_symbol(symbol)
    root = _resolve_root(project_root)
    stocks = [stock for stock in load_watchlist(root) if stock["symbol"] != clean_symbol]
    save_watchlist(stocks, root)
    print(f"[market_data_center] removed stock = {clean_symbol}")


def list_local_stocks(project_root: str | Path | None = None) -> list[dict[str, Any]]:
    """Return enabled watchlist stocks plus metadata about saved local periods."""
    root = _resolve_root(project_root)
    stocks = []
    for stock in load_watchlist(root):
        if not stock.get("enabled", True):
            continue
        record = dict(stock)
        record["available_periods"] = list_available_periods(record["symbol"], root)
        stocks.append(record)
    return stocks


def list_available_periods(symbol: str, project_root: str | Path | None = None) -> list[str]:
    """Return periods that already have a local CSV for one stock."""
    clean_symbol = _normalize_symbol(symbol)
    root = _resolve_root(project_root)
    available = []
    for period in SUPPORTED_PERIODS:
        if get_kline_path(clean_symbol, period, root).exists() or _legacy_kline_path(clean_symbol, period, root).exists():
            available.append(period)
    return available


def get_kline_path(symbol: str, period: str, project_root: str | Path | None = None) -> Path:
    """Return data/market/{symbol}/{period}.csv."""
    clean_symbol = _normalize_symbol(symbol)
    clean_period = _normalize_period(period)
    root = _resolve_root(project_root)
    return root / MARKET_DATA_RELATIVE_DIR / clean_symbol / f"{clean_period}.csv"


def load_kline(
    symbol: str,
    period: str,
    max_bars: int | None = None,
    project_root: str | Path | None = None,
) -> pd.DataFrame:
    """Load one local stock/period K-line CSV with new-path-first legacy compatibility."""
    clean_symbol = _normalize_symbol(symbol)
    clean_period = _normalize_period(period)
    root = _resolve_root(project_root)
    csv_path, source_kind = _resolve_existing_kline_path(clean_symbol, clean_period, root)
    if csv_path is None:
        expected_path = get_kline_path(clean_symbol, clean_period, root)
        expected_text = _relative_text(root, expected_path)
        raise FileNotFoundError(f"本地不存在 {expected_text}，请先点击“下载/更新当前周期”。")

    print(f"[market_data_center] current symbol = {clean_symbol}")
    print(f"[market_data_center] current period = {clean_period}")
    print(f"[market_data_center] reading csv path = {_relative_text(root, csv_path)}")
    standard_df = _read_standard_kline_csv(csv_path)
    total_count = len(standard_df)
    if max_bars is not None and max_bars > 0:
        standard_df = standard_df.tail(max_bars).copy().reset_index(drop=True)
    actual_count = len(standard_df)
    print(f"[market_data_center] local total kline count = {total_count}")
    print(f"[market_data_center] actual kline count for chan = {actual_count}")

    chart_df = _to_chart_kline(standard_df)
    chart_df.attrs["csv_path"] = str(csv_path)
    chart_df.attrs["source_kind"] = source_kind
    chart_df.attrs["total_count"] = total_count
    chart_df.attrs["actual_count"] = actual_count
    return chart_df


def save_kline(
    symbol: str,
    period: str,
    df: pd.DataFrame,
    project_root: str | Path | None = None,
) -> Path:
    """Save normalized K-line data as UTF-8 CSV to data/market/{symbol}/{period}.csv."""
    clean_symbol = _normalize_symbol(symbol)
    clean_period = _normalize_period(period)
    root = _resolve_root(project_root)
    path = get_kline_path(clean_symbol, clean_period, root)
    standard_df = _normalize_standard_kline(df)
    path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = path.with_name(f"{path.name}.tmp")
    standard_df.to_csv(temp_path, index=False, columns=STANDARD_KLINE_COLUMNS, encoding="utf-8")
    temp_path.replace(path)
    print(f"[market_data_center] saved csv path = {_relative_text(root, path)}")
    print(f"[market_data_center] saved kline count = {len(standard_df)}")
    return path


def update_kline(
    symbol: str,
    period: str,
    project_root: str | Path | None = None,
    provider: Any | None = None,
    adjust: str = "qfq",
) -> UpdateKlineResult:
    """Download or incrementally update one stock/period K-line CSV."""
    clean_symbol = _normalize_symbol(symbol)
    clean_period = _normalize_period(period)
    root = _resolve_root(project_root)
    target_path = get_kline_path(clean_symbol, clean_period, root)
    add_stock(clean_symbol, project_root=root)

    old_df = _load_existing_standard_kline(clean_symbol, clean_period, root)
    old_count = len(old_df)
    start = old_df["datetime"].max() if old_count else None
    provider = provider or AkShareProvider()

    try:
        print(f"[market_data_center] download symbol = {clean_symbol}")
        print(f"[market_data_center] download period = {clean_period}")
        print(f"[market_data_center] download start = {start}")
        fetched_df = provider.fetch_kline(clean_symbol, clean_period, start=start, adjust=adjust)
        fetched_df = _normalize_standard_kline(fetched_df)
        if fetched_df.empty:
            raise ValueError("数据源没有返回有效K线")

        downloaded_start = fetched_df["datetime"].min()
        downloaded_end = fetched_df["datetime"].max()
        print(f"[market_data_center] downloaded range = {downloaded_start} -> {downloaded_end}")

        old_datetimes = set(old_df["datetime"]) if old_count else set()
        combined = pd.concat([old_df, fetched_df], ignore_index=True)
        before_dedupe_count = len(combined)
        combined = _normalize_standard_kline(combined)
        duplicate_count = before_dedupe_count - len(combined)
        added_count = len([value for value in combined["datetime"] if value not in old_datetimes])
        saved_path = save_kline(clean_symbol, clean_period, combined, root)
        saved_count = len(combined)

        print(f"[market_data_center] added kline count = {added_count}")
        print(f"[market_data_center] duplicate kline count removed = {duplicate_count}")
        return UpdateKlineResult(
            symbol=clean_symbol,
            period=clean_period,
            success=True,
            message=f"{clean_symbol} {clean_period} 更新成功，新增 {added_count} 根K线，删除重复 {duplicate_count} 根。",
            csv_path=saved_path,
            old_count=old_count,
            fetched_count=len(fetched_df),
            saved_count=saved_count,
            added_count=added_count,
            duplicate_count=duplicate_count,
            start_datetime=str(downloaded_start),
            end_datetime=str(downloaded_end),
        )
    except Exception as exc:
        reason = str(exc) or exc.__class__.__name__
        print(f"[market_data_center] download failed = {reason}")
        return UpdateKlineResult(
            symbol=clean_symbol,
            period=clean_period,
            success=False,
            message=f"{clean_symbol} {clean_period} 下载失败：{reason}。旧数据已保留。",
            csv_path=target_path,
            old_count=old_count,
        )


def get_chan_cache_path(symbol: str, period: str, project_root: str | Path | None = None) -> Path:
    clean_symbol = _normalize_symbol(symbol)
    clean_period = _normalize_period(period)
    root = _resolve_root(project_root)
    return root / CHAN_CACHE_RELATIVE_DIR / clean_symbol / clean_period / "chan_cache.json"


def load_chan_cache(symbol: str, period: str, project_root: str | Path | None = None) -> dict[str, Any] | None:
    path = get_chan_cache_path(symbol, period, project_root)
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def save_chan_cache(
    symbol: str,
    period: str,
    result: dict[str, Any],
    project_root: str | Path | None = None,
) -> Path:
    path = get_chan_cache_path(symbol, period, project_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(result, file, ensure_ascii=False, indent=2, default=str)
    return path


def _resolve_root(project_root: str | Path | None = None) -> Path:
    return Path(project_root).resolve() if project_root is not None else PROJECT_ROOT


def _watchlist_path(root: Path) -> Path:
    return root / WATCHLIST_RELATIVE_PATH


def _write_watchlist_payload(path: Path, payload: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
        file.write("\n")


def _normalize_symbol(symbol: str) -> str:
    clean_symbol = _normalize_symbol_value(symbol)
    if not (clean_symbol.isdigit() and len(clean_symbol) == 6):
        raise ValueError("请输入 6 位 A 股股票代码")
    return clean_symbol


def _normalize_symbol_value(symbol: Any) -> str:
    return (str(symbol or "")).strip().upper()


def _normalize_period(period: str) -> str:
    clean_period = (period or "").strip()
    if clean_period not in SUPPORTED_PERIODS:
        raise ValueError(f"暂不支持周期: {period}")
    return clean_period


def _normalize_stock_record(stock: dict[str, Any]) -> dict[str, Any]:
    clean_symbol = _normalize_symbol_value(stock.get("symbol"))
    today = date.today().isoformat()
    return {
        "symbol": clean_symbol,
        "name": (stock.get("name") or "").strip(),
        "market": (stock.get("market") or "A股").strip(),
        "enabled": bool(stock.get("enabled", True)),
        "created_at": stock.get("created_at") or today,
        "updated_at": stock.get("updated_at") or today,
    }


def _resolve_existing_kline_path(symbol: str, period: str, root: Path) -> tuple[Path | None, str]:
    new_path = get_kline_path(symbol, period, root)
    if new_path.exists():
        return new_path, "real"
    legacy_path = _legacy_kline_path(symbol, period, root)
    if legacy_path.exists():
        return legacy_path, "legacy"
    return None, "missing"


def _legacy_kline_path(symbol: str, period: str, root: Path) -> Path:
    return root / LEGACY_REAL_RELATIVE_DIR / f"{symbol}_{period}.csv"


def _load_existing_standard_kline(symbol: str, period: str, root: Path) -> pd.DataFrame:
    path, _ = _resolve_existing_kline_path(symbol, period, root)
    if path is None:
        return pd.DataFrame(columns=STANDARD_KLINE_COLUMNS)
    return _read_standard_kline_csv(path)


def _read_standard_kline_csv(path: Path) -> pd.DataFrame:
    raw_df = pd.read_csv(path)
    before_count = len(raw_df)
    standard_df = _normalize_standard_kline(raw_df)
    duplicate_count = before_count - len(standard_df)
    if duplicate_count:
        print(f"[market_data_center] invalid_or_duplicate_rows_removed = {duplicate_count}")
    return standard_df


def _normalize_standard_kline(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if "datetime" not in result.columns and "date" in result.columns:
        result = result.rename(columns={"date": "datetime"})
    if "datetime" not in result.columns:
        raise ValueError("K线CSV缺少 datetime 字段")

    result["datetime"] = pd.to_datetime(result["datetime"], errors="coerce")
    for column in ["open", "high", "low", "close"]:
        if column not in result.columns:
            raise ValueError(f"K线CSV缺少 {column} 字段")
        result[column] = pd.to_numeric(result[column], errors="coerce")

    for optional_column in ["volume", "amount"]:
        if optional_column not in result.columns:
            result[optional_column] = 0
        result[optional_column] = pd.to_numeric(result[optional_column], errors="coerce").fillna(0)

    result = result.dropna(subset=["datetime", "open", "high", "low", "close"])
    result = result.sort_values("datetime")
    result = result.drop_duplicates(subset=["datetime"], keep="last")
    return result[STANDARD_KLINE_COLUMNS].reset_index(drop=True)


def _to_chart_kline(standard_df: pd.DataFrame) -> pd.DataFrame:
    chart_df = standard_df.copy()
    chart_df["date"] = chart_df["datetime"]
    return chart_df[["date", "datetime", "open", "high", "low", "close", "volume", "amount"]]


def _relative_text(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path)

