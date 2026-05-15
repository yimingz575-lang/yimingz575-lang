from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.data.providers.base_provider import BaseKlineProvider

STANDARD_KLINE_COLUMNS = ["datetime", "open", "high", "low", "close", "volume", "amount"]
DAILY_PERIODS = {"daily", "weekly", "monthly"}
MINUTE_PERIOD_MAP = {
    "1m": "1",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "60m": "60",
}
SUPPORTED_PERIODS = ("1m", "5m", "15m", "30m", "60m", "daily", "weekly", "monthly")


class AkShareProvider(BaseKlineProvider):
    """AKShare-backed A-share K-line provider."""

    def fetch_kline(
        self,
        symbol: str,
        period: str,
        start: datetime | pd.Timestamp | str | None = None,
        end: datetime | pd.Timestamp | str | None = None,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        clean_symbol = _normalize_symbol(symbol)
        clean_period = (period or "").strip()
        if clean_period not in SUPPORTED_PERIODS:
            raise ValueError(f"AKShare provider 暂不支持周期: {period}")

        try:
            import akshare as ak
        except ModuleNotFoundError as exc:
            raise RuntimeError("缺少 akshare 依赖，请先执行 pip install -r requirements.txt") from exc

        if clean_period in DAILY_PERIODS:
            raw_df = ak.stock_zh_a_hist(
                symbol=clean_symbol,
                period=clean_period,
                start_date=_format_daily_start(start),
                end_date=_format_daily_end(end),
                adjust=adjust or "",
            )
        else:
            raw_df = ak.stock_zh_a_hist_min_em(
                symbol=clean_symbol,
                start_date=_format_minute_start(start),
                end_date=_format_minute_end(end),
                period=MINUTE_PERIOD_MAP[clean_period],
                adjust=adjust or "",
            )

        if raw_df is None or raw_df.empty:
            raise ValueError(f"AKShare 未返回数据: {clean_symbol} {clean_period}")
        return normalize_akshare_kline(raw_df)


def normalize_akshare_kline(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize AKShare Chinese/English columns to the local CSV schema."""
    rename_map = {
        "日期": "datetime",
        "时间": "datetime",
        "datetime": "datetime",
        "date": "datetime",
        "开盘": "open",
        "open": "open",
        "最高": "high",
        "high": "high",
        "最低": "low",
        "low": "low",
        "收盘": "close",
        "close": "close",
        "成交量": "volume",
        "volume": "volume",
        "成交额": "amount",
        "amount": "amount",
    }
    df = raw_df.rename(columns={column: rename_map.get(str(column).strip(), column) for column in raw_df.columns})
    df = df.loc[:, ~df.columns.duplicated()].copy()

    missing_price_columns = [
        column for column in ["datetime", "open", "high", "low", "close"] if column not in df.columns
    ]
    if missing_price_columns:
        missing = ", ".join(missing_price_columns)
        raise ValueError(f"AKShare 返回数据缺少字段: {missing}")

    for optional_column in ["volume", "amount"]:
        if optional_column not in df.columns:
            df[optional_column] = 0

    df = df[STANDARD_KLINE_COLUMNS].copy()
    return normalize_standard_kline(df)


def normalize_standard_kline(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if "datetime" not in result.columns and "date" in result.columns:
        result = result.rename(columns={"date": "datetime"})
    if "datetime" not in result.columns:
        raise ValueError("K线数据缺少 datetime 字段")

    result["datetime"] = pd.to_datetime(result["datetime"], errors="coerce")
    for column in ["open", "high", "low", "close"]:
        if column not in result.columns:
            raise ValueError(f"K线数据缺少 {column} 字段")
        result[column] = pd.to_numeric(result[column], errors="coerce")

    for optional_column in ["volume", "amount"]:
        if optional_column not in result.columns:
            result[optional_column] = 0
        result[optional_column] = pd.to_numeric(result[optional_column], errors="coerce").fillna(0)

    result = result.dropna(subset=["datetime", "open", "high", "low", "close"])
    result = result.sort_values("datetime")
    result = result.drop_duplicates(subset=["datetime"], keep="last")
    return result[STANDARD_KLINE_COLUMNS].reset_index(drop=True)


def _normalize_symbol(symbol: str) -> str:
    clean_symbol = (symbol or "").strip().upper()
    if not (clean_symbol.isdigit() and len(clean_symbol) == 6):
        raise ValueError("请输入 6 位 A 股股票代码")
    return clean_symbol


def _to_timestamp(value: datetime | pd.Timestamp | str | None) -> pd.Timestamp | None:
    if value is None:
        return None
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    return pd.Timestamp(timestamp)


def _format_daily_start(value: datetime | pd.Timestamp | str | None) -> str:
    timestamp = _to_timestamp(value) or pd.Timestamp("1990-01-01")
    return timestamp.strftime("%Y%m%d")


def _format_daily_end(value: datetime | pd.Timestamp | str | None) -> str:
    timestamp = _to_timestamp(value) or pd.Timestamp.now()
    return timestamp.strftime("%Y%m%d")


def _format_minute_start(value: datetime | pd.Timestamp | str | None) -> str:
    timestamp = _to_timestamp(value) or pd.Timestamp("1979-09-01 09:32:00")
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


def _format_minute_end(value: datetime | pd.Timestamp | str | None) -> str:
    timestamp = _to_timestamp(value) or pd.Timestamp.now()
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")
