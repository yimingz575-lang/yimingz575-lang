from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

CSV_COLUMNS = ["date", "open", "high", "low", "close", "volume"]
SUPPORTED_PERIODS = {"daily", "weekly", "monthly"}


def download_a_share_history(
    stock_code: str,
    period: str,
    output_path: str | Path,
) -> pd.DataFrame:
    """Download A-share history data from AkShare and save a normalized CSV."""
    if period not in SUPPORTED_PERIODS:
        raise ValueError("分钟周期数据源尚未接入，当前阶段只支持日线、周线、月线。")

    try:
        import akshare as ak
    except ModuleNotFoundError as exc:
        raise RuntimeError("缺少 akshare 依赖，请先执行 pip install -r requirements.txt") from exc

    end_date = datetime.now().strftime("%Y%m%d")
    raw_df = ak.stock_zh_a_hist(
        symbol=stock_code,
        period=period,
        start_date="19900101",
        end_date=end_date,
        adjust="",
    )
    if raw_df is None or raw_df.empty:
        raise ValueError("未获取到该股票数据，请检查股票代码或网络")

    df = _normalize_akshare_df(raw_df)
    if df.empty:
        raise ValueError("未获取到该股票数据，请检查股票代码或网络")

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, columns=CSV_COLUMNS, encoding="utf-8-sig")
    return df


def _normalize_akshare_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "日期": "date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
    }
    df = raw_df.rename(columns=rename_map)

    missing_columns = [column for column in CSV_COLUMNS if column not in df.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"AkShare 返回数据缺少字段：{missing}")

    df = df[CSV_COLUMNS].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for column in ["open", "high", "low", "close", "volume"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.dropna(subset=["date", "open", "high", "low", "close"])
    df = df.sort_values("date").reset_index(drop=True)
    return df
