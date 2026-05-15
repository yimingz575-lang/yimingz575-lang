from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.data.market_data_center import (
    MARKET_DATA_RELATIVE_DIR,
    STANDARD_KLINE_COLUMNS,
    get_kline_path,
    load_kline,
)

CSV_COLUMNS = ["date", "open", "high", "low", "close", "volume"]
EMPTY_COLUMNS = ["date", "datetime", "open", "high", "low", "close", "volume", "amount"]
SAMPLE_DEMO_RELATIVE_PATH = Path("data") / "sample" / "sample_demo_daily.csv"
REAL_DATA_RELATIVE_DIR = Path("data") / "real"


@dataclass(frozen=True)
class KLineDataResult:
    df: pd.DataFrame
    source_kind: str
    source_label: str
    display_stock_code: str
    message: str
    csv_path: Path


def load_kline_data(
    project_root: str | Path,
    stock_code: str | None = None,
    period: str = "daily",
    max_bars: int | None = None,
) -> KLineDataResult:
    """Load local K-line data without triggering network downloads."""
    root = Path(project_root)
    clean_stock_code = (stock_code or "").strip().upper()
    clean_period = period or "daily"

    if clean_stock_code == "DEMO":
        return _load_demo_result(root)

    if not clean_stock_code:
        return _make_empty_result(
            root=root,
            stock_code="",
            period=clean_period,
            source_kind="missing",
            source_label="未选择股票",
            message="请先添加或选择股票，再下载/读取本地K线数据。",
        )

    try:
        df = load_kline(clean_stock_code, clean_period, max_bars=max_bars, project_root=root)
    except FileNotFoundError as exc:
        return _make_empty_result(
            root=root,
            stock_code=clean_stock_code,
            period=clean_period,
            source_kind="missing",
            source_label="本地数据不存在",
            message=str(exc),
        )
    except Exception as exc:
        error_message = str(exc) or exc.__class__.__name__
        return _make_empty_result(
            root=root,
            stock_code=clean_stock_code,
            period=clean_period,
            source_kind="error",
            source_label="本地数据读取失败",
            message=f"本地K线数据读取失败：{error_message}",
        )

    csv_path = Path(df.attrs.get("csv_path", get_kline_path(clean_stock_code, clean_period, root)))
    source_kind = str(df.attrs.get("source_kind", "real"))
    source_label = "旧路径行情数据" if source_kind == "legacy" else "本地行情数据"
    total_count = int(df.attrs.get("total_count", len(df)))
    actual_count = int(df.attrs.get("actual_count", len(df)))
    path_text = _relative_text(root, csv_path)
    return KLineDataResult(
        df=df,
        source_kind="real" if source_kind in {"real", "legacy"} else source_kind,
        source_label=source_label,
        display_stock_code=clean_stock_code,
        message=(
            f"当前数据来源：{source_label}（{path_text}）。"
            f"本地总K线 {total_count} 根，实际分析 {actual_count} 根。"
        ),
        csv_path=csv_path,
    )


def load_demo_csv(project_root: str | Path) -> pd.DataFrame:
    """Load or create the clearly named demo CSV."""
    root = Path(project_root)
    path = root / SAMPLE_DEMO_RELATIVE_PATH
    if not path.exists():
        create_sample_csv(path)
    return load_csv(path)


def load_real_csv(project_root: str | Path, stock_code: str, period: str = "daily") -> pd.DataFrame:
    """Compatibility helper: load real stock CSV through the new local data center."""
    return load_kline(stock_code, period, project_root=project_root)


def load_or_create_sample_csv(csv_path: str | Path | None = None) -> pd.DataFrame:
    """Compatibility helper: demo data is only created as sample_demo_daily.csv."""
    path = Path(csv_path) if csv_path is not None else SAMPLE_DEMO_RELATIVE_PATH
    if path.name != SAMPLE_DEMO_RELATIVE_PATH.name:
        path = path.parent / SAMPLE_DEMO_RELATIVE_PATH.name
    if not path.exists():
        create_sample_csv(path)
    return load_csv(path)


def load_csv(csv_path: str | Path) -> pd.DataFrame:
    """Read a K-line CSV and normalize it for the chart/Chan pipeline."""
    path = Path(csv_path)
    df = pd.read_csv(path)
    if "date" not in df.columns and "datetime" in df.columns:
        df = df.rename(columns={"datetime": "date"})

    missing_columns = [column for column in CSV_COLUMNS if column not in df.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"CSV file {path} is missing required columns: {missing}")

    result = df.copy()
    result["date"] = pd.to_datetime(result["date"], errors="coerce")
    for column in ["open", "high", "low", "close", "volume"]:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    if "amount" not in result.columns:
        result["amount"] = 0
    result["amount"] = pd.to_numeric(result["amount"], errors="coerce").fillna(0)
    result["datetime"] = result["date"]

    result = result.dropna(subset=["date", "open", "high", "low", "close"])
    result = result.sort_values("date")
    result = result.drop_duplicates(subset=["date"], keep="last")
    return result[EMPTY_COLUMNS].reset_index(drop=True)


def create_sample_csv(csv_path: str | Path, rows: int = 260) -> Path:
    """Create a clearly marked demo daily K-line CSV with fields required by the app."""
    path = Path(csv_path)
    if path.name != SAMPLE_DEMO_RELATIVE_PATH.name:
        path = path.parent / SAMPLE_DEMO_RELATIVE_PATH.name
    path.parent.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(20260511)
    dates = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=rows)

    trend = np.linspace(0, 4.5, rows)
    noise = rng.normal(loc=0.0, scale=0.32, size=rows).cumsum()
    close = np.maximum(8.0, 18.0 + trend + noise)
    open_price = np.roll(close, 1) + rng.normal(loc=0.0, scale=0.18, size=rows)
    open_price[0] = close[0] + rng.normal(loc=0.0, scale=0.18)

    high = np.maximum(open_price, close) + rng.uniform(0.05, 0.75, size=rows)
    low = np.minimum(open_price, close) - rng.uniform(0.05, 0.75, size=rows)
    volume = rng.integers(80_000, 680_000, size=rows)

    df = pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "open": np.round(open_price, 2),
            "high": np.round(high, 2),
            "low": np.round(low, 2),
            "close": np.round(close, 2),
            "volume": volume,
        }
    )
    df.to_csv(path, index=False, columns=CSV_COLUMNS, encoding="utf-8")
    return path


def _load_demo_result(project_root: Path) -> KLineDataResult:
    demo_path = project_root / SAMPLE_DEMO_RELATIVE_PATH
    df = load_demo_csv(project_root)
    demo_path_text = SAMPLE_DEMO_RELATIVE_PATH.as_posix()
    return KLineDataResult(
        df=df,
        source_kind="demo",
        source_label="示例模拟数据",
        display_stock_code="DEMO",
        message=f"当前数据来源：示例模拟数据（{demo_path_text}）。当前为示例模拟数据。",
        csv_path=demo_path,
    )


def _make_empty_result(
    root: Path,
    stock_code: str,
    period: str,
    source_kind: str,
    source_label: str,
    message: str,
) -> KLineDataResult:
    clean_stock = stock_code or "未选择"
    csv_path = root / MARKET_DATA_RELATIVE_DIR / clean_stock / f"{period}.csv"
    return KLineDataResult(
        df=pd.DataFrame(columns=EMPTY_COLUMNS),
        source_kind=source_kind,
        source_label=source_label,
        display_stock_code=clean_stock,
        message=message,
        csv_path=csv_path,
    )


def _relative_text(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path)
