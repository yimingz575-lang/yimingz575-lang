from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.data_source.akshare_loader import download_a_share_history

CSV_COLUMNS = ["date", "open", "high", "low", "close", "volume"]
SAMPLE_DEMO_RELATIVE_PATH = Path("data") / "sample" / "sample_demo_daily.csv"
REAL_DATA_RELATIVE_DIR = Path("data") / "real"
SUPPORTED_REAL_PERIODS = {"daily", "weekly", "monthly"}


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
    stock_code: str | None = "DEMO",
    period: str = "daily",
) -> KLineDataResult:
    """Load demo data or real A-share data, downloading daily/weekly/monthly if missing."""
    root = Path(project_root)
    clean_stock_code = (stock_code or "DEMO").strip().upper() or "DEMO"
    clean_period = period or "daily"

    if clean_stock_code == "DEMO":
        return _load_demo_result(root)

    if not (clean_stock_code.isdigit() and len(clean_stock_code) == 6):
        return _make_empty_result(
            root=root,
            stock_code=clean_stock_code,
            period=clean_period,
            source_kind="error",
            source_label="代码格式错误",
            message="请输入6位A股股票代码",
        )

    if clean_period not in SUPPORTED_REAL_PERIODS:
        return _make_empty_result(
            root=root,
            stock_code=clean_stock_code,
            period=clean_period,
            source_kind="missing",
            source_label="分钟周期未接入",
            message="分钟周期数据源尚未接入，当前阶段只支持日线、周线、月线。",
        )

    real_relative_path = REAL_DATA_RELATIVE_DIR / f"{clean_stock_code}_{clean_period}.csv"
    real_path = root / real_relative_path
    if real_path.exists():
        df = load_csv(real_path)
        real_path_text = real_relative_path.as_posix()
        return KLineDataResult(
            df=df,
            source_kind="real",
            source_label="真实行情数据",
            display_stock_code=clean_stock_code,
            message=f"当前数据来源：真实行情数据（{real_path_text}）",
            csv_path=real_path,
        )

    try:
        df = download_a_share_history(clean_stock_code, clean_period, real_path)
    except Exception as exc:
        error_message = str(exc) or exc.__class__.__name__
        return _make_empty_result(
            root=root,
            stock_code=clean_stock_code,
            period=clean_period,
            source_kind="error",
            source_label="真实行情下载失败",
            message=f"真实行情下载失败：{error_message}",
        )

    real_path_text = real_relative_path.as_posix()
    return KLineDataResult(
        df=df,
        source_kind="real",
        source_label="真实行情数据",
        display_stock_code=clean_stock_code,
        message=f"当前数据来源：真实行情数据（{real_path_text}）",
        csv_path=real_path,
    )


def load_demo_csv(project_root: str | Path) -> pd.DataFrame:
    """Load or create the clearly named demo CSV."""
    root = Path(project_root)
    path = root / SAMPLE_DEMO_RELATIVE_PATH
    if not path.exists():
        create_sample_csv(path)
    return load_csv(path)


def load_real_csv(project_root: str | Path, stock_code: str, period: str = "daily") -> pd.DataFrame:
    """Load a real stock CSV from data/real/{stock_code}_{period}.csv."""
    root = Path(project_root)
    clean_stock_code = stock_code.strip().upper()
    real_relative_path = REAL_DATA_RELATIVE_DIR / f"{clean_stock_code}_{period}.csv"
    real_path = root / real_relative_path
    if not real_path.exists():
        raise FileNotFoundError(
            f"没有找到真实行情CSV，请先下载或导入 {real_relative_path.as_posix()}"
        )
    return load_csv(real_path)


def load_or_create_sample_csv(csv_path: str | Path | None = None) -> pd.DataFrame:
    """Compatibility helper: demo data is only created as sample_demo_daily.csv."""
    path = Path(csv_path) if csv_path is not None else SAMPLE_DEMO_RELATIVE_PATH
    if path.name != SAMPLE_DEMO_RELATIVE_PATH.name:
        path = path.parent / SAMPLE_DEMO_RELATIVE_PATH.name
    if not path.exists():
        create_sample_csv(path)
    return load_csv(path)


def load_csv(csv_path: str | Path) -> pd.DataFrame:
    """Read a K-line CSV and normalize its required columns."""
    path = Path(csv_path)
    df = pd.read_csv(path)

    missing_columns = [column for column in CSV_COLUMNS if column not in df.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"CSV file {path} is missing required columns: {missing}")

    df = df[CSV_COLUMNS].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for column in ["open", "high", "low", "close", "volume"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.dropna(subset=["date", "open", "high", "low", "close"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


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
    df.to_csv(path, index=False, columns=CSV_COLUMNS)
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
    csv_path = root / REAL_DATA_RELATIVE_DIR / f"{stock_code}_{period}.csv"
    return KLineDataResult(
        df=pd.DataFrame(columns=CSV_COLUMNS),
        source_kind=source_kind,
        source_label=source_label,
        display_stock_code=stock_code,
        message=message,
        csv_path=csv_path,
    )
