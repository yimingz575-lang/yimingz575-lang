from __future__ import annotations

import pandas as pd


def calculate_macd(
    close: pd.Series,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> pd.DataFrame:
    """Calculate DIF, DEA and MACD histogram values from close prices."""
    close_series = pd.to_numeric(close, errors="coerce")

    ema_fast = close_series.ewm(span=fast_period, adjust=False).mean()
    ema_slow = close_series.ewm(span=slow_period, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal_period, adjust=False).mean()
    macd = (dif - dea) * 2

    return pd.DataFrame({"dif": dif, "dea": dea, "macd": macd})


def append_macd(
    df: pd.DataFrame,
    close_column: str = "close",
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> pd.DataFrame:
    """Return a copy of df with DIF, DEA and MACD columns appended."""
    result = df.copy()
    macd_values = calculate_macd(
        result[close_column],
        fast_period=fast_period,
        slow_period=slow_period,
        signal_period=signal_period,
    )
    return result.join(macd_values)