from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd


class BaseKlineProvider(ABC):
    """Common interface for pluggable K-line data providers."""

    @abstractmethod
    def fetch_kline(
        self,
        symbol: str,
        period: str,
        start: datetime | pd.Timestamp | str | None = None,
        end: datetime | pd.Timestamp | str | None = None,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        """Fetch normalized K-line data with datetime/open/high/low/close/volume/amount."""

