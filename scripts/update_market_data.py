from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.market_data_center import SUPPORTED_PERIODS, update_kline


def main() -> int:
    parser = argparse.ArgumentParser(description="下载/更新本地多周期K线数据")
    parser.add_argument("--symbol", required=True, help="6位A股股票代码，例如 600497")
    parser.add_argument("--period", choices=SUPPORTED_PERIODS, help="单个周期")
    parser.add_argument("--all-periods", action="store_true", help="下载/更新全部支持周期")
    args = parser.parse_args()

    if not args.all_periods and not args.period:
        parser.error("请指定 --period 或 --all-periods")
    if args.all_periods and args.period:
        parser.error("--period 和 --all-periods 只能二选一")

    periods = SUPPORTED_PERIODS if args.all_periods else (args.period,)
    failed_count = 0
    for period in periods:
        result = update_kline(args.symbol, period, project_root=PROJECT_ROOT)
        print(result.message)
        if not result.success:
            failed_count += 1

    return 1 if failed_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
