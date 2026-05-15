from __future__ import annotations

import json

import pandas as pd

from src.data.market_data_center import (
    STANDARD_KLINE_COLUMNS,
    add_stock,
    get_kline_path,
    list_available_periods,
    load_kline,
    load_watchlist,
    remove_stock,
    save_kline,
    update_kline,
)


def test_watchlist_is_created_and_supports_stock_lifecycle(tmp_path) -> None:
    watchlist_path = tmp_path / "config" / "watchlist.json"

    assert load_watchlist(tmp_path) == []
    assert watchlist_path.exists()

    record = add_stock("600497", name="驰宏锌锗", project_root=tmp_path)

    assert record["symbol"] == "600497"
    assert record["name"] == "驰宏锌锗"
    assert load_watchlist(tmp_path)[0]["symbol"] == "600497"

    remove_stock("600497", project_root=tmp_path)

    payload = json.loads(watchlist_path.read_text(encoding="utf-8"))
    assert payload == {"stocks": []}


def test_save_and_load_kline_uses_standard_market_path_and_max_bars(tmp_path) -> None:
    df = pd.DataFrame(
        {
            "datetime": ["2024-01-02", "2024-01-01", "2024-01-02"],
            "open": [11, 10, 12],
            "high": [12, 11, 13],
            "low": [10, 9, 11],
            "close": [11.5, 10.5, 12.5],
            "volume": [200, 100, 300],
        }
    )

    path = save_kline("600497", "daily", df, project_root=tmp_path)
    saved_df = pd.read_csv(path)
    loaded_df = load_kline("600497", "daily", max_bars=1, project_root=tmp_path)

    assert path == get_kline_path("600497", "daily", tmp_path)
    assert list(saved_df.columns) == STANDARD_KLINE_COLUMNS
    assert saved_df["datetime"].tolist() == ["2024-01-01", "2024-01-02"]
    assert loaded_df["close"].tolist() == [12.5]
    assert loaded_df.attrs["total_count"] == 2
    assert loaded_df.attrs["actual_count"] == 1


def test_load_kline_falls_back_to_legacy_real_path(tmp_path) -> None:
    legacy_dir = tmp_path / "data" / "real"
    legacy_dir.mkdir(parents=True)
    legacy_path = legacy_dir / "600497_daily.csv"
    pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "open": [10, 11],
            "high": [11, 12],
            "low": [9, 10],
            "close": [10.5, 11.5],
            "volume": [100, 200],
        }
    ).to_csv(legacy_path, index=False)

    loaded_df = load_kline("600497", "daily", project_root=tmp_path)

    assert loaded_df.attrs["source_kind"] == "legacy"
    assert loaded_df["close"].tolist() == [10.5, 11.5]
    assert list_available_periods("600497", tmp_path) == ["daily"]


def test_update_kline_merges_incremental_data_without_losing_old_rows(tmp_path) -> None:
    save_kline(
        "600497",
        "daily",
        pd.DataFrame(
            {
                "datetime": ["2024-01-01", "2024-01-02"],
                "open": [10, 11],
                "high": [11, 12],
                "low": [9, 10],
                "close": [10.5, 11.5],
                "volume": [100, 200],
                "amount": [1000, 2000],
            }
        ),
        project_root=tmp_path,
    )

    class FakeProvider:
        def fetch_kline(self, symbol, period, start=None, end=None, adjust="qfq"):
            assert symbol == "600497"
            assert period == "daily"
            assert str(start) == "2024-01-02 00:00:00"
            return pd.DataFrame(
                {
                    "datetime": ["2024-01-02", "2024-01-03"],
                    "open": [11, 12],
                    "high": [12, 13],
                    "low": [10, 11],
                    "close": [11.5, 12.5],
                    "volume": [200, 300],
                    "amount": [2000, 3000],
                }
            )

    result = update_kline("600497", "daily", project_root=tmp_path, provider=FakeProvider())
    loaded_df = load_kline("600497", "daily", project_root=tmp_path)

    assert result.success
    assert result.added_count == 1
    assert result.duplicate_count == 1
    assert loaded_df["close"].tolist() == [10.5, 11.5, 12.5]
