from __future__ import annotations

import pandas as pd

from src.chan.bi import write_bi_debug_report


def _make_df(highs: list[float], lows: list[float]) -> pd.DataFrame:
    rows = len(highs)
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=rows, freq="D"),
            "open": [low + 1 for low in lows],
            "high": highs,
            "low": lows,
            "close": [high - 1 for high in highs],
            "volume": [1000 + index for index in range(rows)],
            "x": list(range(rows)),
        }
    )


def test_write_bi_debug_report_creates_report_and_csv_files(tmp_path) -> None:
    df = _make_df(
        highs=[10, 13, 11, 10, 9, 7, 8, 11, 10, 12, 11],
        lows=[5, 8, 6, 5, 4, 1, 3, 6, 5, 8, 6],
    )

    result = write_bi_debug_report(df, output_dir=tmp_path, manual_expected_bis=[(1, 5)])

    report_path = tmp_path / "bi_debug_report.txt"
    fractals_path = tmp_path / "fractals_debug.csv"
    attempts_path = tmp_path / "bi_attempts_debug.csv"
    tail_region_path = tmp_path / "bi_tail_region_debug.csv"
    rollback_path = tmp_path / "bi_rollback_debug.csv"
    assert report_path.exists()
    assert fractals_path.exists()
    assert attempts_path.exists()
    assert tail_region_path.exists()
    assert rollback_path.exists()
    assert result["report_path"] == str(report_path)

    report = report_path.read_text(encoding="utf-8")
    assert "raw_fractals_count=" in report
    assert "confirmed_bis_count=" in report
    assert "## Tail Region Diagnostics" in report
    assert "rollback_enabled=True" in report

    fractals_debug = pd.read_csv(fractals_path)
    attempts_debug = pd.read_csv(attempts_path)
    tail_region_debug = pd.read_csv(tail_region_path)
    rollback_debug = pd.read_csv(rollback_path)
    for column in ["index", "date", "type", "high", "low", "in_cleaned", "removed_reason"]:
        assert column in fractals_debug.columns
    for column in [
        "anchor_index",
        "anchor_virtual_index",
        "anchor_date",
        "anchor_type",
        "candidate_index",
        "candidate_virtual_index",
        "candidate_date",
        "candidate_type",
        "bar_count",
        "center_gap",
        "neutral_bar_count",
        "extreme_ok",
        "result",
        "reason",
        "would_form_direction",
    ]:
        assert column in attempts_debug.columns
    for column in [
        "candidate_order",
        "virtual_index",
        "raw_start_index",
        "raw_end_index",
        "attempt_result",
        "reject_reason",
    ]:
        assert column in tail_region_debug.columns
    for column in [
        "stuck_candidate_index",
        "rollback_count",
        "old_last_raw_index",
        "new_last_raw_index",
        "accepted",
        "reason",
    ]:
        assert column in rollback_debug.columns


def test_manual_expected_bi_debug_reports_missing_raw_fractal(tmp_path) -> None:
    df = _make_df(
        highs=[10, 13, 11, 10, 9, 7, 8],
        lows=[5, 8, 6, 5, 4, 1, 3],
    )

    result = write_bi_debug_report(df, output_dir=tmp_path, manual_expected_bis=[(99, 5)])

    manual_debug = result["manual_expected_debug"]
    assert manual_debug.loc[0, "reason"] == "raw_fractal_missing"
