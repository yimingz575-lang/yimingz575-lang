from __future__ import annotations

import pandas as pd

from src.chan.bi_zhongshu import build_bi_zhongshu


def _bi(
    start: str,
    end: str,
    start_price: float,
    end_price: float,
    direction: str,
    start_x: int | None = None,
    end_x: int | None = None,
) -> dict:
    record = {
        "start_dt": pd.Timestamp(start),
        "end_dt": pd.Timestamp(end),
        "start_price": start_price,
        "end_price": end_price,
        "direction": direction,
    }
    if start_x is not None:
        record["start_x"] = start_x
    if end_x is not None:
        record["end_x"] = end_x
    return record


def test_build_bi_zhongshu_from_three_overlapping_confirmed_bis() -> None:
    confirmed_bis = pd.DataFrame(
        [
            _bi("2024-01-01", "2024-01-02", 10.0, 20.0, "up"),
            _bi("2024-01-02", "2024-01-03", 18.0, 12.0, "down"),
            _bi("2024-01-03", "2024-01-04", 14.0, 22.0, "up"),
        ]
    )

    result = build_bi_zhongshu(confirmed_bis)

    assert len(result) == 1
    zhongshu = result.iloc[0]
    assert zhongshu["source"] == "bi"
    assert zhongshu["type"] == "bi_zhongshu"
    assert zhongshu["start_bi_index"] == 0
    assert zhongshu["end_bi_index"] == 2
    assert zhongshu["zd"] == 14.0
    assert zhongshu["zg"] == 18.0
    assert zhongshu["high"] == 22.0
    assert zhongshu["low"] == 10.0


def test_bi_zhongshu_extends_until_next_bi_no_longer_overlaps_core_range() -> None:
    confirmed_bis = pd.DataFrame(
        [
            _bi("2024-01-01", "2024-01-02", 10.0, 20.0, "up"),
            _bi("2024-01-02", "2024-01-03", 18.0, 12.0, "down"),
            _bi("2024-01-03", "2024-01-04", 14.0, 22.0, "up"),
            _bi("2024-01-04", "2024-01-05", 19.0, 13.0, "down"),
            _bi("2024-01-05", "2024-01-06", 30.0, 24.0, "up"),
        ]
    )

    result = build_bi_zhongshu(confirmed_bis)

    assert len(result) == 1
    zhongshu = result.iloc[0]
    assert zhongshu["end_bi_index"] == 3
    assert zhongshu["end_dt"] == pd.Timestamp("2024-01-05")
    assert zhongshu["zd"] == 14.0
    assert zhongshu["zg"] == 18.0
    assert zhongshu["high"] == 22.0
    assert zhongshu["low"] == 10.0
    assert zhongshu["breakout_bi_index"] == 4
    assert zhongshu["breakout_direction"] == "up"


def test_adjacent_zhongshu_requires_one_breakout_bi_between_them() -> None:
    confirmed_bis = pd.DataFrame(
        [
            _bi("2024-01-01", "2024-01-02", 10.0, 20.0, "up"),
            _bi("2024-01-02", "2024-01-03", 18.0, 12.0, "down"),
            _bi("2024-01-03", "2024-01-04", 14.0, 22.0, "up"),
            _bi("2024-01-04", "2024-01-05", 28.0, 24.0, "down"),
            _bi("2024-01-05", "2024-01-06", 24.0, 32.0, "up"),
            _bi("2024-01-06", "2024-01-07", 29.0, 25.0, "down"),
            _bi("2024-01-07", "2024-01-08", 26.0, 34.0, "up"),
        ]
    )

    result = build_bi_zhongshu(confirmed_bis)

    assert len(result) == 2
    old_zs = result.iloc[0]
    new_zs = result.iloc[1]
    assert old_zs["end_bi_index"] == 2
    assert old_zs["breakout_bi_index"] == 3
    assert old_zs["breakout_direction"] == "up"
    assert new_zs["start_bi_index"] == 4
    assert old_zs["end_dt"] == pd.Timestamp("2024-01-04")
    assert new_zs["start_dt"] == pd.Timestamp("2024-01-05")
    assert new_zs["start_bi_index"] >= old_zs["end_bi_index"] + 2
    assert new_zs["zd"] == 26.0
    assert new_zs["zg"] == 29.0


def test_breakout_bi_cannot_start_new_zhongshu_even_if_three_bis_overlap() -> None:
    confirmed_bis = pd.DataFrame(
        [
            _bi("2024-01-01", "2024-01-02", 10.0, 20.0, "up"),
            _bi("2024-01-02", "2024-01-03", 18.0, 12.0, "down"),
            _bi("2024-01-03", "2024-01-04", 14.0, 22.0, "up"),
            _bi("2024-01-04", "2024-01-05", 28.0, 24.0, "down"),
            _bi("2024-01-05", "2024-01-06", 25.0, 32.0, "up"),
            _bi("2024-01-06", "2024-01-07", 30.0, 26.0, "down"),
        ]
    )

    result = build_bi_zhongshu(confirmed_bis)

    assert len(result) == 1
    assert result.iloc[0]["breakout_bi_index"] == 3


def test_retrace_bi_that_reenters_old_core_is_not_used_as_new_start() -> None:
    confirmed_bis = pd.DataFrame(
        [
            _bi("2024-01-01", "2024-01-02", 10.0, 20.0, "up"),
            _bi("2024-01-02", "2024-01-03", 18.0, 12.0, "down"),
            _bi("2024-01-03", "2024-01-04", 14.0, 22.0, "up"),
            _bi("2024-01-04", "2024-01-05", 28.0, 24.0, "down"),
            _bi("2024-01-05", "2024-01-06", 16.0, 26.0, "up"),
            _bi("2024-01-06", "2024-01-07", 31.0, 25.0, "down"),
            _bi("2024-01-07", "2024-01-08", 26.0, 34.0, "up"),
            _bi("2024-01-08", "2024-01-09", 29.0, 27.0, "down"),
        ]
    )

    result = build_bi_zhongshu(confirmed_bis)

    assert len(result) == 2
    assert result.iloc[0]["breakout_bi_index"] == 3
    assert result.iloc[1]["start_bi_index"] == 5


def test_later_new_zhongshu_does_not_require_price_separation_from_old_zhongshu() -> None:
    confirmed_bis = pd.DataFrame(
        [
            _bi("2024-01-01", "2024-01-02", 10.0, 20.0, "up"),
            _bi("2024-01-02", "2024-01-03", 18.0, 12.0, "down"),
            _bi("2024-01-03", "2024-01-04", 14.0, 22.0, "up"),
            _bi("2024-01-04", "2024-01-05", 28.0, 24.0, "down"),
            _bi("2024-01-05", "2024-01-06", 16.0, 26.0, "up"),
            _bi("2024-01-06", "2024-01-07", 21.0, 16.0, "down"),
            _bi("2024-01-07", "2024-01-08", 15.0, 19.0, "up"),
            _bi("2024-01-08", "2024-01-09", 22.0, 17.0, "down"),
        ]
    )

    result = build_bi_zhongshu(confirmed_bis)

    assert len(result) == 2
    old_zs = result.iloc[0]
    new_zs = result.iloc[1]
    assert new_zs["start_bi_index"] == 5
    assert new_zs["zd"] == 17.0
    assert new_zs["zg"] == 19.0
    assert new_zs["zd"] <= old_zs["zg"]


def test_zhongshu_boundaries_exclude_breakout_bi_and_start_from_retrace_bi() -> None:
    confirmed_bis = pd.DataFrame(
        [
            _bi("2024-01-01", "2024-01-02", 10.0, 20.0, "up", 0, 1),
            _bi("2024-01-02", "2024-01-03", 18.0, 12.0, "down", 1, 2),
            _bi("2024-01-03", "2024-01-04", 14.0, 22.0, "up", 2, 3),
            _bi("2024-01-04", "2024-01-05", 28.0, 24.0, "down", 3, 4),
            _bi("2024-01-05", "2024-01-06", 24.0, 32.0, "up", 4, 5),
            _bi("2024-01-06", "2024-01-07", 29.0, 25.0, "down", 5, 6),
            _bi("2024-01-07", "2024-01-08", 26.0, 34.0, "up", 6, 7),
        ]
    )

    result = build_bi_zhongshu(confirmed_bis)

    assert len(result) == 2
    old_zs = result.iloc[0]
    new_zs = result.iloc[1]
    breakout_bi = confirmed_bis.iloc[int(old_zs["breakout_bi_index"])]
    retrace_bi = confirmed_bis.iloc[int(old_zs["breakout_bi_index"]) + 1]

    assert old_zs["end_bi_index"] == 2
    assert old_zs["end_dt"] == pd.Timestamp("2024-01-04")
    assert old_zs["end_x"] == 3
    assert old_zs["end_dt"] != breakout_bi["end_dt"]
    assert old_zs["end_x"] != breakout_bi["end_x"]
    assert new_zs["start_bi_index"] == 4
    assert new_zs["start_dt"] == retrace_bi["start_dt"]
    assert new_zs["start_x"] == retrace_bi["start_x"]
    assert new_zs["start_dt"] != breakout_bi["start_dt"]
    assert new_zs["start_x"] != breakout_bi["start_x"]


def test_bi_zhongshu_requires_alternating_directions_and_price_overlap() -> None:
    same_direction = pd.DataFrame(
        [
            _bi("2024-01-01", "2024-01-02", 10.0, 20.0, "up"),
            _bi("2024-01-02", "2024-01-03", 12.0, 22.0, "up"),
            _bi("2024-01-03", "2024-01-04", 14.0, 24.0, "up"),
        ]
    )
    no_overlap = pd.DataFrame(
        [
            _bi("2024-01-01", "2024-01-02", 10.0, 12.0, "up"),
            _bi("2024-01-02", "2024-01-03", 20.0, 18.0, "down"),
            _bi("2024-01-03", "2024-01-04", 30.0, 32.0, "up"),
        ]
    )

    assert build_bi_zhongshu(same_direction).empty
    assert build_bi_zhongshu(no_overlap).empty
