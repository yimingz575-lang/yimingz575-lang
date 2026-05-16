from __future__ import annotations

import math
from typing import Any

import pandas as pd


def build_bi_zhongshu(confirmed_bis: pd.DataFrame) -> pd.DataFrame:
    """Build one-level zhongshu ranges directly from confirmed bi records."""
    normalized_bis = _normalize_bis(confirmed_bis)
    if len(normalized_bis) < 3:
        return _empty_bi_zhongshu()

    records: list[dict[str, Any]] = []
    connector_bi_indices: list[int] = []
    index = 0
    center_id = 0
    while index <= len(normalized_bis) - 3:
        window = normalized_bis.iloc[index : index + 3]
        overlap = _three_bi_overlap(window)
        if overlap is None:
            index += 1
            continue

        overlap_low, overlap_high = overlap
        start_bi_index = index
        end_bi_index = index + 2
        next_index = index + 3
        breakout_bi_index: int | None = None
        breakout_direction: str | None = None
        connector_for_center: list[int] = []
        is_extended = False
        next_search_index: int | None = None

        while next_index < len(normalized_bis):
            next_bi = normalized_bis.iloc[next_index]
            confirmed_break_direction = _confirmed_break_direction(
                normalized_bis=normalized_bis,
                break_bi_index=next_index,
                zd=overlap_low,
                zg=overlap_high,
            )
            if confirmed_break_direction is not None:
                breakout_bi_index = next_index
                breakout_direction = confirmed_break_direction
                connector_for_center.append(breakout_bi_index)
                connector_bi_indices.append(breakout_bi_index)

                future_start_index = breakout_bi_index + 1
                future_connector_indices = [
                    future_start_index,
                    future_start_index + 1,
                    future_start_index + 2,
                ]
                future_forms_center = _three_bi_overlap(
                    normalized_bis.iloc[future_start_index : future_start_index + 3]
                ) is not None
                if future_forms_center:
                    next_search_index = future_start_index
                else:
                    connector_for_center.extend(future_connector_indices)
                    connector_bi_indices.extend(future_connector_indices)
                    next_search_index = breakout_bi_index + 4

                _print_bi_zhongshu_breakout_debug(
                    old_center_id=center_id,
                    break_bi_index=breakout_bi_index,
                    break_direction=breakout_direction,
                    zd=overlap_low,
                    zg=overlap_high,
                    pullback_bi_index=future_start_index,
                    next_bi_1_index=future_start_index + 1,
                    next_bi_2_index=future_start_index + 2,
                    future_forms_center=future_forms_center,
                    connector_bi_indices=connector_for_center,
                )
                break

            if _bi_overlaps_core(next_bi, overlap_low, overlap_high):
                end_bi_index = next_index
                is_extended = True
                next_index += 1
                continue

            next_search_index = next_index
            break

        bi_indices = list(range(start_bi_index, end_bi_index + 1))
        participating = normalized_bis.iloc[start_bi_index : end_bi_index + 1]
        record = {
            "center_id": center_id,
            "source": "bi",
            "type": "bi_zhongshu",
            "bi_indices": bi_indices,
            "start_bi_index": start_bi_index,
            "end_bi_index": end_bi_index,
            "breakout_bi_index": breakout_bi_index,
            "breakout_direction": breakout_direction,
            "start_dt": normalized_bis.iloc[start_bi_index]["start_dt"],
            "end_dt": normalized_bis.iloc[end_bi_index]["end_dt"],
            "start_x": normalized_bis.iloc[start_bi_index]["start_x"],
            "end_x": normalized_bis.iloc[end_bi_index]["end_x"],
            "zd": overlap_low,
            "zg": overlap_high,
            "high": float(participating["high"].max()),
            "low": float(participating["low"].min()),
            "is_initial_three_bi": True,
            "is_extended": is_extended,
            "connector_bi_indices": connector_for_center,
        }
        records.append(record)
        _print_bi_zhongshu_debug(record)
        center_id += 1

        if next_search_index is not None:
            index = next_search_index
        elif breakout_bi_index is None:
            index = next_index
        else:
            index = breakout_bi_index + 1

    print("[bi_zhongshu] connector_bi_indices =", connector_bi_indices)

    if not records:
        return _empty_bi_zhongshu()
    return pd.DataFrame(records)


def _three_bi_overlap(window: pd.DataFrame) -> tuple[float, float] | None:
    if len(window) != 3:
        return None
    if not _is_valid_three_bi_base(window):
        return None

    overlap_low = float(window["low"].max())
    overlap_high = float(window["high"].min())
    if overlap_low > overlap_high:
        return None
    return overlap_low, overlap_high


def _print_bi_zhongshu_debug(record: dict[str, Any]) -> None:
    print(
        "[bi_zhongshu] center_id={center_id}, start_bi_index={start_bi_index}, "
        "end_bi_index={end_bi_index}, ZD={zd}, ZG={zg}".format(**record)
    )
    print(
        "[bi_zhongshu] center_id={center_id}, start_time={start_dt}, "
        "end_time={end_dt}".format(**record)
    )
    print(
        "[bi_zhongshu] center_id={center_id}, is_initial_three_bi={is_initial_three_bi}, "
        "is_extended={is_extended}, connector_bi={connector_bi_indices}".format(**record)
    )


def _print_bi_zhongshu_breakout_debug(
    old_center_id: int,
    break_bi_index: int,
    break_direction: str,
    zd: float,
    zg: float,
    pullback_bi_index: int,
    next_bi_1_index: int,
    next_bi_2_index: int,
    future_forms_center: bool,
    connector_bi_indices: list[int],
) -> None:
    print(
        "[bi_zhongshu] confirmed_breakout old_center_id={old_center_id}, "
        "break_bi_index={break_bi_index}, break_direction={break_direction}, "
        "old_ZD={zd}, old_ZG={zg}".format(
            old_center_id=old_center_id,
            break_bi_index=break_bi_index,
            break_direction=break_direction,
            zd=zd,
            zg=zg,
        )
    )
    print(
        "[bi_zhongshu] confirmed_breakout old_center_id={old_center_id}, "
        "pullback_bi_index={pullback_bi_index}, next_bi_1_index={next_bi_1_index}, "
        "next_bi_2_index={next_bi_2_index}, future_forms_center={future_forms_center}, "
        "connector_bi_indexes={connector_bi_indices}".format(
            old_center_id=old_center_id,
            pullback_bi_index=pullback_bi_index,
            next_bi_1_index=next_bi_1_index,
            next_bi_2_index=next_bi_2_index,
            future_forms_center=future_forms_center,
            connector_bi_indices=connector_bi_indices,
        )
    )


def _normalize_bis(confirmed_bis: pd.DataFrame) -> pd.DataFrame:
    if confirmed_bis.empty:
        return pd.DataFrame(
            columns=[
                "start_dt",
                "end_dt",
                "start_x",
                "end_x",
                "start_price",
                "end_price",
                "direction",
                "high",
                "low",
            ]
        )

    records: list[dict[str, Any]] = []
    for _, bi in confirmed_bis.reset_index(drop=True).iterrows():
        start_price = _to_float(_row_value(bi, "start_price"))
        end_price = _to_float(_row_value(bi, "end_price"))
        if start_price is None or end_price is None:
            continue
        high = max(start_price, end_price)
        low = min(start_price, end_price)
        direction = _normalize_direction(_row_value(bi, "direction"), start_price, end_price)
        records.append(
            {
                "start_dt": _row_value(bi, "start_dt", _row_value(bi, "start_date")),
                "end_dt": _row_value(bi, "end_dt", _row_value(bi, "end_date")),
                "start_x": _row_value(bi, "start_x"),
                "end_x": _row_value(bi, "end_x"),
                "start_price": start_price,
                "end_price": end_price,
                "direction": direction,
                "high": high,
                "low": low,
            }
        )
    return pd.DataFrame(records)


def _is_valid_three_bi_base(window: pd.DataFrame) -> bool:
    directions = window["direction"].tolist()
    if any(direction not in {"up", "down"} for direction in directions):
        return False
    return directions[0] == directions[2] and directions[0] != directions[1]


def _bi_overlaps_core(bi: pd.Series, zd: float, zg: float) -> bool:
    return float(bi["low"]) <= zg and float(bi["high"]) >= zd


def _confirmed_break_direction(
    normalized_bis: pd.DataFrame,
    break_bi_index: int,
    zd: float,
    zg: float,
) -> str | None:
    if break_bi_index + 3 >= len(normalized_bis):
        return None

    break_bi = normalized_bis.iloc[break_bi_index]
    if _is_up_break_candidate(break_bi, zg):
        future = normalized_bis.iloc[break_bi_index + 1 : break_bi_index + 4]
        if all(float(bi["low"]) > zg for _, bi in future.iterrows()):
            return "up"

    if _is_down_break_candidate(break_bi, zd):
        future = normalized_bis.iloc[break_bi_index + 1 : break_bi_index + 4]
        if all(float(bi["high"]) < zd for _, bi in future.iterrows()):
            return "down"

    return None


def _is_up_break_candidate(bi: pd.Series, zg: float) -> bool:
    return bi["direction"] == "up" and float(bi["high"]) > zg


def _is_down_break_candidate(bi: pd.Series, zd: float) -> bool:
    return bi["direction"] == "down" and float(bi["low"]) < zd


def _empty_bi_zhongshu() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "center_id",
            "source",
            "type",
            "bi_indices",
            "start_bi_index",
            "end_bi_index",
            "breakout_bi_index",
            "breakout_direction",
            "start_dt",
            "end_dt",
            "start_x",
            "end_x",
            "zd",
            "zg",
            "high",
            "low",
            "is_initial_three_bi",
            "is_extended",
            "connector_bi_indices",
        ]
    )


def _row_value(row: pd.Series, key: str, default: Any = None) -> Any:
    if key not in row.index:
        return default
    value = row[key]
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        return value
    return value


def _to_float(value: Any) -> float | None:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return None
    number = float(number)
    if math.isnan(number):
        return None
    return number


def _normalize_direction(value: Any, start_price: float, end_price: float) -> str:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"up", "down"}:
            return normalized
    if end_price > start_price:
        return "up"
    if end_price < start_price:
        return "down"
    return "unknown"
