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
    index = 0
    while index <= len(normalized_bis) - 3:
        window = normalized_bis.iloc[index : index + 3]
        if not _is_valid_three_bi_base(window):
            index += 1
            continue

        overlap_low = float(window["low"].max())
        overlap_high = float(window["high"].min())
        if overlap_low > overlap_high:
            index += 1
            continue

        start_bi_index = index
        end_bi_index = index + 2
        participating = normalized_bis.iloc[start_bi_index : end_bi_index + 1]
        record = {
            "source": "bi",
            "type": "bi_zhongshu",
            "start_bi_index": start_bi_index,
            "end_bi_index": end_bi_index,
            "breakout_bi_index": None,
            "breakout_direction": None,
            "start_dt": normalized_bis.iloc[start_bi_index]["start_dt"],
            "end_dt": normalized_bis.iloc[end_bi_index]["end_dt"],
            "start_x": normalized_bis.iloc[start_bi_index]["start_x"],
            "end_x": normalized_bis.iloc[end_bi_index]["end_x"],
            "zd": overlap_low,
            "zg": overlap_high,
            "high": float(participating["high"].max()),
            "low": float(participating["low"].min()),
        }

        next_index = end_bi_index + 1
        breakout_bi_index: int | None = None
        while next_index < len(normalized_bis):
            next_bi = normalized_bis.iloc[next_index]
            if not _bi_overlaps_core(next_bi, float(record["zd"]), float(record["zg"])):
                breakout_bi_index = next_index
                record["breakout_bi_index"] = breakout_bi_index
                record["breakout_direction"] = _breakout_direction(
                    next_bi,
                    float(record["zd"]),
                    float(record["zg"]),
                )
                break
            record["end_bi_index"] = next_index
            record["end_dt"] = next_bi["end_dt"]
            record["end_x"] = next_bi["end_x"]
            record["high"] = max(float(record["high"]), float(next_bi["high"]))
            record["low"] = min(float(record["low"]), float(next_bi["low"]))
            next_index += 1

        records.append(record)
        if breakout_bi_index is None:
            index = next_index
            continue

        # breakout_bi is only the connection bi between two zhongshu; never use it
        # as the first bi of the next zhongshu candidate.
        breakout_bi = normalized_bis.iloc[breakout_bi_index]
        retrace_bi_index = breakout_bi_index + 1
        if retrace_bi_index >= len(normalized_bis):
            index = retrace_bi_index
            continue

        # If the immediate retrace is valid, it may start the next candidate.
        # Later candidates are judged only by their own three-bi overlap; there
        # is intentionally no price-overlap constraint between old and new zs.
        retrace_bi = normalized_bis.iloc[retrace_bi_index]
        if _is_valid_retrace_after_breakout(
            breakout_bi=breakout_bi,
            retrace_bi=retrace_bi,
            breakout_direction=str(record["breakout_direction"]),
            old_zd=float(record["zd"]),
            old_zg=float(record["zg"]),
        ):
            index = retrace_bi_index
        else:
            index = retrace_bi_index + 1

    if not records:
        return _empty_bi_zhongshu()
    return pd.DataFrame(records)


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
        high = _to_float(_row_value(bi, "high"))
        low = _to_float(_row_value(bi, "low"))
        if start_price is None or end_price is None:
            continue
        if high is None:
            high = max(start_price, end_price)
        if low is None:
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


def _breakout_direction(bi: pd.Series, zd: float, zg: float) -> str:
    if float(bi["low"]) > zg:
        return "up"
    if float(bi["high"]) < zd:
        return "down"
    return "none"


def _is_valid_retrace_after_breakout(
    breakout_bi: pd.Series,
    retrace_bi: pd.Series,
    breakout_direction: str,
    old_zd: float,
    old_zg: float,
) -> bool:
    if breakout_bi["direction"] not in {"up", "down"} or retrace_bi["direction"] not in {"up", "down"}:
        return False
    if breakout_bi["direction"] == retrace_bi["direction"]:
        return False
    if breakout_direction == "up":
        return float(retrace_bi["low"]) > old_zg
    if breakout_direction == "down":
        return float(retrace_bi["high"]) < old_zd
    return False


def _empty_bi_zhongshu() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "source",
            "type",
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
