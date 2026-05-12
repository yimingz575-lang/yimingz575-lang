from __future__ import annotations

import pandas as pd

from src.chan.bi import build_bis_incremental
from src.chan.fractal import detect_candidate_fractals
from src.chan.inclusion import detect_inclusion_marks, process_inclusions


def detect_fractal_marks(df: pd.DataFrame) -> pd.DataFrame:
    """Return effective fractal marks confirmed by valid bi."""
    return analyze_chan_marks(df)["valid_fractals_for_bi"]


def detect_candidate_fractal_marks(df: pd.DataFrame) -> pd.DataFrame:
    """Return candidate fractals before bi confirmation."""
    return analyze_chan_marks(df)["candidate_fractals"]


def detect_bi_marks(df: pd.DataFrame) -> pd.DataFrame:
    """Return valid bi records mapped back to original K-line coordinates."""
    return analyze_chan_marks(df)["confirmed_bis"]


def analyze_chan_marks(df: pd.DataFrame, symbol: str | None = None) -> dict[str, object]:
    """Return current-stage Chan analysis results without changing original K-lines."""
    display_symbol = (symbol or "").strip().upper() or "UNKNOWN"
    is_demo = display_symbol == "DEMO"
    inclusion_result = process_inclusions(df)
    standard_bars = inclusion_result.standard_bars
    inclusion_groups = inclusion_result.inclusion_groups
    candidate_fractals = detect_candidate_fractals(standard_bars)
    valid_fractals, confirmed_bis = build_bis_incremental(standard_bars, candidate_fractals)
    print("[engine] symbol =", display_symbol)
    print("[engine] is_demo =", is_demo)
    print("[engine] standard_bars count =", len(standard_bars))
    print("[engine] candidate_fractals count =", len(candidate_fractals))
    print("[engine] confirmed_bis count =", len(confirmed_bis))
    return {
        "raw_bars": df,
        "standard_bars": standard_bars,
        "inclusion_groups": inclusion_groups,
        "candidate_fractals": candidate_fractals,
        "valid_fractals_for_bi": valid_fractals,
        "confirmed_bis": confirmed_bis,
        "inclusions": detect_inclusion_marks(df),
        "fractals": valid_fractals,
        "bis": confirmed_bis,
    }
