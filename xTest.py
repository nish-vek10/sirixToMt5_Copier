#!/usr/bin/env python
"""
XAUUSD trail_start_R helper backtest
====================================

- Reads MT5 trade history report (XLSX) for account 52633821.
- Reads MT5-exported XAUUSD M1 candles (HTML).
- Detects trades from:
    * Inv_ATRtrail_buy / Inv_ATRtrail_sel  -> ATR trailing engine
    * Inv_Chandelier_b / Inv_Chandelier_s -> Chandelier engine
- For those trades only, re-simulates exits under different trail_start_R gates:

    trail_start_R in [None, 0.3, 0.5, 0.7, 1.0]

- Outputs:
    * Summary table (per engine x trail_start_R):
        n_trades, win_rate, avg_R, median_R, total_R
    * Equity curves (cumulative R) per engine and per trail_start_R.

Assumptions / Notes:
- Uses the S/L from the report as the "initial risk" anchor.
- Constructs a theoretical 1R TP as:
    BUY : TP = entry + |entry - SL|
    SELL: TP = entry - |entry - SL|
- Uses MT5 M1 candles to:
    * Compute ATR(5)
    * Compute HH30 / LL30 for Chandelier logic
    * Simulate bar-by-bar SL trailing and SL/TP hits.
- If no SL/TP hit occurs before the recorded ExitTime,
  the trade is closed at the report ExitPrice.

You can tweak:
- TRAIL_START_GRID to test other R-gates.
- ATR_PERIOD, ATR_TRAIL_MULT, CHAN_LOOKBACK if your engines change.
"""


# pip install pandas numpy openpyxl matplotlib lxml


from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ========= USER PATHS =========
TRADE_REPORT_PATH = Path(r"C:\Users\anish\OneDrive\Desktop\Algos\Trade-Reports\ReportHistory-52633821.xlsx")
CANDLE_HTML_PATH  = Path(r"C:\Users\anish\OneDrive\Desktop\MT5 TICK DATA\XAUUSD_M1_202512040100_202512091332.htm")

OUTPUT_DIR = Path(r"C:\Users\anish\OneDrive\Desktop\Algos\Trade-Reports\CLuster-Analysis")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ========= ENGINE PARAMS (match your bot) =========
ATR_PERIOD       = 5
ATR_TRAIL_MULT   = 2.0
CHAN_LOOKBACK    = 30

# Values to test for trail_start_R
TRAIL_START_GRID: List[Optional[float]] = [None, 0.3, 0.5, 0.7, 1.0]


# ========= ENGINE DEFAULT trail_start_R (match live bot) =========
ENGINE_DEFAULT_TRAIL_START = {
    "atr_trailing": 0.3,   # ENGINE A live: Inv_ATRtrail
    "chandelier":  None,   # ENGINE C live: Inv_Chandelier (no R gate)
}


# --------------------------------------------------
# 1. LOAD & PREPARE TRADES
# --------------------------------------------------

def load_trades(path: Path) -> pd.DataFrame:
    """
    Load MT5 positions report (XLSX) and extract:
    - XAUUSD trades only
    - Separately identify entry rows (with Strategy in Swap)
      and closing rows (with Profit numeric)
    - Attach Strategy to the closing rows.

    Returns a DataFrame with:
        EntryTime, ExitTime, EntryPrice, ExitPrice,
        SL, TP, Profit, Type (buy/sell), Strategy, Engine
    """
    df_raw = pd.read_excel(path)

    # Header row is row 5 (0-indexed), then data from row 6 onwards.
    header = df_raw.iloc[5].tolist()
    df = df_raw.iloc[6:].copy()
    df.columns = header

    # XAUUSD only & rows where we have at least some SL/TP
    df = df[df["Symbol"] == "XAUUSD"].copy()
    df = df.dropna(subset=["S / L", "T / P"], how="any")

    # Give explicit names to the duplicated columns:
    # Cols structure is:
    # [Time, Position, Symbol, Type, Volume, Price, S/L, T/P, Time, Price, Commission, Swap, Profit, (NaN)]
    df.columns = [
        "EntryTime",
        "Position",
        "Symbol",
        "Type",
        "Volume",
        "EntryPrice",
        "SL",
        "TP",
        "ExitTime",
        "ExitPrice",
        "Commission",
        "Swap",
        "Profit",
        "Extra",
    ]

    # safer datetime parsing
    df["EntryTime"] = pd.to_datetime(
        df["EntryTime"],
        format="%Y.%m.%d %H:%M:%S",
        errors="coerce",
    )
    df["ExitTime"] = pd.to_datetime(
        df["ExitTime"],
        format="%Y.%m.%d %H:%M:%S",
        errors="coerce",
    )

    for col in ["EntryPrice", "ExitPrice", "SL", "TP", "Profit"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # drop any junk rows with bad timestamps
    df = df.dropna(subset=["EntryTime", "ExitTime"])

    # Entries = lines with "Profit" NaN and "Swap" containing the strategy comment
    entries = df[df["Profit"].isna()].copy()
    closes = df[df["Profit"].notna()].copy()

    entries["Strategy"] = entries["Swap"].astype(str)
    merged = closes.merge(entries[["Position", "Strategy"]], on="Position", how="left")

    mask_trailing = merged["Strategy"].astype(str).str.contains("ATRtrail|Chandelier", case=False, na=False)
    trailing = merged[mask_trailing].copy()

    def infer_engine(s: str) -> str:
        s = s.lower()
        if "atrtrail" in s:
            return "atr_trailing"
        if "chandelier" in s:
            return "chandelier"
        return "unknown"

    trailing["Engine"] = trailing["Strategy"].astype(str).apply(infer_engine)
    trailing = trailing[trailing["Engine"] != "unknown"].copy()

    trailing["Type"] = trailing["Type"].str.lower().str.strip()
    trailing = trailing.sort_values("EntryTime").reset_index(drop=True)

    return trailing


# --------------------------------------------------
# 2. LOAD & PREPARE CANDLES
# --------------------------------------------------

def load_candles(path: Path) -> pd.DataFrame:
    """
    Load MT5-exported M1 XAUUSD candles (HTML table).
    Computes:
      - ATR(5)
      - HH30 / LL30 for chandelier trailing.
    """
    tables = pd.read_html(path)
    raw = tables[0]

    cols = raw.iloc[0].tolist()
    data = raw.iloc[1:].copy()
    data.columns = cols

    data["time"] = pd.to_datetime(data["Date"], format="%Y.%m.%d %H:%M")
    for col in ["Open", "High", "Low", "Close"]:
        data[col] = pd.to_numeric(data[col])

    data = data.sort_values("time").reset_index(drop=True)

    high = data["High"]
    low = data["Low"]
    close = data["Close"]
    prev_close = close.shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    data["ATR5"] = tr.rolling(ATR_PERIOD).mean()

    data["HH30"] = high.rolling(CHAN_LOOKBACK).max()
    data["LL30"] = low.rolling(CHAN_LOOKBACK).min()

    return data


# --------------------------------------------------
# 3. TRADE SIMULATION (per trade, per trail_start_R)
# --------------------------------------------------

def simulate_trade_R(
    trade: pd.Series,
    candles: pd.DataFrame,
    trail_start_R: Optional[float],
) -> float:
    """
    Re-simulate a single trade under a given trail_start_R gate.

    - Uses "Engine" column to decide if this is ATR-trailing or chandelier.
    - Uses S/L as initial risk (1R).
    - Constructs 1R TP from entry +/- |entry - SL|.
    - Walks forward through M1 candles from entry → original exit time.
    - On each bar:
        * Optionally trail SL if:
            Engine is trailing AND ATR is available AND open_R >= trail_start_R
            (or trail_start_R is None / no gate)
        * Check if SL/TP hit within the bar (simplified: worst-case order).
    - If no SL/TP hit before original ExitTime, uses report ExitPrice.

    Returns:
        R multiple for this trade under the chosen trail_start_R.
        np.nan if something is missing / inconsistent.
    """
    engine    = trade["Engine"]
    direction = str(trade["Type"]).lower()
    entry_t   = trade["EntryTime"]
    exit_t    = trade["ExitTime"]
    entry_px  = float(trade["EntryPrice"])
    sl_px     = float(trade["SL"])
    exit_px   = float(trade["ExitPrice"])

    # Basic sanity checks
    if pd.isna(entry_px) or pd.isna(sl_px) or pd.isna(exit_px):
        return np.nan

    sl_dist = abs(entry_px - sl_px)
    if sl_dist <= 0:
        return np.nan

    # Build theoretical 1R TP from entry +/- sl_dist
    if direction == "buy":
        tp_px = entry_px + sl_dist
    else:
        tp_px = entry_px - sl_dist

    # Slice candles between entry and exit times
    # We take the bar that contains entry_time, then step forward.
    idx_start = candles["time"].searchsorted(entry_t) - 1
    if idx_start < 0:
        idx_start = 0
    idx_end = candles["time"].searchsorted(exit_t, side="right")

    sub = candles.iloc[idx_start:idx_end].copy()
    if len(sub) < ATR_PERIOD + 2:
        return np.nan

    current_sl = sl_px
    current_tp = tp_px
    exited_price: Optional[float] = None

    # We skip the entry bar for trailing (like your live bot), so start at i=1.
    for i in range(1, len(sub)):
        bar = sub.iloc[i]
        bar_close = float(bar["Close"])
        bar_high  = float(bar["High"])
        bar_low   = float(bar["Low"])
        atr       = float(bar["ATR5"]) if not pd.isna(bar["ATR5"]) else np.nan
        hh        = float(bar["HH30"]) if not pd.isna(bar["HH30"]) else np.nan
        ll        = float(bar["LL30"]) if not pd.isna(bar["LL30"]) else np.nan

        # Compute "open R" from entry to current close
        if direction == "buy":
            open_R = (bar_close - entry_px) / sl_dist
        else:
            open_R = (entry_px - bar_close) / sl_dist

        # --- Trailing logic ---
        if engine in ("atr_trailing", "chandelier") and not np.isnan(atr):
            # Gate: only trail once open_R >= trail_start_R
            can_trail = False
            if trail_start_R is None:
                can_trail = True
            else:
                can_trail = open_R >= trail_start_R

            if can_trail:
                if engine == "atr_trailing":
                    if direction == "buy":
                        trail_sl = bar_close - ATR_TRAIL_MULT * atr
                        current_sl = max(current_sl, trail_sl)
                    else:
                        trail_sl = bar_close + ATR_TRAIL_MULT * atr
                        current_sl = min(current_sl, trail_sl)
                elif engine == "chandelier" and not np.isnan(hh) and not np.isnan(ll):
                    if direction == "buy":
                        chand_sl = hh - ATR_TRAIL_MULT * atr
                        current_sl = max(current_sl, chand_sl)
                    else:
                        chand_sl = ll + ATR_TRAIL_MULT * atr
                        current_sl = min(current_sl, chand_sl)

        # --- Check for SL/TP hits on this bar ---
        if direction == "buy":
            hit_sl = bar_low <= current_sl
            hit_tp = bar_high >= current_tp
        else:
            hit_sl = bar_high >= current_sl
            hit_tp = bar_low <= current_tp

        if hit_sl or hit_tp:
            # Conservative assumption: SL is hit first if both in same bar.
            if hit_sl:
                exited_price = current_sl
            elif hit_tp:
                exited_price = current_tp
            break

    # If still open by original exit time, use the recorded exit price
    if exited_price is None:
        exited_price = exit_px

    # Compute R outcome
    if direction == "buy":
        R = (exited_price - entry_px) / sl_dist
    else:
        R = (entry_px - exited_price) / sl_dist

    return float(R)


# --------------------------------------------------
# 4. RUN MULTI-scenario backtest
# --------------------------------------------------

def run_scenarios(
    trades: pd.DataFrame,
    candles: pd.DataFrame,
    trail_start_grid: List[Optional[float]],
) -> Dict[Tuple[str, Optional[float]], pd.Series]:
    """
    For each (engine, trail_start_R) pair, compute a Series of R results,
    aligned by trade index.
    """
    results: Dict[Tuple[str, Optional[float]], pd.Series] = {}

    for engine in trades["Engine"].unique():
        engine_trades = trades[trades["Engine"] == engine].reset_index(drop=True)

        for ts in trail_start_grid:
            R_list: List[float] = []

            for _, row in engine_trades.iterrows():
                R = simulate_trade_R(row, candles, trail_start_R=ts)
                R_list.append(R)

            key = (engine, ts)
            results[key] = pd.Series(R_list, name="R")

    return results


def summarise_results(
    trades: pd.DataFrame,
    results: Dict[Tuple[str, Optional[float]], pd.Series],
) -> pd.DataFrame:
    """
    Build a tidy summary DataFrame with:
        engine, trail_start_R, trail_start_R_display,
        is_live, n_trades, win_rate, avg_R, median_R, total_R
    """
    rows = []
    for (engine, ts), R_series in results.items():
        clean = R_series.dropna()
        n = len(clean)
        if n == 0:
            continue

        win_rate = (clean > 0).mean()
        avg_R    = clean.mean()
        med_R    = clean.median()
        total_R  = clean.sum()

        # compare against your live bot defaults
        live_ts = ENGINE_DEFAULT_TRAIL_START.get(engine)
        is_live = (ts == live_ts)

        rows.append({
            "engine": engine,
            "trail_start_R": ts,  # numeric or None
            "trail_start_R_display": "None" if ts is None else ts,
            "is_live": is_live,
            "n_trades": n,
            "win_rate": win_rate,
            "avg_R": avg_R,
            "median_R": med_R,
            "total_R": total_R,
        })

    summary = pd.DataFrame(rows)
    # sort by engine, then numeric trail_start_R (None values will be last)
    summary = summary.sort_values(
        ["engine", "trail_start_R"],
        na_position="last"
    ).reset_index(drop=True)
    return summary


def plot_equity_curves(
    trades: pd.DataFrame,
    results: Dict[Tuple[str, Optional[float]], pd.Series],
):
    """
    Plot cumulative R equity curves per engine and trail_start_R.
    """
    engines = trades["Engine"].unique()
    for engine in engines:
        engine_trades = trades[trades["Engine"] == engine].reset_index(drop=True)
        t_index = range(len(engine_trades))

        plt.figure()
        for ts in TRAIL_START_GRID:
            key = (engine, ts)
            if key not in results:
                continue
            R_series = results[key].dropna()
            if R_series.empty:
                continue
            eq = R_series.cumsum()
            label = f"trail_start_R={ts}" if ts is not None else "trail_start_R=None"
            plt.plot(t_index[: len(eq)], eq.values, label=label)

        plt.title(f"Equity curves (R) — engine={engine}")
        plt.xlabel("Trade # (time-ordered)")
        plt.ylabel("Cumulative R")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()

    plt.show()


# --------------------------------------------------
# 5. MAIN
# --------------------------------------------------

def main():
    print("=== Loading trades & candles ===")
    trades = load_trades(TRADE_REPORT_PATH)
    candles = load_candles(CANDLE_HTML_PATH)

    print(f"Loaded {len(trades)} trades from trailing engines:")
    print(trades["Engine"].value_counts())

    print("=== Running trail_start_R scenarios ===")
    results = run_scenarios(trades, candles, TRAIL_START_GRID)

    summary = summarise_results(trades, results)
    print("\n=== Summary (per engine x trail_start_R) ===")
    print(summary.drop(columns=["trail_start_R"]).to_string(
        index=False,
        float_format=lambda x: f"{x:0.3f}"
    ))

    # save summary
    summary.to_csv(OUTPUT_DIR / "trail_start_summary.csv", index=False)
    summary.to_excel(OUTPUT_DIR / "trail_start_summary.xlsx", index=False)
    print(f"\n[Saved] {OUTPUT_DIR / 'trail_start_summary.csv'}")
    print(f"[Saved] {OUTPUT_DIR / 'trail_start_summary.xlsx'}")

    print("\n=== Plotting equity curves ===")
    plot_equity_curves(trades, results)


if __name__ == "__main__":
    main()
