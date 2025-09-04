
"""
SiRiX -> MT5 Trade Copier (Polling Version, Hardened)
===================================================

This script polls a *master* trader on a SiRiX trading server and mirrors (copies)
that trader's **open / close / size-change trade actions** into a *follower* MetaTrader 5 (MT5) account.

Copies live trading activity from a SiRiX "master" user into an MT5 "follower"
demo account:

- Open: Follower trade created when master opens.
- Close: Follower closed when master closes.
- Size change: Follower scaled.
- SL/TP sync: Copied on open; later changes detected & updated.
- Fixed follower lot size (0.10) regardless of master size (configurable).
- Optional symbol translation (e.g., NQ100 -> NAS100.i).
- Explicit LIVE_CONFIRM_TOKEN required to trade live.
- Symbol whitelist + mapping enforcement (no silent passthrough).
- Per-trade lot cap, total exposure cap, max position count.
- Equity floor guard.
- Daily loss limit (percent/absolute), session drawdown limit.
- Optional per-trade risk% sizing using SL distance (master SL or default).
- Circuit breaker: halts trading; optional auto-close all copier positions.
- Re-links existing copier positions on restart to avoid duplicates.
- SiRiX fetch retry/backoff. Order filling fallback.

SAFE FIRST: run on demo, confirm mapping, then go live.

----------------------------------
IMPORTANT BEFORE LIVE / DEMO USE
----------------------------------
1. Launch MT5 and log in to the follower account *before* running this script.
2. Configure `sirix_token`, `master_user_id`, `symbol_map`, and sizing mode below.
3. Start in `dry_run = True` (used as logging only) until confident flipping real orders.
4. Use a *demo* follower account first.

----------------------------------
LIMITATIONS
----------------------------------
~ No persistence across restarts (link_map lost). Use DB/JSON if needed.
~ Pending order mirroring disabled by default.
~ No retry/backoff on network errors (add for production).
~ Symbol/contract conversion must be customised.

---------------------------------------------------
REQUIRED SETUP
---------------------------------------------------
1. MT5 installed & running on this machine.
2. Demo account credentials (below) are correct OR MT5 terminal is already
   logged into the follower account (if you set auto_login=False).
3. Valid SiRiX bearer token + master_user_id.
4. Symbols visible in MT5 "Market Watch" (script will try to select).

---------------------------------------------------
Risk = 10× Master Risk %  + Daily Loss Cap (No strict caps)
---------------------------------------------------

• Sizing: follower risks K × (master's % risk) per trade, K=RISK_MULTIPLIER (default 10.0).
• If master has SL: use master SL distance; else use DEFAULT_STOP_DISTANCE_PRICE.
• Master % risk is approximated using mapped symbol tick_value/tick_size on the follower's MT5.
• Daily loss cap: halts trading if today's realized PnL <= -DAILY_LOSS_LIMIT_PCT * day_start_equity
  (optionally closes all copier positions).
• Restart-safe relink; mapping whitelist; fill-mode fallback; SL/TP validation; consent prompt.

"""

import re
import os
import json
import math
import time
import signal
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, List, Tuple
from datetime import datetime, date

import requests
import MetaTrader5 as mt5

import subprocess
import shlex
import platform
from pathlib import Path


# ============================ CONFIG =============================

# ---- Core ----
DRY_RUN: bool = False                          # True = logs only. Flip to False for LIVE.
SHOW_PREVIEW_TABLE: bool = True

# ---- SiRiX master ----
SIRIX_API_URL = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
SIRIX_TOKEN: str = "t1_a7xeQOJPnfBzuCncH60yjLFu"
SIRIX_MASTER_USER_ID: str = "214422"

# ---- Polling ----
POLL_INTERVAL_SEC: float = 1.0

# ---- MT5 follower (terminal must be running) ----
AUTO_LOGIN: bool = True
MT5_LOGIN: int = 17111934
MT5_PASSWORD: str = "DDJjp$6B"
MT5_SERVER: str = "STARTRADERFinancial-Live 3"

# ---- MT5 terminal launching (run a specific/second terminal) ----
AUTO_LAUNCH_MT5: bool = True

# Full path to the terminal executable you want to use for this copier:
MT5_TERMINAL_PATH: str = r"C:\MT5\TradeCopierSinu\terminal64.exe"

# Optional command-line args (common: '/portable' to keep data in the exe folder)
MT5_TERMINAL_ARGS = []

# Working directory to start the terminal in (None => folder of MT5_TERMINAL_PATH)
MT5_WORKDIR: Optional[str] = None

# How long to wait (seconds) after launching MT5 before giving up on initialize
MT5_STARTUP_WAIT_SEC: float = 25.0


# ---- Copier behavior ----
MAGIC_NUMBER: int = 86543210
MAX_DEVIATION_POINTS: int = 20
COPY_STOPS: bool = True
VALIDATE_STOPS: bool = True
CLOSE_ON_MASTER_CLOSE: bool = True
COPY_PENDING_ORDERS: bool = False


# ---- Instrument mapping (SiRiX -> MT5) ----
SYMBOL_MAP = {
    "NQ100.": "NAS100.r",
    "GER40.": "GER40.r",
    "WS30.":  "DJ30.r",
    "S&P500": "SP500.r",
    "XAUUSD": "XAUUSD",
    "XAGUSD": "XAGUSD",
    "EURUSD": "EURUSD",
    "GBPUSD": "GBPUSD",
    "USDCAD": "USDCAD",
    "USDJPY": "USDJPY",
}

# ---- Allowed MT5 symbols (whitelist). Empty set => allow any mapped symbol. ----
ALLOWED_SYMBOLS = set(SYMBOL_MAP.values())

# ---- Mapping policy ----
REQUIRE_SYMBOL_MAPPING: bool = True
WARN_ON_UNMAPPED_SYMBOL: bool = True

# ---- Master amount interpretation (used to compute master lots) ----
MASTER_AMOUNT_MODE = "units"  # "units" | "lots" | "contracts"
SYMBOL_UNITS_PER_LOT = {
    "EURUSD": 100_000,
    "GBPUSD": 100_000,
    "USDCAD": 100_000,
    "USDJPY": 100_000,
    "XAUUSD": 100,
    "XAGUSD": 1000,
    "S&P500": 50,
    "NQ100.": 20,
    "WS30.":  5,
    "GER40.": 25,
}

FOLLOWER_UNITS_PER_LOT = {
    # "XAUUSDm": 100,
}

# ---- Sizing rule: K × (master % risk) ----
VOLUME_MODE: str = "fixed"                 # "risk_master_multiple", "fixed", "multiplier"
RISK_MULTIPLIER: float = 10.0              # "10× the master's risk %"

# (Other sizing modes kept available for completeness; not used unless you switch VOLUME_MODE)
FIXED_LOTS: float = 0.01
LOT_MULTIPLIER: float = 1.0
PER_TRADE_RISK_PCT: float = 0.10  # only used if you switch VOLUME_MODE="risk_percent"

# Default SL distance (price units) if master has NO SL or invalid distance
DEFAULT_STOP_DISTANCE_PRICE = {
    "XAUUSD":  10.0,   # $10
    "XAGUSD": 0.50,    # $0.50
    "NAS100.r": 50.0,  # index pts
    "GER40.r": 50.0,
    "DJ30.r":  100.0,
    "SP500.r": 25.0,
    "EURUSD": 0.0020,  # 20 pips
    "GBPUSD": 0.0020,
    "USDCAD": 0.0020,
    "USDJPY": 0.20,    # 20 pips on 2dp quoting
}

# ---- Daily loss cap (ONLY cap retained) ----
DAILY_LOSS_LIMIT_PCT: float = 0.10   # e.g., 0.10 = 10% of day-start equity
HALT_CLOSE_ALL: bool = True          # close all copier positions on halt

# ---- Files ----
STATE_PATH: str = "copier_state.json"

# ---- Misc ----
STOP_CHANGE_ABS_TOLERANCE: float = 1e-6
SIRIX_BUY_VALUE, SIRIX_SELL_VALUE = 0, 1


# ---- Debug (one-time prints on startup) ----
DEBUG_PRINT_SYMBOLS_ON_START: bool = True   # prints master symbols + mapping + MT5 tick spec
DEBUG_PRINT_RISK_PREVIEW: bool = True       # prints mLots, stop distance, master % risk, follower lots
RISK_PREVIEW_LIMIT: int = 12                # how many positions to show in the preview
DEBUG_PRINT_SIRIX_SUMMARY: bool = True
DEBUG_DUMP_SIRIX_USERDATA: bool = True


# Attach to MT5 in dry-run just to read symbol specs, but still never place orders
DEBUG_ATTACH_MT5_IN_DRY_RUN: bool = True
# Use real tick_value/tick_size for risk preview even in dry-run (if MT5 attached)
DEBUG_USE_REAL_TICK_IN_DRY_RUN: bool = True


# =================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("copier").log

running = True
TRADING_HALTED = False
SESSION_DAY_KEY: Optional[str] = None
DAY_START_EQUITY: Optional[float] = None

last_sirix_equity: Optional[float] = None
preview_printed = False

_warned_unmapped: Dict[str, bool] = {}
_symbol_filling_cache: Dict[str, int] = {}
reverse_symbol_map: Dict[str, str] = {}

_comment_oid_re = re.compile(r"Copy SiRiX\s+(\d+)")

@dataclass
class MasterPosition:
    order_number: Any
    symbol: str
    side: int
    amount: float
    open_time: Optional[str] = None
    open_rate: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None

@dataclass
class FollowerLink:
    master_order_number: Any
    master_symbol: str
    follower_symbol: str
    follower_ticket: int
    follower_volume: float
    side: int
    sl: Optional[float] = None
    tp: Optional[float] = None

link_map: Dict[Any, FollowerLink] = {}
master_open_snapshot: Dict[Any, MasterPosition] = {}

# ------------------------- Utilities -------------------------

def LOG(msg: str, level=logging.INFO):
    log(level, msg)

def sirix_is_buy(side: int) -> bool:
    return side == SIRIX_BUY_VALUE

def sirix_dir_01(side: int) -> int:
    return 1 if sirix_is_buy(side) else 0

def sirix_dir_char(side: int) -> str:
    return "B" if sirix_is_buy(side) else "S"

def today_key() -> str:
    return date.today().isoformat()

def load_state() -> Dict[str, Any]:
    try:
        with open(STATE_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state: Dict[str, Any]) -> None:
    try:
        with open(STATE_PATH, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        LOG(f"[WARN] Failed to save state: {e}", logging.WARNING)

# ------------------------- MT5 helpers -----------------------

def launch_mt5_terminal() -> bool:
    """Start the specified MT5 terminal in a separate console/window."""
    exe = (MT5_TERMINAL_PATH or "").strip()
    if not exe:
        LOG("[LAUNCH] MT5_TERMINAL_PATH is empty; cannot launch.", logging.ERROR)
        return False

    # Build arg list
    if isinstance(MT5_TERMINAL_ARGS, (list, tuple)):
        extra = list(MT5_TERMINAL_ARGS)
    elif isinstance(MT5_TERMINAL_ARGS, str):
        extra = shlex.split(MT5_TERMINAL_ARGS)
    else:
        extra = []

    args = [exe] + extra
    workdir = MT5_WORKDIR or str(Path(exe).resolve().parent)

    try:
        if platform.system() == "Windows":
            # Open in a separate console window
            creationflags = subprocess.CREATE_NEW_CONSOLE
            subprocess.Popen(args, cwd=workdir, creationflags=creationflags)
        else:
            # macOS/Linux/Wine/CrossOver — just start the process
            subprocess.Popen(args, cwd=workdir)
        LOG(f"[LAUNCH] Started MT5: {args} (cwd={workdir})")
        return True
    except Exception as e:
        LOG(f"[LAUNCH-ERR] Failed to start MT5: {e}", logging.ERROR)
        return False

def init_mt5() -> bool:
    """
    Initialize MT5, launching the specified terminal if needed, then login.
    """

    LOG(f"[init] MT5 path exists? {os.path.exists(MT5_TERMINAL_PATH)}")

    # Allow attaching in dry-run solely for reading symbol specs
    if DRY_RUN and not DEBUG_ATTACH_MT5_IN_DRY_RUN:
        LOG("[init] DRY RUN: skipping MT5 initialize")
        return True

    # Try to attach to the specified terminal first (bind to the exact instance)
    path_kw = {"path": MT5_TERMINAL_PATH} if MT5_TERMINAL_PATH else {}

    if mt5.initialize(**path_kw):
        LOG("[init] Attached to MT5 (already running).")
    else:
        LOG(f"[init] MT5 initialize failed: {mt5.last_error()}", logging.WARNING)
        if AUTO_LAUNCH_MT5:
            ok = launch_mt5_terminal()
            if not ok:
                return False
            # Wait and retry initialize until the terminal is ready
            deadline = time.time() + MT5_STARTUP_WAIT_SEC
            while time.time() < deadline:
                time.sleep(1.0)
                if mt5.initialize(**path_kw):
                    LOG("[init] Attached to MT5 after launch.")
                    break
            else:
                LOG("[ERR] Could not attach to MT5 within startup window.", logging.ERROR)
                return False
        else:
            return False

    # Perform API-level login if requested
    if AUTO_LOGIN:
        if not (MT5_LOGIN and MT5_PASSWORD and MT5_SERVER):
            LOG("[ERR] AUTO_LOGIN=True but credentials missing.", logging.ERROR)
            return False
        if not mt5.login(login=MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER):
            LOG(f"[ERR] MT5 login failed: {mt5.last_error()}", logging.ERROR)
            return False
        LOG(f"[init] Logged into MT5 account {MT5_LOGIN} ({MT5_SERVER}).")
    else:
        LOG("[init] Using whatever account the terminal is already logged into.")

    # Nice banner (handles dry-run banner too)
    print_account_banner()

    return True

def print_account_banner():
    """
    Print a nice one-liner showing which account is connected.
    """
    if DRY_RUN:
        # In dry-run we aren’t attached to MT5, but we can still show a banner
        eq = account_equity()  # returns 10000.0 dummy in DRY_RUN per code
        print(
            "\nMT5 (DRY RUN)\n"
            f"TERMINAL: {MT5_TERMINAL_PATH}\n"
            f"EQUITY (dummy): ${eq:.2f}\n"
        )
        return

    ai = mt5.account_info()
    if not ai:
        LOG("[init] Connected but could not retrieve account info.", logging.WARNING)
        return

    print(
        "\nMT5 ACCOUNT CONNECTED!\n"
        f"ACCOUNT: {ai.login}  ||  SERVER:  {ai.server}  ||  BALANCE: ${ai.balance:.2f}  ||  EQUITY:  ${ai.equity:.2f}"
    )

def shutdown_mt5():
    if not DRY_RUN:
        mt5.shutdown()

def mt5_symbol_prepare(symbol: str) -> bool:
    if DRY_RUN:
        return True
    info = mt5.symbol_info(symbol)
    if info and info.visible:
        return True
    if mt5.symbol_select(symbol, True):
        return True
    LOG(f"[ERR] Unable to select MT5 symbol {symbol}.", logging.ERROR)
    return False

def mt5_current_prices(symbol: str) -> Tuple[Optional[float], Optional[float]]:
    if DRY_RUN:
        return 1.0, 1.0
    tick = mt5.symbol_info_tick(symbol)
    if not tick:
        return None, None
    return tick.bid, tick.ask

def account_equity() -> Optional[float]:
    if DRY_RUN:
        return 10000.0  # dummy equity for dry-run sizing
    ai = mt5.account_info()
    return float(ai.equity) if ai else None

def _positions_with_magic() -> List[Any]:
    if DRY_RUN:
        return []
    pos = mt5.positions_get()
    if not pos:
        return []
    return [p for p in pos if getattr(p, "magic", 0) == MAGIC_NUMBER]

# ------------------------- Mapping & order send ---------------

def map_symbol(sirix_symbol: str) -> Optional[str]:
    if sirix_symbol in SYMBOL_MAP:
        mapped = SYMBOL_MAP[sirix_symbol]
    else:
        if REQUIRE_SYMBOL_MAPPING and not DRY_RUN:
            LOG(f"[FATAL] No mapping for SiRiX '{sirix_symbol}' and REQUIRE_SYMBOL_MAPPING=True.", logging.ERROR)
            return None
        mapped = sirix_symbol
        if WARN_ON_UNMAPPED_SYMBOL and not _warned_unmapped.get(sirix_symbol):
            LOG(f"[WARN] No mapping for {sirix_symbol}; using same name.")
            _warned_unmapped[sirix_symbol] = True
    if ALLOWED_SYMBOLS and mapped not in ALLOWED_SYMBOLS and not DRY_RUN:
        LOG(f"[FATAL] '{mapped}' not in ALLOWED_SYMBOLS; refusing to trade.", logging.ERROR)
        return None
    return mapped

def pick_filling_mode(symbol: str) -> int:
    if symbol in _symbol_filling_cache:
        return _symbol_filling_cache[symbol]
    default_seq = [mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_RETURN]
    info = mt5.symbol_info(symbol) if not DRY_RUN else None
    if info is not None:
        reported = getattr(info, "filling_mode", None)
        if reported in default_seq:
            _symbol_filling_cache[symbol] = reported
            LOG(f"[filling] Using broker-reported filling {reported} for {symbol}.")
            return reported
    _symbol_filling_cache[symbol] = mt5.ORDER_FILLING_FOK
    return mt5.ORDER_FILLING_FOK

def _order_send_with_fallback(request: dict, symbol: str):
    if DRY_RUN:
        class _Dummy: retcode=mt5.TRADE_RETCODE_DONE; order=-1; deal=-1
        return _Dummy()
    seq = [pick_filling_mode(symbol)]
    for m in (mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_RETURN):
        if m not in seq: seq.append(m)
    last = None
    for mode in seq:
        request["type_filling"] = mode
        res = mt5.order_send(request)
        last = res
        if res.retcode == 10030:  # unsupported filling
            LOG(f"[filling] {symbol} rejected mode={mode}; trying next...", logging.WARNING)
            continue
        if res.retcode in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_PLACED, mt5.TRADE_RETCODE_DONE_PARTIAL):
            _symbol_filling_cache[symbol] = mode
        return res
    return last

def normalize_lot(symbol: str, lots: float) -> float:
    lots = max(lots, 0.0)
    if DRY_RUN:
        return round(lots, 2)
    info = mt5.symbol_info(symbol)
    if info is None:
        return round(lots, 2)
    step = getattr(info, "volume_step", 0.01) or 0.01
    min_vol = getattr(info, "volume_min", 0.01) or 0.01
    max_vol = getattr(info, "volume_max", 100.0) or 100.0
    lots = math.floor(lots / step) * step
    lots = max(min_vol, min(lots, max_vol))
    return lots

def validate_stops_for_mt5(symbol: str, side: int, dir01: int,
                           sl: Optional[float], tp: Optional[float],
                           entry_price: float) -> Tuple[Optional[float], Optional[float]]:
    if not VALIDATE_STOPS or DRY_RUN:
        return sl, tp
    info = mt5.symbol_info(symbol)
    if info is None:
        return sl, tp
    point = getattr(info, "point", 0.0) or 0.0
    stop_level_points = getattr(info, "stop_level", 0) or 0
    min_dist = stop_level_points * point if point else 0.0
    digits = getattr(info, "digits", None)
    if sl is not None:
        if dir01 == 1 and sl >= entry_price: sl = None
        elif dir01 == 0 and sl <= entry_price: sl = None
        elif min_dist and abs(entry_price - sl) < min_dist: sl = None
    if tp is not None:
        if dir01 == 1 and tp <= entry_price: tp = None
        elif dir01 == 0 and tp >= entry_price: tp = None
        elif min_dist and abs(entry_price - tp) < min_dist: tp = None
    if digits is not None and digits >= 0:
        if sl is not None: sl = round(sl, digits)
        if tp is not None: tp = round(tp, digits)
    return sl, tp

# ----------------------- Risk helpers ------------------------

def _money_loss_per_lot(symbol: str, stop_distance_price: float) -> Optional[float]:
    """
    Approximate cash loss for 1.0 lot if price moves 'stop_distance_price' against the position.
    """

    # In dry-run, use real tick specs if attached; otherwise dummy fallback
    if DRY_RUN and not DEBUG_USE_REAL_TICK_IN_DRY_RUN:
        return max(stop_distance_price, 0.0) * 1.0

    info = mt5.symbol_info(symbol)

    if info is None:
        # If we couldn't attach in dry-run, still provide a harmless dummy
        if DRY_RUN:
            return max(stop_distance_price, 0.0) * 1.0
        return None

    tick_value = getattr(info, "trade_tick_value", 0.0) or getattr(info, "tick_value", 0.0) or 0.0
    tick_size  = getattr(info, "trade_tick_size", 0.0)  or getattr(info, "tick_size", 0.0)  or 0.0
    if tick_value <= 0 or tick_size <= 0:
        return None
    ticks = stop_distance_price / tick_size
    return float(ticks * tick_value)

def _master_amount_to_lots_raw(master_symbol: str, master_amount: float) -> float:
    """Convert SiRiX 'Amount' to LOTS ignoring the follower sizing mode."""
    if master_amount is None:
        return 0.0
    if MASTER_AMOUNT_MODE == "lots":
        return float(master_amount)
    elif MASTER_AMOUNT_MODE == "units":
        units_per_lot = SYMBOL_UNITS_PER_LOT.get(master_symbol, 100_000.0)
        return float(master_amount) / float(units_per_lot)
    else:  # "contracts"
        return float(master_amount)

def _stop_distance_for(mp: MasterPosition, mapped_symbol: str) -> float:
    # Prefer master SL distance if provided
    try:
        if mp.stop_loss is not None and mp.open_rate:
            d = abs(float(mp.open_rate) - float(mp.stop_loss))
            if d > 0:
                return d
    except Exception:
        pass
    # Fallback to default per symbol
    return DEFAULT_STOP_DISTANCE_PRICE.get(mapped_symbol, DEFAULT_STOP_DISTANCE_PRICE.get(mp.symbol, 10.0))

def _risk_multiple_of_master_lots(mp: MasterPosition) -> float:
    """Core: follower lots = (RISK_MULTIPLIER × master's % risk) applied to follower equity."""
    mapped_symbol = map_symbol(mp.symbol)
    if mapped_symbol is None:
        return 0.0

    stop_distance = _stop_distance_for(mp, mapped_symbol)
    money_per_lot = _money_loss_per_lot(mapped_symbol, stop_distance)
    if not money_per_lot or money_per_lot <= 0:
        LOG(f"[RISK] Cannot evaluate tick value for {mapped_symbol}; falling back to FIXED_LOTS={FIXED_LOTS}", logging.WARNING)
        return FIXED_LOTS

    master_lots = _master_amount_to_lots_raw(mp.symbol, mp.amount)
    if master_lots <= 0:
        LOG(f"[RISK] Master lots computed <= 0 for {mp.symbol}; using FIXED_LOTS.", logging.WARNING)
        return FIXED_LOTS

    master_eq = last_sirix_equity
    if not master_eq or master_eq <= 0:
        LOG("[RISK] Master equity unknown; will wait for next poll to size accurately.", logging.WARNING)
        return 0.0  # safer: skip until we know master equity

    # Master % risk (approx, using follower tick values for mapped symbol)
    master_risk_cash = master_lots * money_per_lot
    master_risk_pct  = master_risk_cash / float(master_eq)

    # Follower risk %
    follower_eq = account_equity()
    if not follower_eq or follower_eq <= 0:
        return 0.0
    follower_risk_pct  = RISK_MULTIPLIER * master_risk_pct
    follower_risk_cash = follower_eq * follower_risk_pct

    lots = follower_risk_cash / money_per_lot
    return max(lots, 0.0)

def risk_percent_lots(symbol: str, stop_distance_price: float) -> float:
    """Legacy: follower risks PER_TRADE_RISK_PCT of equity."""
    eq = account_equity()
    if eq is None or eq <= 0:
        return 0.0
    risk_cash = eq * PER_TRADE_RISK_PCT
    money_per_lot = _money_loss_per_lot(symbol, stop_distance_price)
    if not money_per_lot or money_per_lot <= 0:
        LOG(f"[RISK] Cannot evaluate tick value for {symbol}; falling back to FIXED_LOTS={FIXED_LOTS}", logging.WARNING)
        return FIXED_LOTS
    lots = risk_cash / money_per_lot
    return max(lots, 0.0)

# ------------------------- Sizing modes ----------------------

def get_units_per_lot(symbol: str) -> float:
    return SYMBOL_UNITS_PER_LOT.get(symbol, 100_000.0)

def get_follower_units_per_lot(symbol: str) -> float:
    return FOLLOWER_UNITS_PER_LOT.get(symbol, get_units_per_lot(symbol))

def calc_equity_ratio() -> float:
    global last_sirix_equity
    try:
        follower_equity = account_equity()
    except Exception:
        follower_equity = None
    master_eq = last_sirix_equity
    if not follower_equity or not master_eq or master_eq <= 0:
        return 1.0
    return float(follower_equity) / float(master_eq)

def convert_master_amount_to_lots(master_symbol: str, master_amount: float, follower_symbol: Optional[str] = None) -> float:
    """Used only by non-risk modes; kept for completeness."""
    if master_amount is None:
        return 0.0
    if MASTER_AMOUNT_MODE == "lots":
        base_lots = float(master_amount)
    elif MASTER_AMOUNT_MODE == "units":
        units_per_lot = get_follower_units_per_lot(follower_symbol) if follower_symbol else get_units_per_lot(master_symbol)
        base_lots = float(master_amount) / float(units_per_lot)
    else:
        base_lots = float(master_amount)
    if VOLUME_MODE == "fixed":
        lots = FIXED_LOTS
    elif VOLUME_MODE == "multiplier":
        lots = base_lots * LOT_MULTIPLIER
    elif VOLUME_MODE == "equity_ratio":
        lots = base_lots * calc_equity_ratio()
    else:
        lots = base_lots
    return max(lots, 0.0)

# ------------------------- MT5 ops ---------------------------

def side_to_mt5_order_type(side: int) -> int:
    return mt5.ORDER_TYPE_BUY if sirix_is_buy(side) else mt5.ORDER_TYPE_SELL

def side_to_reverse_order_type(side: int) -> int:
    return mt5.ORDER_TYPE_SELL if sirix_is_buy(side) else mt5.ORDER_TYPE_BUY

def safety_checks_ok(symbol: str, requested_lots: float, opening: bool) -> bool:
    if TRADING_HALTED:
        LOG("[SAFETY] Trading is halted by daily loss cap.", logging.ERROR)
        return False
    if requested_lots <= 0:
        LOG("[SAFETY] Non-positive lot request; refusing.", logging.WARNING)
        return False
    return True

def mt5_open_trade(master_pos: MasterPosition, lots: float, sl: Optional[float], tp: Optional[float]) -> Optional[int]:
    symbol = map_symbol(master_pos.symbol)
    if symbol is None:
        return None
    if not mt5_symbol_prepare(symbol):
        return None
    lots = normalize_lot(symbol, lots)
    if not safety_checks_ok(symbol, lots, opening=True):
        return None
    side = side_to_mt5_order_type(master_pos.side)
    bid, ask = mt5_current_prices(symbol)
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        LOG(f"[ERR] No valid price for {symbol}; cannot trade.", logging.ERROR)
        return None
    price = ask if side == mt5.ORDER_TYPE_BUY else bid
    if not COPY_STOPS:
        sl = None; tp = None
    else:
        sl, tp = validate_stops_for_mt5(symbol, master_pos.side, sirix_dir_01(master_pos.side), sl, tp, price)
    req = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": lots,
        "type": side,
        "price": price,
        "deviation": MAX_DEVIATION_POINTS,
        "magic": MAGIC_NUMBER,
        "comment": f"Copy SiRiX {master_pos.order_number}",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK,
    }
    if sl is not None: req["sl"] = sl
    if tp is not None: req["tp"] = tp
    if DRY_RUN:
        LOG(f"[DRY] Would open {symbol} {lots} lots side={master_pos.side} @ {price} (SL={sl} TP={tp}).")
        return -1
    res = _order_send_with_fallback(req, symbol)
    if res.retcode not in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_PLACED, mt5.TRADE_RETCODE_DONE_PARTIAL):
        LOG(f"[ERR] MT5 open failed ({symbol}): retcode={res.retcode} details={res}", logging.ERROR)
        return None
    ticket = res.order if res.order else res.deal
    LOG(f"[MT5] Opened {symbol} {lots} lots (ticket {ticket}) for master {master_pos.order_number}.")
    return ticket

def mt5_close_trade(link: FollowerLink) -> bool:
    symbol = link.follower_symbol
    if not mt5_symbol_prepare(symbol):
        return False
    if DRY_RUN:
        LOG(f"[DRY] Would close ticket {link.follower_ticket} ({symbol}).")
        return True
    pos_list = mt5.positions_get(ticket=link.follower_ticket)
    if not pos_list:
        LOG(f"[WARN] No MT5 position found for ticket {link.follower_ticket} (already closed?).", logging.WARNING)
        return False
    pos = pos_list[0]
    close_side = side_to_reverse_order_type(link.side)
    bid, ask = mt5_current_prices(symbol)
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        LOG(f"[ERR] No valid price for {symbol}; cannot close.", logging.ERROR)
        return False
    price = ask if close_side == mt5.ORDER_TYPE_BUY else bid
    req = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": pos.volume,
        "type": close_side,
        "position": link.follower_ticket,
        "price": price,
        "deviation": MAX_DEVIATION_POINTS,
        "magic": MAGIC_NUMBER,
        "comment": f"Close copy of SiRiX {link.master_order_number}",
    }
    res = _order_send_with_fallback(req, symbol)
    if res.retcode not in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_DONE_PARTIAL):
        LOG(f"[ERR] MT5 close failed ticket {link.follower_ticket}: retcode={res.retcode}", logging.ERROR)
        return False
    LOG(f"[MT5] Closed ticket {link.follower_ticket} (master {link.master_order_number}).")
    return True

def mt5_adjust_volume(link: FollowerLink, new_lots: float) -> bool:
    symbol = link.follower_symbol
    if not mt5_symbol_prepare(symbol):
        return False
    if DRY_RUN:
        if new_lots <= 0: LOG(f"[DRY] SCALE->0 so would CLOSE ticket {link.follower_ticket}")
        else: LOG(f"[DRY] SCALE ticket {link.follower_ticket} -> {new_lots} lots")
        link.follower_volume = new_lots
        return True
    pos_list = mt5.positions_get(ticket=link.follower_ticket)
    if not pos_list:
        LOG(f"[WARN] No MT5 position found for ticket {link.follower_ticket}; cannot adjust.", logging.WARNING)
        return False
    pos = pos_list[0]
    current_lots = float(pos.volume)
    new_lots = normalize_lot(symbol, new_lots)
    if not safety_checks_ok(symbol, new_lots, opening=False):
        return False
    if new_lots <= 0:
        LOG(f"[adj] Target lots=0. Closing ticket {link.follower_ticket}.")
        ok = mt5_close_trade(link)
        if ok: link.follower_volume = 0.0
        return ok
    diff = new_lots - current_lots
    if abs(diff) < 1e-8:
        return True
    bid, ask = mt5_current_prices(symbol)
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        LOG(f"[ERR] No price for {symbol}; cannot adjust volume.", logging.ERROR)
        return False
    if diff > 0:
        side = side_to_mt5_order_type(link.side)
        price = ask if side == mt5.ORDER_TYPE_BUY else bid
        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": diff,
            "type": side,
            "price": price,
            "deviation": MAX_DEVIATION_POINTS,
            "magic": MAGIC_NUMBER,
            "comment": f"Volume add copy of SiRiX {link.master_order_number}",
        }
    else:
        reduce_side = side_to_reverse_order_type(link.side)
        price = ask if reduce_side == mt5.ORDER_TYPE_BUY else bid
        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": abs(diff),
            "type": reduce_side,
            "position": link.follower_ticket,
            "price": price,
            "deviation": MAX_DEVIATION_POINTS,
            "magic": MAGIC_NUMBER,
            "comment": f"Volume reduce copy of SiRiX {link.master_order_number}",
        }
    res = _order_send_with_fallback(req, symbol)
    if res.retcode not in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_DONE_PARTIAL, mt5.TRADE_RETCODE_PLACED):
        LOG(f"[ERR] Volume adjust failed ticket {link.follower_ticket}: retcode={res.retcode}", logging.ERROR)
        return False
    new_pos = mt5.positions_get(ticket=link.follower_ticket)
    if new_pos:
        link.follower_volume = float(new_pos[0].volume)
        LOG(f"[MT5] Adjusted volume for {link.follower_ticket} -> now {link.follower_volume} lots.")
    else:
        link.follower_volume = 0.0
        LOG(f"[MT5] After adjust, ticket {link.follower_ticket} not found (closed?).", logging.WARNING)
    return True

def mt5_update_stops(link: FollowerLink, sl: Optional[float], tp: Optional[float]) -> bool:
    symbol = link.follower_symbol; ticket = link.follower_ticket
    if not mt5_symbol_prepare(symbol):
        return False
    old_sl, old_tp = link.sl, link.tp
    if sl is not None and old_sl is not None and abs(sl - old_sl) < STOP_CHANGE_ABS_TOLERANCE: sl = old_sl
    if tp is not None and old_tp is not None and abs(tp - old_tp) < STOP_CHANGE_ABS_TOLERANCE: tp = old_tp
    if sl == old_sl and tp == old_tp:
        return True
    if DRY_RUN:
        LOG(f"[DRY] UPDATE SLTP ticket {ticket} SL={sl} TP={tp}")
        link.sl, link.tp = sl, tp
        return True
    req = {
        "action": mt5.TRADE_ACTION_SLTP,
        "symbol": symbol,
        "position": ticket,
        "sl": sl if sl is not None else 0.0,
        "tp": tp if tp is not None else 0.0,
        "magic": MAGIC_NUMBER,
        "comment": f"Stops sync {link.master_order_number}",
    }
    res = _order_send_with_fallback(req, symbol)
    if res.retcode not in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_PLACED, mt5.TRADE_RETCODE_DONE_PARTIAL):
        LOG(f"[ERR] SL/TP update failed ticket {ticket}: retcode={res.retcode}", logging.ERROR)
        return False
    LOG(f"[MT5] Updated stops ticket {ticket}: SL={sl} TP={tp}")
    link.sl, link.tp = sl, tp
    return True

# ------------------------- SiRiX API -------------------------

def fetch_userstatus_payload(user_id: str,
                             get_open=True, get_pending=False, get_closed=True, get_monetary=False,
                             token: Optional[str] = None) -> Optional[Dict[str, Any]]:
    tk = token if token is not None else SIRIX_TOKEN
    headers = {"Authorization": f"Bearer {tk}", "Content-Type": "application/json", "Accept": "application/json"}
    payload = {
        "UserID": str(user_id),
        "GetOpenPositions": bool(get_open),
        "GetPendingPositions": bool(get_pending),
        "GetClosePositions": bool(get_closed),
        "GetMonetaryTransactions": bool(get_monetary),
    }
    backoffs = [0.2, 0.5, 1.0, 2.0]
    last_err = None
    for i, delay in enumerate(backoffs, start=1):
        try:
            resp = requests.post(SIRIX_API_URL, headers=headers, json=payload, timeout=30)
            if resp.status_code == 200:
                try:
                    return resp.json()
                except Exception as e:
                    last_err = f"JSON parse error: {e}"
            else:
                last_err = f"HTTP {resp.status_code}: {resp.text[:300]}"
        except Exception as e:
            last_err = f"Network error: {e}"
        LOG(f"[WARN] SiRiX fetch attempt {i} failed: {last_err}. Backing off {delay}s.", logging.WARNING)
        time.sleep(delay)
    LOG(f"[ERR] SiRiX fetch failed after retries: {last_err}", logging.ERROR)
    return None

# ---------------------- Snapshot build -----------------------

def extract_stops(pos: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    sl = pos.get("StopLoss"); tp = pos.get("TakeProfit")
    try: sl = float(sl) if sl not in (None, "", 0) else None
    except Exception: sl = None
    try: tp = float(tp) if tp not in (None, "", 0) else None
    except Exception: tp = None
    return sl, tp

def build_master_snapshot(data: Dict[str, Any]) -> Dict[Any, MasterPosition]:
    snap: Dict[Any, MasterPosition] = {}
    for p in data.get("OpenPositions", []) or []:
        order_number = p.get("OrderNumber")
        symbol = p.get("Symbol")
        side = p.get("Side")
        if side not in (SIRIX_BUY_VALUE, SIRIX_SELL_VALUE):
            LOG(f"[WARN] Invalid side '{side}' in SiRiX data (order {order_number}); default BUY.", logging.WARNING)
            side = SIRIX_BUY_VALUE
        amount = p.get("Amount") or 0
        sl, tp = extract_stops(p)
        mp = MasterPosition(
            order_number=order_number,
            symbol=symbol,
            side=side,
            amount=amount,
            open_time=p.get("OpenTime"),
            open_rate=p.get("OpenRate"),
            stop_loss=sl,
            take_profit=tp,
        )
        snap[order_number] = mp
    return snap

def update_master_equity(data: Dict[str, Any]) -> None:
    global last_sirix_equity
    try:
        bal = (data.get("UserData") or {}).get("AccountBalance") or {}
        eq = bal.get("Equity")
        if eq is not None:
            last_sirix_equity = float(eq)
    except Exception:
        pass

# ---------------------- Preview table ------------------------

def print_preview_table(master_snap: Dict[Any, MasterPosition]) -> None:
    if not master_snap:
        LOG("[preview] No open master positions.")
        return
    LOG(f"[preview-debug] Master symbols in snapshot: {set(mp.symbol for mp in master_snap.values())}")
    headers = ("MasterOrd", "M.Symbol", "Side", "M.Amt", "F.Symbol", "F.Lots", "Action")
    fmt = "{:<12} {:<10} {:<4} {:>12} {:<12} {:>8} {:<10}"
    print("\n" + fmt.format(*headers))
    print("-" * 76)
    for oid, mp in master_snap.items():
        m_sym = mp.symbol
        f_sym = map_symbol(m_sym)
        # In risk_master_multiple, follower target lots depend on master equity/tick data; preview shows 0
        f_lots = 0.0 if f_sym else 0.0
        act = "BLOCKED" if f_sym is None else ("OPEN" if oid not in link_map else "NONE")
        print(fmt.format(str(oid), m_sym, sirix_dir_char(mp.side), f"{mp.amount:.2f}", str(f_sym), f"{f_lots:.2f}", act))
    print()

# ------------------------ Copy engine ------------------------

def stops_changed(old: Optional[float], new: Optional[float]) -> bool:
    if (old is None) ^ (new is None): return True
    if old is None and new is None:  return False
    return abs(float(old) - float(new)) > STOP_CHANGE_ABS_TOLERANCE

def _target_lots_for_open(mp: MasterPosition) -> float:
    mapped_symbol = map_symbol(mp.symbol)
    if mapped_symbol is None:
        return 0.0
    if VOLUME_MODE == "risk_master_multiple":
        lots = _risk_multiple_of_master_lots(mp)
        return normalize_lot(mapped_symbol, lots)
    elif VOLUME_MODE == "risk_percent":
        d = _stop_distance_for(mp, mapped_symbol)
        lots = risk_percent_lots(mapped_symbol, d)
        return normalize_lot(mapped_symbol, lots)
    else:
        return normalize_lot(mapped_symbol, convert_master_amount_to_lots(mp.symbol, mp.amount, follower_symbol=mapped_symbol))

def open_follower_for_master(mp: MasterPosition) -> None:
    lots = _target_lots_for_open(mp)
    if lots <= 0:
        LOG(f"[INFO] No lots (blocked/0 or waiting for master equity) for master {mp.order_number}.")
        return
    mapped_symbol = map_symbol(mp.symbol)
    if mapped_symbol is None:
        return
    ticket = mt5_open_trade(mp, lots, mp.stop_loss, mp.take_profit)
    if ticket is None:
        LOG(f"[WARN] Failed to open follower for master {mp.order_number} ({mp.symbol}).", logging.WARNING)
        return
    link = FollowerLink(
        master_order_number=mp.order_number,
        master_symbol=mp.symbol,
        follower_symbol=mapped_symbol,
        follower_ticket=ticket,
        follower_volume=lots,
        side=mp.side,
        sl=mp.stop_loss,
        tp=mp.take_profit,
    )
    link_map[mp.order_number] = link

def close_follower_for_master(master_order_number: Any) -> None:
    if not CLOSE_ON_MASTER_CLOSE:
        LOG(f"[SKIP] Master {master_order_number} closed but auto-close disabled.")
        return
    link = link_map.get(master_order_number)
    if not link:
        LOG(f"[WARN] No follower link found for closed master {master_order_number}.", logging.WARNING)
        return
    if DRY_RUN and link.follower_ticket == -1:
        LOG(f"[DRY] Removing dry-run link for master {master_order_number}.")
        del link_map[master_order_number]
        return
    if mt5_close_trade(link):
        del link_map[master_order_number]

def adjust_follower_for_master(mp: MasterPosition) -> None:
    link = link_map.get(mp.order_number)
    if not link:
        LOG(f"[INFO] No existing follower for master {mp.order_number}; opening.")
        open_follower_for_master(mp)
        return
    mapped_symbol = map_symbol(mp.symbol)
    if mapped_symbol is None:
        return
    if VOLUME_MODE == "risk_master_multiple":
        target_lots = normalize_lot(mapped_symbol, _risk_multiple_of_master_lots(mp))
    elif VOLUME_MODE == "risk_percent":
        d = _stop_distance_for(mp, mapped_symbol)
        target_lots = normalize_lot(mapped_symbol, risk_percent_lots(mapped_symbol, d))
    else:
        target_lots = normalize_lot(mapped_symbol, convert_master_amount_to_lots(mp.symbol, mp.amount, follower_symbol=mapped_symbol))
    if DRY_RUN:
        LOG(f"[DRY] SCALE follower {link.follower_ticket} -> {target_lots} lots")
        link.follower_volume = target_lots
        return
    ok = mt5_adjust_volume(link, target_lots)
    if ok and link.follower_volume <= 0:
        if link.master_order_number in link_map:
            del link_map[link.master_order_number]

def update_follower_stops_for_master(mp: MasterPosition) -> None:
    link = link_map.get(mp.order_number)
    if not link:
        LOG(f"[WARN] Stop update: no follower for {mp.order_number}", logging.WARNING)
        return
    sl, tp = mp.stop_loss, mp.take_profit
    if not COPY_STOPS:
        sl = tp = None
    if VALIDATE_STOPS:
        bid, ask = mt5_current_prices(link.follower_symbol)
        if bid is not None and ask is not None:
            entry = ask if sirix_is_buy(link.side) else bid
            sl, tp = validate_stops_for_mt5(link.follower_symbol, link.side, sirix_dir_01(link.side), sl, tp, entry)
    mt5_update_stops(link, sl, tp)

def process_master_changes(new_snap: Dict[Any, MasterPosition]) -> None:
    global master_open_snapshot
    # Daily loss gate every loop
    maybe_update_daily_loss_halt()

    for order_number, mp in new_snap.items():
        if TRADING_HALTED:
            break
        if order_number not in master_open_snapshot:
            open_follower_for_master(mp)
        else:
            prev = master_open_snapshot[order_number]
            if abs((prev.amount or 0) - (mp.amount or 0)) > 1e-8:
                adjust_follower_for_master(mp)
            if COPY_STOPS and (stops_changed(prev.stop_loss, mp.stop_loss) or
                               stops_changed(prev.take_profit, mp.take_profit)):
                update_follower_stops_for_master(mp)

    closed_ids = set(master_open_snapshot.keys()) - set(new_snap.keys())
    for order_number in closed_ids:
        close_follower_for_master(order_number)

    master_open_snapshot = new_snap

# -------------------- Relink on start ------------------------

def load_links_from_mt5() -> None:
    for p in _positions_with_magic():
        mo = _comment_oid_re.search(getattr(p, "comment", "") or "")
        if not mo:
            continue
        try:
            master_oid = mo.group(1)
        except Exception:
            continue
        follower_symbol = p.symbol
        master_symbol = reverse_symbol_map.get(follower_symbol, follower_symbol)
        side = SIRIX_BUY_VALUE if p.type == mt5.POSITION_TYPE_BUY else SIRIX_SELL_VALUE
        link_map[master_oid] = FollowerLink(
            master_order_number=master_oid,
            master_symbol=master_symbol,
            follower_symbol=follower_symbol,
            follower_ticket=p.ticket,
            follower_volume=float(p.volume),
            side=side,
            sl=float(p.sl) if getattr(p, "sl", 0) else None,
            tp=float(p.tp) if getattr(p, "tp", 0) else None,
        )
    if link_map:
        LOG(f"[init] Re-linked {len(link_map)} existing copier positions from MT5.")

# ------------------------ Signals ----------------------------

def _signal_handler(sig, frame):
    global running
    LOG(f"[signal] Received {sig}; stopping...")
    running = False

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ------------------------- Daily loss ------------------------

def history_today_profit_magic() -> float:
    if DRY_RUN:
        return 0.0
    start = datetime.combine(date.today(), datetime.min.time())
    end = datetime.now()
    total = 0.0
    deals = mt5.history_deals_get(start, end)
    if not deals:
        return 0.0
    for d in deals:
        if getattr(d, "magic", 0) == MAGIC_NUMBER:
            total += float(getattr(d, "profit", 0.0))
    return total

def maybe_update_daily_loss_halt():
    global TRADING_HALTED, DAY_START_EQUITY
    if TRADING_HALTED:
        return
    if DAILY_LOSS_LIMIT_PCT <= 0:
        return
    if DAY_START_EQUITY is None or DAY_START_EQUITY <= 0:
        return
    realized_today = history_today_profit_magic()
    loss_today = -realized_today if realized_today < 0 else 0.0
    limit_cash = DAY_START_EQUITY * DAILY_LOSS_LIMIT_PCT
    if loss_today >= limit_cash:
        TRADING_HALTED = True
        LOG(f"[HALT] Daily loss {loss_today:.2f} >= {limit_cash:.2f} ({DAILY_LOSS_LIMIT_PCT*100:.1f}% of day start equity).", logging.ERROR)
        if HALT_CLOSE_ALL:
            positions_close_all("Daily loss limit reached")

def positions_close_all(reason: str):
    if DRY_RUN:
        LOG(f"[HALT-DRY] Would close all copier positions: {reason}")
        return
    for p in list(_positions_with_magic()):
        symbol = p.symbol
        if not mt5_symbol_prepare(symbol):
            continue
        close_side = mt5.ORDER_TYPE_BUY if p.type == mt5.POSITION_TYPE_SELL else mt5.ORDER_TYPE_SELL
        bid, ask = mt5_current_prices(symbol)
        if bid is None or ask is None:
            continue
        price = ask if close_side == mt5.ORDER_TYPE_BUY else bid
        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(p.volume),
            "type": close_side,
            "position": p.ticket,
            "price": price,
            "deviation": MAX_DEVIATION_POINTS,
            "magic": MAGIC_NUMBER,
            "comment": "DailyLossClose",
        }
        res = _order_send_with_fallback(req, symbol)
        if res and res.retcode in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_DONE_PARTIAL):
            LOG(f"[HALT] Closed {symbol} ticket {p.ticket}")
        else:
            LOG(f"[HALT] Failed close ticket {p.ticket}: {getattr(res,'retcode',None)}", logging.ERROR)

# ------------------------- Main loop -------------------------

def _prompt_live_consent():
    print("\n*** LIVE MODE CONFIRMATION ***")
    print("You are about to run the copier LIVE (DRY_RUN=False).")
    print("This will send REAL orders on the follower MT5 account.")
    print("Type exactly: I_UNDERSTAND   (then press Enter)  to proceed.")
    ans = input("> ").strip()
    if ans != "I_UNDERSTAND":
        print("Consent not provided. Exiting.")
        return False
    return True

def _ensure_daily_state():
    global SESSION_DAY_KEY, DAY_START_EQUITY
    SESSION_DAY_KEY = today_key()
    st = load_state()
    day = st.get(SESSION_DAY_KEY, {})
    if "day_start_equity" not in day:
        eq = account_equity()
        DAY_START_EQUITY = float(eq) if eq is not None else None
        day["day_start_equity"] = DAY_START_EQUITY
        day["halted"] = False
        st[SESSION_DAY_KEY] = day
        save_state(st)
    else:
        DAY_START_EQUITY = day.get("day_start_equity")

def _debug_print_symbols_once():
    """One-time: show master symbols seen + mapping + follower tick specs."""
    data = fetch_userstatus_payload(
        SIRIX_MASTER_USER_ID,
        get_open=True, get_pending=True, get_closed=True, get_monetary=False,
        token=SIRIX_TOKEN,
    )
    if not data:
        LOG("[debug] No data from SiRiX for symbol scan.", logging.WARNING)
        return

    seen = set()
    for sec in ("OpenPositions", "PendingPositions", "ClosePositions"):
        for p in (data.get(sec) or []):
            s = p.get("Symbol")
            if s:
                seen.add(s)

    if not seen:
        print("\n[debug] Master symbols seen: (none)\n")
        return

    print("\n[debug] Master symbols seen:", seen)
    for ms in sorted(seen):
        fs = map_symbol(ms)
        state = "(MAPPED)" if ms in SYMBOL_MAP else "(PASSTHRU)"
        print(f"  {ms:10s} -> {str(fs):10s} {state}")
        if fs and not DRY_RUN:
            info = mt5.symbol_info(fs)
            if info:
                ts = getattr(info, "trade_tick_size", None) or getattr(info, "tick_size", None)
                tv = getattr(info, "trade_tick_value", None) or getattr(info, "tick_value", None)
                vs = getattr(info, "volume_step", None)
                print(f"     tick_size={ts}  tick_value={tv}  volume_step={vs}")
    print()


def _debug_print_risk_preview_once():
    """One-time: for current master snapshot, print sizing math (mLots, stop distance, 10× lots)."""
    data = fetch_userstatus_payload(
        SIRIX_MASTER_USER_ID,
        get_open=True, get_pending=False, get_closed=True, get_monetary=False,
        token=SIRIX_TOKEN,
    )
    if not data:
        LOG("[debug] No data from SiRiX for risk preview.", logging.WARNING)
        return

    # capture master equity from payload
    update_master_equity(data)
    snap = build_master_snapshot(data)

    if not snap:
        print("\n[debug] Risk preview: no open master positions right now.\n")
        return

    print("\n[debug] Risk preview (first", RISK_PREVIEW_LIMIT, "positions):")
    shown = 0
    for oid, mp in snap.items():
        mapped = map_symbol(mp.symbol)
        if mapped is None:
            continue

        # stop distance used (master SL if present, else fallback default for mapped symbol)
        stop_d = _stop_distance_for(mp, mapped)

        # master lots from Amount using your SYMBOL_UNITS_PER_LOT
        m_lots = _master_amount_to_lots_raw(mp.symbol, mp.amount)

        # $ loss for 1.0 lot at that stop distance (using follower tick values)
        money_per_lot = _money_loss_per_lot(mapped, stop_d) or 0.0

        # master % risk approximation
        m_eq = float(last_sirix_equity or 0.0)
        m_risk_pct = (m_lots * money_per_lot / m_eq) if (m_eq > 0 and money_per_lot > 0) else 0.0

        # follower target at 10× master risk
        f_eq = float(account_equity() or 0.0)
        f_risk_pct = RISK_MULTIPLIER * m_risk_pct
        f_cash = f_eq * f_risk_pct
        f_lots = (f_cash / money_per_lot) if money_per_lot > 0 else 0.0
        f_lots = normalize_lot(mapped, f_lots)

        print(
            f"  OID={oid}  {mp.symbol:8s}->{mapped:8s}  Amt={mp.amount}  "
            f"mLots={m_lots:.4f}  stopΔ={stop_d}  "
            f"m%={m_risk_pct*100:.3f}%  f%={f_risk_pct*100:.3f}%  fLots≈{f_lots:.4f}"
        )

        # Example check for S&P500/SPX type symbol (optional hint)
        if mp.symbol in ("S&P500", "SPX"):
            try:
                units_per_lot = SYMBOL_UNITS_PER_LOT.get(mp.symbol)
                if units_per_lot:
                    hint_lots = float(mp.amount) / float(units_per_lot)
                    print(f"      ↳ sanity: {mp.symbol} Amt {mp.amount} / {units_per_lot} = {hint_lots:.2f} master lots")
            except Exception:
                pass

        shown += 1
        if shown >= RISK_PREVIEW_LIMIT:
            break
    print()

def _debug_print_sirix_summary_once():
    data = fetch_userstatus_payload(
        SIRIX_MASTER_USER_ID,
        get_open=True, get_pending=True, get_closed=True, get_monetary=True,
        token=SIRIX_TOKEN,
    )
    if not data:
        print("\n[debug] SiRiX summary: no data (auth/network?).\n")
        return

    ud = (data.get("UserData") or {})
    bal = (ud.get("AccountBalance") or {})
    eq  = bal.get("Equity")
    details = (ud.get("UserDetails") or {})

    # Try common fields at the top level, then inside UserDetails
    first = details.get("FirstName") or ud.get("FirstName")
    last  = details.get("LastName")  or ud.get("LastName")
    fullname = details.get("FullName") or ud.get("FullName")
    username = details.get("UserName") or ud.get("UserName") or details.get("Login") or ud.get("Login")

    if fullname and fullname.strip():
        name = fullname
    elif (first or last):
        name = " ".join(p for p in [first, last] if p)
    else:
        name = username or "(unknown)"

    acct_id = ud.get("AccountId") or ud.get("ID") or SIRIX_MASTER_USER_ID

    print("\n[debug] SiRiX summary:")
    print(f"  Master ID: {SIRIX_MASTER_USER_ID}  (payload AccountId: {acct_id})")
    print(f"  User: {name}   Equity: {eq}")

    for sec in ("OpenPositions", "PendingPositions", "ClosePositions", "MonetaryTransactions"):
        items = data.get(sec) or []
        print(f"  {sec}: {len(items)}")

    if DEBUG_DUMP_SIRIX_USERDATA:
        # Show what fields actually exist so you can map them if needed
        print("  UserData keys:", list(ud.keys()))
        print("  AccountBalance keys:", list(bal.keys()))
        # If there are open positions, show a sample of available fields
        sample = (data.get("OpenPositions") or data.get("ClosePositions") or [])
        if sample:
            print("  Sample position keys:", list(sample[0].keys()))

    print()


def poll_loop():
    global preview_printed, reverse_symbol_map, TRADING_HALTED
    reverse_symbol_map = {v: k for k, v in SYMBOL_MAP.items()}

    if not DRY_RUN:
        if not _prompt_live_consent():
            return

    if not init_mt5():
        LOG("[fatal] MT5 init failed. Exiting.", logging.ERROR)
        return

    if DEBUG_PRINT_SYMBOLS_ON_START:
        _debug_print_symbols_once()
    if DEBUG_PRINT_RISK_PREVIEW:
        _debug_print_risk_preview_once()
    if DEBUG_PRINT_SIRIX_SUMMARY:
        _debug_print_sirix_summary_once()

    _ensure_daily_state()
    load_links_from_mt5()

    LOG("[init] Copier running. Ctrl+C to stop.")

    while running:
        # Midnight rollover resets daily state & halt
        if today_key() != SESSION_DAY_KEY:
            _ensure_daily_state()
            TRADING_HALTED = False

        data = fetch_userstatus_payload(
            SIRIX_MASTER_USER_ID,
            get_open=True,
            get_pending=False,
            get_closed=True,
            get_monetary=False,
            token=SIRIX_TOKEN,
        )
        if data is None:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        update_master_equity(data)
        new_snap = build_master_snapshot(data)

        if SHOW_PREVIEW_TABLE and not preview_printed:
            print_preview_table(new_snap)
            preview_printed = True

        process_master_changes(new_snap)
        time.sleep(POLL_INTERVAL_SEC)

    shutdown_mt5()
    LOG("[done] Copier stopped.")

# --------------------------- Main ----------------------------

if __name__ == "__main__":
    if not SIRIX_TOKEN or SIRIX_TOKEN.startswith("PUT_"):
        print("ERROR: Please set SIRIX_TOKEN at the top of the script.")
    elif not SIRIX_MASTER_USER_ID or SIRIX_MASTER_USER_ID.startswith("PUT_"):
        print("ERROR: Please set SIRIX_MASTER_USER_ID at the top of the script.")
    elif not DRY_RUN and AUTO_LOGIN and (not MT5_LOGIN or not MT5_PASSWORD or not MT5_SERVER):
        print("ERROR: AUTO_LOGIN=True requires MT5_LOGIN / MT5_PASSWORD / MT5_SERVER.")
    else:
        poll_loop()
