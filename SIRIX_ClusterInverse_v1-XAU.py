"""
XAU_SiRiX_ClusterInverse_multi — Multi-Engine SiRiX → MT5 XAUUSD Cluster Inverse Bot
====================================================================================

- Polls SiRiX open positions for groups: ["Audition", "Funded", "Purchases"].
- Builds a rolling T-second window of *new* open positions (per OrderID).
- Detects BUY/SELL clusters (>= K_UNIQUE unique traders in window).
- Each strategy independently decides:
    • its cluster window (T_SECONDS),
    • its stop mode (fixed / atr_static / atr_trailing / chandelier),
    • its hold_minutes, TP_R_multiple, ATR params, etc,
    • its risk mode (dynamic_pct or fixed_lots),
    • its MAGIC number, so all engines safely share 1 MT5 account.

- Direction is inverse of cluster (BUY cluster → SELL, SELL cluster → BUY).
- All strategies trade the same symbol (XAUUSD) but are fully independent.

Default risk (per your request):
- risk_mode="dynamic_pct"
- risk_percent=0.02  # 2% of equity per trade per engine
"""

BOT_NAME = "XAU_SiRiX_ClusterInverse_multi"

import sys
import time
import json
from pathlib import Path
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

from pytz import timezone
import MetaTrader5 as mt5
import requests
import pandas as pd

# =========================
# STATE LOGGING CONFIG
# =========================

STATE_PATH = Path("xau_cluster_inverse_multi_state.json")

# =========================
# MT5 ACCOUNT CONFIG
# =========================

MT5_LOGIN         = 52611391
MT5_PASSWORD      = "D&KvS$2KPBrYso"
MT5_SERVER        = "ICMarketsSC-Demo"
MT5_TERMINAL_PATH = r"C:\MT5\TradeCopier-Cluster\terminal64.exe"

LOCAL_TZ   = timezone("Europe/London")
SIRIX_TZ   = timezone("Asia/Jerusalem")  # Sirix server timezone
MT5_SYMBOL = "XAUUSD"   # adjust if broker uses suffix
SYMBOL_INFO = None      # filled by init_mt5()

# =========================
# MT5 COMMENT HELPER
# =========================

# Many brokers are strict about the comment field (length / charset).
# We'll keep comments short and safe.
MAX_COMMENT_LEN = 20

def make_comment(text: str) -> str:
    """
    Truncate any comment to a safe length for MT5.
    """
    if text is None:
        return ""
    return str(text)[:MAX_COMMENT_LEN]

# =========================
# GLOBAL LOOP CONFIG
# =========================

POLL_INTERVAL_SECONDS = 1
CLUSTER_REFRACTORY_SECONDS = 1  # min gap between clusters of same SIDE per strategy

# =========================
# LOGGING / VERBOSITY
# =========================

VERBOSE_CLUSTERS = True   # set True if you ever want to debug cluster timing
VERBOSE_CLUSTER_DEBUG = False    # extra noisy [CLUSTER_DEBUG] spam

# =========================
# CLOSE POSITION LOG
# =========================

# Track explicit close reasons when closed via our own logic
RECENT_CLOSED_REASONS: Dict[int, str] = {}


# =========================
# SiRiX API CONFIG
# =========================

BASE_URL       = "https://restapi-real3.sirixtrader.com"
SIRIX_ENDPOINT = "/api/ManagementService/GetOpenPositionsForGroups"

TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"

GROUPS           = ["Audition", "Funded", "Purchases"]
SIRIX_INSTRUMENT = "XAUUSD"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# =========================
# SESSION FILTER (optional)
# =========================

USE_SESSION_FILTER = False
SESSION_START_HHMM = "08:00"
SESSION_END_HHMM   = "18:00"

# =========================
# DATA CLASSES
# =========================

@dataclass
class SirixPositionEvent:
    order_id: str
    user_id: str
    side: str        # "buy" or "sell"
    lots: float
    time: datetime   # UTC-aware


@dataclass
class BotPositionInfo:
    ticket: int
    direction: str      # "buy" or "sell"
    entry_time: datetime
    entry_price: float
    sl_price: float
    tp_price: Optional[float]


@dataclass
class StrategyConfig:
    # Identity
    name: str
    magic: int

    # Cluster parameters
    t_seconds: int
    k_unique: int

    # Exit / stops parameters
    hold_minutes: int
    sl_distance: float
    tp_R_multiple: float
    stop_mode: str                 # "fixed", "atr_static", "atr_trailing", "chandelier"
    atr_period: int
    atr_init_mult: float
    atr_trail_mult: float
    chan_lookback: Optional[int]

    # Exit toggles
    use_tp_exit: bool
    use_time_exit: bool

    # Misc
    direction_mode: str = "inverse"   # keep as "inverse"
    max_open_positions: int = 1

    # Risk
    # "fixed_lots"  -> use fixed_lots only
    # "dynamic_pct" -> risk_percent * current equity
    # "static_pct"  -> risk_percent * static_risk_base_balance
    risk_mode: str = "dynamic_pct"
    risk_percent: float = 0.02  # 2% per trade
    fixed_lots: float = 0.10  # used when risk_mode="fixed_lots"

    # static base balance for static_pct mode (e.g. 10_000)
    static_risk_base_balance: float = 10_000.0


@dataclass
class StrategyState:
    config: StrategyConfig
    cluster_engine: "ClusterEngine"
    open_positions: Dict[int, BotPositionInfo] = field(default_factory=dict)


# =========================
# TIME HELPERS
# =========================

def now_local():
    return datetime.now(LOCAL_TZ)


def utc_now():
    # Use timezone-aware UTC now (no deprecation warning)
    return datetime.now(timezone("UTC"))


def parse_iso8601_utc(ts: str) -> datetime:
    """
    Parse Sirix ISO8601 string and convert to a UTC-aware datetime.

    - If it ends with 'Z' or already has tzinfo, we respect that.
    - If it's naive (no timezone), we assume it's Sirix server time (Israel)
      and then convert to UTC.
    """
    # Normalise trailing Z → +00:00 so fromisoformat can handle it
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        # Fallback in case Sirix gives something slightly odd
        # e.g. "2025-12-03T10:15:30"
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")

    # If no timezone info, assume Israel local time (Sirix server)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SIRIX_TZ)

    # Always return UTC-aware
    return dt.astimezone(timezone("UTC"))


def within_session_filter() -> bool:
    if not USE_SESSION_FILTER:
        return True

    now = now_local()
    h_s, m_s = map(int, SESSION_START_HHMM.split(":"))
    h_e, m_e = map(int, SESSION_END_HHMM.split(":"))

    start_dt = now.replace(hour=h_s, minute=m_s, second=0, microsecond=0)
    end_dt   = now.replace(hour=h_e, minute=m_e, second=0, microsecond=0)
    if end_dt <= start_dt:
        end_dt += timedelta(days=1)

    return start_dt <= now <= end_dt


# =========================
# MT5 INIT & HELPERS
# =========================

def init_mt5():
    global SYMBOL_INFO

    if not mt5.initialize(login=MT5_LOGIN,
                          password=MT5_PASSWORD,
                          server=MT5_SERVER,
                          path=MT5_TERMINAL_PATH):
        print("[ERROR] MT5 initialization failed:", mt5.last_error())
        sys.exit(1)

    acct = mt5.account_info()
    if acct is None:
        print("[ERROR] Failed to retrieve MT5 account info:", mt5.last_error())
        sys.exit(1)

    print(f"[MT5] Connected: login={acct.login}, "
          f"balance={acct.balance:.2f}, equity={acct.equity:.2f}")

    SYMBOL_INFO = mt5.symbol_info(MT5_SYMBOL)
    if SYMBOL_INFO is None:
        print(f"[ERROR] Symbol not found: {MT5_SYMBOL}")
        sys.exit(1)

    if not SYMBOL_INFO.visible:
        if not mt5.symbol_select(MT5_SYMBOL, True):
            print(f"[ERROR] Failed to select symbol: {MT5_SYMBOL}")
            sys.exit(1)

    print(f"[MT5] Symbol {MT5_SYMBOL}: digits={SYMBOL_INFO.digits}, "
          f"contract_size={SYMBOL_INFO.trade_contract_size}, "
          f"vol_min={SYMBOL_INFO.volume_min}, vol_max={SYMBOL_INFO.volume_max}, "
          f"step={SYMBOL_INFO.volume_step}")


def round_price(x: float) -> float:
    d = SYMBOL_INFO.digits
    return round(float(x), d)


def round_lots(lots: float) -> float:
    step    = max(SYMBOL_INFO.volume_step, 0.01)
    vol_min = SYMBOL_INFO.volume_min
    vol_max = SYMBOL_INFO.volume_max
    rounded = round(round(lots / step) * step, 2)
    return max(vol_min, min(rounded, vol_max))


def enforce_stop_level(order_type, price, sl_price, tp_price):
    stop_level_points = getattr(SYMBOL_INFO, "trade_stops_level", 0)
    point = SYMBOL_INFO.point
    if stop_level_points and stop_level_points > 0:
        min_dist = stop_level_points * point
        if order_type == mt5.ORDER_TYPE_BUY:
            if price - sl_price < min_dist:
                sl_price = price - min_dist
            if tp_price is not None and tp_price - price < min_dist:
                tp_price = price + min_dist
        else:
            if sl_price - price < min_dist:
                sl_price = price + min_dist
            if tp_price is not None and price - tp_price < min_dist:
                tp_price = price - min_dist

    sl_price = round_price(sl_price)
    tp_price = round_price(tp_price) if tp_price is not None else None
    return sl_price, tp_price


def calc_lot_size(stop_distance_price: float, cfg: StrategyConfig) -> float:
    """
    Lot sizing logic:

    - fixed_lots:
        lots = cfg.fixed_lots

    - dynamic_pct:
        risk_dollars = account_equity * cfg.risk_percent

    - static_pct:
        risk_dollars = cfg.static_risk_base_balance * cfg.risk_percent
        (e.g. 10,000 * 0.02 = 200 risk per trade, regardless of current equity)
    """
    # Mode 1: fixed lots
    if cfg.risk_mode == "fixed_lots":
        return round_lots(cfg.fixed_lots)

    # Determine risk_dollars for the % modes
    if cfg.risk_mode == "static_pct":
        # Use fixed base balance (e.g. 10k)
        risk_dollars = cfg.static_risk_base_balance * cfg.risk_percent
    else:
        # Default: dynamic_pct (use current equity)
        acct = mt5.account_info()
        if acct is None:
            print("[ERROR] account_info() is None")
            return 0.0
        risk_dollars = acct.equity * cfg.risk_percent

    contract_size = SYMBOL_INFO.trade_contract_size
    if stop_distance_price <= 0 or contract_size <= 0:
        return round_lots(SYMBOL_INFO.volume_min)

    raw_lots = risk_dollars / (stop_distance_price * contract_size)
    return round_lots(raw_lots)


def get_positions_for_strategy(cfg: StrategyConfig) -> Dict[int, BotPositionInfo]:
    poss = mt5.positions_get(symbol=MT5_SYMBOL)
    result: Dict[int, BotPositionInfo] = {}
    if not poss:
        return result

    for p in poss:
        if p.magic != cfg.magic:
            continue
        direction = "buy" if p.type == mt5.POSITION_TYPE_BUY else "sell"
        entry_time = datetime.fromtimestamp(p.time, tz=timezone("UTC"))
        info = BotPositionInfo(
            ticket=p.ticket,
            direction=direction,
            entry_time=entry_time,
            entry_price=p.price_open,
            sl_price=p.sl,
            tp_price=p.tp if p.tp > 0 else None
        )
        result[p.ticket] = info
    return result


def close_position_for_strategy(ticket: int, cfg: StrategyConfig, reason: str = "Exit"):
    poss = mt5.positions_get()
    pos = None
    if poss:
        for p in poss:
            if p.ticket == ticket and p.magic == cfg.magic:
                pos = p
                break
    if pos is None:
        return

    if pos.type == mt5.POSITION_TYPE_BUY:
        action = mt5.ORDER_TYPE_SELL
    else:
        action = mt5.ORDER_TYPE_BUY

    tick = mt5.symbol_info_tick(MT5_SYMBOL)
    if tick is None:
        print("[ERROR] No tick when trying to close", ticket)
        return

    price = tick.bid if action == mt5.ORDER_TYPE_SELL else tick.ask
    price = round_price(price)

    req = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": MT5_SYMBOL,
        "volume": pos.volume,
        "type": action,
        "position": ticket,
        "price": price,
        "deviation": 20,
        "magic": cfg.magic,
        "comment": make_comment(f"{cfg.name}_{reason}"),
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    res = mt5.order_send(req)
    if res is None or res.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"[ERROR] Close failed [{cfg.name}] ticket={ticket}, "
              f"retcode={getattr(res, 'retcode', None)}, "
              f"comment={getattr(res, 'comment', None)}")
    else:
        print(f"[OK] [{cfg.name}] Closed ticket={ticket} @ {price:.{SYMBOL_INFO.digits}f} (reason={reason})")
        # Remember we closed this ourselves with a known reason
        RECENT_CLOSED_REASONS[ticket] = reason


def infer_close_reason_from_history(ticket: int) -> str:
    """
    Try to get a detailed close reason from MT5 history:
      - SL / TP / StopOut / Manual / Expert / Other
      - also gives us exit price & profit for logging

    Returns a short string, e.g.:
      "TP (price=4210.50, profit=123.45)"
    """
    from datetime import timezone as dt_timezone

    try:
        # Use timezone-aware UTC datetimes (fixes DeprecationWarning)
        end = datetime.now(dt_timezone.utc)
        start = end - timedelta(days=3)

        deals = mt5.history_deals_get(start, end)
        if deals is None:
            return "unknown"

        # We care about deals that CLOSE the position:
        # - DEAL_ENTRY_OUT      (normal exit)
        # - DEAL_ENTRY_OUT_BY   (closed by SL/TP/StopOut/etc)
        exit_deals = [
            d for d in deals
            if getattr(d, "position_id", None) == ticket
            and getattr(d, "entry", None) in (
                mt5.DEAL_ENTRY_OUT,
                mt5.DEAL_ENTRY_OUT_BY,
            )
        ]

        if not exit_deals:
            return "unknown"

        # Take the latest closing deal
        last = exit_deals[-1]
        profit = getattr(last, "profit", 0.0)
        price  = getattr(last, "price", 0.0)
        reason_code = getattr(last, "reason", None)

        reason_map = {
            mt5.DEAL_REASON_SL:      "SL",
            mt5.DEAL_REASON_TP:      "TP",
            mt5.DEAL_REASON_SO:      "StopOut",
            mt5.DEAL_REASON_CLIENT:  "Manual",
            mt5.DEAL_REASON_EXPERT:  "Expert",
            mt5.DEAL_REASON_MARGINAL: "Margin",
        }

        base = reason_map.get(reason_code, f"Other(reason={reason_code})")
        return f"{base} (price={price}, profit={profit})"

    except Exception as e:
        # If anything weird happens, don't break the bot, just log "unknown"
        print(f"[CLOSE_REASON_DEBUG] Error inferring close reason for ticket={ticket}: {e}")
        return "unknown"


def refresh_positions_and_log_closes(state: StrategyState):
    """
    Sync state.open_positions with live MT5 positions for this strategy,
    and log any tickets that have disappeared since the last loop.
    """
    cfg = state.config

    # Previous snapshot
    prev_positions = state.open_positions
    prev_tickets = set(prev_positions.keys())

    # Current snapshot from MT5
    current_positions = get_positions_for_strategy(cfg)
    current_tickets = set(current_positions.keys())

    closed_tickets = prev_tickets - current_tickets

    for ticket in closed_tickets:
        info = prev_positions.get(ticket)
        if info is None:
            continue

        # If we closed it ourselves, we already know the reason
        reason = RECENT_CLOSED_REASONS.pop(ticket, None)
        if reason is None:
            # Otherwise, best-effort from history (TP vs SL-ish)
            reason = infer_close_reason_from_history(ticket)

        print(
            f"[CLOSED] [{cfg.name}] ticket={ticket} "
            f"dir={info.direction} "
            f"entry={info.entry_price:.{SYMBOL_INFO.digits}f} "
            f"closed_reason={reason}"
        )

    # Update to current positions
    state.open_positions = current_positions


# =========================
# ATR / CANDLES
# =========================

def fetch_mt5_rates(symbol: str, timeframe=mt5.TIMEFRAME_M1, bars: int = 200) -> pd.DataFrame:
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, bars)
    if rates is None:
        raise RuntimeError(f"Failed to fetch MT5 rates for {symbol}")
    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    return df


def compute_atr(df: pd.DataFrame, period: int) -> float:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(period).mean().iloc[-1]
    return float(atr)


# =========================
# SiRiX API
# =========================

def map_sirix_side(
    action_type: Optional[int],
    open_rate: Optional[float],
    sl: Optional[float],
    tp: Optional[float],
) -> Optional[str]:
    """
    Infer trade side (buy/sell) from Sirix open position.

    Priority:
      1) If SL/TP make it obvious (SL below, TP above => BUY; SL above, TP below => SELL),
         use that.
      2) Fall back to ActionType mapping:
         - 0 => buy
         - 1 => sell
         - 2 => sell (best guess / legacy)
    """
    # --- 1) Try to infer from SL/TP geometry ---
    try:
        if open_rate is not None and sl is not None and tp is not None:
            # Guard against zeros or nonsense
            if sl > 0 and tp > 0:
                if sl < open_rate < tp:
                    return "buy"
                if tp < open_rate < sl:
                    return "sell"
    except Exception:
        # If anything weird happens, just fall back to ActionType below
        pass

    # --- 2) Fallback to ActionType numeric mapping ---
    if action_type is None:
        return None

    # New pattern you just showed:
    #   0 => BUY, 1 => SELL
    if action_type == 0:
        return "buy"
    if action_type == 1:
        return "sell"

    # Legacy / other pattern we saw earlier (1/2):
    # If server ever sends 2, safest to treat as SELL (you still invert later anyway).
    if action_type == 2:
        return "sell"

    return None


def fetch_sirix_open_positions(lookback_seconds: int) -> List[dict]:
    end_time = utc_now()
    start_time = end_time - timedelta(seconds=lookback_seconds)

    payload = {
        "groups": GROUPS,
        "startTime": start_time.isoformat().replace("+00:00", "Z"),
        "endTime": end_time.isoformat().replace("+00:00", "Z"),
    }

    url = BASE_URL + SIRIX_ENDPOINT
    try:
        resp = requests.post(url, headers=HEADERS, json=payload, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"[SIRIX] Error fetching positions: {e}")
        return []

    data = resp.json()
    if isinstance(data, dict) and "OpenPositions" in data:
        return data["OpenPositions"]
    elif isinstance(data, list):
        return data
    else:
        print("[SIRIX] Unexpected response format")
        return []


def build_events_from_sirix(
    raw_positions: List[dict],
    seen_order_ids: set,
    min_open_time: Optional[datetime] = None,
) -> List[SirixPositionEvent]:

    events = []
    for pos in raw_positions:
        try:
            instr = pos.get("InstrumentName")
            if instr != SIRIX_INSTRUMENT:
                continue

            order_id = str(pos.get("OrderID"))
            if not order_id or order_id in seen_order_ids:
                continue

            user_id = str(pos.get("UserID"))

            action_type = pos.get("ActionType")
            open_rate   = pos.get("OpenRate")
            sl_val      = pos.get("StopLoss")
            tp_val      = pos.get("TakeProfit")

            side = map_sirix_side(action_type, open_rate, sl_val, tp_val)
            if side is None:
                # Could log for debugging if you want:
                # print(f"[SIRIX] Skipping order {order_id}: could not infer side (ActionType={action_type})")
                continue

            lots = float(pos.get("AmountLots", 0.0))
            open_time_str = pos.get("OpenTime")
            open_time = parse_iso8601_utc(open_time_str)

            # ignore positions that opened before the bot started, if requested
            if min_open_time is not None and open_time < min_open_time:
                continue

            ev = SirixPositionEvent(
                order_id=order_id,
                user_id=user_id,
                side=side,
                lots=lots,
                time=open_time,
            )
            events.append(ev)
            seen_order_ids.add(order_id)

            # Optional: debug what we are actually seeing
            if VERBOSE_CLUSTERS:
                now_utc = utc_now()
                now_loc = now_local()
                print(
                    f"[EVENT] order={order_id} user={user_id} side={side} "
                    f"lots={lots} "
                    f"open_time_utc={open_time.isoformat()} "
                    f"seen_utc={now_utc.isoformat()} "
                    f"seen_local={now_loc.isoformat()}"
                )

        except Exception as e:
            print(f"[SIRIX] Parse error: {e}")
            continue

    return events


# =========================
# CLUSTER ENGINE
# =========================

class ClusterEngine:
    def __init__(self, window_seconds: int, k_unique: int):
        self.window_seconds = window_seconds
        self.k_unique = k_unique
        self.events: deque[SirixPositionEvent] = deque()
        self.last_cluster_time: Optional[datetime] = None
        self.last_cluster_side: Optional[str] = None

    def add_events(self, new_events: List[SirixPositionEvent]) -> Optional[str]:
        """
        Add new Sirix events and check for a cluster.

        IMPORTANT:
        - The rolling window is based on **event timestamps**, not wall-clock.
        - This matches the backtest logic: "T seconds between trades" means
          T seconds between their OpenTime values, not "within T seconds of now".
        """
        # 1) Append new events
        for ev in new_events:
            self.events.append(ev)

        if not self.events:
            return None

        # 2) Use the LATEST event time as reference for the T-second window
        latest_time = self.events[-1].time
        cutoff = latest_time - timedelta(seconds=self.window_seconds)

        # Keep only events whose time is within [latest_time - T, latest_time]
        while self.events and self.events[0].time < cutoff:
            self.events.popleft()

        if not self.events:
            return None

        # 3) Count unique users by side
        buy_users = set()
        sell_users = set()
        for ev in self.events:
            if ev.side == "buy":
                buy_users.add(ev.user_id)
            else:
                sell_users.add(ev.user_id)

        # detailed cluster window stats (noisy)
        if VERBOSE_CLUSTER_DEBUG:
            print(
                f"[CLUSTER_DEBUG] T={self.window_seconds}s | "
                f"events_in_window={len(self.events)} | "
                f"unique_buy={len(buy_users)} | unique_sell={len(sell_users)} | "
                f"latest_event={latest_time.isoformat()}"
            )

        # 4) Decide cluster side
        cluster_side = None
        if len(buy_users) >= self.k_unique:
            cluster_side = "buy"
        elif len(sell_users) >= self.k_unique:
            cluster_side = "sell"

        if cluster_side is None:
            return None

        # 5) Simple refractory: avoid spam clusters of the same side
        if self.last_cluster_time and self.last_cluster_side == cluster_side:
            # Use latest_time here as well for consistency
            if (latest_time - self.last_cluster_time) < timedelta(seconds=CLUSTER_REFRACTORY_SECONDS):
                return None

        self.last_cluster_time = latest_time
        self.last_cluster_side = cluster_side

        if VERBOSE_CLUSTERS:
            print(
                f"[CLUSTER] {cluster_side.upper()} cluster "
                f"(T={self.window_seconds}, K={self.k_unique})"
            )

        return cluster_side


def inverse_side(side: str) -> str:
    if side == "buy":
        return "sell"
    if side == "sell":
        return "buy"
    raise ValueError(f"Unknown side: {side}")


# =========================
# ENTRY / SLTP / MANAGEMENT
# =========================

def calc_initial_sl_tp(side: str,
                       entry_price: float,
                       cfg: StrategyConfig,
                       atr_value: Optional[float]) -> Tuple[float, Optional[float]]:
    if cfg.stop_mode == "fixed" or atr_value is None:
        sl_distance = cfg.sl_distance
    else:
        sl_distance = cfg.atr_init_mult * atr_value

    if side == "buy":
        sl_price = entry_price - sl_distance
    else:
        sl_price = entry_price + sl_distance

    tp_price = None
    if cfg.use_tp_exit:
        tp_distance = cfg.tp_R_multiple * sl_distance
        if side == "buy":
            tp_price = entry_price + tp_distance
        else:
            tp_price = entry_price - tp_distance

    order_type = mt5.ORDER_TYPE_BUY if side == "buy" else mt5.ORDER_TYPE_SELL
    sl_price, tp_price = enforce_stop_level(order_type, entry_price, sl_price, tp_price)
    return sl_price, tp_price


def place_cluster_entry(trade_side: str, cfg: StrategyConfig) -> Optional[BotPositionInfo]:
    tick = mt5.symbol_info_tick(MT5_SYMBOL)
    if tick is None:
        print(f"[ERROR] [{cfg.name}] No tick data for {MT5_SYMBOL}")
        return None

    entry_price = tick.ask if trade_side == "buy" else tick.bid
    entry_price = round_price(entry_price)

    atr_val = None
    if cfg.stop_mode in ("atr_static", "atr_trailing", "chandelier"):
        df = fetch_mt5_rates(
            MT5_SYMBOL,
            timeframe=mt5.TIMEFRAME_M1,
            bars=max(cfg.atr_period + 20, (cfg.chan_lookback or 0) + 5),
        )
        atr_val = compute_atr(df, cfg.atr_period)

    sl_price, tp_price = calc_initial_sl_tp(trade_side, entry_price, cfg, atr_val)
    sl_distance = abs(entry_price - sl_price)

    lots = calc_lot_size(sl_distance, cfg)
    if lots <= 0:
        print(f"[ERROR] [{cfg.name}] Lot size <= 0, skipping entry.")
        return None

    order_type = mt5.ORDER_TYPE_BUY if trade_side == "buy" else mt5.ORDER_TYPE_SELL

    req = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": MT5_SYMBOL,
        "volume": lots,
        "type": order_type,
        "price": entry_price,
        "sl": sl_price,
        "tp": tp_price if tp_price is not None else 0.0,
        "deviation": 50,
        "magic": cfg.magic,
        "comment": make_comment(f"{cfg.name}_{trade_side}"),
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    res = mt5.order_send(req)

    # If MT5 returns no result object at all
    if res is None:
        code, msg = mt5.last_error()
        print(
            f"[ERROR] [{cfg.name}] Entry failed: order_send() returned None, "
            f"last_error={code} ({msg})"
        )
        return None

    if res.retcode != mt5.TRADE_RETCODE_DONE:
        print(
            f"[ERROR] [{cfg.name}] Entry failed: retcode={res.retcode}, "
            f"comment={getattr(res, 'comment', None)}"
        )
        # Helpful hint for the common case you just hit
        if res.retcode == mt5.TRADE_RETCODE_AUTOTRADING_DISABLED_BY_CLIENT:
            print(
                "[HINT] Enable 'Algo Trading' in the MT5 terminal and check "
                "Tools → Options → Expert Advisors for algorithmic trading permissions."
            )
        return None

    ticket = res.order
    print(
        f"[OK] [{cfg.name}] {trade_side.upper()} {MT5_SYMBOL} {lots:.2f} @ "
        f"{entry_price:.{SYMBOL_INFO.digits}f}, SL={sl_price:.{SYMBOL_INFO.digits}f}, "
        f"TP={tp_price}"
    )

    return BotPositionInfo(
        ticket=ticket,
        direction=trade_side,
        entry_time=utc_now(),
        entry_price=entry_price,
        sl_price=sl_price,
        tp_price=tp_price,
    )


def modify_sl_tp(ticket: int, cfg: StrategyConfig, new_sl: float, new_tp: Optional[float]):
    poss = mt5.positions_get()
    pos = None
    if poss:
        for p in poss:
            if p.ticket == ticket and p.magic == cfg.magic:
                pos = p
                break
    if pos is None:
        return

    req = {
        "action": mt5.TRADE_ACTION_SLTP,
        "symbol": MT5_SYMBOL,
        "position": ticket,
        "sl": new_sl,
        "tp": new_tp if new_tp is not None else 0.0,
        "magic": cfg.magic,
        "comment": make_comment(f"{cfg.name}_ModSLTP"),
    }
    res = mt5.order_send(req)

    if res is None:
        code, msg = mt5.last_error()
        print(
            f"[ERROR] [{cfg.name}] Modify SLTP failed ticket={ticket}, "
            f"order_send() returned None, last_error={code} ({msg})"
        )
        return

    if res.retcode != mt5.TRADE_RETCODE_DONE:
        print(
            f"[ERROR] [{cfg.name}] Modify SLTP failed ticket={ticket}, "
            f"retcode={res.retcode}, comment={getattr(res, 'comment', None)}"
        )
    else:
        print(f"[OK] [{cfg.name}] Modified SL/TP ticket={ticket} -> SL={new_sl}, TP={new_tp}")


def manage_trailing_stops(state: StrategyState):
    cfg = state.config
    if cfg.stop_mode not in ("atr_trailing", "chandelier"):
        return
    if not state.open_positions:
        return

    df = fetch_mt5_rates(
        MT5_SYMBOL,
        timeframe=mt5.TIMEFRAME_M1,
        bars=max(cfg.atr_period + 20, (cfg.chan_lookback or 0) + 5),
    )
    atr_val = compute_atr(df, cfg.atr_period)
    highest_high = df["high"].iloc[-(cfg.chan_lookback or 1):].max()
    lowest_low = df["low"].iloc[-(cfg.chan_lookback or 1):].min()

    point = SYMBOL_INFO.point

    for ticket, info in list(state.open_positions.items()):
        poss = mt5.positions_get()
        pos = None
        if poss:
            for p in poss:
                if p.ticket == ticket and p.magic == cfg.magic:
                    pos = p
                    break
        if pos is None:
            state.open_positions.pop(ticket, None)
            continue

        current_price = pos.price_current
        new_sl = info.sl_price  # start from current SL

        # --- Compute candidate SL for this stop_mode ---
        if cfg.stop_mode == "atr_trailing":
            if info.direction == "buy":
                trail_sl = current_price - cfg.atr_trail_mult * atr_val
                new_sl = max(info.sl_price, trail_sl)
            else:
                trail_sl = current_price + cfg.atr_trail_mult * atr_val
                new_sl = min(info.sl_price, trail_sl)

        elif cfg.stop_mode == "chandelier":
            if info.direction == "buy":
                chand_sl = highest_high - cfg.atr_trail_mult * atr_val
                new_sl = max(info.sl_price, chand_sl)
            else:
                chand_sl = lowest_low + cfg.atr_trail_mult * atr_val
                new_sl = min(info.sl_price, chand_sl)

        # --- Enforce broker stop-level rules relative to CURRENT price ---
        order_type = mt5.ORDER_TYPE_BUY if info.direction == "buy" else mt5.ORDER_TYPE_SELL
        new_sl, _ = enforce_stop_level(order_type, current_price, new_sl, info.tp_price)

        # --- Extra safety: SL must be on the correct side of price ---
        if info.direction == "buy":
            # For a BUY, SL must be below current_price
            if new_sl >= current_price:
                # Skip this update; broker would likely reject
                # print(f"[SKIP] [{cfg.name}] New SL {new_sl} not below current price {current_price} for BUY")
                continue
            # Also keep at least 3 points away
            if current_price - new_sl < 3 * point:
                continue
        else:
            # For a SELL, SL must be above current_price
            if new_sl <= current_price:
                # print(f"[SKIP] [{cfg.name}] New SL {new_sl} not above current price {current_price} for SELL")
                continue
            if new_sl - current_price < 3 * point:
                continue

        # --- Only send modification if it's meaningfully different ---
        if abs(new_sl - info.sl_price) > (2 * point):
            modify_sl_tp(ticket, cfg, new_sl, info.tp_price)
            info.sl_price = new_sl


def manage_time_exits(state: StrategyState):
    cfg = state.config
    if not cfg.use_time_exit or cfg.hold_minutes <= 0:
        return
    now = utc_now()
    for ticket, info in list(state.open_positions.items()):
        elapsed_min = (now - info.entry_time).total_seconds() / 60.0
        if elapsed_min >= cfg.hold_minutes:
            print(f"[EXIT] [{cfg.name}] Time exit ticket={ticket}, elapsed={elapsed_min:.1f} min")
            close_position_for_strategy(ticket, cfg, reason="TimeExit")
            state.open_positions.pop(ticket, None)


# =========================
# STATE JSON SNAPSHOT
# =========================

def write_state_json(strategy_states: List[StrategyState]):
    try:
        payload = {
            "bot_name": BOT_NAME,
            "updated_at_utc": utc_now().isoformat(),
            "strategies": []
        }

        for st in strategy_states:
            cfg = st.config
            ce = st.cluster_engine
            events_list = list(ce.events)
            if events_list:
                last_event_time = max(ev.time for ev in events_list)
                buy_users = {ev.user_id for ev in events_list if ev.side == "buy"}
                sell_users = {ev.user_id for ev in events_list if ev.side == "sell"}
            else:
                last_event_time = None
                buy_users = set()
                sell_users = set()

            recent_events = []
            for ev in events_list[-10:]:
                recent_events.append({
                    "order_id": ev.order_id,
                    "user_id": ev.user_id,
                    "side": ev.side,
                    "lots": ev.lots,
                    "time_utc": ev.time.isoformat(),
                })

            open_pos_list = []
            for ticket, info in st.open_positions.items():
                open_pos_list.append({
                    "ticket": ticket,
                    "direction": info.direction,
                    "entry_time_utc": info.entry_time.isoformat(),
                    "entry_price": float(info.entry_price),
                    "sl_price": float(info.sl_price),
                    "tp_price": float(info.tp_price) if info.tp_price is not None else None,
                })

            payload["strategies"].append({
                "name": cfg.name,
                "magic": cfg.magic,
                "config": {
                    "t_seconds": cfg.t_seconds,
                    "k_unique": cfg.k_unique,
                    "hold_minutes": cfg.hold_minutes,
                    "sl_distance": cfg.sl_distance,
                    "tp_R_multiple": cfg.tp_R_multiple,
                    "stop_mode": cfg.stop_mode,
                    "atr_period": cfg.atr_period,
                    "atr_init_mult": cfg.atr_init_mult,
                    "atr_trail_mult": cfg.atr_trail_mult,
                    "chan_lookback": cfg.chan_lookback,
                    "direction_mode": cfg.direction_mode,
                    "use_tp_exit": cfg.use_tp_exit,
                    "use_time_exit": cfg.use_time_exit,
                    "risk_mode": cfg.risk_mode,
                    "risk_percent": cfg.risk_percent,
                    "fixed_lots": cfg.fixed_lots,
                },
                "cluster": {
                    "window_seconds": ce.window_seconds,
                    "k_unique": ce.k_unique,
                    "last_cluster_side": ce.last_cluster_side,
                    "last_cluster_time_utc": (
                        ce.last_cluster_time.isoformat()
                        if ce.last_cluster_time else None
                    ),
                    "events_in_window": len(events_list),
                    "unique_buy_users_in_window": len(buy_users),
                    "unique_sell_users_in_window": len(sell_users),
                    "last_event_time_utc": last_event_time.isoformat() if last_event_time else None,
                    "recent_events": recent_events,
                },
                "open_positions": open_pos_list,
            })

        with STATE_PATH.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    except Exception as e:
        print(f"[STATE] Failed to write state JSON: {type(e).__name__}: {e}")


# =========================
# STRATEGY DEFINITIONS
# =========================

def build_strategies() -> List[StrategyState]:
    """
    Hard-wired with your 3 best grid performers:

    1) T=29, K=3, H=5, SL=2, TP=1, atr_trailing, ATR(5, I=3, T=2)
    2) T=29, K=3, H=5, SL=2, TP=1, atr_static,   ATR(5, I=4)
    3) T=30, K=3, H=15, SL=2, TP=1, chandelier,  ATR(5, I=3, T=2, LB=30)

    All are inverse of cluster, TP exit ON, time exit OFF (per table).
    Risk: dynamic_pct 2% per trade per engine by default.
    """
    strategies: List[StrategyState] = []

    # ENGINE A: atr_trailing
    cfg_a = StrategyConfig(
        name="Inv_ATRtrail",
        magic=880001,
        t_seconds=29,
        k_unique=3,
        hold_minutes=5,
        sl_distance=2.0,
        tp_R_multiple=1.0,
        stop_mode="atr_trailing",
        atr_period=5,
        atr_init_mult=3.0,
        atr_trail_mult=2.0,
        chan_lookback=None,      # not used for atr_trailing
        use_tp_exit=True,
        use_time_exit=False,
        direction_mode="inverse",
        max_open_positions=2,
        risk_mode="dynamic_pct",
        risk_percent=0.02,
        fixed_lots=0.10,
    )
    strategies.append(StrategyState(
        config=cfg_a,
        cluster_engine=ClusterEngine(window_seconds=cfg_a.t_seconds,
                                     k_unique=cfg_a.k_unique)
    ))

    # ENGINE B: atr_static
    cfg_b = StrategyConfig(
        name="Inv_ATRstatic",
        magic=880002,
        t_seconds=29,
        k_unique=3,
        hold_minutes=5,
        sl_distance=2.0,
        tp_R_multiple=1.0,
        stop_mode="atr_static",
        atr_period=5,
        atr_init_mult=4.0,       # from table
        atr_trail_mult=2.0,      # not used, but kept for compatibility
        chan_lookback=None,
        use_tp_exit=True,
        use_time_exit=False,
        direction_mode="inverse",
        max_open_positions=2,
        risk_mode="dynamic_pct",
        risk_percent=0.02,
        fixed_lots=0.10,
    )
    strategies.append(StrategyState(
        config=cfg_b,
        cluster_engine=ClusterEngine(window_seconds=cfg_b.t_seconds,
                                     k_unique=cfg_b.k_unique)
    ))

    # ENGINE C: chandelier
    cfg_c = StrategyConfig(
        name="Inv_Chandelier",
        magic=880003,
        t_seconds=30,
        k_unique=3,
        hold_minutes=15,
        sl_distance=2.0,
        tp_R_multiple=1.0,
        stop_mode="chandelier",
        atr_period=5,
        atr_init_mult=3.0,
        atr_trail_mult=2.0,
        chan_lookback=30,
        use_tp_exit=True,
        use_time_exit=False,
        direction_mode="inverse",
        max_open_positions=2,
        risk_mode="dynamic_pct",
        risk_percent=0.02,
        fixed_lots=0.10,
    )
    strategies.append(StrategyState(
        config=cfg_c,
        cluster_engine=ClusterEngine(window_seconds=cfg_c.t_seconds,
                                     k_unique=cfg_c.k_unique)
    ))

    return strategies


# =========================
# MAIN LOOP
# =========================

def main_loop():
    strategies = build_strategies()

    max_t_seconds = max(st.config.t_seconds for st in strategies)
    sirix_lookback_seconds = max(max_t_seconds, 60)

    print(f"=== {BOT_NAME} — Multi-Engine Cluster Inverse Bot ===")
    for st in strategies:
        cfg = st.config
        print(f"[STRATEGY] {cfg.name} | MAGIC={cfg.magic} | "
              f"T={cfg.t_seconds}, K={cfg.k_unique}, "
              f"hold={cfg.hold_minutes}, stop_mode={cfg.stop_mode}, "
              f"ATR(p={cfg.atr_period}, I={cfg.atr_init_mult}, T={cfg.atr_trail_mult}), "
              f"LB={cfg.chan_lookback}, risk_mode={cfg.risk_mode}, "
              f"risk={cfg.risk_percent*100:.2f}%")

    if USE_SESSION_FILTER:
        print(f"Session filter: {SESSION_START_HHMM} → {SESSION_END_HHMM} (Europe/London)")

    # =========== BOOTSTRAP ===========

    # We will treat ALL current Sirix open positions as "old"
    # so that we NEVER trade based on them.
    seen_order_ids: set[str] = set()

    bootstrap_raw = fetch_sirix_open_positions(sirix_lookback_seconds)
    _ = build_events_from_sirix(
        bootstrap_raw,
        seen_order_ids,
        min_open_time=None,   # just want to populate seen_order_ids
    )
    print(f"[BOOTSTRAP] Ignoring {len(seen_order_ids)} existing Sirix orders at startup.")

    # =========== MAIN LOOP ===========

    while True:
        try:
            # 1) Fetch SiRiX events ONCE per loop
            raw_positions = fetch_sirix_open_positions(sirix_lookback_seconds)

            # Only consider truly NEW OrderIDs
            new_events = build_events_from_sirix(
                raw_positions,
                seen_order_ids,
                min_open_time=None,   # bootstrap already took care of "old" trades
            )

            # 2) Feed events into each strategy's ClusterEngine
            for st in strategies:
                cfg = st.config

                # a) Refresh positions and log any that just closed
                refresh_positions_and_log_closes(st)

                # b) If we already have a trade, do NOT look for new entries
                if len(st.open_positions) >= cfg.max_open_positions:
                    continue

                # c) Engine is flat → we can listen for clusters
                cluster_side = st.cluster_engine.add_events(new_events)
                if cluster_side is not None and within_session_filter():
                    trade_side = cluster_side
                    if cfg.direction_mode == "inverse":
                        trade_side = inverse_side(cluster_side)

                    bot_pos = place_cluster_entry(trade_side, cfg)
                    if bot_pos:
                        st.open_positions[bot_pos.ticket] = bot_pos

                        # Reset cluster state once we enter a trade
                        st.cluster_engine.events.clear()
                        st.cluster_engine.last_cluster_time = None
                        st.cluster_engine.last_cluster_side = None

            # 3) Refresh and manage each strategy
            for st in strategies:
                # First, sync and log any closes since last loop
                refresh_positions_and_log_closes(st)

                # Then apply trailing / time exits on what is still open
                manage_trailing_stops(st)
                manage_time_exits(st)

            # 4) State snapshot
            write_state_json(strategies)

            time.sleep(POLL_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\n[EXIT] Stopping bot (KeyboardInterrupt).")
            break
        except Exception as e:
            print("[EXCEPTION]", type(e).__name__, str(e))
            time.sleep(2)


if __name__ == "__main__":
    init_mt5()
    try:
        main_loop()
    finally:
        mt5.shutdown()
        print("[MT5] Shutdown complete.")
