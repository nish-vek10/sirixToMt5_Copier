#!/usr/bin/env python

"""
BTC_SiRiX_ClusterInverse_multi — Multi-Engine SiRiX → MT5 BTCUSD Cluster Inverse Bot
====================================================================================

- Polls SiRiX open positions for groups: ["Audition", "Funded", "Purchases"].
- Builds a rolling T-second window of *new* open positions (per OrderID).
- Detects BUY/SELL clusters (>= K_UNIQUE unique traders in window).
- For each strategy (engine):

    • Cluster window:      T_SECONDS
    • Unique traders:      K_UNIQUE
    • Stop mode:           fixed / atr_static / atr_trailing / chandelier /
                           chandelier_trailing_only
    • Exit toggles:        use_tp_exit, use_time_exit
    • ATR params:          atr_period, atr_init_mult, atr_trail_mult, chan_lookback
    • Risk mode:           dynamic_pct / static_pct / fixed_lots
    • MAGIC:               distinct per engine

- Direction is inverse of cluster (BUY cluster → SELL, SELL cluster → BUY).
- All strategies trade BTCUSD (or your broker's BTC symbol) but are independent.

NEW vs XAU version:
===================

- Uses **pending orders** (limit/stop) instead of market orders.
- Pending order **price** = MT5 bid/ask ± LIMIT_OFFSET_DOLLARS at cluster trigger:
    • BUY cluster  → bot SELL → SELL_LIMIT at (ask + LIMIT_OFFSET_DOLLARS)
    • SELL cluster → bot BUY  → BUY_LIMIT at (bid - LIMIT_OFFSET_DOLLARS)
- If pending order is **not filled in 3 minutes**, it is cancelled.
- Max **2 open or pending positions** per engine.
"""

BOT_NAME = "BTC_SiRiX_ClusterInverse_multi"

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

STATE_PATH = Path("btc_cluster_inverse_multi_state.json")

# =========================
# MT5 ACCOUNT CONFIG
# =========================

MT5_LOGIN         = 52611391
MT5_PASSWORD      = "D&KvS$2KPBrYso"
MT5_SERVER        = "ICMarketsSC-Demo"
MT5_TERMINAL_PATH = r"C:\MT5\TradeCopier-Cluster _DEMO\terminal64.exe"

LOCAL_TZ   = timezone("Europe/London")
SIRIX_TZ   = timezone("Asia/Jerusalem")  # Sirix server timezone
MT5_SYMBOL = "BTCUSD"
SYMBOL_INFO = None      # filled by init_mt5()

# Timeout for pending (limit/stop) orders in minutes
PENDING_ORDER_TIMEOUT_MIN = 3

# =========================
# MT5 COMMENT HELPER
# =========================

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
# ENTRY OFFSET CONFIG
# =========================

# Offset in "dollars" (instrument price units) for limit entries:
# - Inverse SELL  -> SELL_LIMIT at ask + LIMIT_OFFSET_DOLLARS
# - Inverse BUY   -> BUY_LIMIT at bid - LIMIT_OFFSET_DOLLARS
LIMIT_OFFSET_DOLLARS = 2.0

# =========================
# LOGGING / VERBOSITY
# =========================

VERBOSE_CLUSTERS = True          # set True if you want to see cluster events
VERBOSE_CLUSTER_DEBUG = False    # extra noisy [CLUSTER_DEBUG] spam

# =========================
# CLOSE POSITION LOG
# =========================

RECENT_CLOSED_REASONS: Dict[int, str] = {}

# =========================
# SiRiX API CONFIG
# =========================

BASE_URL       = "https://restapi-real3.sirixtrader.com"
SIRIX_ENDPOINT = "/api/ManagementService/GetOpenPositionsForGroups"

TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"

GROUPS           = ["Audition", "Funded", "Purchases"]
SIRIX_INSTRUMENT = "BTCUSD."

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
    open_rate: float # price they entered at (from SiRiX)


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
    # stop_mode: "fixed", "atr_static", "atr_trailing",
    #            "chandelier", "chandelier_trailing_only"
    stop_mode: str
    atr_period: int
    atr_init_mult: float
    atr_trail_mult: float
    chan_lookback: Optional[int]

    # Exit toggles
    use_tp_exit: bool
    use_time_exit: bool

    # Misc
    direction_mode: str = "inverse"
    max_open_positions: int = 1

    # Risk
    risk_mode: str = "dynamic_pct"   # "fixed_lots", "dynamic_pct", "static_pct"
    risk_percent: float = 0.02       # 2% per trade
    fixed_lots: float = 0.10         # used when risk_mode="fixed_lots"
    static_risk_base_balance: float = 10_000.0  # used when risk_mode="static_pct"

    # Trailing start in R (open_R):
    #   None  -> no R threshold (trail as in the original behaviour)
    #   0.5   -> start trailing from +0.5R
    #   1.0   -> start trailing from +1R, etc.
    trail_start_R: Optional[float] = 1.0


@dataclass
class StrategyState:
    config: StrategyConfig
    cluster_engine: "ClusterEngine"
    open_positions: Dict[int, BotPositionInfo] = field(default_factory=dict)
    # pending_orders: ticket -> created_at_utc
    pending_orders: Dict[int, datetime] = field(default_factory=dict)

# =========================
# TIME HELPERS
# =========================

def now_local():
    return datetime.now(LOCAL_TZ)


def utc_now():
    return datetime.now(timezone("UTC"))


def parse_iso8601_utc(ts: str) -> datetime:
    """
    Parse Sirix ISO8601 string and convert to a UTC-aware datetime.
    """
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SIRIX_TZ)

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

    # Debug: see if we actually have a tick right after init
    tick = mt5.symbol_info_tick(MT5_SYMBOL)
    print(f"[DEBUG] Initial tick for {MT5_SYMBOL} -> {tick}")


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
    Lot sizing logic (same as XAU version, works for BTC too).
    """
    if cfg.risk_mode == "fixed_lots":
        return round_lots(cfg.fixed_lots)

    if cfg.risk_mode == "static_pct":
        risk_dollars = cfg.static_risk_base_balance * cfg.risk_percent
    else:
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
        "comment": make_comment(f"{cfg.name}"),
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
        RECENT_CLOSED_REASONS[ticket] = reason


def infer_close_reason_from_history(ticket: int) -> str:
    from datetime import timezone as dt_timezone

    try:
        end = datetime.now(dt_timezone.utc)
        start = end - timedelta(days=3)

        deals = mt5.history_deals_get(start, end)
        if deals is None:
            return "unknown"

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
        print(f"[CLOSE_REASON_DEBUG] Error inferring close reason for ticket={ticket}: {e}")
        return "unknown"


def refresh_positions_and_log_closes(state: StrategyState):
    cfg = state.config

    prev_positions = state.open_positions
    prev_tickets = set(prev_positions.keys())

    current_positions = get_positions_for_strategy(cfg)
    current_tickets = set(current_positions.keys())

    closed_tickets = prev_tickets - current_tickets
    new_tickets    = current_tickets - prev_tickets

    for ticket in closed_tickets:
        info = prev_positions.get(ticket)
        if info is None:
            continue

        reason = RECENT_CLOSED_REASONS.pop(ticket, None)
        if reason is None:
            reason = infer_close_reason_from_history(ticket)

        print(
            f"[CLOSED] [{cfg.name}] ticket={ticket} "
            f"dir={info.direction} "
            f"entry={info.entry_price:.{SYMBOL_INFO.digits}f} "
            f"closed_reason={reason}"
        )

    for ticket in new_tickets:
        info = current_positions[ticket]
        print(
            f"[OPENED] [{cfg.name}] ticket={ticket} "
            f"dir={info.direction} "
            f"entry={info.entry_price:.{SYMBOL_INFO.digits}f}"
        )

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
    """
    try:
        if open_rate is not None and sl is not None and tp is not None:
            if sl > 0 and tp > 0:
                if sl < open_rate < tp:
                    return "buy"
                if tp < open_rate < sl:
                    return "sell"
    except Exception:
        pass

    if action_type is None:
        return None

    if action_type == 0:
        return "buy"
    if action_type == 1:
        return "sell"
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
                continue

            lots = float(pos.get("AmountLots", 0.0))
            open_time_str = pos.get("OpenTime")
            open_time = parse_iso8601_utc(open_time_str)

            if min_open_time is not None and open_time < min_open_time:
                continue

            ev = SirixPositionEvent(
                order_id=order_id,
                user_id=user_id,
                side=side,
                lots=lots,
                time=open_time,
                open_rate=float(open_rate) if open_rate is not None else 0.0
            )
            events.append(ev)
            seen_order_ids.add(order_id)

            if VERBOSE_CLUSTERS:
                open_time_local = open_time.astimezone(LOCAL_TZ)
                seen_utc = utc_now()
                seen_local = seen_utc.astimezone(LOCAL_TZ)

                print(
                    f"[EVENT] order={order_id} user={user_id} side={side} "
                    f"lots={lots} "
                    f"open_time_utc={open_time_local.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"seen_local={seen_local.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"open_rate={ev.open_rate}"
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

    def add_events(self, new_events: List[SirixPositionEvent]) -> Tuple[Optional[str], Optional[SirixPositionEvent]]:
        """
        Add new Sirix events and check for a cluster.

        Returns:
            (cluster_side, trigger_event) or (None, None)
        """
        for ev in new_events:
            self.events.append(ev)

        if not self.events:
            return None, None

        latest_time = self.events[-1].time
        cutoff = latest_time - timedelta(seconds=self.window_seconds)

        while self.events and self.events[0].time < cutoff:
            self.events.popleft()

        if not self.events:
            return None, None

        buy_users = set()
        sell_users = set()
        for ev in self.events:
            if ev.side == "buy":
                buy_users.add(ev.user_id)
            else:
                sell_users.add(ev.user_id)

        if VERBOSE_CLUSTER_DEBUG:
            print(
                f"[CLUSTER_DEBUG] T={self.window_seconds}s | "
                f"events_in_window={len(self.events)} | "
                f"unique_buy={len(buy_users)} | unique_sell={len(sell_users)} | "
                f"latest_event={latest_time.isoformat()}"
            )

        cluster_side = None
        if len(buy_users) >= self.k_unique:
            cluster_side = "buy"
        elif len(sell_users) >= self.k_unique:
            cluster_side = "sell"

        if cluster_side is None:
            return None, None

        if self.last_cluster_time and self.last_cluster_side == cluster_side:
            if (latest_time - self.last_cluster_time) < timedelta(seconds=CLUSTER_REFRACTORY_SECONDS):
                return None, None

        self.last_cluster_time = latest_time
        self.last_cluster_side = cluster_side

        # Choose trigger event as the latest event on that side
        trigger_event = None
        for ev in reversed(self.events):
            if ev.side == cluster_side:
                trigger_event = ev
                break

        if VERBOSE_CLUSTERS:
            print(
                f"[CLUSTER] {cluster_side.upper()} cluster "
                f"(T={self.window_seconds}, K={self.k_unique}) "
                f"trigger_order={trigger_event.order_id if trigger_event else 'N/A'} "
                f"open_rate={trigger_event.open_rate if trigger_event else 'N/A'}"
            )

        return cluster_side, trigger_event


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


def place_cluster_pending_entry(
    trade_side: str,
    cfg: StrategyConfig,
    offset_dollars: float,
) -> Optional[int]:
    """
    Place a pending LIMIT order using MT5 bid/ask ± offset:

    - If trade_side == "buy"  (bot wants to BUY inverse a SELL cluster):
        BUY_LIMIT at (bid - offset_dollars)

    - If trade_side == "sell" (bot wants to SELL inverse a BUY cluster):
        SELL_LIMIT at (ask + offset_dollars)
    """

    # 1) Get current tick
    tick = mt5.symbol_info_tick(MT5_SYMBOL)

    # Fallback: if no tick or invalid, use last M1 close as proxy for both bid/ask
    if tick is None or tick.bid <= 0 or tick.ask <= 0:
        print(f"[WARN] [{cfg.name}] No live tick for {MT5_SYMBOL}, "
              f"falling back to last M1 close for limit logic.")
        try:
            df_last = fetch_mt5_rates(MT5_SYMBOL, timeframe=mt5.TIMEFRAME_M1, bars=1)
            last_close = float(df_last["close"].iloc[-1])

            class _Tick:
                pass

            tick = _Tick()
            tick.bid = last_close
            tick.ask = last_close
        except Exception as e:
            print(f"[ERROR] [{cfg.name}] Could not fetch last candle for {MT5_SYMBOL}: {e}")
            return None

    bid = float(tick.bid)
    ask = float(tick.ask)

    # 2) Decide LIMIT price from bid/ask ± offset
    if trade_side == "buy":
        # Buy cheaper than current bid
        raw_price = bid - offset_dollars
        order_type = mt5.ORDER_TYPE_BUY_LIMIT
    else:
        # Sell higher than current ask
        raw_price = ask + offset_dollars
        order_type = mt5.ORDER_TYPE_SELL_LIMIT

    entry_price = round_price(raw_price)

    # 3) ATR-based SL/TP around this LIMIT price
    atr_val = None
    if cfg.stop_mode in ("atr_static", "atr_trailing", "chandelier", "chandelier_trailing_only"):
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

    stops_level = getattr(SYMBOL_INFO, "trade_stops_level", 0) or 0

    # Debug log for sanity
    print(
        f"[PENDING_DEBUG] {cfg.name} side={trade_side} "
        f"type={order_type} entry={entry_price} "
        f"bid={bid} ask={ask} offset={offset_dollars} "
        f"stops_level={stops_level}"
    )

    # 4) Enforce broker stop-level rules **for SL/TP**, relative to entry price
    base_order_type = mt5.ORDER_TYPE_BUY if trade_side == "buy" else mt5.ORDER_TYPE_SELL
    sl_price, tp_price = enforce_stop_level(base_order_type, entry_price, sl_price, tp_price)

    # 5) Send the pending order
    req = {
        "action": mt5.TRADE_ACTION_PENDING,
        "symbol": MT5_SYMBOL,
        "volume": lots,
        "type": order_type,
        "price": entry_price,
        "sl": sl_price,
        "tp": tp_price if tp_price is not None else 0.0,
        "deviation": 50,
        "magic": cfg.magic,
        "comment": make_comment(f"{cfg.name}_pend"),
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    res = mt5.order_send(req)

    if res is None:
        code, msg = mt5.last_error()
        print(
            f"[ERROR] [{cfg.name}] Pending entry failed: order_send() returned None, "
            f"last_error={code} ({msg})"
        )
        return None

    if res.retcode != mt5.TRADE_RETCODE_DONE:
        print(
            f"[ERROR] [{cfg.name}] Pending entry failed: retcode={res.retcode}, "
            f"comment={getattr(res, 'comment', None)}"
        )
        return None

    ticket = res.order
    print(
        f"[OK] [{cfg.name}] PENDING {trade_side.upper()} {MT5_SYMBOL} {lots:.2f} @ "
        f"{entry_price:.{SYMBOL_INFO.digits}f}, SL={sl_price:.{SYMBOL_INFO.digits}f}, "
        f"TP={tp_price}"
    )
    return ticket


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
        "comment": make_comment(f"{cfg.name}"),
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
    if cfg.stop_mode not in ("atr_trailing", "chandelier", "chandelier_trailing_only"):
        return
    if not state.open_positions:
        return

    # Use M1 candle data, just like the backtest
    df = fetch_mt5_rates(
        MT5_SYMBOL,
        timeframe=mt5.TIMEFRAME_M1,
        bars=max(cfg.atr_period + 20, (cfg.chan_lookback or 0) + 5),
    )
    atr_val = compute_atr(df, cfg.atr_period)

    highest_high = df["high"].iloc[-(cfg.chan_lookback or 1):].max()
    lowest_low = df["low"].iloc[-(cfg.chan_lookback or 1):].min()

    last_bar_time = df["time"].iloc[-1]          # bar open time (UTC)
    last_close = float(df["close"].iloc[-1])     # use close for trailing price

    point = SYMBOL_INFO.point

    for ticket, info in list(state.open_positions.items()):
        # Re-fetch MT5 position to confirm it still exists
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

        # 1) Do NOT trail on the entry candle (give it a bar to breathe)
        if info.entry_time >= last_bar_time:
            continue

        current_price = last_close

        # 2) Compute current open R, based on ORIGINAL SL distance
        sl_dist = abs(info.entry_price - info.sl_price)
        if sl_dist <= 0:
            continue

        if info.direction == "buy":
            open_R = (current_price - info.entry_price) / sl_dist
        else:
            open_R = (info.entry_price - current_price) / sl_dist

        # If not yet in enough profit, do not trail at all.
        # If trail_start_R is None → no R threshold (original behaviour).
        if cfg.trail_start_R is not None and open_R < cfg.trail_start_R:
            continue

        new_sl = info.sl_price

        # 3) Compute candidate SL from ATR / chandelier logic
        if cfg.stop_mode == "atr_trailing":
            if info.direction == "buy":
                trail_sl = current_price - cfg.atr_trail_mult * atr_val
                new_sl = max(info.sl_price, trail_sl)
            else:
                trail_sl = current_price + cfg.atr_trail_mult * atr_val
                new_sl = min(info.sl_price, trail_sl)

        elif cfg.stop_mode in ("chandelier", "chandelier_trailing_only"):
            if info.direction == "buy":
                chand_sl = highest_high - cfg.atr_trail_mult * atr_val
                new_sl = max(info.sl_price, chand_sl)
            else:
                chand_sl = lowest_low + cfg.atr_trail_mult * atr_val
                new_sl = min(info.sl_price, chand_sl)

        # 4) Enforce broker stop-level rules against current price
        order_type = mt5.ORDER_TYPE_BUY if info.direction == "buy" else mt5.ORDER_TYPE_SELL
        new_sl, _ = enforce_stop_level(order_type, current_price, new_sl, info.tp_price)

        # 5) Safety: SL must be on the correct side of price and not too close
        if info.direction == "buy":
            if new_sl >= current_price:
                continue
            if current_price - new_sl < 3 * point:
                continue
        else:
            if new_sl <= current_price:
                continue
            if new_sl - current_price < 3 * point:
                continue

        # 6) Only send modification if it's meaningfully different
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


def manage_pending_orders(state: StrategyState):
    """
    - Remove internal pending records for orders that are no longer active.
    - Cancel any active pending order older than PENDING_ORDER_TIMEOUT_MIN minutes.
    """
    cfg = state.config
    if not state.pending_orders:
        return

    now = utc_now()

    orders = mt5.orders_get(symbol=MT5_SYMBOL)
    active_for_magic = set()
    if orders:
        for o in orders:
            if o.magic == cfg.magic:
                active_for_magic.add(o.ticket)

    # First, drop any pending_orders that no longer exist (filled or externally canceled)
    for ticket in list(state.pending_orders.keys()):
        if ticket not in active_for_magic:
            state.pending_orders.pop(ticket, None)

    # Rebuild active_for_magic after prune
    if not state.pending_orders:
        return

    orders = mt5.orders_get(symbol=MT5_SYMBOL)
    active_for_magic = {}
    if orders:
        for o in orders:
            if o.magic == cfg.magic:
                active_for_magic[o.ticket] = o

    for ticket, created_at in list(state.pending_orders.items()):
        o = active_for_magic.get(ticket)
        if o is None:
            # No longer an active pending order → drop from tracking
            state.pending_orders.pop(ticket, None)
            continue

        age_min = (now - created_at).total_seconds() / 60.0
        if age_min < PENDING_ORDER_TIMEOUT_MIN:
            # Not yet expired → keep tracking
            continue

        # TTL exceeded → send cancel
        req = {
            "action": mt5.TRADE_ACTION_REMOVE,
            "order": ticket,
            "symbol": MT5_SYMBOL,
            "magic": cfg.magic,
            "comment": make_comment(f"{cfg.name}_TTL"),
        }
        res = mt5.order_send(req)
        if res is None or res.retcode != mt5.TRADE_RETCODE_DONE:
            print(
                f"[ERROR] [{cfg.name}] Failed to cancel pending order ticket={ticket}, "
                f"retcode={getattr(res, 'retcode', None)}, "
                f"comment={getattr(res, 'comment', None)}"
            )
        else:
            cancel_time_local = now.astimezone(LOCAL_TZ)
            created_local = created_at.astimezone(LOCAL_TZ)
            print(
                f"[CANCEL] [{cfg.name}] Pending order ticket={ticket} "
                f"(local {cancel_time_local.strftime('%Y-%m-%d %H:%M:%S')}) "
                f"after {age_min:.1f} min (created local "
                f"{created_local.strftime('%Y-%m-%d %H:%M:%S')}) (TTL)"
            )

        # Now we are done with this ticket (cancelled or attempted), so remove it
        state.pending_orders.pop(ticket, None)


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
                    "open_rate": ev.open_rate,
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

            pending_list = []
            for ticket, created_at in st.pending_orders.items():
                pending_list.append({
                    "ticket": ticket,
                    "created_at_utc": created_at.isoformat(),
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
                    "trail_start_R": cfg.trail_start_R,
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
                "pending_orders": pending_list,
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
    Build the two BTC engines:

    Engine 1: chandelier_trailing_only, TP OFF, time exit OFF.
    Engine 2: chandelier, TP ON (2R), time exit OFF.

    Both:
      T=60s, K=2, hold=30min, SL50, ATR(5, I=3, T=2, LB=10),
      direction_mode="inverse", max_open_positions=2, risk=1% dynamic_pct.
    """
    strategies: List[StrategyState] = []

    # ENGINE 1: Chandelier trailing only (no TP, no time exit)
    cfg_1 = StrategyConfig(
        name="BTC_ChandTrail",
        magic=990001,
        t_seconds=60,
        k_unique=2,
        hold_minutes=30,          # not used (time exit OFF), but kept
        sl_distance=50.0,
        tp_R_multiple=2.0,
        stop_mode="chandelier_trailing_only",
        atr_period=5,
        atr_init_mult=3.0,
        atr_trail_mult=2.0,
        chan_lookback=10,
        use_tp_exit=False,        # tp0
        use_time_exit=False,      # time0
        direction_mode="inverse",
        max_open_positions=2,
        risk_mode="dynamic_pct",
        risk_percent=0.01,
        fixed_lots=0.10,
        trail_start_R=1.0,
    )
    strategies.append(StrategyState(
        config=cfg_1,
        cluster_engine=ClusterEngine(window_seconds=cfg_1.t_seconds,
                                     k_unique=cfg_1.k_unique)
    ))

    # ENGINE 2: Chandelier with TP at 2R, no time exit
    cfg_2 = StrategyConfig(
        name="BTC_ChandTP",
        magic=990002,
        t_seconds=60,
        k_unique=2,
        hold_minutes=30,
        sl_distance=50.0,
        tp_R_multiple=2.0,
        stop_mode="chandelier",
        atr_period=5,
        atr_init_mult=3.0,
        atr_trail_mult=2.0,
        chan_lookback=10,
        use_tp_exit=True,         # tp1
        use_time_exit=False,      # time0
        direction_mode="inverse",
        max_open_positions=2,
        risk_mode="dynamic_pct",
        risk_percent=0.01,
        fixed_lots=0.10,
        trail_start_R=None,
    )
    strategies.append(StrategyState(
        config=cfg_2,
        cluster_engine=ClusterEngine(window_seconds=cfg_2.t_seconds,
                                     k_unique=cfg_2.k_unique)
    ))

    return strategies

# =========================
# MAIN LOOP
# =========================

def main_loop():
    strategies = build_strategies()

    max_t_seconds = max(st.config.t_seconds for st in strategies)
    sirix_lookback_seconds = max(max_t_seconds, 60)

    print(f"=== {BOT_NAME} — Multi-Engine Cluster Inverse Bot (BTC) ===")
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

    # BOOTSTRAP: ignore already open Sirix orders at startup
    seen_order_ids: set[str] = set()
    bootstrap_raw = fetch_sirix_open_positions(sirix_lookback_seconds)
    _ = build_events_from_sirix(
        bootstrap_raw,
        seen_order_ids,
        min_open_time=None,
    )
    print(f"[BOOTSTRAP] Ignoring {len(seen_order_ids)} existing Sirix orders at startup.")

    while True:
        try:
            # 1) Fetch new Sirix events once per loop
            raw_positions = fetch_sirix_open_positions(sirix_lookback_seconds)
            new_events = build_events_from_sirix(
                raw_positions,
                seen_order_ids,
                min_open_time=None,
            )

            # 2) Sync positions and manage pending orders (TTL) for each strategy
            for st in strategies:
                refresh_positions_and_log_closes(st)
                manage_pending_orders(st)

            # 3) Cluster detection & pending entries
            for st in strategies:
                cfg = st.config

                # Cap by positions + pending orders
                if len(st.open_positions) + len(st.pending_orders) >= cfg.max_open_positions:
                    continue

                cluster_side, trigger_ev = st.cluster_engine.add_events(new_events)
                if cluster_side is None or trigger_ev is None:
                    continue

                if not within_session_filter():
                    continue

                trade_side = cluster_side
                if cfg.direction_mode == "inverse":
                    trade_side = inverse_side(cluster_side)

                # Use MT5 bid/ask ± LIMIT_OFFSET_DOLLARS instead of Sirix open_rate
                ticket = place_cluster_pending_entry(
                    trade_side=trade_side,
                    cfg=cfg,
                    offset_dollars=LIMIT_OFFSET_DOLLARS,
                )
                if ticket:
                    st.pending_orders[ticket] = utc_now()

                    # reset cluster engine after an entry
                    st.cluster_engine.events.clear()
                    st.cluster_engine.last_cluster_time = None
                    st.cluster_engine.last_cluster_side = None

            # 4) Manage trailing stops and time exits
            for st in strategies:
                manage_trailing_stops(st)
                manage_time_exits(st)

            # 5) State snapshot
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
