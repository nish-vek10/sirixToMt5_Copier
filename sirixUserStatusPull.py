"""
SiRiX UserStatus Full Pull
==========================

Calls the SiRiX REST endpoint:
    POST https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions

Request body (from Swagger + Postman working example):
{
  "UserID": "string",
  "GetOpenPositions": true,
  "GetPendingPositions": true,
  "GetClosePositions": true,
  "GetMonetaryTransactions": true
}

Response model includes:
- UserData (profile, margin, account balances, labels)
- OpenPositions
- PendingOrders
- ClosedPositions
- MonetaryTransactions

This script:
  • Sends the above payload (configurable)
  • Parses all sections
  • Prints ALL documented fields in an easy-to-read terminal format
  • Safely handles missing/empty arrays

NOTE: Hardcoded token for demo; move to environment variable for production.
"""

import os
import requests
from datetime import datetime
from typing import Any, Dict, List, Optional


# ================== CONFIG ==================
API_URL  = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
TOKEN   = "t1_a7xeQOJPnfBzuCncH60yjLFu"     # Bearer token for authentication

# USER_ID = "164228"      # UserID (as string)
USER_ID = input("Enter User ID: ").strip()

# Boolean flags to determine which sections are fetched
REQ_OPEN_POSITIONS       = True
REQ_PENDING_POSITIONS    = True
REQ_CLOSED_POSITIONS     = True
REQ_MONETARY_TRANSACTIONS = True

# Pretty-print options
MAX_LIST_PREVIEW = None    # None = print all rows; set int to limit
# ===========================================


# ---------- Utility formatting ----------
def _fmt_num(v: Any, nd: int = 2) -> Any:
    """
    Format numbers with comma separators and a fixed decimal precision.
    Example: 12345.678 -> "12,345.68"
    """
    if isinstance(v, (int, float)):
        return f"{v:,.{nd}f}"
    return v

def _fmt_dt(ts: Any) -> Any:
    """
    Format ISO date strings (from API) into "YYYY-MM-DD HH:MM:SS".
    Returns raw value if parsing fails.
    """
    if not ts:
        return ts
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ts  # fallback raw string if parse fails


# ---------- API Call ----------
def fetch_userstatus_payload(
    user_id: str,
    get_open: bool = True,
    get_pending: bool = True,
    get_closed: bool = True,
    get_monetary: bool = True,
    token: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Execute the POST call to /api/UserStatus/GetUserTransactions using the provided parameters.
    Returns parsed JSON dict on success, None on failure.
    """
    tk = token if token is not None else TOKEN
    headers = {
        "Authorization": f"Bearer {tk}",    # Bearer token for authentication
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "UserID": str(user_id),
        "GetOpenPositions": bool(get_open),
        "GetPendingPositions": bool(get_pending),
        "GetClosePositions": bool(get_closed),
        "GetMonetaryTransactions": bool(get_monetary),
    }

    try:
        resp = requests.post(API_URL, headers=headers, json=payload, timeout=60)
    except Exception as e:
        print(f"[!] Network error: {e}")
        return None

    if resp.status_code != 200:
        # Print error details if response isn't OK
        print(f"[!] HTTP {resp.status_code}")
        print(resp.text)
        return None

    try:
        return resp.json()
    except Exception as e:
        print(f"[!] JSON parse error: {e}")
        return None


# ---------- Printing Sections ----------
def print_user_data(data: Dict[str, Any]) -> None:
    """
    Print all user-related data, including personal details,
    margin requirements, balances, and labels.
    """
    u = data.get("UserData", {}) or {}
    det = u.get("UserDetails", {}) or {}
    marg = u.get("MarginRequirements", {}) or {}
    bal = u.get("AccountBalance", {}) or {}
    grp = u.get("GroupInfo", {}) or {}
    term = u.get("TradingTermInfo", {}) or {}
    labels = u.get("Labels") or []

    print("\n========== USER DATA ==========")
    print(f"UserID                 : {u.get('UserID')}")
    print(f"FullName               : {det.get('FullName')}")
    print(f"IDNumber               : {det.get('IDNumber')}")
    print(f"Phone                  : {det.get('Phone')}")
    print(f"Email                  : {det.get('Email')}")
    print(f"CreationTime           : {_fmt_dt(det.get('CreationTime'))}")
    print(f"Source                 : {det.get('Source')}")
    print(f"Comment                : {det.get('Comment')}")
    print(f"Country                : {det.get('Country')}")
    print(f"State                  : {det.get('State')}")
    print(f"City                   : {det.get('City')}")
    print(f"Address                : {det.get('Address')}")
    print(f"ZipCode                : {det.get('ZipCode')}")
    print(f"TradingState           : {u.get('TradingState')}")
    print(f"Tradability            : {u.get('Tradability')}")

    # Group Info
    print("\n--- Group Info ---")
    print(f"GroupId                : {grp.get('GroupId')}")
    print(f"GroupName              : {grp.get('GroupName')}")

    # Trading Term Info
    print("\n--- Trading Term Info ---")
    print(f"TradingTermId          : {term.get('TradingTermId')}")
    print(f"TradingTermName        : {term.get('TradingTermName')}")

    # Margin Requirements
    print("\n--- Margin Requirements ---")
    print(f"Leverage               : {marg.get('Leverage')}")
    print(f"MarginCoefficient      : {marg.get('MarginCoefficient')}")
    print(f"UseAcctMarginReq       : {marg.get('UseAccountMarginRequirements')}")
    print(f"AcctHedgingRiskMode    : {marg.get('AccountHedgingRiskMode')}")

    # Account Balance
    print("\n--- Account Balance ---")
    print(f"BalanceUserID          : {bal.get('UserID')}")
    print(f"Balance                : {_fmt_num(bal.get('Balance'))}")
    print(f"Equity                 : {_fmt_num(bal.get('Equity'))}")
    print(f"Margin                 : {_fmt_num(bal.get('Margin'))}")
    print(f"Credit                 : {_fmt_num(bal.get('Credit'))}")
    print(f"OpenPnL                : {_fmt_num(bal.get('OpenPnL'))}")
    print(f"MarginLevel            : {_fmt_num(bal.get('MarginLevel'))}")
    print(f"FreeMargin             : {_fmt_num(bal.get('FreeMargin'))}")
    print(f"Currency               : {u.get('Currency')}")

    # Labels
    if labels:
        print("\n--- Labels ---")
        for L in labels:
            print(f"LabelId {L.get('LabelId')} : {L.get('LabelName')}")


def _preview_list(lst: List[Any]) -> List[Any]:
    """
    Returns a list preview limited by MAX_LIST_PREVIEW.
    """
    if MAX_LIST_PREVIEW is None:
        return lst
    return lst[:MAX_LIST_PREVIEW]


def print_open_positions(data: Dict[str, Any]) -> None:
    """
    Print all open trading positions (if any).
    """
    rows = data.get("OpenPositions") or []
    print("\n========== OPEN POSITIONS ==========")
    print("\033[3mSide 0 = BUY  |  Side 1 = SELL\033[0m")  # Italic text
    if not rows:
        print("None.")
        return

    for p in _preview_list(rows):
        print(
            f"{_fmt_dt(p.get('OpenTime'))} | {p.get('Symbol')} | "
            f"Side: {p.get('Side')} | Amt: {p.get('Amount')} | "
            f"OpenRate: {p.get('OpenRate')} | CurrentRate: {p.get('CurrentRate')} | "
            f"StopLoss: {p.get('StopLoss')} | TakeProfit: {p.get('TakeProfit')} | "
            f"TraderSpread: {p.get('TraderSpread')} | Swap: {p.get('Swap')} | "
            f"Commission: {p.get('Commission')} | Profit: {p.get('Profit')} | "
            f"TotalProfit: {p.get('TotalProfit')} | PendingOrder#: {p.get('PendingOrderNumber')} | "
            f"ClientPlatformType: {p.get('ClientPlatformType')} | Comment: {p.get('Comment')}"
        )
    print(f"\nTotal Open Positions: {len(rows)}")


def print_pending_orders(data: Dict[str, Any]) -> None:
    """
    Print all pending orders (if any).
    """
    rows = data.get("PendingOrders") or []
    print("\n========== PENDING ORDERS ==========")
    print("\033[3mSide 0 = BUY  |  Side 1 = SELL\033[0m")  # Italic text
    if not rows:
        print("None.")
        return

    for o in _preview_list(rows):
        print(
            f"{_fmt_dt(o.get('CreationTime'))} | {o.get('Symbol')} | "
            f"Type: {o.get('PendingOrderType')} | Amt: {o.get('Amount')} | "
            f"Price: {o.get('Price')} | SL: {o.get('StopLoss')} | TP: {o.get('TakeProfit')} | "
            f"Expiration: {_fmt_dt(o.get('ExpirationTime'))} | "
            f"Order#: {o.get('OrderNumber')} | Comment: {o.get('Comment')}"
        )
    print(f"\nTotal Pending Orders: {len(rows)}")


def print_closed_positions(data: Dict[str, Any]) -> None:
    """
    Print all closed positions sorted by CloseTime (newest first).
    """
    rows = data.get("ClosedPositions") or []
    print("\n========== CLOSED POSITIONS ==========")
    print("\033[3mSide 0 = BUY  |  Side 1 = SELL\033[0m")  # Italic text
    if not rows:
        print("None.")
        return

    # newest first by CloseTime
    rows = sorted(rows, key=lambda r: r.get("CloseTime"), reverse=True)

    for c in _preview_list(rows):
        print(
            f"{_fmt_dt(c.get('CloseTime'))} | {c.get('Symbol')} | "
            f"Side: {c.get('Side')} | Amt: {c.get('Amount')} | "
            f"Open: {_fmt_dt(c.get('OpenTime'))} @{c.get('OpenRate')} | "
            f"Close: {_fmt_dt(c.get('CloseTime'))} @{c.get('CloseRate')} | "
            f"StopLoss: {c.get('StopLoss')} | TakeProfit: {c.get('TakeProfit')} | "
            f"TraderSpread: {c.get('TraderSpread')} | Swap: {c.get('Swap')} | "
            f"Commission: {c.get('Commission')} | Profit: {c.get('Profit')} | "
            f"TotalProfit: {c.get('TotalProfit')} | "
            f"OpenedFrom: {c.get('OpenedFrom')} | ClosedFrom: {c.get('ClosedFrom')} | "
            f"OpenPositionId: {c.get('OpenPositionId')} | Order#: {c.get('OrderNumber')} | "
            f"Comment: {c.get('Comment')}"
        )
    print(f"\nTotal Closed Positions: {len(rows)}")


def print_monetary_transactions(data: Dict[str, Any]) -> None:
    """
    Print all monetary transactions (deposits, withdrawals, etc.)
    sorted by time (newest first).
    """
    rows = data.get("MonetaryTransactions") or []
    print("\n========== MONETARY TRANSACTIONS ==========")
    if not rows:
        print("None.")
        return

    # newest first
    rows = sorted(rows, key=lambda r: r.get("Time"), reverse=True)

    for t in _preview_list(rows):
        print(
            f"{_fmt_dt(t.get('Time'))} | Order#: {t.get('OrderNumber')} | "
            f"Type: {t.get('Type')} | Amount: {t.get('Amount')} | Comment: {t.get('Comment')}"
        )
    print(f"\nTotal Monetary Transactions: {len(rows)}")


# ---------- Main ----------
if __name__ == "__main__":
    data = fetch_userstatus_payload(
        USER_ID,
        get_open=REQ_OPEN_POSITIONS,
        get_pending=REQ_PENDING_POSITIONS,
        get_closed=REQ_CLOSED_POSITIONS,
        get_monetary=REQ_MONETARY_TRANSACTIONS,
    )

    if not data:
        raise SystemExit("[!] No data returned.")

    # Debug counts
    print("[DEBUG] Top-level keys:", list(data.keys()))
    print("[DEBUG] Counts:",
          "Open:", len(data.get("OpenPositions", []) or []),
          "| Pending:", len(data.get("PendingOrders", []) or []),
          "| Closed:", len(data.get("ClosedPositions", []) or []),
          "| Tx:", len(data.get("MonetaryTransactions", []) or []))

    # Full Data Prints
    print_user_data(data)
    print_open_positions(data)
    print_pending_orders(data)
    print_closed_positions(data)
    print_monetary_transactions(data)


