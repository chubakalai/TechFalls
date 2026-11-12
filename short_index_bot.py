#!/usr/bin/env python3
"""
Index Short DCA Bot — shorts S&P 500 and NASDAQ-100 perps on MEXC.

Flow
  1. wake up       load specs + persisted state
  2. test order    min-size limit SHORT far above market, wait, cancel
  3. schedule      at 00 / 06 / 12 / 18 UTC place the next order

Spending rule
  rate(a)       = 10 * 0.5 ** (a / 30)      USD per day, a = days since START
  cumulative(a) = integral of rate from 0 to a
                = 10 * 30 / ln2 * (1 - 0.5 ** (a / 30))

  At each slot the bot orders:   cumulative(slot_time) - spent_so_far
  so any amount that could not be ordered (below the exchange minimum,
  rounding remainder, rejected order, missed slot) stays in the gap and
  is picked up by the next slot. Nothing is lost, nothing is doubled.

No double spending
  - Slots are fixed wall-clock instants (UTC 0/6/12/18). The slot is
    CLAIMED and persisted before any order is sent, so a crash/restart
    can never replay it.
  - The notional is reserved in `spent` before the order is sent and
    rolled back only if the exchange rejects it.
  - A slot older than GRACE_SEC when the bot wakes is skipped; the
    cumulative rule makes the next slot cover it.

Env:  MEXC (api key), MEXCSECRET, optional DCA_STATE_FILE, PORT
"""

import datetime
import hashlib
import hmac
import http.server
import json
import logging
import math
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ── config ────────────────────────────────────────────────────────────────────

SYMBOLS: List[str] = ["SPX500_USDT", "NAS100_USDT"]

LEVERAGE = 20

# spending rule: rate(a) = RATE0 * 0.5 ** (a / HALF_LIFE_DAYS)   [USD / day]
START          = datetime.datetime(2026, 11, 13, tzinfo=datetime.timezone.utc)
RATE0          = 10.0
HALF_LIFE_DAYS = 30.0

# True : the rule is the TOTAL across all symbols (each gets an equal share)
# False: the rule applies to EACH symbol separately (double the total)
SPLIT_BETWEEN_SYMBOLS = True

SLOT_HOURS = 6           # slots fall on 0 / 6 / 12 / 18 UTC
GRACE_SEC  = 30 * 60     # skip a slot we wake up to more than this late
POLL_SEC   = 10

TEST_ORDER_PREMIUM  = 1.10   # test short limit sits 10% above mark -> won't fill
TEST_ORDER_WAIT_SEC = 20

# MEXC futures: side 3 = open short, order type 5 = market, 1 = limit
SIDE_OPEN_SHORT = 3
TYPE_LIMIT      = 1
TYPE_MARKET     = 5

UTC         = datetime.timezone.utc
MEXC_KEY    = os.getenv("MEXC")
MEXC_SECRET = os.getenv("MEXCSECRET")
MEXC_BASE   = "https://api.mexc.co"

STATE_FILE = os.getenv("DCA_STATE_FILE", "/data/short_index_state.json")
HTTP_HOST  = "0.0.0.0"
HTTP_PORT  = int(os.getenv("PORT", "8080"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger()

specs: Dict[str, Dict] = {}
FAILED: Dict[str, str] = {}


# ── the schedule ──────────────────────────────────────────────────────────────

def rate(a_days: float) -> float:
    """Spending rate in USD/day, a_days since START."""
    return RATE0 * 0.5 ** (a_days / HALF_LIFE_DAYS)


def cumulative(a_days: float) -> float:
    """Integral of rate() from 0 to a_days. Zero before START."""
    if a_days <= 0:
        return 0.0
    k = math.log(2) / HALF_LIFE_DAYS
    return RATE0 / k * (1.0 - math.exp(-k * a_days))


def target_usd(at: datetime.datetime) -> float:
    """Cumulative amount each symbol should have spent by `at`."""
    a = (at - START).total_seconds() / 86400.0
    total = cumulative(a)
    return total / len(SYMBOLS) if SPLIT_BETWEEN_SYMBOLS else total


def slot_of(now: datetime.datetime) -> datetime.datetime:
    """The most recent fixed slot at or before `now`."""
    return now.replace(
        hour=(now.hour // SLOT_HOURS) * SLOT_HOURS,
        minute=0, second=0, microsecond=0,
    )


# ── persisted state ───────────────────────────────────────────────────────────

def _default_state() -> Dict:
    return {"spent": {}, "last_slot": {}, "orders": []}


def load_state() -> Dict:
    try:
        with open(STATE_FILE) as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("not a dict")
        for k, v in _default_state().items():
            data.setdefault(k, v)
        return data
    except FileNotFoundError:
        log.info(f"no state file at {STATE_FILE} — starting fresh")
    except Exception as e:
        log.error(f"state file unreadable ({e}) — starting fresh")
    return _default_state()


STATE = load_state()
LOCK = threading.Lock()


def save_state():
    """Call with LOCK held."""
    try:
        os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(STATE, f)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log.error(f"failed to persist state: {e}")


def spent(sym: str) -> float:
    return float(STATE["spent"].get(sym, 0.0))


# ── MEXC plumbing ─────────────────────────────────────────────────────────────

def _http(method, url, headers=None, data=None, params=None):
    if params:
        url += "?" + urllib.parse.urlencode(sorted(params.items()))
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read()
    except urllib.error.HTTPError as e:
        body = e.read()
    return json.loads(body) if body.strip() else {}


def mexc(method, endpoint, params=None, body=None):
    params = params or {}
    ts = str(int(time.time() * 1000))

    sp = (
        "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        if method == "GET"
        else (json.dumps(body, separators=(",", ":"), sort_keys=True) if body else "")
    )
    sig = hmac.new(
        MEXC_SECRET.encode(), (MEXC_KEY + ts + sp).encode(), hashlib.sha256
    ).hexdigest()

    hdr = {
        "ApiKey": MEXC_KEY,
        "Request-Time": ts,
        "Signature": sig,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    raw = (
        json.dumps(body, separators=(",", ":"), sort_keys=True).encode()
        if body and method not in ("GET", "DELETE")
        else None
    )
    try:
        return _http(
            method, MEXC_BASE + endpoint, headers=hdr, data=raw,
            params=params if method in ("GET", "DELETE") else None,
        )
    except Exception as e:
        log.error(f"mexc {method} {endpoint}: {e}")
        return {}


def flag_failed(sym: str, reason: str):
    FAILED[sym] = reason
    log.error(f"[{sym}] FAILED — {reason} — symbol will not trade")


def load_specs():
    rows = mexc("GET", "/api/v1/contract/detail").get("data") or []
    if not rows:
        for sym in SYMBOLS:
            flag_failed(sym, "empty contract detail response")
        return

    by_sym = {c.get("symbol", "").upper(): c for c in rows}
    for sym in SYMBOLS:
        c = by_sym.get(sym)
        if c is None:
            flag_failed(sym, "symbol not found in contract detail")
            continue

        vu = float(c.get("volUnit", 1))
        pu = float(c.get("priceUnit", 0.01))
        cs = float(c.get("contractSize", vu))
        raw = f"{vu:.10f}".rstrip("0")
        p = len(raw.split(".")[1]) if "." in raw else 0

        try:
            ml = float(c["maxLeverage"]) if c.get("maxLeverage") else None
            if ml is not None and ml <= 0:
                ml = None
        except Exception:
            ml = None

        specs[sym] = {"p": p, "t": pu, "vu": vu, "cs": cs, "max_lev": ml}
        log.info(f"[{sym}] specs: {specs[sym]}")


def _leverage(sym: str) -> int:
    ml = specs.get(sym, {}).get("max_lev")
    return int(ml) if ml is not None and ml < LEVERAGE else LEVERAGE


def _fmt_price(sym: str, v: float) -> str:
    t = specs[sym]["t"]
    s = f"{t:.10f}".rstrip("0")
    dec = len(s.split(".")[1]) if "." in s else 0
    return f"{round(v / t) * t:.{dec}f}"


def _fmt_vol(sym: str, v: float) -> str:
    p = specs[sym]["p"]
    f = 10 ** p
    return f"{math.floor((v + 1e-9) * f) / f:.{p}f}"


def min_vol(sym: str) -> float:
    return specs[sym]["vu"]


def contracts_for(sym: str, usd: float, price: float) -> float:
    return float(_fmt_vol(sym, max(0.0, usd / (specs[sym]["cs"] * price))))


def get_mark(sym: str) -> float:
    d = mexc("GET", "/api/v1/contract/ticker", params={"symbol": sym}).get("data") or {}
    return float(d.get("fairPrice", d.get("lastPrice", 0)) or 0)


def open_ids(sym: str) -> set:
    data = mexc(
        "GET", "/api/v1/private/order/list/open_orders",
        params={"symbol": sym, "page_num": 1, "page_size": 100},
    ).get("data") or []
    if isinstance(data, dict):
        data = data.get("resultList", [])
    return {str(o.get("orderId", "")) for o in data if o.get("symbol", "").upper() == sym}


def place_short(sym: str, vol: float, order_type: int, price: Optional[float] = None) -> Optional[str]:
    body = {
        "leverage": _leverage(sym),
        "openType": 2,          # cross
        "positionMode": 1,      # hedge
        "side": SIDE_OPEN_SHORT,
        "symbol": sym,
        "type": order_type,
        "vol": _fmt_vol(sym, vol),
    }
    if price is not None:
        body["price"] = _fmt_price(sym, price)

    r = mexc("POST", "/api/v1/private/order/create", body=body)
    data = r.get("data")
    if not r.get("success") or not isinstance(data, dict) or not data.get("orderId"):
        log.error(f"[{sym}] short order rejected: {r}")
        return None

    oid = str(data["orderId"])
    log.info(f"[{sym}] SHORT {body['vol']} type={order_type} "
             f"price={body.get('price', 'market')} lev={body['leverage']}x id={oid}")
    return oid


def cancel_order(sym: str, oid: str) -> bool:
    ok = bool(mexc("POST", "/api/v1/private/order/cancel", body=[oid]).get("success"))
    log.info(f"[{sym}] cancel id={oid}: {'ok' if ok else 'FAILED'}")
    return ok


# ── step 2: startup test order ────────────────────────────────────────────────

def run_test_orders():
    log.info(f"══ test orders: opening ({len(SYMBOLS)} symbols) ══")
    pending = []

    for sym in SYMBOLS:
        if sym in FAILED or sym not in specs:
            continue
        try:
            mark = get_mark(sym)
            if mark <= 0:
                flag_failed(sym, f"invalid mark price ({mark})")
                continue
            price = mark * TEST_ORDER_PREMIUM
            vol = min_vol(sym)
            oid = place_short(sym, vol, TYPE_LIMIT, price)
            if oid is None:
                flag_failed(sym, "test order rejected")
                continue
            pending.append((sym, oid, vol, price))
        except Exception as e:
            flag_failed(sym, f"test order exception: {e}")

    log.info(f"══ test orders: waiting {TEST_ORDER_WAIT_SEC}s ══")
    time.sleep(TEST_ORDER_WAIT_SEC)

    for sym, oid, vol, price in pending:
        try:
            if oid not in open_ids(sym):
                # filled: it is now a real short, so count it as spent
                notional = vol * specs[sym]["cs"] * price
                log.warning(f"[{sym}] test order FILLED — real short, counting ${notional:.2f} as spent")
                with LOCK:
                    STATE["spent"][sym] = spent(sym) + notional
                    STATE["orders"].append({
                        "symbol": sym, "kind": "test_filled", "order_id": oid,
                        "vol": vol, "price": price,
                        "time": datetime.datetime.now(UTC).isoformat(),
                    })
                    save_state()
            elif not cancel_order(sym, oid):
                flag_failed(sym, f"test order {oid} could not be cancelled")
        except Exception as e:
            flag_failed(sym, f"test order close exception: {e}")

    ok = [s for s in SYMBOLS if s not in FAILED]
    log.info(f"══ test orders done: ok={ok} failed={FAILED or '-'} ══")


# ── step 3: follow the schedule ───────────────────────────────────────────────

def process_slot(sym: str, slot: datetime.datetime, now: datetime.datetime):
    slot_ts = int(slot.timestamp())

    # Claim the slot FIRST and persist it: a crash after this point can
    # never replay the slot, so it can never double spend.
    with LOCK:
        if slot_ts <= STATE["last_slot"].get(sym, 0):
            return
        STATE["last_slot"][sym] = slot_ts
        save_state()

    late = (now - slot).total_seconds()
    if late > GRACE_SEC:
        log.warning(f"[{sym}] slot {slot:%Y-%m-%d %H:%M} is {late/60:.0f} min old — "
                    "skipped; next slot catches up")
        return
    if sym in FAILED or sym not in specs:
        log.warning(f"[{sym}] slot {slot:%Y-%m-%d %H:%M} skipped — symbol failed")
        return

    target = target_usd(slot)
    gap = target - spent(sym)
    log.info(f"[{sym}] slot {slot:%Y-%m-%d %H:%M} UTC  target=${target:.4f}  "
             f"spent=${spent(sym):.4f}  gap=${gap:.4f}")

    if gap <= 0:
        return

    mark = get_mark(sym)
    if mark <= 0:
        log.error(f"[{sym}] bad mark price {mark} — gap ${gap:.4f} carries to next slot")
        return

    vol = contracts_for(sym, gap, mark)
    if vol < min_vol(sym):
        log.info(f"[{sym}] gap ${gap:.4f} < exchange minimum — carries to next slot")
        return

    notional = vol * specs[sym]["cs"] * mark

    # Reserve before sending; roll back only if the exchange rejects it.
    with LOCK:
        STATE["spent"][sym] = spent(sym) + notional
        save_state()

    oid = place_short(sym, vol, TYPE_MARKET)

    with LOCK:
        if oid is None:
            STATE["spent"][sym] = spent(sym) - notional
            log.error(f"[{sym}] order failed — ${notional:.4f} returned to the gap")
        else:
            STATE["orders"].append({
                "symbol": sym, "kind": "scheduled", "order_id": oid,
                "slot": slot.isoformat(), "vol": vol, "mark": mark,
                "notional_usd": notional,
                "time": datetime.datetime.now(UTC).isoformat(),
            })
        save_state()


def run():
    # 1. wake up
    log.info(f"wake up — {len(SYMBOLS)} symbols, START={START.isoformat()}")
    load_specs()

    # 2. test order
    run_test_orders()

    # 3. follow the schedule
    log.info("following the schedule (00 / 06 / 12 / 18 UTC)")
    while True:
        now = datetime.datetime.now(UTC)
        slot = slot_of(now)
        for sym in SYMBOLS:
            try:
                process_slot(sym, slot, now)
            except Exception as e:
                log.error(f"[{sym}] slot processing failed: {e}", exc_info=True)
        time.sleep(POLL_SEC)


# ── tiny status server ────────────────────────────────────────────────────────

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        now = datetime.datetime.now(UTC)
        nxt = slot_of(now) + datetime.timedelta(hours=SLOT_HOURS)
        with LOCK:
            body = json.dumps({
                "now": now.isoformat(),
                "next_slot": nxt.isoformat(),
                "target_now_per_symbol": target_usd(now),
                "spent": STATE["spent"],
                "failed": FAILED,
                "recent_orders": STATE["orders"][-20:],
            }, indent=2).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    if not MEXC_KEY or not MEXC_SECRET:
        log.error("MEXC / MEXCSECRET not set")
        raise SystemExit(1)

    server = http.server.ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info(f"status on {HTTP_HOST}:{HTTP_PORT}")
    run()


if __name__ == "__main__":
    main()
