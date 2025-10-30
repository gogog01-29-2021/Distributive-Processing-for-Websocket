from __future__ import annotations
"""
Order-book WebSocket publishers for Coinbase Advanced Trade (and a generic base).

Overview
--------
This module defines two classes:

1) `OrderBookPublisher` (abstract)
   - Exchange-agnostic L2 (price-level) order-book publisher built atop
     `WebSocketPublisher`.
   - Tracks per-product in-memory books as:
         bids: {price:str -> (qty:str, last_event_us:int)}
         asks: {price:str -> (qty:str, last_event_us:int)}
   - Emits a normalized, depth-bounded envelope via a `PubTransport`.
   - Supports compact periodic logging of top-of-book state.
   - Provides an optional REST snapshot recovery hook to resynchronize a single
     product without tearing down the WebSocket session. If not implemented,
     falls back to WS unsubscribe→subscribe.

2) `CoinbaseOrderBookPublisher`
   - Concrete implementation for Coinbase Advanced Trade “level2” channel.
   - Parses exchange frames, normalizes deltas, detects connection-wide sequence
     gaps, and triggers per-product recovery.

Normalization
-------------
Inbound deltas (internal representation):

    {"side": "bid"|"ask", "price": str, "qty": str, "event_us": int}

`event_us` is kept internally for recency/monotonic checks and recovery replay,
but it is **not** published to downstream consumers.

Publishing envelope shape (per product update):
    {
        "exchange":        <str>,        # e.g., "coinbase"
        "symbol":          <str>,        # product_id
        "sequence_num":    <int>,        # connection-wide seq (if available)
        "bids":            List[Tuple[price:str, qty:str]],  # no timestamps
        "asks":            List[Tuple[price:str, qty:str]],  # no timestamps
        "time_exchange":   <int>,        # exchange frame timestamp (µs)
        "time_received":   <int>,        # local receive timestamp (µs)
        "time_published":  <int>,        # now_in_micros()
    }

Threading Model
---------------
- `WebSocketPublisher` (base) runs a WS thread that calls `on_message`.
- `OrderBookPublisher` maintains per-product state under `_state_lock`.
- A background *book-logger* thread emits concise snapshots for observability.
- Recovery per product runs on dedicated daemon threads, gated by a semaphore to
  avoid request stampedes.

Recovery Semantics
------------------
- If `_fetch_rest_snapshot(product_id)` is implemented:
    * Apply the snapshot (replace state) → drop cached deltas with
      `event_us <= snapshot_boundary` → replay the remaining cached deltas
      (ascending by `event_us`) → mark in-sync → publish consolidated state.
- Otherwise:
    * WS unsubscribe→subscribe for the product and keep caching deltas until a
      fresh `snapshot` arrives via WS (at which point `ingest_event` replaces
      state and flips to in-sync).

Limitations
-----------
- In-process at-least-once semantics (to the in-memory backlog only).
- Depth in published/logged views is bounded by `max_level`. Internal dicts may
  contain deeper levels unless reset by `snapshot` or pruning logic.
"""

import heapq
import logging
import os
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple

from hft.mdms.base.publisher import WebSocketPublisher, now_in_micros
from hft.mdms.base.transport import PubTransport

# Single JSON loader (fast if orjson is available, graceful fallback otherwise).
try:
    import orjson as json_parser  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    import json as json_parser  # type: ignore[assignment]

_log = logging.getLogger(__name__)

# Coinbase Advanced Trade WS endpoints
_AT_WS_PROD = "wss://advanced-trade-ws.coinbase.com"
_AT_WS_SANDBOX = "wss://advanced-trade-ws.sandbox.coinbase.com"

# Coinbase Advanced Trade REST (JWT/Bearer) for product_book snapshots
_AT_REST_AT = "https://api.coinbase.com/api/v3/brokerage"

# Fallback: legacy public Exchange REST (no auth) for L2 book
_AT_REST_EXCHANGE = "https://api.exchange.coinbase.com"


# --------------------------------------------------------------------------- #
# Base: exchange-agnostic L2 book + logging + publish + recovery scaffolding
# --------------------------------------------------------------------------- #
class OrderBookPublisher(WebSocketPublisher, ABC):
    """
    Abstract L2 order-book publisher with optional snapshot-based recovery.
    """

    def __init__(
        self,
        ws_url: str,
        transport: PubTransport,
        *,
        initial_product_ids: Optional[Sequence[str]] = None,
        max_level: int = 50,
        channel_name: str = "level2",
        exchange_name: str = "unknown",
        book_log_interval_sec: float = 1.0,
        book_log_depth: int = 5,
        log_interval_sec: Optional[int] = None,
        verbose: bool = True,
        max_backlog: int = 100_000,
        on_full: str = "block",
        drain_timeout_sec: float = 5.0,
    ):
        super().__init__(
            ws_url,
            transport,
            log_interval_sec=log_interval_sec,
            verbose=verbose,
            max_backlog=max_backlog,
            on_full=on_full,
            drain_timeout_sec=drain_timeout_sec,
        )
        # Current subscription set and its guard.
        self._subs_lock = threading.Lock()
        self._subs: set[str] = set(initial_product_ids or ())

        # Publishing/logging configuration.
        self._max_level = int(max(1, max_level))
        self._channel_name = channel_name
        self._exchange_name = exchange_name

        # Per-product state guarded by a single lock to keep updates atomic.
        self._state_lock = threading.Lock()
        # price -> (qty:str, last_event_us:int)
        self._bids: Dict[str, Dict[str, Tuple[str, int]]] = {p: {} for p in self._subs}
        self._asks: Dict[str, Dict[str, Tuple[str, int]]] = {p: {} for p in self._subs}
        self._seq_log: Dict[str, Optional[int]] = {p: None for p in self._subs}
        self._sync: Dict[str, bool] = {p: True for p in self._subs}

        # Bounded per-product recovery cache to avoid unbounded memory growth.
        self._cache_max = 200_000
        # (event_us, side, price, qty)
        self._cache: Dict[str, Deque[Tuple[int, str, str, str]]] = {
            p: deque(maxlen=self._cache_max) for p in self._subs
        }
        self._cache_dropped: Dict[str, int] = {p: 0 for p in self._subs}

        # Concurrency cap to avoid recovery stampedes on large subscription sets.
        self._recover_sem = threading.Semaphore(8)

        # Configurable small gap between unsubscribe→subscribe.
        self._resub_sleep_s = 0.05

        # Book-logger thread control.
        self._book_log_interval = float(book_log_interval_sec)
        self._book_log_depth = int(max(1, book_log_depth))
        self._book_log_stop = threading.Event()
        self._book_logger_thread: Optional[threading.Thread] = None

        # ---------------- Watchdog state / tunables ----------------
        # Count normalized updates per product since start (sum of per-frame len(updates)).
        self._updates_seen: Dict[str, int] = {p: 0 for p in self._subs}
        # How long a best level can remain unchanged while updates flow.
        self._stuck_watch_sec = 60.0
        # How many per-level updates should pass during that interval to consider it "stuck".
        self._stuck_updates_threshold = 2000
        # Cooldown between auto-recover attempts per product.
        self._recover_cooldown_sec = 45.0

    # ---- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        super().start()
        if self._book_log_interval > 0:
            self._book_log_stop.clear()
            self._book_logger_thread = threading.Thread(
                target=self._book_logger_loop,
                name=f"{self.__class__.__name__}-booklogger",
                daemon=True,
            )
            self._book_logger_thread.start()

    def stop(self) -> None:
        self._book_log_stop.set()
        t = self._book_logger_thread
        if t:
            t.join(timeout=2.0)
            self._book_logger_thread = None
        super().stop()

    # ---- subscribe / unsubscribe ------------------------------------------

    def on_open(self, ws) -> None:
        super().on_open(ws)
        with self._subs_lock:
            if self._subs:
                try:
                    frame = self._build_subscribe_frame(sorted(self._subs))
                    self._ws_send_json(frame)
                    _log.info("%s: subscribed %s", self.__class__.__name__, sorted(self._subs))
                except Exception:
                    _log.exception("subscribe on_open failed")

    def subscribe(self, product_ids: Iterable[str]) -> None:
        ids = [p for p in product_ids]
        if not ids:
            return
        with self._subs_lock:
            self._subs.update(ids)
        with self._state_lock:
            for p in ids:
                self._bids.setdefault(p, {})
                self._asks.setdefault(p, {})
                self._seq_log.setdefault(p, None)
                self._sync.setdefault(p, True)
                if p not in self._cache:
                    self._cache[p] = deque(maxlen=self._cache_max)
                self._cache_dropped.setdefault(p, 0)
                self._updates_seen.setdefault(p, 0)
        self._ws_send_json(self._build_subscribe_frame(ids))
        _log.info("Subscribe requested: %s", ids)

    def unsubscribe(self, product_ids: Optional[Iterable[str]] = None) -> None:
        if product_ids is None:
            with self._subs_lock:
                ids = list(self._subs)
                self._subs.clear()
            self._ws_send_json(self._build_unsubscribe_frame(None))
            _log.info("Unsubscribe all requested (%d products)", len(ids))
            return

        ids = [p for p in product_ids]
        if not ids:
            return
        with self._subs_lock:
            for p in ids:
                self._subs.discard(p)
        self._ws_send_json(self._build_unsubscribe_frame(ids))
        _log.info("Unsubscribe requested: %s", ids)

    # ---- exchange-specific control frames ---------------------------------

    @abstractmethod
    def _build_subscribe_frame(self, product_ids: Sequence[str]) -> dict:
        ...

    @abstractmethod
    def _build_unsubscribe_frame(self, product_ids: Optional[Sequence[str]]) -> dict:
        ...

    # ---- optional REST snapshot hook (subclasses MAY override) ------------

    def _fetch_rest_snapshot(self, product_id: str) -> Optional[Tuple[List[Dict[str, Any]], int]]:
        return None

    # ---- apply normalized deltas + publish --------------------------------

    def ingest_event(
        self,
        *,
        product_id: str,
        etype: str,                    # "snapshot" | "update"
        updates: List[Dict[str, Any]],
        seq: Optional[int],            # connection/channel seq (for logging only)
        t_exch_us: int,
        t_recv_us: int,
    ) -> bool:
        changed = False
        with self._state_lock:
            bids = self._bids.setdefault(product_id, {})
            asks = self._asks.setdefault(product_id, {})
            self._sync.setdefault(product_id, True)
            self._cache.setdefault(product_id, deque(maxlen=self._cache_max))
            self._cache_dropped.setdefault(product_id, 0)
            self._updates_seen[product_id] = self._updates_seen.get(product_id, 0) + len(updates)

            if not self._sync[product_id]:
                # Buffer while waiting for recovery (drop malformed gracefully).
                buf = self._cache[product_id]
                before_full = len(buf) == buf.maxlen
                for u in updates:
                    side = u.get("side")
                    price = str(u.get("price"))
                    qty = str(u.get("qty", "0"))
                    ev_us = int(u.get("event_us", t_recv_us))
                    if side in ("bid", "ask") and price != "None":
                        buf.append((ev_us, side, price, qty))
                if before_full and updates:
                    self._cache_dropped[product_id] += len(updates)
                return False

            if etype == "snapshot":
                bids.clear()
                asks.clear()

            # --- Fix #1 & #2: zero-ish deletes + allow older decrements ---
            EPS = 1e-12

            def _is_zeroish(q: str) -> bool:
                try:
                    return abs(float(q)) <= EPS
                except Exception:
                    return False

            for u in updates:
                side = u.get("side")
                price = str(u.get("price"))
                qty = str(u.get("qty", "0"))
                ev_us = int(u.get("event_us", 0))
                book = bids if side == "bid" else asks if side == "ask" else None
                if book is None or price == "None":
                    continue

                # Always honor deletes, even if timestamp looks "older".
                if _is_zeroish(qty):
                    if price in book:
                        del book[price]
                        changed = True
                    continue  # nothing else to do

                # For non-zero updates, keep the monotonic guard—but allow older *decreases*.
                prev = book.get(price)
                if prev is not None:
                    prev_qty_str, prev_ts = prev
                    # Try numeric compare; if parsing fails, fall back to strict monotonic guard.
                    try:
                        prev_qty = float(prev_qty_str)
                        new_qty = float(qty)
                    except Exception:
                        if ev_us and prev_ts and ev_us < prev_ts:
                            # Stale non-zero update; ignore if we can't safely compare amounts.
                            continue
                        # Proceed to write below.
                    else:
                        if ev_us and prev_ts and ev_us < prev_ts:
                            # Late frame: permit only decreasing qty (helps fix stuck bloat).
                            if new_qty + EPS < prev_qty:
                                # Keep *newer* timestamp to avoid regressing ts on the level.
                                book[price] = (qty, prev_ts)
                                changed = True
                            # Never accept an older increase.
                            continue

                # Normal (non-stale or first) write.
                book[price] = (qty, ev_us)
                changed = True

            if changed:
                if seq is not None:
                    self._seq_log[product_id] = seq
                self._publish_book(product_id, seq if seq is not None else -1, t_exch_us, t_recv_us)
        return changed

    # ---- recovery helpers --------------------------------------------------

    def _mark_out_of_sync(self, product_ids: Iterable[str]) -> None:
        with self._state_lock:
            for p in product_ids:
                self._sync[p] = False
                self._bids.setdefault(p, {}).clear()
                self._asks.setdefault(p, {}).clear()
                if p not in self._cache:
                    self._cache[p] = deque(maxlen=self._cache_max)
                self._cache_dropped.setdefault(p, 0)

    def _recover_product(self, product_id: str) -> None:
        with self._recover_sem:
            try_rest = True
            try:
                snap = self._fetch_rest_snapshot(product_id)
            except NotImplementedError:
                snap = None
            except Exception:
                _log.exception("REST snapshot fetch failed for %s", product_id)
                snap = None

            if snap is None:
                try_rest = False

            if try_rest:
                updates, boundary_us = snap  # type: ignore[misc]
                with self._state_lock:
                    self._apply_levels(product_id, updates, replace=True)

                    buf = self._cache.setdefault(product_id, deque(maxlen=self._cache_max))
                    cached = [c for c in buf if c[0] > int(boundary_us)]
                    cached.sort(key=lambda x: x[0])

                    bids = self._bids[product_id]
                    asks = self._asks[product_id]
                    for ev_us, side, price, qty in cached:
                        book = bids if side == "bid" else asks
                        prev = book.get(price)
                        if prev is not None and ev_us < prev[1]:
                            continue
                        if qty == "0":
                            book.pop(price, None)
                        else:
                            book[price] = (qty, ev_us)

                    buf.clear()
                    self._sync[product_id] = True

                    self._publish_book(
                        product_id,
                        self._seq_log.get(product_id) or -1,
                        now_in_micros(),
                        now_in_micros(),
                    )
                _log.info("Recovered %s via REST snapshot", product_id)
                return

            self._ws_resubscribe_product(product_id)
            with self._state_lock:
                _log.info("Triggered WS resubscribe for %s (no REST snapshot)", product_id)

    def _ws_resubscribe_product(self, product_id: str) -> None:
        try:
            self._ws_send_json(self._build_unsubscribe_frame([product_id]))
            time.sleep(self._resub_sleep_s)
            self._ws_send_json(self._build_subscribe_frame([product_id]))
        except Exception:
            _log.exception("WS resubscribe failed for %s", product_id)

    def _apply_levels(self, product: str, updates: List[Dict[str, Any]], *, replace: bool) -> None:
        bids = self._bids.setdefault(product, {})
        asks = self._asks.setdefault(product, {})
        if replace:
            bids.clear()
            asks.clear()
        for u in updates:
            side = u.get("side")
            price = str(u.get("price"))
            qty = str(u.get("qty", "0"))
            ev_us = int(u.get("event_us", 0))
            if side not in ("bid", "ask") or price == "None":
                continue
            book = bids if side == "bid" else asks
            if qty == "0":
                book.pop(price, None)
            else:
                book[price] = (qty, ev_us)

    # ---- publish normalized envelope --------------------------------------

    def _publish_book(self, product: str, seq: int, t_exch_us: int, t_recv_us: int) -> None:
        bids = self._bids[product]
        asks = self._asks[product]

        def _select_top(
            d: Dict[str, Tuple[str, int]],
            n: int,
            reverse: bool,
        ) -> List[Tuple[str, str]]:
            it = ((p, qts[0]) for p, qts in d.items())
            key = lambda x: float(x[0])
            return heapq.nlargest(n, it, key=key) if reverse else heapq.nsmallest(n, it, key=key)

        top_bids = _select_top(bids, self._max_level, True)
        top_asks = _select_top(asks, self._max_level, False)

        out = {
            "exchange": self._exchange_name,
            "symbol": product,
            "sequence_num": seq,
            "bids": top_bids,
            "asks": top_asks,
            "time_exchange": t_exch_us,
            "time_received": t_recv_us,
            "time_published": now_in_micros(),
        }
        topic = f"orderbook.{self._channel_name}.{self._exchange_name}.{product}"
        self._append_backlog(topic, out)

    # ---- periodic compact logger + watchdogs --------------------------------

    def _book_logger_loop(self) -> None:
        """
        Emit concise per-product summaries at a fixed cadence.

        Enhancements:
        - While a product is out-of-sync, log `state=recovering` with cache stats.
        - Fix #3: auto-recover if crossed (ask < bid) or if best is "stuck but not crossed".
        """
        interval = max(0.0, self._book_log_interval)
        depth = self._book_log_depth

        def _levels_for_log(d: Dict[str, Tuple[str, int]], reverse: bool) -> List[Tuple[str, str]]:
            it = ((p, qts[0]) for p, qts in d.items())
            key = lambda x: float(x[0])
            sel = heapq.nlargest(depth, it, key=key) if reverse else heapq.nsmallest(depth, it, key=key)
            return sel

        # Local state for the watchdog (persists across iterations)
        last_best: Dict[str, Tuple[Optional[float], Optional[str], Optional[float], Optional[str]]] = {}
        last_change_ts: Dict[str, float] = {}
        updates_mark: Dict[str, int] = {}
        last_recover_ts: Dict[str, float] = {}

        while not self._book_log_stop.wait(timeout=interval if interval > 0 else 1.0):
            try:
                with self._state_lock:
                    for prod in sorted(self._bids.keys()):
                        # Recovery status shortcut
                        if not self._sync.get(prod, True):
                            cache_len = len(self._cache.get(prod, ())) if isinstance(self._cache.get(prod, ()), deque) else 0
                            evicted = self._cache_dropped.get(prod, 0)
                            _log.info("book[%s] state=recovering cache=%d evicted=%d", prod, cache_len, evicted)
                            continue

                        bids = _levels_for_log(self._bids[prod], True)
                        asks = _levels_for_log(self._asks[prod], False)

                        bbp = float(bids[0][0]) if bids else None
                        bap = float(asks[0][0]) if asks else None
                        bbq = bids[0][1] if bids else None
                        baq = asks[0][1] if asks else None
                        spread = (bap - bbp) if (bap is not None and bbp is not None) else None

                        def fmt(levels: List[Tuple[str, str]]) -> str:
                            return ", ".join(f"{p}@{q}" for p, q in levels)

                        _log.info(
                            "book[%s] seq=%s | best_bid=%s@%s best_ask=%s@%s spread=%s | top%d bids=[%s] asks=[%s]",
                            prod,
                            self._seq_log.get(prod),
                            f"{bbp:.2f}" if bbp is not None else "n/a",
                            bbq if bbq is not None else "n/a",
                            f"{bap:.2f}" if bap is not None else "n/a",
                            baq if baq is not None else "n/a",
                            f"{spread:.2f}" if spread is not None else "n/a",
                            depth,
                            fmt(bids),
                            fmt(asks),
                        )

                        # ----- Immediate cross protection (ask < bid) -----
                        if bbp is not None and bap is not None and bap + 1e-9 < bbp:
                            tnow = time.monotonic()
                            if tnow - last_recover_ts.get(prod, 0.0) >= self._recover_cooldown_sec:
                                _log.warning("book[%s] crossed (bid=%.2f ask=%.2f) → scheduling recovery", prod, bbp, bap)
                                last_recover_ts[prod] = tnow
                                threading.Thread(target=self._recover_product, args=(prod,), daemon=True).start()

                        # ----- “Stuck but not crossed” watchdog -----
                        cur = (bbp, bbq, bap, baq)
                        tnow = time.monotonic()
                        if prod not in last_best or cur != last_best[prod]:
                            # Best-of-side changed → reset markers
                            last_best[prod] = cur
                            last_change_ts[prod] = tnow
                            updates_mark[prod] = self._updates_seen.get(prod, 0)
                        else:
                            # Unchanged best; check duration + activity
                            dt = tnow - last_change_ts.get(prod, tnow)
                            upd_since = self._updates_seen.get(prod, 0) - updates_mark.get(prod, 0)
                            if (
                                dt >= self._stuck_watch_sec and
                                upd_since >= self._stuck_updates_threshold and
                                bbp is not None and bap is not None and bap >= bbp  # not crossed
                            ):
                                if tnow - last_recover_ts.get(prod, 0.0) >= self._recover_cooldown_sec:
                                    _log.warning(
                                        "book[%s] best unchanged for %.1fs with %d updates → scheduling recovery",
                                        prod, dt, upd_since
                                    )
                                    last_recover_ts[prod] = tnow
                                    threading.Thread(target=self._recover_product, args=(prod,), daemon=True).start()

            except Exception:
                _log.exception("book logger loop error")


# --------------------------------------------------------------------------- #
# Coinbase Advanced Trade: mapping & control-plane + gap detection
# --------------------------------------------------------------------------- #
class CoinbaseOrderBookPublisher(OrderBookPublisher):
    """
    Advanced Trade Level2 publisher with connection-wide gap detection & recovery.
    """

    def __init__(
        self,
        transport: PubTransport,
        *,
        product_ids: Sequence[str],
        sandbox: bool = False,
        max_level: int = 50,
        book_log_interval_sec: float = 1.0,
        book_log_depth: int = 5,
        jwt: Optional[str] = None,
        # REST snapshot controls
        enable_rest_snapshot: bool = False,
        rest_base_url: str = _AT_REST_AT,
    ) -> None:
        ws_url = _AT_WS_SANDBOX if sandbox else _AT_WS_PROD

        super().__init__(
            ws_url=ws_url,
            transport=transport,
            initial_product_ids=tuple(product_ids),
            max_level=max_level,
            channel_name="level2",
            exchange_name="coinbase",
            book_log_interval_sec=book_log_interval_sec,
            book_log_depth=book_log_depth,
        )
        self._jwt = jwt
        self._ch_seq: Optional[int] = None  # connection-wide sequence

        # REST snapshot settings
        self._enable_rest_snapshot = bool(enable_rest_snapshot)
        self._rest_base_url = str(rest_base_url)

    @classmethod
    def from_env(
        cls,
        transport: PubTransport,
        *,
        product_ids: Sequence[str],
        sandbox: bool = False,
        max_level: int = 50,
        book_log_interval_sec: float = 1.0,
        book_log_depth: int = 5,
        prefix: str = "CB_AT_",
        enable_rest_snapshot: bool = False,
        rest_base_url: str = _AT_REST_AT,
    ) -> "CoinbaseOrderBookPublisher":
        jwt = os.getenv(f"{prefix}JWT")
        return cls(
            transport=transport,
            product_ids=product_ids,
            sandbox=sandbox,
            max_level=max_level,
            book_log_interval_sec=book_log_interval_sec,
            book_log_depth=book_log_depth,
            jwt=jwt,
            enable_rest_snapshot=enable_rest_snapshot,
            rest_base_url=rest_base_url,
        )

    # ---- control frames ----------------------------------------------------

    def _build_subscribe_frame(self, product_ids: Sequence[str]) -> dict:
        frame = {"type": "subscribe", "channel": "level2", "product_ids": list(product_ids)}
        if self._jwt:
            frame["jwt"] = self._jwt
        return frame

    def _build_unsubscribe_frame(self, product_ids: Optional[Sequence[str]]) -> dict:
        frame = {"type": "unsubscribe", "channel": "level2"}
        if product_ids:
            frame["product_ids"] = list(product_ids)
        if self._jwt:
            frame["jwt"] = self._jwt
        return frame

    # ---- optional REST snapshot (Advanced Trade preferred) -----------------

    def _fetch_rest_snapshot(self, product_id: str) -> Optional[Tuple[List[Dict[str, Any]], int]]:
        """
        Fetch an L2 snapshot via REST and normalize it.
        """
        if not self._enable_rest_snapshot:
            return None

        try:
            import requests  # type: ignore
        except Exception:
            _log.warning("requests not available; REST snapshot disabled")
            return None

        # Try Advanced Trade first if we have a JWT.
        if self._jwt:
            try:
                url = f"{self._rest_base_url}/product_book"
                headers = {"Authorization": f"Bearer {self._jwt}"}
                params = {"product_id": product_id, "limit": 1000}
                r = requests.get(url, headers=headers, params=params, timeout=2.5)
                if r.status_code == 200:
                    data = r.json() or {}
                    pb = data.get("pricebook") or {}
                    bids = pb.get("bids") or []
                    asks = pb.get("asks") or []
                    ts = pb.get("time")
                    boundary_us = self._rfc3339_to_micros(str(ts) if ts is not None else None, now_in_micros())

                    updates: List[Dict[str, Any]] = []
                    for b in bids:
                        px, sz = str(b.get("price")), str(b.get("size"))
                        if px and sz:
                            updates.append({"side": "bid", "price": px, "qty": sz, "event_us": boundary_us})
                    for a in asks:
                        px, sz = str(a.get("price")), str(a.get("size"))
                        if px and sz:
                            updates.append({"side": "ask", "price": px, "qty": sz, "event_us": boundary_us})

                    if updates:
                        return updates, boundary_us
                    _log.warning("AT snapshot empty for %s", product_id)
                else:
                    _log.warning("AT snapshot %s failed: %s %s", product_id, r.status_code, r.text[:200])
            except Exception:
                _log.exception("AT REST snapshot failed for %s", product_id)

        # Fallback: public Exchange book (best-effort; timestamps = now)
        try:
            import requests  # re-import ok
            url = f"{_AT_REST_EXCHANGE}/products/{product_id}/book"
            r = requests.get(url, params={"level": 2}, timeout=2.5)
            if r.status_code != 200:
                _log.warning("Exchange snapshot %s failed: %s %s", product_id, r.status_code, r.text[:200])
                return None
            data = r.json() or {}
        except Exception:
            _log.exception("Exchange snapshot GET failed for %s", product_id)
            return None

        bids = data.get("bids") or []
        asks = data.get("asks") or []
        now_us = now_in_micros()

        updates: List[Dict[str, Any]] = []
        for row in bids:
            px, sz = (row[0], row[1]) if isinstance(row, list) and len(row) >= 2 else (None, None)
            if px is not None and sz is not None:
                updates.append({"side": "bid", "price": str(px), "qty": str(sz), "event_us": now_us})
        for row in asks:
            px, sz = (row[0], row[1]) if isinstance(row, list) and len(row) >= 2 else (None, None)
            if px is not None and sz is not None:
                updates.append({"side": "ask", "price": str(px), "qty": str(sz), "event_us": now_us})

        if not updates:
            return None
        return updates, now_us

    # ---- WS callbacks ------------------------------------------------------

    def on_open(self, ws) -> None:
        # Reset channel-sequence baseline per connection
        self._ch_seq = None
        super().on_open(ws)

    def on_close(self, ws, code, msg) -> None:
        # Ensure seq state doesn't leak across sessions
        self._ch_seq = None
        super().on_close(ws, code, msg)

    def on_message(self, ws, message) -> None:
        """
        Parse Coinbase L2 frames, normalize updates, detect seq gaps, and ingest.
        """
        t_recv = now_in_micros()

        if isinstance(message, bytes):
            message = message.decode(errors="replace")
        if not isinstance(message, str):
            return

        try:
            m: Dict[str, Any] = json_parser.loads(message)  # type: ignore[assignment]
        except Exception:
            _log.exception("AT decode: JSON parse failure")
            return

        # Connection-wide sequence tracking + gap detection.
        seq = None
        if "sequence_num" in m:
            try:
                seq_raw = int(m["sequence_num"])
                # Defensively ignore non-positive seq values
                seq = seq_raw if seq_raw > 0 else None
            except Exception:
                seq = None

        if seq is not None:
            last = self._ch_seq
            self._ch_seq = seq
            if last is not None and seq != last + 1:
                with self._subs_lock:
                    affected = sorted(self._subs)
                if affected:
                    _log.warning(
                        "sequence gap detected: last=%s, got=%s → recover %d products",
                        last, seq, len(affected),
                    )
                    self._mark_out_of_sync(affected)
                    for p in affected:
                        threading.Thread(
                            target=self._recover_product,
                            args=(p, ),
                            name=f"{self.__class__.__name__}-recover-{p}",
                            daemon=True,
                        ).start()

        # Process L2 frames only.
        if m.get("channel") == "l2_data" and "events" in m:
            t_exch_us = self._rfc3339_to_micros(m.get("timestamp"), t_recv)

            for ev in m.get("events", []):
                etype = ev.get("type")  # "snapshot" | "update"
                product = ev.get("product_id") or "unknown"
                raw_updates = ev.get("updates") or []

                updates: List[Dict[str, Any]] = []
                for u in raw_updates:
                    raw_side = str(u.get("side", "")).lower()
                    if raw_side in ("bid", "buy"):
                        side = "bid"
                    elif raw_side in ("offer", "ask", "sell"):
                        side = "ask"
                    else:
                        continue

                    price = u.get("price_level") or u.get("price")
                    qty = u.get("new_quantity") or u.get("size") or u.get("quantity") or "0"
                    ev_ts = u.get("event_time") or u.get("time")
                    if price is None:
                        continue

                    updates.append(
                        {
                            "side": side,
                            "price": str(price),
                            "qty": str(qty),
                            "event_us": self._rfc3339_to_micros(
                                str(ev_ts) if ev_ts is not None else None, t_recv
                            ),
                        }
                    )

                if updates or etype == "snapshot":
                    self.ingest_event(
                        product_id=product,
                        etype=etype,
                        updates=updates,
                        seq=seq,
                        t_exch_us=t_exch_us,
                        t_recv_us=t_recv,
                    )

    # ---- utilities ---------------------------------------------------------

    @staticmethod
    def _rfc3339_to_micros(ts: str | None, default: int) -> int:
        if not ts:
            return default
        try:
            ts = ts.replace("Z", "+00:00")
            import datetime as _dt
            dt = _dt.datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_dt.timezone.utc)
            return int(dt.timestamp() * 1_000_000)
        except Exception:
            return default
