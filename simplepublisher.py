import asyncio
import json
import time
from collections import defaultdict
from pathlib import Path
import aiohttp
import websockets

BINANCE_WS_BASE = "wss://stream.binance.us:9443/ws"
BINANCE_REST_BASE = "https://www.binance.us/api/v1/depth"
SAVE_DIR = Path("orderbooks")
SAVE_DIR.mkdir(exist_ok=True)

# You can list as many pairs as you want here:
SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD"]
SAVE_INTERVAL_SEC = 30


# ====================================================================== #
#                           LocalOrderBook Class                         #
# ====================================================================== #

class LocalOrderBook:
    """Maintains a local copy of Binance US order book."""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids = defaultdict(float)
        self.asks = defaultdict(float)
        self.last_update_id = None
        self.snapshot_loaded = asyncio.Event()
        self.snapshot_timestamp = None

    def apply_snapshot(self, snapshot):
        self.last_update_id = snapshot["lastUpdateId"]
        self.snapshot_timestamp = time.time()
        self.bids = {float(p): float(q) for p, q in snapshot["bids"]}
        self.asks = {float(p): float(q) for p, q in snapshot["asks"]}
        self.snapshot_loaded.set()

    def apply_update(self, update):
        """Apply incremental depth updates."""
        for side, updates in [("bids", update["b"]), ("asks", update["a"])]:
            book = self.bids if side == "bids" else self.asks
            for price, qty in updates:
                p, q = float(price), float(qty)
                if q == 0:
                    book.pop(p, None)
                else:
                    book[p] = q
        self.last_update_id = update["u"]

    def to_dict(self):
        """Return a lightweight snapshot suitable for JSON serialization."""
        return {
            "symbol": self.symbol,
            "timestamp": time.time(),
            "lastUpdateId": self.last_update_id,
            "bids": sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:20],
            "asks": sorted(self.asks.items(), key=lambda x: x[0])[:20],
        }

    def save_to_disk(self):
        path = SAVE_DIR / f"{self.symbol.lower()}_orderbook.json"
        path.write_text(json.dumps(self.to_dict(), indent=2))
        print(f"[{time.strftime('%X')}] Saved {self.symbol} → {path}")


# ====================================================================== #
#                          Core Async Routines                           #
# ====================================================================== #

async def fetch_snapshot(session: aiohttp.ClientSession, symbol: str):
    """Fetch REST snapshot."""
    url = f"{BINANCE_REST_BASE}?symbol={symbol}&limit=1000"
    async with session.get(url) as resp:
        return await resp.json()


async def stream_order_book(symbol: str):
    """Maintain and persist a single symbol’s local order book."""
    order_book = LocalOrderBook(symbol)
    ws_url = f"{BINANCE_WS_BASE}/{symbol.lower()}@depth"

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                print(f"[{symbol}] Fetching initial snapshot…")
                snapshot = await fetch_snapshot(session, symbol)
                order_book.apply_snapshot(snapshot)
                print(f"[{symbol}] Snapshot lastUpdateId = {order_book.last_update_id}")

                buffer = []

                async with websockets.connect(ws_url) as ws:
                    # Buffer incoming events until snapshot ready
                    async for msg in ws:
                        event = json.loads(msg)
                        buffer.append(event)
                        if event["u"] >= order_book.last_update_id:
                            break  # Enough buffered data

                    # Apply relevant buffered events
                    for event in buffer:
                        if event["u"] <= order_book.last_update_id:
                            continue
                        if event["U"] <= order_book.last_update_id + 1 <= event["u"]:
                            order_book.apply_update(event)
                    buffer.clear()
                    print(f"[{symbol}] Reconciled and ready for live updates.")

                    # Start saver coroutine
                    asyncio.create_task(periodic_saver(order_book))

                    # Apply live stream updates
                    async for msg in ws:
                        update = json.loads(msg)
                        if update["U"] == order_book.last_update_id + 1:
                            order_book.apply_update(update)
                        elif update["u"] <= order_book.last_update_id:
                            continue
                        else:
                            print(f"[{symbol}] Desync detected → restarting…")
                            break

        except Exception as e:
            print(f"[{symbol}] Error: {e} → retrying in 5s…")
            await asyncio.sleep(5)


async def periodic_saver(order_book: LocalOrderBook):
    """Periodically save each symbol’s order book to disk."""
    while True:
        await asyncio.sleep(SAVE_INTERVAL_SEC)
        order_book.save_to_disk()


# ====================================================================== #
#                              Entrypoint                                #
# ====================================================================== #

async def main():
    tasks = [stream_order_book(symbol) for symbol in SYMBOLS]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped manually.")
