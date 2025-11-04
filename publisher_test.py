import asyncio
import json
import time
from collections import defaultdict
from pathlib import Path

import websockets
import aiohttp

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = DATA_DIR / "orderbook_stream.jsonl"


# ================= 공통 오더북 =================
class LocalOrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids = defaultdict(float)
        self.asks = defaultdict(float)
        self.last_update_id = 0

    def upsert_side(self, side_dict, updates):
        for price, qty in updates:
            p, q = float(price), float(qty)
            if q == 0:
                side_dict.pop(p, None)
            else:
                side_dict[p] = q

    def snapshot(self, top_n=20):
        return {
            "symbol": self.symbol,
            "ts": time.time(),
            "bids": sorted(self.bids.items(), key=lambda x: -x[0])[:top_n],
            "asks": sorted(self.asks.items(), key=lambda x: x[0])[:top_n],
            "last_update_id": self.last_update_id,
        }


# ================= 베이스 =================
class BaseStreamer:
    def __init__(self, exchange: str, symbol_map: dict, out_q: asyncio.Queue):
        # symbol_map: 우리심볼 -> 거래소심볼
        self.exchange = exchange
        self.symbol_map = symbol_map
        self.out_q = out_q
        self.books = {sym: LocalOrderBook(sym) for sym in symbol_map.keys()}

    async def run(self):
        raise NotImplementedError

    async def publish(self, *, symbol, event_ts, seq, depth, raw):
        ev = {
            "exchange": self.exchange,
            "symbol": symbol,
            "event_ts": event_ts,
            "recv_ts": time.time(),
            "seq": seq,
            "depth": depth,
            "raw": raw,
        }
        await self.out_q.put(ev)


# ================= 1) BINANCE =================
class BinanceStreamer(BaseStreamer):
    BASE = "wss://fstream.binance.com/stream?streams="

    async def run(self):
        tasks = []
        for unified_sym, venue_sym in self.symbol_map.items():
            tasks.append(asyncio.create_task(self._one(unified_sym, venue_sym)))
        await asyncio.gather(*tasks)

    async def _one(self, unified_sym: str, venue_sym: str):
        url = f"{self.BASE}{venue_sym.lower()}@depth@100ms"
        lob = self.books[unified_sym]
        while True:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    print(f"[binance:{unified_sym}] connected")
                    async for msg in ws:
                        data = json.loads(msg)["data"]
                        lob.upsert_side(lob.bids, data["b"])
                        lob.upsert_side(lob.asks, data["a"])
                        lob.last_update_id = data["u"]

                        await self.publish(
                            symbol=unified_sym,
                            event_ts=data["E"] / 1000.0,
                            seq=data["u"],
                            depth=lob.snapshot(20),
                            raw=data,
                        )
            except Exception as e:
                print(f"[binance:{unified_sym}] error {e} → reconnect in 5s")
                await asyncio.sleep(5)


# ================= 2) OKX =================
# ================= 2) OKX =================
class OKXStreamer(BaseStreamer):
    WS = "wss://ws.okx.com:8443/ws/v5/public"

    async def run(self):
        while True:
            try:
                async with websockets.connect(self.WS, ping_interval=20) as ws:
                    sub_msg = {
                        "op": "subscribe",
                        "args": [
                            {"channel": "books5", "instId": venue}
                            for _, venue in self.symbol_map.items()
                        ],
                    }
                    await ws.send(json.dumps(sub_msg))
                    print("[okx] subscribed", sub_msg)

                    async for raw in ws:
                        msg = json.loads(raw)

                        # 1) subscribe 성공/에러 메시지는 data 가 없음 → 패스
                        if "event" in msg:
                            continue

                        # 2) 우리가 구독한 channel 이 아니면 패스
                        if msg.get("arg", {}).get("channel") != "books5":
                            continue

                        # 3) 진짜 orderbook 데이터인지 체크
                        data_list = msg.get("data")
                        if not data_list:
                            continue

                        book_data = data_list[0]

                        # 여기서 venue → 우리 심볼로 매핑
                        venue = msg["arg"]["instId"]       # 예: "BTC-USDT"
                        unified = self._find_unified(venue)
                        if not unified:
                            # 우리가 모르는 심볼이면 버림
                            continue

                        lob = self.books[unified]

                        # OKX books5 는 5레벨 전체를 주니까 덮어쓰기 해도 됨
                        lob.bids = {float(p): float(sz) for p, sz, *_ in book_data["bids"]}
                        lob.asks = {float(p): float(sz) for p, sz, *_ in book_data["asks"]}
                        lob.last_update_id = int(book_data.get("seq", 0))

                        await self.publish(
                            symbol=unified,
                            event_ts=float(book_data["ts"]) / 1000.0,
                            seq=int(book_data.get("seq", 0)),
                            depth=lob.snapshot(20),
                            raw=msg,
                        )

            except Exception as e:
                print(f"[okx] error {e} → reconnect in 5s")
                await asyncio.sleep(5)

    # 
    def _find_unified(self, venue: str):
        for unified, v in self.symbol_map.items():
            if v == venue:
                return unified
        return None


# ================= 3) BYBIT =================
class BybitStreamer(BaseStreamer):
    # 네가 잘 됐다고 한 버전과 똑같이 spot + REST 스냅샷 먼저 쓴다
    # 참고: :contentReference[oaicite:1]{index=1}
    WS = "wss://stream.bybit.com/v5/public/spot"
    REST = "https://api.bybit.com/v5/market/orderbook"

    async def run(self):
        # 심볼 여러 개면 한 커넥션으로도 할 수 있지만,
        # 여기서는 각 심볼을 독립 태스크로 돌려서 디버깅하기 쉽게 하자
        tasks = []
        for unified_sym, venue_sym in self.symbol_map.items():
            tasks.append(asyncio.create_task(self._one(unified_sym, venue_sym)))
        await asyncio.gather(*tasks)

    async def _one(self, unified_sym: str, venue_sym: str):
        lob = self.books[unified_sym]

        while True:
            try:
                # 1) REST 스냅샷 먼저
                async with aiohttp.ClientSession() as session:
                    params = {"category": "spot", "symbol": venue_sym, "limit": "200"}
                    async with session.get(self.REST, params=params) as resp:
                        snap = await resp.json()
                        if snap.get("retCode") != 0:
                            print(f"[bybit:{unified_sym}] REST error {snap}")
                        else:
                            result = snap.get("result", {})
                            lob.bids = {float(p): float(q) for p, q in result.get("b", [])}
                            lob.asks = {float(p): float(q) for p, q in result.get("a", [])}
                            lob.last_update_id = int(result.get("ts", int(time.time()*1000)))
                            # 스냅샷도 MQ로 한 번 보내줄 수 있음
                            await self.publish(
                                symbol=unified_sym,
                                event_ts=lob.last_update_id / 1000.0,
                                seq=lob.last_update_id,
                                depth=lob.snapshot(20),
                                raw=snap,
                            )
                            print(f"[bybit:{unified_sym}] snapshot loaded")

                # 2) WS 연결
                async with websockets.connect(self.WS, ping_interval=20) as ws:
                    sub_msg = {
                        "op": "subscribe",
                        "args": [f"orderbook.50.{venue_sym}"],
                    }
                    await ws.send(json.dumps(sub_msg))
                    print(f"[bybit:{unified_sym}] subscribed {sub_msg}")

                    async for raw in ws:
                        msg = json.loads(raw)

                        # ping/pong, subscribe ok 등은 스킵
                        if msg.get("op") in ("ping", "pong"):
                            continue
                        if msg.get("success") and msg.get("op") == "subscribe":
                            continue
                        if "topic" not in msg:
                            continue

                        topic = msg["topic"]
                        if not topic.startswith("orderbook."):
                            continue

                        data = msg.get("data", {})
                        # bybit는 b/a 가 델타로 옴
                        lob.bids = {float(p): float(q) for p, q in data.get("b", [])}
                        lob.asks = {float(p): float(q) for p, q in data.get("a", [])}
                        ts = float(data.get("t", time.time() * 1000))
                        lob.last_update_id = int(data.get("u", ts))

                        await self.publish(
                            symbol=unified_sym,
                            event_ts=ts / 1000.0,
                            seq=lob.last_update_id,
                            depth=lob.snapshot(20),
                            raw=msg,
                        )

            except Exception as e:
                print(f"[bybit:{unified_sym}] error {e} → reconnect in 5s")
                await asyncio.sleep(5)


# ================= fan-out & consumers =================
async def broadcaster(src_q: asyncio.Queue,
                      realtime_q: asyncio.Queue,
                      store_q: asyncio.Queue):
    while True:
        ev = await src_q.get()
        await realtime_q.put(ev)
        await store_q.put(ev)
        src_q.task_done()


async def realtime_consumer(q: asyncio.Queue):
    last_seen = {}
    while True:
        ev = await q.get()
        ex = ev["exchange"]
        sym = ev["symbol"]

        # BINANCE 너무 많으니까 안 찍음
        if ex == "binance" or ex == "okx":
            q.task_done()
            continue

        now = ev["recv_ts"]
        prev = last_seen.get((ex, sym), now)
        dt = now - prev
        last_seen[(ex, sym)] = now

        depth = ev["depth"]
        bid = depth["bids"][0][0] if depth["bids"] else None
        ask = depth["asks"][0][0] if depth["asks"] else None

        print(f"[{ex.upper()}:{sym}] Δt={dt:.3f}s bid={bid} ask={ask}")
        q.task_done()


async def store_consumer(q: asyncio.Queue):
    with OUTPUT_FILE.open("a", encoding="utf-8") as f:
        while True:
            ev = await q.get()
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
            f.flush()
            q.task_done()

async def batcher(src_q: asyncio.Queue,
                  dst_q: asyncio.Queue,
                  batch_size: int = 100,
                  max_wait: float = 0.1):
    """
    src_q 에서는 이벤트가 1개씩 오고,
    dst_q 로는 이벤트를 [ ... ] 리스트로 묶어서 보낸다.
    - batch_size: 몇 개 모이면 바로 보낼지
    - max_wait: 이 시간 안에 batch_size 가 안 찼어도 일단 보냄 (초 단위)
    """
    buf = []
    last_flush = time.time()

    while True:
        try:
            ev = await asyncio.wait_for(src_q.get(), timeout=max_wait)
            buf.append(ev)
            src_q.task_done()
        except asyncio.TimeoutError:
            # 일정 시간 동안 아무것도 안 왔으면 여기로 옴
            pass

        now = time.time()
        should_flush = False

        if len(buf) >= batch_size:
            should_flush = True
        elif buf and (now - last_flush) >= max_wait:
            should_flush = True

        if should_flush:
            await dst_q.put(buf)
            buf = []
            last_flush = now


# ================= main =================
async def main():
    central_q = asyncio.Queue()
    realtime_q = asyncio.Queue()
    store_q = asyncio.Queue()

    binance_map = {
        "BTC-USD": "btcusdt",
        "ETH-USD": "ethusdt",
        "SOL-USD": "solusdt",
    }
    okx_map = {
        "BTC-USD": "BTC-USDT",
        "ETH-USD": "ETH-USDT",
        "SOL-USD": "SOL-USDT",
    }
    bybit_map = {
        "BTC-USD": "BTCUSDT",
        "ETH-USD": "ETHUSDT",
        "SOL-USD": "SOLUSDT",
    }

    binance = BinanceStreamer("binance", binance_map, central_q)
    okx = OKXStreamer("okx", okx_map, central_q)
    bybit = BybitStreamer("bybit", bybit_map, central_q)

    tasks = [
        asyncio.create_task(binance.run()),
        asyncio.create_task(okx.run()),
        asyncio.create_task(bybit.run()),
        asyncio.create_task(broadcaster(central_q, realtime_q, store_q)),
        asyncio.create_task(realtime_consumer(realtime_q)),
        asyncio.create_task(store_consumer(store_q)),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # 여기서 끊으면 윈도우에서 약간의 warning은 나올 수 있음
        print("stopped by user")
