
"""GexFlow market gateway: cTrader XAUUSD + Binance/Bybit BTCUSDT.

Render Build Command:
pip install aiohttp ctrader-open-api service_identity

Render Start Command:
python server.py

Credentials belong in Render Environment Variables, never in source code.
"""

import asyncio
import json
import os
import threading
import time

from aiohttp import web, ClientSession, WSMsgType

ROOT = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.getenv("PORT", "10000"))

CLIENT_ID = os.getenv("CTRADER_CLIENT_ID", "43370_XnVtFKCM1AkBJKWzgwDJQvqM1MBViHWo076zWdjfCf8Je1fsgE").strip()
CLIENT_SECRET = os.getenv("CTRADER_CLIENT_SECRET", "eEgvRhPB8FZGzj7TML0wSYpCcsflXxp0apMpi9JbjFKACLIzBC").strip()
ENV = "live" if os.getenv("CTRADER_ENV", "demo").strip().lower() == "live" else "demo"
SYMBOL = os.getenv("CTRADER_SYMBOL", "XAUUSD").strip() or "XAUUSD"

ACCESS_TOKEN = os.getenv("CTRADER_ACCESS_TOKEN", "O5H5KPy7xOoExTyWhoE4HxaEDiyzxqhoAmuBdsm_Gfs").strip()
REFRESH_TOKEN = os.getenv("CTRADER_REFRESH_TOKEN", "91hVNcjJxMr1vPtNt1CA51TpVuBHlTkSdbneD35SnuY").strip()
ACCOUNT_ID = os.getenv("CTRADER_ACCOUNT_ID", "10143677").strip()

try:
    TOKEN_EXPIRES_AT = int(os.getenv("CTRADER_ACCESS_TOKEN_EXPIRES_AT", "0") or "0")
except ValueError:
    TOKEN_EXPIRES_AT = 0

BOOT = time.time()
LOOP = None
CLIENTS = set()
CTRADE_CLIENT = None
CTRADE_THREAD = None

FEEDS = {
    "ctrader": {
        "provider": "cTrader",
        "symbol": SYMBOL,
        "status": "WAITING",
        "price": None,
        "bid": None,
        "ask": None,
        "timestamp": None,
        "error": None,
    },
    "binance": {
        "provider": "Binance USD-M Futures",
        "symbol": "BTCUSDT",
        "status": "CONNECTING",
        "price": None,
        "bid": None,
        "ask": None,
        "timestamp": None,
        "error": None,
    },
    "bybit": {
        "provider": "Bybit Linear Perpetual",
        "symbol": "BTCUSDT",
        "status": "CONNECTING",
        "price": None,
        "bid": None,
        "ask": None,
        "timestamp": None,
        "error": None,
    },
}


def now_ms():
    return int(time.time() * 1000)


async def broadcast(payload):
    dead = []
    for ws in tuple(CLIENTS):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        CLIENTS.discard(ws)


def set_feed(name, **kwargs):
    FEEDS[name].update(kwargs)
    FEEDS[name]["updatedAt"] = now_ms()
    if LOOP and LOOP.is_running():
        try:
            asyncio.run_coroutine_threadsafe(
                broadcast({"type": "feed", "feed": name, "data": FEEDS[name]}),
                LOOP,
            )
        except Exception:
            pass


def public_state():
    return {
        "ok": True,
        "service": "GexFlow Python market gateway",
        "uptimeSeconds": int(time.time() - BOOT),
        "feeds": FEEDS,
        "ctraderConfigured": bool(CLIENT_ID and CLIENT_SECRET and ACCESS_TOKEN),
        "ctraderEnvironment": ENV,
        "note": "Prices are live only when feed status is LIVE.",
    }


async def home(req):
    path = os.path.join(ROOT, "index.html")
    if not os.path.isfile(path):
        return web.Response(text="index.html not found beside server.py", status=404)
    return web.FileResponse(path)


async def health(req):
    return web.json_response({
        "ok": True,
        "backend": "python",
        "feeds": {k: v["status"] for k, v in FEEDS.items()},
        "uptimeSeconds": int(time.time() - BOOT),
    })


async def api_markets(req):
    return web.json_response(public_state())


async def api_quote(req):
    name = req.query.get("source", "ctrader").lower()
    if name not in FEEDS:
        return web.json_response(
            {"ok": False, "error": "source must be ctrader, binance, or bybit"},
            status=400,
        )
    quote = FEEDS[name]
    return web.json_response(quote, status=200 if quote["status"] == "LIVE" else 503)


async def ws_handler(req):
    ws = web.WebSocketResponse(heartbeat=25, max_msg_size=32768)
    await ws.prepare(req)
    CLIENTS.add(ws)
    await ws.send_json({"type": "snapshot", "state": public_state()})

    async for msg in ws:
        if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
            break

    CLIENTS.discard(ws)
    return ws


async def refresh_tokens(session, force=False):
    """Refresh existing credentials when needed; do not log token values."""
    global ACCESS_TOKEN, REFRESH_TOKEN, TOKEN_EXPIRES_AT

    expiring = TOKEN_EXPIRES_AT > 0 and TOKEN_EXPIRES_AT <= int(time.time()) + 120
    if not REFRESH_TOKEN:
        return bool(ACCESS_TOKEN)

    if not (force or not ACCESS_TOKEN or expiring):
        return bool(ACCESS_TOKEN)

    params = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    async with session.get(
        "https://openapi.ctrader.com/apps/token",
        params=params,
        timeout=20,
    ) as response:
        data = await response.json(content_type=None)
        if response.status >= 400 or not data.get("accessToken"):
            reason = data.get("description") or data.get("errorCode") or f"Token refresh HTTP {response.status}"
            raise RuntimeError(str(reason)[:180])

    ACCESS_TOKEN = data["accessToken"]
    REFRESH_TOKEN = data.get("refreshToken") or REFRESH_TOKEN
    TOKEN_EXPIRES_AT = int(time.time()) + int(data.get("expiresIn", 0))

    # A rotated refresh token is retained in memory only.
    # Update Render's CTRADER_REFRESH_TOKEN if cTrader rotates it.
    print("[cTrader] Token refresh succeeded.", flush=True)
    return True


async def binance_feed(app):
    url = "wss://fstream.binance.com/ws/btcusdt@bookTicker"
    while True:
        try:
            set_feed("binance", status="CONNECTING", error=None)
            async with app["session"].ws_connect(url, heartbeat=25, timeout=20) as ws:
                async for message in ws:
                    if message.type == WSMsgType.TEXT:
                        data = json.loads(message.data)
                        bid, ask = float(data["b"]), float(data["a"])
                        if bid > 0 and ask >= bid:
                            set_feed(
                                "binance", status="LIVE", bid=bid, ask=ask,
                                price=(bid + ask) / 2, timestamp=now_ms(), error=None,
                            )
                    elif message.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            set_feed("binance", status="RECONNECTING", error=str(exc)[:180])
        await asyncio.sleep(3)


async def bybit_feed(app):
    url = "wss://stream.bybit.com/v5/public/linear"
    while True:
        try:
            set_feed("bybit", status="CONNECTING", error=None)
            async with app["session"].ws_connect(url, heartbeat=20, timeout=20) as ws:
                await ws.send_json({"op": "subscribe", "args": ["orderbook.1.BTCUSDT"]})
                async for message in ws:
                    if message.type == WSMsgType.TEXT:
                        data = json.loads(message.data)
                        if data.get("topic", "").startswith("orderbook.1.") and data.get("data"):
                            book = data["data"]
                            bids, asks = book.get("b", []), book.get("a", [])
                            if bids and asks:
                                bid, ask = float(bids[0][0]), float(asks[0][0])
                                if bid > 0 and ask >= bid:
                                    set_feed(
                                        "bybit", status="LIVE", bid=bid, ask=ask,
                                        price=(bid + ask) / 2, timestamp=now_ms(), error=None,
                                    )
                    elif message.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            set_feed("bybit", status="RECONNECTING", error=str(exc)[:180])
        await asyncio.sleep(3)


def start_ctrader_worker():
    global CTRADE_CLIENT, CTRADE_THREAD

    if CTRADE_THREAD and CTRADE_THREAD.is_alive():
        return

    if not (CLIENT_ID and CLIENT_SECRET and ACCESS_TOKEN):
        set_feed(
            "ctrader", status="CONFIG_REQUIRED",
            error="Set CTRADER_CLIENT_ID, CTRADER_CLIENT_SECRET and CTRADER_ACCESS_TOKEN in Render.",
        )
        return

    def runner():
        global CTRADE_CLIENT
        try:
            from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
            from ctrader_open_api.messages.OpenApiMessages_pb2 import (
                ProtoOAApplicationAuthReq,
                ProtoOAApplicationAuthRes,
                ProtoOAGetAccountListByAccessTokenReq,
                ProtoOAGetAccountListByAccessTokenRes,
                ProtoOAAccountAuthReq,
                ProtoOAAccountAuthRes,
                ProtoOASymbolsListReq,
                ProtoOASymbolsListRes,
                ProtoOASubscribeSpotsReq,
                ProtoOASubscribeSpotsRes,
                ProtoOASpotEvent,
                ProtoOAErrorRes,
            )
            from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoHeartbeatEvent
            from twisted.internet import reactor

            host = EndPoints.PROTOBUF_LIVE_HOST if ENV == "live" else EndPoints.PROTOBUF_DEMO_HOST
            client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
            CTRADE_CLIENT = client
            state = {"account_id": None, "symbol_id": None, "digits": 2}

            def send(request):
                name = request.__class__.__name__
                deferred = client.send(request, responseTimeoutInSeconds=15)

                def failed(failure):
                    msg = failure.getErrorMessage() if hasattr(failure, "getErrorMessage") else str(failure)
                    set_feed("ctrader", status="ERROR", error=f"{name}: {msg}"[:220])
                    print(f"[cTrader] {name} failed: {msg[:180]}", flush=True)
                    return None

                deferred.addErrback(failed)
                return deferred

            def heartbeat():
                if getattr(reactor, "running", False):
                    try:
                        client.send(ProtoHeartbeatEvent())
                    except Exception:
                        pass
                    reactor.callLater(10, heartbeat)

            def connected(_client):
                set_feed("ctrader", status="CONNECTED", error=None)
                req = ProtoOAApplicationAuthReq()
                req.clientId = CLIENT_ID
                req.clientSecret = CLIENT_SECRET
                send(req)
                reactor.callLater(10, heartbeat)

            def disconnected(_client, reason):
                set_feed("ctrader", status="RECONNECTING", error=("Disconnected: " + str(reason))[:180])

            def received(_client, message):
                try:
                    payload = message.payloadType

                    if payload == ProtoOAApplicationAuthRes().payloadType:
                        set_feed("ctrader", status="APP_AUTHENTICATED", error=None)
                        req = ProtoOAGetAccountListByAccessTokenReq()
                        req.accessToken = ACCESS_TOKEN
                        send(req)

                    elif payload == ProtoOAGetAccountListByAccessTokenRes().payloadType:
                        response = Protobuf.extract(message)
                        accounts = list(response.ctidTraderAccount)
                        if not accounts:
                            set_feed(
                                "ctrader", status="NO_AUTHORIZED_ACCOUNTS",
                                error="This access token returned no authorized accounts. Check token and demo/live environment.",
                            )
                            return

                        ids = [int(a.ctidTraderAccountId) for a in accounts]
                        configured = int(ACCOUNT_ID) if ACCOUNT_ID.isdigit() else None

                        if configured is not None:
                            if configured not in ids:
                                set_feed(
                                    "ctrader", status="ACCOUNT_ID_MISMATCH",
                                    error="CTRADER_ACCOUNT_ID is not in the accounts authorized by this token: " + ", ".join(map(str, ids[:15])),
                                )
                                return
                            selected = configured
                        elif len(ids) == 1:
                            selected = ids[0]
                        else:
                            set_feed(
                                "ctrader", status="ACCOUNT_SELECTION_REQUIRED",
                                error="Set CTRADER_ACCOUNT_ID to one of these authorized numeric IDs: " + ", ".join(map(str, ids[:15])),
                            )
                            return

                        state["account_id"] = selected
                        req = ProtoOAAccountAuthReq()
                        req.ctidTraderAccountId = selected
                        req.accessToken = ACCESS_TOKEN
                        send(req)

                    elif payload == ProtoOAAccountAuthRes().payloadType:
                        response = Protobuf.extract(message)
                        confirmed = int(getattr(response, "ctidTraderAccountId", state["account_id"] or 0))
                        state["account_id"] = confirmed
                        set_feed("ctrader", status="ACCOUNT_AUTHENTICATED", error=None)
                        req = ProtoOASymbolsListReq()
                        req.ctidTraderAccountId = confirmed
                        send(req)

                    elif payload == ProtoOASymbolsListRes().payloadType:
                        response = Protobuf.extract(message)
                        wanted = SYMBOL.upper().replace("/", "").replace(".", "").replace("_", "")
                        found = None

                        for item in response.symbol:
                            name = getattr(item, "symbolName", "").upper().replace("/", "").replace(".", "").replace("_", "")
                            if name == wanted or (wanted in ("XAUUSD", "GOLD") and name in ("XAUUSD", "GOLD")):
                                found = item
                                break

                        if not found:
                            names = [
                                getattr(item, "symbolName", "")
                                for item in response.symbol
                                if any(x in getattr(item, "symbolName", "").upper() for x in ("XAU", "GOLD"))
                            ]
                            set_feed(
                                "ctrader", status="SYMBOL_NOT_FOUND",
                                error="Gold symbol not found. Available matches: " + (", ".join(names[:15]) or "none"),
                            )
                            return

                        state["symbol_id"] = int(found.symbolId)
                        try:
                            state["digits"] = int(found.digits)
                        except Exception:
                            state["digits"] = 2

                        req = ProtoOASubscribeSpotsReq()
                        req.ctidTraderAccountId = state["account_id"]
                        req.symbolId.append(state["symbol_id"])
                        req.subscribeToSpotTimestamp = True
                        send(req)
                        set_feed("ctrader", status="SUBSCRIBING", error=None)

                    elif payload == ProtoOASubscribeSpotsRes().payloadType:
                        set_feed("ctrader", status="LIVE_WAITING_FOR_TICK", error=None)

                    elif payload == ProtoOAErrorRes().payloadType:
                        err = Protobuf.extract(message)
                        code = getattr(err, "errorCode", "")
                        desc = getattr(err, "description", "cTrader API error")
                        set_feed("ctrader", status="ERROR", error=f"{code}: {desc}".strip(": ")[:220])

                    elif payload == ProtoOASpotEvent().payloadType:
                        event = Protobuf.extract(message)
                        if state["symbol_id"] and int(event.symbolId) == state["symbol_id"]:
                            # cTrader spot prices are scaled by 100000.
                            bid = float(event.bid) / 100000.0 if event.HasField("bid") else None
                            ask = float(event.ask) / 100000.0 if event.HasField("ask") else None

                            old = FEEDS["ctrader"]
                            bid = bid if bid and bid > 0 else old.get("bid")
                            ask = ask if ask and ask > 0 else old.get("ask")

                            if bid and ask and bid > 0 and ask >= bid:
                                set_feed(
                                    "ctrader", status="LIVE", bid=bid, ask=ask,
                                    price=(bid + ask) / 2, timestamp=now_ms(), error=None,
                                )
                except Exception as exc:
                    set_feed("ctrader", status="ERROR", error=("Message handling: " + str(exc))[:220])

            client.setConnectedCallback(connected)
            client.setDisconnectedCallback(disconnected)
            client.setMessageReceivedCallback(received)
            client.startService()
            reactor.run(installSignalHandlers=False)

        except Exception as exc:
            set_feed("ctrader", status="ERROR", error=("cTrader worker: " + str(exc))[:220])
            print("[cTrader] Worker error: " + str(exc)[:180], flush=True)

    CTRADE_THREAD = threading.Thread(target=runner, name="ctrader-openapi", daemon=True)
    CTRADE_THREAD.start()


async def startup(app):
    global LOOP
    LOOP = asyncio.get_running_loop()
    app["session"] = ClientSession()
    app["tasks"] = [
        asyncio.create_task(binance_feed(app)),
        asyncio.create_task(bybit_feed(app)),
    ]

    if not (CLIENT_ID and CLIENT_SECRET):
        set_feed("ctrader", status="CONFIG_REQUIRED", error="Set CTRADER_CLIENT_ID and CTRADER_CLIENT_SECRET.")
    elif not ACCESS_TOKEN and not REFRESH_TOKEN:
        set_feed("ctrader", status="TOKEN_REQUIRED", error="Set CTRADER_ACCESS_TOKEN or CTRADER_REFRESH_TOKEN in Render.")
    else:
        if REFRESH_TOKEN:
            try:
                await refresh_tokens(app["session"])
            except Exception as exc:
                set_feed("ctrader", status="TOKEN_REFRESH_FAILED", error=str(exc)[:200])

        if ACCESS_TOKEN:
            start_ctrader_worker()
        else:
            set_feed("ctrader", status="TOKEN_REQUIRED", error="No usable access token. Check the Render token environment variables.")

    print("GexFlow started. No credentials are logged.", flush=True)


async def cleanup(app):
    for task in app.get("tasks", []):
        task.cancel()
    if app.get("tasks"):
        await asyncio.gather(*app["tasks"], return_exceptions=True)
    await app["session"].close()


app = web.Application(client_max_size=16384)
app.router.add_get("/", home)
app.router.add_get("/health", health)
app.router.add_get("/api/markets", api_markets)
app.router.add_get("/api/quote", api_quote)
app.router.add_get("/ws", ws_handler)
app.on_startup.append(startup)
app.on_cleanup.append(cleanup)

web.run_app(app, host="0.0.0.0", port=PORT)
