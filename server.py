"""GexFlow single-file Python backend: cTrader XAUUSD + Binance/Bybit BTCUSDT.
Render Build Command: pip install aiohttp ctrader-open-api
Render Start Command: python server.py
Keep credentials in Render Environment Variables; never hard-code tokens in source.
"""
import asyncio
import json
import os
import threading
import time
from urllib.parse import urlencode

from aiohttp import web, ClientSession, WSMsgType

ROOT = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.getenv("PORT", "10000"))
CLIENT_ID = os.getenv("CTRADER_CLIENT_ID", "43370_XnVtFKCM1AkBJKWzgwDJQvqM1MBViHWo076zWdjfCf8Je1fsgE").strip()
CLIENT_SECRET = os.getenv("CTRADER_CLIENT_SECRET", "eEgvRhPB8FZGzj7TML0wSYpCcsflXxp0apMpi9JbjFKACLIzBC").strip()
REDIRECT_URI = os.getenv("CTRADER_REDIRECT_URI", "https://timedge-render.onrender.com/auth/callback").strip()
ENV = "live" if os.getenv("CTRADER_ENV", "demo").strip().lower() == "live" else "demo"
SYMBOL = os.getenv("CTRADER_SYMBOL", "XAUUSD").strip() or "XAUUSD"
ACCESS_TOKEN = os.getenv("CTRADER_ACCESS_TOKEN", "O5H5KPy7xOoExTyWhoE4HxaEDiyzxqhoAmuBdsm_Gfs").strip()
REFRESH_TOKEN = os.getenv("CTRADER_REFRESH_TOKEN", "91hVNcjJxMr1vPtNt1CA51TpVuBHlTkSdbneD35SnuY").strip()
# This is the user's configured account ID, but it is only used if it appears in the
# authorized account list returned by cTrader for the current access token.
ACCOUNT_ID = os.getenv("CTRADER_ACCOUNT_ID", "10143677").strip()
try:
    TOKEN_EXPIRES_AT = int(os.getenv("CTRADER_ACCESS_TOKEN_EXPIRES_AT", "0") or "0")
except ValueError:
    TOKEN_EXPIRES_AT = 0

BOOT = time.time()
FEEDS = {
    "ctrader": {"provider": "cTrader", "symbol": SYMBOL, "status": "WAITING", "price": None, "bid": None, "ask": None, "timestamp": None, "error": None},
    "binance": {"provider": "Binance USD-M Futures", "symbol": "BTCUSDT", "status": "CONNECTING", "price": None, "bid": None, "ask": None, "timestamp": None, "error": None},
    "bybit": {"provider": "Bybit Linear Perpetual", "symbol": "BTCUSDT", "status": "CONNECTING", "price": None, "bid": None, "ask": None, "timestamp": None, "error": None},
}
CLIENTS = set()
CANDLES = []
LOOP = None
CTRADE_CLIENT = None
CTRADE_THREAD = None


def now_ms():
    return int(time.time() * 1000)


def set_feed(name, **kwargs):
    FEEDS[name].update(kwargs)
    FEEDS[name]["updatedAt"] = now_ms()
    if LOOP and LOOP.is_running():
        try:
            asyncio.run_coroutine_threadsafe(
                broadcast({"type": "feed", "feed": name, "data": FEEDS[name]}), LOOP
            )
        except Exception:
            pass


async def broadcast(payload):
    dead = []
    for ws in tuple(CLIENTS):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        CLIENTS.discard(ws)


def public_state():
    return {
        "ok": True,
        "service": "GexFlow Python market gateway",
        "uptimeSeconds": int(time.time() - BOOT),
        "feeds": FEEDS,
        "ctraderConfigured": bool(CLIENT_ID and CLIENT_SECRET and REDIRECT_URI),
        "ctraderEnvironment": ENV,
        "note": "Prices are live only when the corresponding feed status is LIVE.",
    }


async def home(req):
    path = os.path.join(ROOT, "index.html")
    if not os.path.isfile(path):
        return web.Response(text="index.html not found beside server.py", status=404)
    return web.FileResponse(path)


async def health(req):
    return web.json_response({"ok": True, "backend": "python", "feeds": {k: v["status"] for k, v in FEEDS.items()}, "uptimeSeconds": int(time.time() - BOOT)})


async def api_markets(req):
    return web.json_response(public_state())


async def api_candles(req):
    """Recent real cTrader trendbars. Empty means cTrader history is not available yet."""
    try:
        limit = max(1, min(5000, int(req.query.get("limit", "3000"))))
    except ValueError:
        limit = 3000
    bars = CANDLES[-limit:]
    return web.json_response({"ok": True, "source": "cTrader", "symbol": SYMBOL, "timeframe": "M1", "candles": bars, "count": len(bars)})


async def api_quote(req):
    name = req.query.get("source", "ctrader").lower()
    if name not in FEEDS:
        return web.json_response({"ok": False, "error": "source must be ctrader, binance, or bybit"}, status=400)
    quote = FEEDS[name]
    return web.json_response(quote, status=200 if quote.get("status") == "LIVE" else 503)


async def auth_login(req):
    if not (CLIENT_ID and CLIENT_SECRET and REDIRECT_URI):
        return web.Response(text="Set CTRADER_CLIENT_ID, CTRADER_CLIENT_SECRET, and CTRADER_REDIRECT_URI in Render Environment.", status=500)
    query = urlencode({"client_id": CLIENT_ID, "redirect_uri": REDIRECT_URI, "scope": "accounts", "product": "web"})
    raise web.HTTPFound("https://id.ctrader.com/my/settings/openapi/grantingaccess/?" + query)


async def refresh_oauth_tokens(session, force=False):
    """Refresh only when needed. cTrader rotates refresh tokens, so save the new one securely."""
    global ACCESS_TOKEN, REFRESH_TOKEN, TOKEN_EXPIRES_AT
    token_is_expiring = TOKEN_EXPIRES_AT > 0 and TOKEN_EXPIRES_AT <= int(time.time()) + 120
    if not REFRESH_TOKEN or not (force or not ACCESS_TOKEN or token_is_expiring):
        return bool(ACCESS_TOKEN)
    params = {"grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
    async with session.get("https://openapi.ctrader.com/apps/token", params=params, timeout=20) as response:
        data = await response.json(content_type=None)
        if response.status >= 400 or not data.get("accessToken"):
            raise RuntimeError(str(data.get("description") or data.get("errorCode") or f"Token refresh HTTP {response.status}")[:180])
    ACCESS_TOKEN = data["accessToken"]
    REFRESH_TOKEN = data.get("refreshToken") or REFRESH_TOKEN
    TOKEN_EXPIRES_AT = int(time.time()) + int(data.get("expiresIn", 0))
    return True


async def auth_callback(req):
    global ACCESS_TOKEN, REFRESH_TOKEN, TOKEN_EXPIRES_AT
    code = req.query.get("code", "")
    if not code:
        error = req.query.get("error_description") or req.query.get("error")
        return web.Response(text="cTrader authorization did not return a code. " + str(error or "Start at /auth/login."), status=400)
    params = {"grant_type": "authorization_code", "code": code, "redirect_uri": REDIRECT_URI, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
    try:
        async with ClientSession() as session:
            async with session.get("https://openapi.ctrader.com/apps/token", params=params, timeout=20) as response:
                data = await response.json(content_type=None)
                if response.status >= 400 or not data.get("accessToken"):
                    raise RuntimeError(str(data.get("description") or data.get("errorCode") or f"OAuth HTTP {response.status}")[:180])
        ACCESS_TOKEN = data["accessToken"]
        REFRESH_TOKEN = data.get("refreshToken", "") or REFRESH_TOKEN
        TOKEN_EXPIRES_AT = int(time.time()) + int(data.get("expiresIn", 0))
        set_feed("ctrader", status="TOKEN_RECEIVED", error="OAuth token received; resolving authorized accounts.")
        start_ctrader_worker()
        return web.Response(text="<h2>GexFlow: cTrader authorization received</h2><p>Account discovery has started. Check <a href='/api/markets'>/api/markets</a> and the Render logs/feed status.</p>", content_type="text/html")
    except Exception as exc:
        set_feed("ctrader", status="ERROR", error=("OAuth: " + str(exc))[:220])
        return web.Response(text="cTrader OAuth failed: " + str(exc)[:220], status=500)


async def ws_handler(req):
    ws = web.WebSocketResponse(heartbeat=25, max_msg_size=32768)
    await ws.prepare(req)
    CLIENTS.add(ws)
    await ws.send_json({"type": "snapshot", "state": public_state()})
    if CANDLES:
        await ws.send_json({"type": "candles", "feed": "ctrader", "symbol": SYMBOL, "candles": CANDLES[-5000:]})
    async for msg in ws:
        if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
            break
    CLIENTS.discard(ws)
    return ws


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
                            set_feed("binance", status="LIVE", bid=bid, ask=ask, price=(bid + ask) / 2, timestamp=now_ms(), error=None)
                    elif message.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                        break
            set_feed("binance", status="RECONNECTING", error="Binance WebSocket closed; reconnecting.")
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
                                    set_feed("bybit", status="LIVE", bid=bid, ask=ask, price=(bid + ask) / 2, timestamp=now_ms(), error=None)
                    elif message.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                        break
            set_feed("bybit", status="RECONNECTING", error="Bybit WebSocket closed; reconnecting.")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            set_feed("bybit", status="RECONNECTING", error=str(exc)[:180])
        await asyncio.sleep(3)


# cTrader SDK owns a Twisted reactor in a dedicated thread.
def start_ctrader_worker():
    global CTRADE_CLIENT, CTRADE_THREAD
    if CTRADE_THREAD and CTRADE_THREAD.is_alive():
        return
    if not (CLIENT_ID and CLIENT_SECRET and ACCESS_TOKEN):
        set_feed("ctrader", status="AUTH_REQUIRED", error="Set cTrader app credentials and access token, then open /auth/login.")
        return

    def runner():
        global CTRADE_CLIENT
        try:
            from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
            from ctrader_open_api.messages.OpenApiMessages_pb2 import (
                ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes,
                ProtoOAGetAccountListByAccessTokenReq, ProtoOAGetAccountListByAccessTokenRes,
                ProtoOAAccountAuthReq, ProtoOAAccountAuthRes,
                ProtoOASymbolsListReq, ProtoOASymbolsListRes,
                ProtoOASubscribeSpotsReq, ProtoOASubscribeSpotsRes,
                ProtoOASpotEvent, ProtoOAErrorRes,
                ProtoOAGetTrendbarsReq, ProtoOAGetTrendbarsRes,
            )
            from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOATrendbarPeriod
            from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoHeartbeatEvent
            from twisted.internet import reactor

            host = EndPoints.PROTOBUF_LIVE_HOST if ENV == "live" else EndPoints.PROTOBUF_DEMO_HOST
            client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
            CTRADE_CLIENT = client
            state = {"account_id": None, "symbol_id": None, "digits": 2, "history_requested": False}

            def send(request):
                deferred = client.send(request)
                deferred.addErrback(on_error)
                return deferred

            def on_error(failure):
                try:
                    message = failure.getErrorMessage() if hasattr(failure, "getErrorMessage") else str(failure)
                except Exception:
                    message = str(failure)
                set_feed("ctrader", status="ERROR", error=("cTrader request failed: " + message)[:220])
                return failure

            def connected(c):
                set_feed("ctrader", status="CONNECTED", error=None)
                request = ProtoOAApplicationAuthReq()
                request.clientId = CLIENT_ID
                request.clientSecret = CLIENT_SECRET
                send(request)

            def disconnected(c, reason):
                set_feed("ctrader", status="RECONNECTING", error=("cTrader disconnected: " + str(reason))[:180])

            def received(c, message):
                try:
                    payload = message.payloadType
                    if payload == ProtoOAApplicationAuthRes().payloadType:
                        set_feed("ctrader", status="APP_AUTHENTICATED", error=None)
                        request = ProtoOAGetAccountListByAccessTokenReq()
                        request.accessToken = ACCESS_TOKEN
                        send(request)

                    elif payload == ProtoOAGetAccountListByAccessTokenRes().payloadType:
                        response = Protobuf.extract(message)
                        accounts = list(response.ctidTraderAccount)
                        if not accounts:
                            set_feed("ctrader", status="NO_AUTHORIZED_ACCOUNTS", error="cTrader returned no accounts for this token. Re-authorize via /auth/login and grant account access.")
                            return
                        ids = [int(account.ctidTraderAccountId) for account in accounts]
                        configured = int(ACCOUNT_ID) if ACCOUNT_ID.isdigit() else None
                        if configured is not None and configured in ids:
                            selected = configured
                        elif len(ids) == 1:
                            selected = ids[0]
                            set_feed("ctrader", status="ACCOUNT_ID_SELECTED", error=f"Configured ID was not in token account list; using the only authorized account ID {selected}.")
                        else:
                            set_feed("ctrader", status="ACCOUNT_SELECTION_REQUIRED", error="Configured account ID is not in the token's authorized account list. Authorized cTrader account IDs: " + ", ".join(map(str, ids[:20])) + ". Set CTRADER_ACCOUNT_ID to the intended ID in Render.")
                            return
                        state["account_id"] = selected
                        request = ProtoOAAccountAuthReq()
                        request.ctidTraderAccountId = selected
                        request.accessToken = ACCESS_TOKEN
                        send(request)

                    elif payload == ProtoOAAccountAuthRes().payloadType:
                        response = Protobuf.extract(message)
                        confirmed = int(getattr(response, "ctidTraderAccountId", state["account_id"] or 0))
                        state["account_id"] = confirmed
                        set_feed("ctrader", status="ACCOUNT_AUTHENTICATED", error=None)
                        request = ProtoOASymbolsListReq()
                        request.ctidTraderAccountId = confirmed
                        send(request)

                    elif payload == ProtoOASymbolsListRes().payloadType:
                        response = Protobuf.extract(message)
                        wanted = SYMBOL.upper().replace("/", "").replace(".", "").replace("_", "")
                        found = None
                        for symbol in response.symbol:
                            name = getattr(symbol, "symbolName", "").upper().replace("/", "").replace(".", "").replace("_", "")
                            if name == wanted or (wanted in ("XAUUSD", "GOLD") and name in ("XAUUSD", "GOLD")):
                                found = symbol
                                break
                        if not found:
                            names = [getattr(item, "symbolName", "") for item in response.symbol if any(term in getattr(item, "symbolName", "").upper() for term in ("XAU", "GOLD"))]
                            set_feed("ctrader", status="SYMBOL_NOT_FOUND", error="Requested " + SYMBOL + " not found. Gold symbols available: " + (", ".join(names[:15]) or "none returned"))
                            return
                        state["symbol_id"] = int(found.symbolId)
                        try:
                            state["digits"] = int(found.digits)
                        except Exception:
                            state["digits"] = 2
                        request = ProtoOASubscribeSpotsReq()
                        request.ctidTraderAccountId = state["account_id"]
                        request.symbolId.append(state["symbol_id"])
                        request.subscribeToSpotTimestamp = True
                        send(request)
                        # Request real cTrader M1 history, up to the provider's supported limit.
                        try:
                            history = ProtoOAGetTrendbarsReq()
                            history.ctidTraderAccountId = state["account_id"]
                            history.symbolId = state["symbol_id"]
                            history.period = ProtoOATrendbarPeriod.M1
                            history.fromTimestamp = now_ms() - 3 * 86400000
                            history.toTimestamp = now_ms()
                            state["history_requested"] = True
                            send(history)
                        except Exception as exc:
                            set_feed("ctrader", error=("History request: " + str(exc))[:180])
                        set_feed("ctrader", status="SUBSCRIBING", error=None)

                    elif payload == ProtoOASubscribeSpotsRes().payloadType:
                        set_feed("ctrader", status="LIVE_WAITING_FOR_TICK", error=None)

                    elif payload == ProtoOAGetTrendbarsRes().payloadType:
                        response = Protobuf.extract(message)
                        bars = []
                        for bar in getattr(response, " trendbar", getattr(response, "trendbar", [])):
                            try:
                                low_raw = int(bar.low)
                                low = low_raw / 100000.0
                                o = low + (int(bar.deltaOpen) / 100000.0)
                                h = low + (int(bar.deltaHigh) / 100000.0)
                                c = low + (int(bar.deltaClose) / 100000.0)
                                t = int(bar.utcTimestampInMinutes) * 60
                                if t > 0 and min(o, h, low, c) > 0 and h >= max(o, c, low) and low <= min(o, c):
                                    bars.append({"time": t, "open": o, "high": h, "low": low, "close": c, "t": t * 1000, "o": o, "h": h, "l": low, "c": c})
                            except Exception:
                                continue
                        bars.sort(key=lambda item: item["time"])
                        CANDLES.clear()
                        CANDLES.extend(bars[-5000:])
                        if LOOP and LOOP.is_running():
                            asyncio.run_coroutine_threadsafe(broadcast({"type": "candles", "feed": "ctrader", "symbol": SYMBOL, "candles": CANDLES}), LOOP)
                        set_feed("ctrader", historyBars=len(CANDLES), historyStatus="LIVE" if CANDLES else "EMPTY")

                    elif payload == ProtoOAErrorRes().payloadType:
                        error = Protobuf.extract(message)
                        description = getattr(error, "description", "cTrader API error")
                        code = getattr(error, "errorCode", "")
                        set_feed("ctrader", status="ERROR", error=(str(code) + ": " + str(description)).strip(": ")[:220])

                    elif payload == ProtoOASpotEvent().payloadType:
                        event = Protobuf.extract(message)
                        if state["symbol_id"] and int(event.symbolId) == state["symbol_id"]:
                            bid = (float(event.bid) / 100000.0) if event.HasField("bid") else None
                            ask = (float(event.ask) / 100000.0) if event.HasField("ask") else None
                            old = FEEDS["ctrader"]
                            bid = bid if bid and bid > 0 else old.get("bid")
                            ask = ask if ask and ask > 0 else old.get("ask")
                            if bid and ask and bid > 0 and ask >= bid:
                                set_feed("ctrader", status="LIVE", bid=bid, ask=ask, price=(bid + ask) / 2, timestamp=now_ms(), error=None)
                except Exception as exc:
                    set_feed("ctrader", status="ERROR", error=("Message handling: " + str(exc))[:220])

            client.setConnectedCallback(connected)
            client.setDisconnectedCallback(disconnected)
            client.setMessageReceivedCallback(received)
            client.startService()
            reactor.run(installSignalHandlers=False)
        except Exception as exc:
            set_feed("ctrader", status="ERROR", error=("cTrader worker: " + str(exc))[:220])

    CTRADE_THREAD = threading.Thread(target=runner, name="ctrader-openapi", daemon=True)
    CTRADE_THREAD.start()


async def startup(app):
    global LOOP
    LOOP = asyncio.get_running_loop()
    app["session"] = ClientSession()
    app["tasks"] = [asyncio.create_task(binance_feed(app)), asyncio.create_task(bybit_feed(app))]
    if CLIENT_ID and CLIENT_SECRET:
        try:
            await refresh_oauth_tokens(app["session"])
        except Exception as exc:
            set_feed("ctrader", status="TOKEN_REFRESH_FAILED", error=str(exc)[:200])
    if ACCESS_TOKEN and CLIENT_ID and CLIENT_SECRET:
        start_ctrader_worker()
    elif CLIENT_ID and CLIENT_SECRET and REDIRECT_URI:
        set_feed("ctrader", status="AUTH_REQUIRED", error="Open /auth/login and grant access to the intended cTrader account.")
    else:
        set_feed("ctrader", status="CONFIG_REQUIRED", error="Configure CTRADER_CLIENT_ID and CTRADER_CLIENT_SECRET in Render Environment.")
    print("GexFlow Python service started; feeds are connecting. No credentials are logged.")


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
app.router.add_get("/api/candles", api_candles)
app.router.add_get("/auth/login", auth_login)
app.router.add_get("/auth/callback", auth_callback)
app.router.add_get("/ws", ws_handler)
app.on_startup.append(startup)
app.on_cleanup.append(cleanup)
web.run_app(app, host="0.0.0.0", port=PORT)
