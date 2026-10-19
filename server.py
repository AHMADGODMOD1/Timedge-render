"""GexFlow single-file Python backend: cTrader XAUUSD + Binance/Bybit BTCUSDT.
Render: Build `pip install aiohttp ctrader-open-api`; Start `python server.py`.
Only index.html and server.py are required. No prices are invented; feeds are marked by source/status.
"""
import asyncio, json, os, time, threading, traceback
from urllib.parse import urlencode
from aiohttp import web, ClientSession, WSMsgType, ClientWebSocketResponse

ROOT = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.getenv('PORT', '10000'))
CLIENT_ID = os.getenv('CTRADER_CLIENT_ID', '43370_XnVtFKCM1AkBJKWzgwDJQvqM1MBViHWo076zWdjfCf8Je1fsgE').strip()
CLIENT_SECRET = os.getenv('CTRADER_CLIENT_SECRET', 'eEgvRhPB8FZGzj7TML0wSYpCcsflXxp0apMpi9JbjFKACLIzBC').strip()
REDIRECT_URI = os.getenv('CTRADER_REDIRECT_URI', 'https://timedge-render.onrender.com/auth/callback').strip()
ENV = 'live' if os.getenv('CTRADER_ENV', 'demo').lower() == 'live' else 'demo'
SYMBOL = os.getenv('CTRADER_SYMBOL', 'XAUUSD').strip()
ACCESS_TOKEN = os.getenv('CTRADER_ACCESS_TOKEN', 'e0o8NVNSg6fLDsYwEm67K88OzCF95YuZaEEAFb49OPg').strip()
REFRESH_TOKEN = os.getenv('CTRADER_REFRESH_TOKEN', '5S3PHlZkXf1cjfygDiA5oaf4gK8Jy8KMnIthqIjiqGg').strip()
ACCOUNT_ID = os.getenv('CTRADER_ACCOUNT_ID', '10143677').strip()
TOKEN_EXPIRES_AT = os.getenv('CTRADER_ACCESS_TOKEN_EXPIRES_AT', '2628000').strip()

BOOT = time.time()
FEEDS = {
  'ctrader': {'provider':'cTrader','symbol':SYMBOL,'status':'WAITING','price':None,'bid':None,'ask':None,'timestamp':None,'error':None},
  'binance': {'provider':'Binance USD-M Futures','symbol':'BTCUSDT','status':'CONNECTING','price':None,'bid':None,'ask':None,'timestamp':None,'error':None},
  'bybit': {'provider':'Bybit Linear Perpetual','symbol':'BTCUSDT','status':'CONNECTING','price':None,'bid':None,'ask':None,'timestamp':None,'error':None},
}
CLIENTS=set(); LOOP=None; APP=None; CTRADE_CLIENT=None; CTRADE_THREAD=None

def now_ms(): return int(time.time()*1000)
def set_feed(name, **kw):
    FEEDS[name].update(kw)
    FEEDS[name]['updatedAt'] = now_ms()
    if LOOP and LOOP.is_running():
        try: asyncio.run_coroutine_threadsafe(broadcast({'type':'feed','feed':name,'data':FEEDS[name]}), LOOP)
        except Exception: pass
async def broadcast(payload):
    dead=[]
    for ws in tuple(CLIENTS):
        try: await ws.send_json(payload)
        except Exception: dead.append(ws)
    for ws in dead: CLIENTS.discard(ws)
def public_state():
    return {'ok':True,'service':'GexFlow Python market gateway','uptimeSeconds':int(time.time()-BOOT),'feeds':FEEDS,
            'ctraderConfigured':bool(CLIENT_ID and CLIENT_SECRET and REDIRECT_URI), 'ctraderEnvironment':ENV,
            'note':'Prices are live only when the corresponding feed status is LIVE.'}

async def home(req): return web.FileResponse(os.path.join(ROOT,'index.html'))
async def health(req):
    f={k:v['status'] for k,v in FEEDS.items()}
    return web.json_response({'ok':True,'backend':'python','feeds':f,'uptimeSeconds':int(time.time()-BOOT)})
async def api_markets(req): return web.json_response(public_state())
async def api_quote(req):
    name=req.query.get('source','ctrader').lower()
    if name not in FEEDS: return web.json_response({'ok':False,'error':'source must be ctrader, binance, or bybit'},status=400)
    q=FEEDS[name]
    return web.json_response(q, status=200 if q.get('status')=='LIVE' else 503)
async def auth_login(req):
    if not (CLIENT_ID and CLIENT_SECRET and REDIRECT_URI):
        return web.Response(text='Set CTRADER_CLIENT_ID, CTRADER_CLIENT_SECRET, and CTRADER_REDIRECT_URI in Render Environment.',status=500)
    q=urlencode({'client_id':CLIENT_ID,'redirect_uri':REDIRECT_URI,'scope':'accounts','product':'web'})
    raise web.HTTPFound('https://id.ctrader.com/my/settings/openapi/grantingaccess/?'+q)
async def auth_callback(req):
    global ACCESS_TOKEN, REFRESH_TOKEN, TOKEN_EXPIRES_AT
    code=req.query.get('code','')
    if not code: return web.Response(text='Missing OAuth code. Start at /auth/login.',status=400)
    params={'grant_type':'authorization_code','code':code,'redirect_uri':REDIRECT_URI,'client_id':CLIENT_ID,'client_secret':CLIENT_SECRET}
    try:
        async with ClientSession() as s:
            async with s.get('https://openapi.ctrader.com/apps/token',params=params,timeout=20) as r:
                data=await r.json(content_type=None)
                if r.status>=400 or not data.get('accessToken'): raise RuntimeError(data.get('description') or data.get('errorCode') or f'HTTP {r.status}')
        ACCESS_TOKEN=data['accessToken']; REFRESH_TOKEN=data.get('refreshToken','') or REFRESH_TOKEN
        TOKEN_EXPIRES_AT=str(int(time.time())+int(data.get('expiresIn',0)))
        set_feed('ctrader',status='TOKEN_RECEIVED',error='OAuth token received; waiting for cTrader stream.')
        if ACCOUNT_ID and CLIENT_ID and CLIENT_SECRET: start_ctrader_worker()
        # Tokens are deliberately not written to source files or printed to logs.
        return web.Response(text='<h2>GexFlow: cTrader authorization received</h2><p>Token received. Check <a href="/api/markets">/api/markets</a> for the live stream status.</p>',content_type='text/html')
    except Exception as e:
        set_feed('ctrader',status='ERROR',error=str(e)[:240]); return web.Response(text='cTrader OAuth failed: '+str(e)[:240],status=500)

async def ws_handler(req):
    ws=web.WebSocketResponse(heartbeat=25,max_msg_size=32768); await ws.prepare(req)
    CLIENTS.add(ws); await ws.send_json({'type':'snapshot','state':public_state()})
    async for msg in ws:
        if msg.type in (WSMsgType.CLOSE,WSMsgType.CLOSED,WSMsgType.ERROR): break
    CLIENTS.discard(ws); return ws

async def binance_feed(app):
    url='wss://fstream.binance.com/ws/btcusdt@bookTicker'
    while True:
        try:
            set_feed('binance',status='CONNECTING',error=None)
            async with app['session'].ws_connect(url,heartbeat=25,timeout=20) as ws:
                set_feed('binance',status='LIVE',error=None)
                async for m in ws:
                    if m.type==WSMsgType.TEXT:
                        d=json.loads(m.data); bid=float(d['b']); ask=float(d['a'])
                        if bid>0 and ask>0: set_feed('binance',status='LIVE',bid=bid,ask=ask,price=(bid+ask)/2,timestamp=now_ms(),error=None)
                    elif m.type in (WSMsgType.CLOSED,WSMsgType.ERROR): break
        except asyncio.CancelledError: raise
        except Exception as e: set_feed('binance',status='RECONNECTING',error=str(e)[:180])
        await asyncio.sleep(3)

async def bybit_feed(app):
    url='wss://stream.bybit.com/v5/public/linear'
    while True:
        try:
            set_feed('bybit',status='CONNECTING',error=None)
            async with app['session'].ws_connect(url,heartbeat=20,timeout=20) as ws:
                await ws.send_json({'op':'subscribe','args':['orderbook.1.BTCUSDT']})
                set_feed('bybit',status='LIVE',error=None)
                async for m in ws:
                    if m.type==WSMsgType.TEXT:
                        d=json.loads(m.data)
                        if d.get('topic','').startswith('orderbook.1.') and d.get('data'):
                            book=d['data']; bids=book.get('b',[]); asks=book.get('a',[])
                            if bids and asks:
                                bid=float(bids[0][0]); ask=float(asks[0][0])
                                if bid>0 and ask>0: set_feed('bybit',status='LIVE',bid=bid,ask=ask,price=(bid+ask)/2,timestamp=now_ms(),error=None)
                    elif m.type in (WSMsgType.CLOSED,WSMsgType.ERROR): break
        except asyncio.CancelledError: raise
        except Exception as e: set_feed('bybit',status='RECONNECTING',error=str(e)[:180])
        await asyncio.sleep(3)

# cTrader Open API SDK runs its own Twisted reactor thread. It subscribes to actual spot events.
def start_ctrader_worker():
    global CTRADE_CLIENT, CTRADE_THREAD
    if not (CLIENT_ID and CLIENT_SECRET and ACCESS_TOKEN and ACCOUNT_ID):
        set_feed('ctrader',status='AUTH_REQUIRED' if CLIENT_ID and CLIENT_SECRET else 'CONFIG_REQUIRED',error='Set cTrader app credentials, access token, and account ID in Render Environment; authorize at /auth/login.')
        return
    def runner():
        global CTRADE_CLIENT
        try:
            from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
            from ctrader_open_api.messages.OpenApiMessages_pb2 import (ProtoOAApplicationAuthReq,ProtoOAApplicationAuthRes,ProtoOAAccountAuthReq,ProtoOAAccountAuthRes,ProtoOASymbolsListReq,ProtoOASymbolsListRes,ProtoOASubscribeSpotsReq,ProtoOASubscribeSpotsRes,ProtoOAErrorRes)
            from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoHeartbeatEvent
            from twisted.internet import reactor
            host=EndPoints.PROTOBUF_LIVE_HOST if ENV=='live' else EndPoints.PROTOBUF_DEMO_HOST
            client=Client(host,EndPoints.PROTOBUF_PORT,TcpProtocol); CTRADE_CLIENT=client
            chosen={'symbol_id':None,'digits':2}
            def on_error(err):
                set_feed('ctrader',status='ERROR',error=str(err)[:200])
            def connected(c):
                set_feed('ctrader',status='CONNECTED',error=None)
                req=ProtoOAApplicationAuthReq(); req.clientId=CLIENT_ID; req.clientSecret=CLIENT_SECRET
                c.send(req).addErrback(on_error)
            def disconnected(c,reason):
                set_feed('ctrader',status='RECONNECTING',error=str(reason)[:180])
            def received(c,msg):
                try:
                    pt=msg.payloadType
                    if pt==ProtoOAApplicationAuthRes().payloadType:
                        r=ProtoOAAccountAuthReq(); r.ctidTraderAccountId=int(ACCOUNT_ID); r.accessToken=ACCESS_TOKEN
                        c.send(r).addErrback(on_error)
                    elif pt==ProtoOAAccountAuthRes().payloadType:
                        set_feed('ctrader',status='ACCOUNT_AUTHENTICATED',error=None)
                        r=ProtoOASymbolsListReq(); r.ctidTraderAccountId=int(ACCOUNT_ID)
                        c.send(r).addErrback(on_error)
                    elif pt==ProtoOASymbolsListRes().payloadType:
                        res=Protobuf.extract(msg); wanted=SYMBOL.upper().replace('/','').replace('.','')
                        found=None
                        for sym in res.symbol:
                            name=getattr(sym,'symbolName','').upper().replace('/','').replace('.','')
                            if name==wanted or (wanted in ('XAUUSD','GOLD') and name in ('XAUUSD','GOLD')):
                                found=sym; break
                        if not found:
                            names=[getattr(x,'symbolName','') for x in res.symbol if any(t in getattr(x,'symbolName','').upper() for t in ('XAU','GOLD'))]
                            set_feed('ctrader',status='SYMBOL_NOT_FOUND',error='Requested '+SYMBOL+' not found. Available gold symbols: '+', '.join(names[:15])); return
                        chosen['symbol_id']=int(found.symbolId)
                        try: chosen['digits']=int(found.digits)
                        except Exception: chosen['digits']=2
                        r=ProtoOASubscribeSpotsReq(); r.ctidTraderAccountId=int(ACCOUNT_ID); r.symbolId.append(chosen['symbol_id']); r.subscribeToSpotTimestamp=True
                        c.send(r).addErrback(on_error)
                        set_feed('ctrader',status='SUBSCRIBING',error=None)
                    elif pt==ProtoOASubscribeSpotsRes().payloadType:
                        set_feed('ctrader',status='LIVE_WAITING_FOR_TICK',error=None)
                    elif pt==ProtoOAErrorRes().payloadType:
                        e=Protobuf.extract(msg); set_feed('ctrader',status='ERROR',error=str(getattr(e,'description','cTrader API error'))[:200])
                    else:
                        # Spot events are identified by their payload type without requiring a hard-coded enum number.
                        from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOASpotEvent
                        if pt==ProtoOASpotEvent().payloadType:
                            ev=Protobuf.extract(msg)
                            if chosen['symbol_id'] and int(ev.symbolId)==chosen['symbol_id']:
                                # cTrader spot prices use 1e-5 relative units.
                                bid=(float(ev.bid)/100000.0) if ev.HasField('bid') else None
                                ask=(float(ev.ask)/100000.0) if ev.HasField('ask') else None
                                if bid and ask and bid>0 and ask>0:
                                    set_feed('ctrader',status='LIVE',bid=bid,ask=ask,price=(bid+ask)/2,timestamp=now_ms(),error=None)
                except Exception as e: set_feed('ctrader',status='ERROR',error=('Message handling: '+str(e))[:200])
            client.setConnectedCallback(connected); client.setDisconnectedCallback(disconnected)
            client.setMessageReceivedCallback(received); client.setMessageErrorCallback(on_error)
            client.startService(); reactor.run(installSignalHandlers=False)
        except Exception as e:
            set_feed('ctrader',status='ERROR',error=('cTrader worker: '+str(e))[:220])
    CTRADE_THREAD=threading.Thread(target=runner,name='ctrader-openapi',daemon=True); CTRADE_THREAD.start()

async def startup(app):
    global LOOP, ACCESS_TOKEN, REFRESH_TOKEN
    LOOP=asyncio.get_running_loop(); app['session']=ClientSession()
    app['tasks']=[asyncio.create_task(binance_feed(app)),asyncio.create_task(bybit_feed(app))]
    # Refresh an expired/missing access token when a refresh token is configured.
    if not ACCESS_TOKEN and REFRESH_TOKEN and CLIENT_ID and CLIENT_SECRET:
        try:
            params={'grant_type':'refresh_token','refresh_token':REFRESH_TOKEN,'client_id':CLIENT_ID,'client_secret':CLIENT_SECRET}
            async with app['session'].get('https://openapi.ctrader.com/apps/token',params=params,timeout=20) as r:
                data=await r.json(content_type=None)
                if r.status < 400 and data.get('accessToken'):
                    ACCESS_TOKEN=data['accessToken']; REFRESH_TOKEN=data.get('refreshToken',REFRESH_TOKEN) or REFRESH_TOKEN
                else: set_feed('ctrader',status='TOKEN_REFRESH_FAILED',error=str(data.get('description') or data.get('errorCode') or 'Refresh token rejected')[:180])
        except Exception as e: set_feed('ctrader',status='TOKEN_REFRESH_FAILED',error=str(e)[:180])
    # Credentials are loaded from environment; no secrets are printed.
    if ACCESS_TOKEN and ACCOUNT_ID and CLIENT_ID and CLIENT_SECRET: start_ctrader_worker()
    elif CLIENT_ID and CLIENT_SECRET and REDIRECT_URI:
        set_feed('ctrader',status='AUTH_REQUIRED',error='Open /auth/login. Set CTRADER_ACCOUNT_ID and keep access/refresh tokens in Render Environment for reconnects.')
    else: set_feed('ctrader',status='CONFIG_REQUIRED',error='Configure cTrader environment variables in Render.')
    print('GexFlow Python service started; market feeds are connecting.')
async def cleanup(app):
    for t in app.get('tasks',[]): t.cancel()
    if app.get('tasks'): await asyncio.gather(*app['tasks'],return_exceptions=True)
    await app['session'].close()

app=web.Application(client_max_size=16384)
app.router.add_get('/',home); app.router.add_get('/health',health); app.router.add_get('/api/markets',api_markets); app.router.add_get('/api/quote',api_quote)
app.router.add_get('/auth/login',auth_login); app.router.add_get('/auth/callback',auth_callback); app.router.add_get('/ws',ws_handler)
app.on_startup.append(startup); app.on_cleanup.append(cleanup)
web.run_app(app,host='0.0.0.0',port=PORT)
