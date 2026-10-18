import 'dotenv/config';
import express from 'express';
import http from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';
import { CTraderClient } from 'ctrader-js';

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '16kb' }));
// Serve the TimEdge frontend from this same Render service.
app.get('/', (_req, res) => res.sendFile(new URL('./timedge_ctrader_render.html', import.meta.url).pathname));
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true, maxPayload: 16 * 1024 });
const PORT = Number(process.env.PORT || 10000);
const CLIENT_ID = process.env.CTRADER_CLIENT_ID;
const CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET;
const REDIRECT_URI = process.env.CTRADER_REDIRECT_URI;
const ENV = (process.env.CTRADER_ENV || 'demo').toLowerCase() === 'live' ? 'live' : 'demo';
const FRONTEND_ORIGIN = (process.env.FRONTEND_ORIGIN || '').replace(/\/$/, '');

let client = null;
let accountId = process.env.CTRADER_ACCOUNT_ID ? Number(process.env.CTRADER_ACCOUNT_ID) : null;
let symbolId = process.env.CTRADER_SYMBOL_ID ? Number(process.env.CTRADER_SYMBOL_ID) : null;
let chosenSymbol = process.env.CTRADER_SYMBOL || 'XAUUSD';
let latestQuote = null;
let availableSymbols = [];
let state = 'NOT_AUTHORIZED';
let lastError = null;
let refreshToken = process.env.CTRADER_REFRESH_TOKEN || '';
let newlyIssuedRefreshToken = '';
let newlyIssuedAccessToken = '';
let accessTokenExpiresAt = process.env.CTRADER_ACCESS_TOKEN_EXPIRES_AT || '';
let currentAccessToken = process.env.CTRADER_ACCESS_TOKEN || '';
let tokenExpiresIn = 0;
let reconnecting = false;

function publicStatus() {
  return { provider: 'cTrader', status: state, environment: ENV, symbol: chosenSymbol, symbolId: symbolId || null, hasQuote: !!latestQuote, lastQuoteAt: latestQuote?.timestamp || null, error: lastError };
}
function broadcast(payload) {
  const raw = JSON.stringify(payload);
  for (const ws of wss.clients) if (ws.readyState === WebSocket.OPEN) ws.send(raw);
}
function setState(next, error = null) {
  state = next;
  lastError = error ? String(error).slice(0, 240) : null;
  broadcast({ type: 'status', ...publicStatus() });
}
function normalizeName(s) { return String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, ''); }
function extractSymbols(response) {
  if (Array.isArray(response)) return response;
  return response?.symbol || response?.symbols || response?.symbolList || [];
}
function getSymbolName(s) { return s?.symbolName || s?.name || s?.symbol || ''; }
function getSymbolId(s) { return Number(s?.symbolId ?? s?.id); }
function findTargetSymbol(symbols) {
  if (symbolId) return symbols.find(s => getSymbolId(s) === symbolId) || null;
  const target = normalizeName(chosenSymbol);
  let hit = symbols.find(s => normalizeName(getSymbolName(s)) === target);
  if (!hit && target === 'XAUUSD') hit = symbols.find(s => ['GOLD', 'XAUUSD', 'XAUUSDSPOT'].includes(normalizeName(getSymbolName(s))));
  if (!hit && target.includes('XAU')) hit = symbols.find(s => normalizeName(getSymbolName(s)).includes('XAU'));
  return hit || null;
}

async function obtainAccessTokenFromRefreshToken(rt) {
  const url = new URL('https://openapi.ctrader.com/apps/token');
  url.searchParams.set('grant_type', 'refresh_token');
  url.searchParams.set('refresh_token', rt);
  url.searchParams.set('client_id', CLIENT_ID);
  url.searchParams.set('client_secret', CLIENT_SECRET);
  const response = await fetch(url, { headers: { Accept: 'application/json' } });
  const data = await response.json();
  if (!response.ok || !data.accessToken) throw new Error(data.description || data.errorCode || `Token refresh failed (${response.status})`);
  if (data.refreshToken) refreshToken = data.refreshToken;
  currentAccessToken = data.accessToken;
  tokenExpiresIn = Number(data.expiresIn || 0);
  accessTokenExpiresAt = new Date(Date.now() + tokenExpiresIn * 1000).toISOString();
  return data.accessToken;
}
async function exchangeAuthorizationCode(code) {
  const url = new URL('https://openapi.ctrader.com/apps/token');
  url.searchParams.set('grant_type', 'authorization_code');
  url.searchParams.set('code', code);
  url.searchParams.set('redirect_uri', REDIRECT_URI);
  url.searchParams.set('client_id', CLIENT_ID);
  url.searchParams.set('client_secret', CLIENT_SECRET);
  const response = await fetch(url, { headers: { Accept: 'application/json' } });
  const data = await response.json();
  if (!response.ok || !data.accessToken) throw new Error(data.description || data.errorCode || `Authorization exchange failed (${response.status})`);
  if (data.refreshToken) { refreshToken = data.refreshToken; newlyIssuedRefreshToken = data.refreshToken; }
  currentAccessToken = data.accessToken;
  newlyIssuedAccessToken = data.accessToken;
  tokenExpiresIn = Number(data.expiresIn || 0);
  accessTokenExpiresAt = new Date(Date.now() + tokenExpiresIn * 1000).toISOString();
  return data.accessToken;
}

async function connectCTrader(accessToken) {
  if (reconnecting) return;
  reconnecting = true;
  try {
    if (client) { try { await client.disconnect(); } catch {} }
    setState('CONNECTING');
    client = new CTraderClient({ clientId: CLIENT_ID, clientSecret: CLIENT_SECRET, env: ENV, autoReconnect: true, debug: false });
    client.on('error', err => setState('ERROR', err?.message || err));
    client.on('disconnected', info => setState('DISCONNECTED', info?.reason || 'cTrader disconnected'));
    client.on('connected', () => { if (state !== 'CONNECTED') setState('CONNECTED'); });
    client.on('accountsTokenInvalidated', () => setState('AUTH_REQUIRED', 'cTrader access token was invalidated; authorize again.'));
    await client.connect();
    const accountResponse = await client.getAccountsByToken(accessToken);
    const accounts = accountResponse?.ctidTraderAccount || accountResponse?.ctidTraderAccounts || [];
    if (!accounts.length) throw new Error('No cTrader accounts returned for this token. Re-authorize and grant account access.');
    if (!accountId) accountId = Number(accounts[0].ctidTraderAccountId);
    const account = accounts.find(a => Number(a.ctidTraderAccountId) === accountId);
    if (!account) throw new Error('CTRADER_ACCOUNT_ID does not match an account authorized by this token. Clear it in Render or authorize the correct account.');
    await client.authorizeAccount(accountId, accessToken);
    const symbolResponse = await client.getSymbols(accountId);
    const symbols = extractSymbols(symbolResponse);
    availableSymbols = symbols.map(s => ({ symbol: getSymbolName(s), symbolId: getSymbolId(s) })).filter(s => s.symbol && Number.isFinite(s.symbolId));
    const match = findTargetSymbol(symbols);
    if (!match) {
      const candidates = symbols.filter(s => /XAU|GOLD/i.test(getSymbolName(s))).slice(0, 20).map(s => `${getSymbolName(s)} (${getSymbolId(s)})`);
      throw new Error(`Could not find ${chosenSymbol}. Gold symbols available: ${candidates.join(', ') || 'none returned'}. Set CTRADER_SYMBOL_ID or CTRADER_SYMBOL in Render.`);
    }
    symbolId = getSymbolId(match);
    chosenSymbol = getSymbolName(match) || chosenSymbol;
    client.on('spot', event => {
      if (Number(event?.symbolId) !== symbolId) return;
      const bidRaw = Number(event.bid);
      const askRaw = Number(event.ask);
      const scale = 100000;
      const bid = Number.isFinite(bidRaw) && bidRaw > 0 ? bidRaw / scale : null;
      const ask = Number.isFinite(askRaw) && askRaw > 0 ? askRaw / scale : null;
      const price = bid !== null && ask !== null ? (bid + ask) / 2 : bid ?? ask;
      if (!(price > 0)) return;
      latestQuote = { provider: 'cTrader', symbol: chosenSymbol, symbolId, bid, ask, price, timestamp: Date.now() };
      setState('CONNECTED');
      broadcast({ type: 'quote', ...latestQuote });
    });
    await client.subscribeSpots(accountId, [symbolId]);
    setState('CONNECTED');
  } catch (err) {
    setState('ERROR', err?.message || err);
    throw err;
  } finally { reconnecting = false; }
}

function safeOrigin(origin) {
  if (!origin) return true; // non-browser health checks
  if (FRONTEND_ORIGIN && origin === FRONTEND_ORIGIN) return true;
  return process.env.NODE_ENV !== 'production' && /^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/.test(origin);
}
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (origin && safeOrigin(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  }
  if (req.method === 'OPTIONS') return res.sendStatus(origin && !safeOrigin(origin) ? 403 : 204);
  next();
});

app.get('/health', (_req, res) => res.json({ ok: true, service: 'timedge-ctrader-marketdata', ...publicStatus() }));
app.get('/api/status', (_req, res) => res.json(publicStatus()));
app.post('/admin/tokens', (req, res) => {
  const expected = process.env.SETUP_ADMIN_KEY;
  if (!expected || req.get('x-setup-key') !== expected) return res.status(403).json({ ok: false, message: 'Forbidden' });
  res.setHeader('Cache-Control', 'no-store');
  res.json({ ok: true, accessToken: currentAccessToken || null, refreshToken: refreshToken || null, accessTokenExpiresAt: accessTokenExpiresAt || null, note: 'Keep these values private. Update Render environment variables after a token refresh.' });
});
app.get('/api/quote', (_req, res) => latestQuote ? res.json(latestQuote) : res.status(503).json({ ok: false, message: 'No live cTrader quote received yet.', ...publicStatus() }));
app.get('/api/symbols', (_req, res) => {
  if (!client || !accountId) return res.status(503).json({ ok: false, message: 'Authorize cTrader first.' });
  res.json({ ok: true, selected: { symbol: chosenSymbol, symbolId }, goldCandidates: availableSymbols.filter(s => /XAU|GOLD/i.test(s.symbol)).slice(0, 50) });
});
app.get('/auth/login', (req, res) => {
  if (!CLIENT_ID || !CLIENT_SECRET || !REDIRECT_URI) return res.status(500).send('Set CTRADER_CLIENT_ID, CTRADER_CLIENT_SECRET and CTRADER_REDIRECT_URI in Render environment variables first.');
  const url = new URL('https://id.ctrader.com/my/settings/openapi/grantingaccess/');
  url.searchParams.set('client_id', CLIENT_ID);
  url.searchParams.set('redirect_uri', REDIRECT_URI);
  url.searchParams.set('scope', 'accounts');
  url.searchParams.set('product', 'web');
  res.redirect(url.toString());
});
app.get('/auth/callback', async (req, res) => {
  const code = String(req.query.code || '');
  if (!code) return res.status(400).send('No cTrader authorization code was received. Start again from /auth/login.');
  try {
    const accessToken = await exchangeAuthorizationCode(code);
    await connectCTrader(accessToken);
    const refreshBlock = newlyIssuedRefreshToken ? `<h2>One-time setup</h2><p>Add these values in Render → Environment, then save/redeploy. Keep them private. Access token expires around ${accessTokenExpiresAt}.</p><label>CTRADER_ACCESS_TOKEN</label><textarea readonly style="width:100%;min-height:70px;background:#111827;color:#f9fafb;padding:10px">${newlyIssuedAccessToken.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}</textarea><button onclick="navigator.clipboard.writeText(document.querySelectorAll('textarea')[0].value)">Copy access token</button><label>CTRADER_REFRESH_TOKEN</label><textarea readonly style="width:100%;min-height:70px;background:#111827;color:#f9fafb;padding:10px">${newlyIssuedRefreshToken.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}</textarea><button onclick="navigator.clipboard.writeText(document.querySelectorAll('textarea')[1].value)">Copy refresh token</button><p>Also set <code>CTRADER_ACCESS_TOKEN_EXPIRES_AT</code> to <code>${accessTokenExpiresAt}</code>. Add <code>SETUP_ADMIN_KEY</code> as a long random secret for the protected token-rotation endpoint.</p>` : `<p>A refresh token was not returned by the provider. If the service restarts and loses authorization, open <code>/auth/login</code> again.</p>`;
    res.type('html').send(`<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Tim Edge cTrader setup</title><body style="font:16px system-ui;max-width:760px;margin:40px auto;padding:0 18px;background:#0b1220;color:#e5e7eb"><h1>cTrader authorization succeeded</h1><p>Backend connection attempt completed. Current status: <b>${state}</b>.</p>${refreshBlock}<p><a style="color:#60a5fa" href="/health">Check service status</a></p><p>Never share access tokens or refresh tokens.</p></body>`);
  } catch (err) {
    res.status(500).type('text').send(`cTrader authorization failed: ${String(err?.message || err).slice(0, 500)}. Check Render logs, account permissions, redirect URI and symbol configuration.`);
  }
});

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const origin = req.headers.origin;
  if (url.pathname !== '/ws' || (origin && !safeOrigin(origin))) { socket.write('HTTP/1.1 403 Forbidden\r\n\r\n'); socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});
wss.on('connection', ws => {
  ws.send(JSON.stringify({ type: 'status', ...publicStatus() }));
  if (latestQuote) ws.send(JSON.stringify({ type: 'quote', ...latestQuote }));
  ws.on('error', () => {});
});

async function startup() {
  if (!CLIENT_ID || !CLIENT_SECRET) {
    setState('CONFIG_REQUIRED', 'Set CTRADER_CLIENT_ID and CTRADER_CLIENT_SECRET in Render.');
    return;
  }
  if (currentAccessToken && accessTokenExpiresAt && Date.parse(accessTokenExpiresAt) > Date.now() + 5 * 60 * 1000) {
    try { await connectCTrader(currentAccessToken); return; }
    catch (err) { setState('AUTH_REQUIRED', err?.message || err); }
  }
  if (refreshToken) {
    try {
      const token = await obtainAccessTokenFromRefreshToken(refreshToken);
      await connectCTrader(token);
      return;
    } catch (err) { setState('AUTH_REQUIRED', err?.message || err); }
  } else {
    setState('AUTH_REQUIRED', 'Open /auth/login once to authorize your cTrader account.');
  }
}
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Tim Edge cTrader market-data service listening on ${PORT}`);
  startup().catch(err => setState('ERROR', err?.message || err));
});
