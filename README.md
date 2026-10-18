# Tim Edge — cTrader Render Backend

This service keeps cTrader credentials on Render and streams read-only cTrader spot quotes to the existing Tim Edge HTML over a WebSocket.

## What this package does

- OAuth login/callback using cTrader Open API.
- Keeps Client ID, Client Secret and refresh token server-side.
- Discovers the account's symbols and selects XAUUSD/GOLD (or the configured symbol ID).
- Subscribes to cTrader spot bid/ask updates.
- Exposes `/health`, `/api/status`, `/api/quote`, `/api/symbols`, `/auth/login`, `/auth/callback`, and WebSocket `/ws`.
- Sends quotes to the browser without sending broker credentials to the browser.

## Important current limitation

The supplied Tim Edge HTML's existing main Gold chart/footprint/order-flow pipeline is built around Binance XAUUSDT futures and other existing feeds. This patch replaces the former local FXCM status adapter with a Render-hosted cTrader quote adapter, but **does not yet switch the main chart's candle/order-flow engine to cTrader**. The panel emits `window` event `ctrader-quote`; main chart integration must be implemented separately and must not invent trade volume or absorption from bid/ask ticks.

Binance and Bybit public crypto sockets are preserved as they were in the source HTML. Their public market-data streams do not require private API credentials, so they remain browser-direct in this version. No OANDA integration is included in this cTrader-only package.

## 1. Register the redirect URI

In the cTrader Open API application settings, add the exact callback URI that will be used by Render:

`https://YOUR-RENDER-SERVICE.onrender.com/auth/callback`

Use the same URI as `CTRADER_REDIRECT_URI` below. The cTrader application must be approved and must allow account access.

Official docs:
- https://help.ctrader.com/open-api/
- https://help.ctrader.com/open-api/account-authentication/

## 2. Deploy on Render

1. Create a private GitHub repository and upload the contents of this folder (not the ZIP itself).
2. In Render, choose **New + → Web Service**, connect the repository.
3. Runtime: Node. Build command: `npm install`. Start command: `npm start`.
4. Set these Environment Variables in Render (do not commit real values):
   - `CTRADER_CLIENT_ID`
   - `CTRADER_CLIENT_SECRET`
   - `CTRADER_REDIRECT_URI` = `https://YOUR-RENDER-SERVICE.onrender.com/auth/callback`
   - `CTRADER_ENV` = `demo` initially
   - `CTRADER_SYMBOL` = `XAUUSD`
   - `FRONTEND_ORIGIN` = exact `https://...` origin of the Tim Edge InfinityFree site
   - `PORT` is supplied by Render; usually leave it unset
5. Deploy and open `https://YOUR-RENDER-SERVICE.onrender.com/health`.
6. Open `https://YOUR-RENDER-SERVICE.onrender.com/auth/login`, sign in to cTrader ID, and grant **accounts/read-only** access.
7. If authorization succeeds, copy both tokens shown on the one-time setup page into Render Environment Variables `CTRADER_ACCESS_TOKEN` and `CTRADER_REFRESH_TOKEN`. Also set `CTRADER_ACCESS_TOKEN_EXPIRES_AT` to the displayed expiry timestamp, and set a long random `SETUP_ADMIN_KEY`. Save/redeploy. Do not share any token. This avoids refreshing/rotating the refresh token on every normal restart.
8. Check `/health`, then `/api/symbols` and `/api/quote`.

If the account does not have a symbol named XAUUSD, `/api/symbols` reports available gold-like symbols. Set `CTRADER_SYMBOL_ID` to the correct ID and redeploy. Do not guess symbol IDs.

## 3. Connect the HTML to Render

Open `timedge_ctrader_render.html` and find:

`var WS_URL = 'wss://YOUR-RENDER-SERVICE.onrender.com/ws';`

Replace only the hostname with your actual Render service hostname. Keep `wss://`.

Upload this HTML to InfinityFree as the website's HTML file. Do not add cTrader secrets to the HTML.

## 4. Test locally

Requires Node.js 20 or newer.

```bash
npm install
npm run check
npm start
```

Set environment variables in Render or in a local untracked `.env` file. `.env.example` contains placeholders only. Never commit `.env`.

## Security and deployment notes

- The API is read-only: no order placement code is exposed through the backend routes.
- WebSocket data is public market data; the endpoint does not send access tokens to clients.
- Set `FRONTEND_ORIGIN` to the exact website origin. This is an origin allowlist, not user authentication.
- If the access token eventually expires and the service refreshes it, the refresh token may rotate. Use `POST /admin/tokens` with header `x-setup-key: <SETUP_ADMIN_KEY>` over HTTPS to retrieve the current tokens privately, then update the Render environment variables. Never put this key or tokens in the frontend.
- Do not use a live account until demo data has been verified.
- Render free instances may sleep or restart depending on current plan rules. A sleeping instance cannot guarantee uninterrupted streaming. Check Render's current free-plan limits before relying on it.
- This code is syntax-checked, but a live cTrader connection has not been tested in this environment. Real connection requires your Render environment variables, approved cTrader application, and account authorization.
