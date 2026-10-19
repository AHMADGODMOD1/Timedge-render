GexFlow cTrader XAUUSD Chart Fix

Deploy these files together on the existing Render Python service:
- index.html
- server.py

Fixes included:
- Removed all references to flow.timedge.in from index.html.
- Removed the intrusive fixed cTrader status popup and the gold waiting toast.
- Added cTrader Open API M1 trendbar request (up to the last 3 days / 5,000 bars) and WebSocket delivery to the chart.
- Added live cTrader bid/ask midpoint updates to the current XAUUSD candle.
- When the page reconnects, the backend replays cached cTrader candles to the browser.
- Removed the manually scheduled Twisted heartbeat send that was producing repeated Deferred TimeoutError messages every 10 seconds; cTrader SDK connection handling remains in place.
- Kept the existing interface and crypto feed code.

Render settings:
Build command: pip install aiohttp ctrader-open-api
Start command: python server.py
Keep cTrader credentials in Render Environment Variables only.

Data integrity:
Candles are real cTrader trendbars when cTrader returns them, followed by quote-derived live minute updates. cTrader OTC spot quotes do not provide true aggressor-side executed trades or a historical exchange order book; therefore this does not fabricate genuine XAUUSD footprint/CVD/depth history. The old relay replay endpoints are not provided by this backend.

After deploy, verify /health and /api/markets. In /api/markets, cTrader should reach LIVE; historyBars should be nonzero after the trendbar response arrives. If cTrader shows ERROR, share the new error text (do not share tokens/secrets).
