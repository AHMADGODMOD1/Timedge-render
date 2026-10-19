GexFlow cTrader XAUUSD source patch

Files:
- index.html: existing UI preserved; removes the old flow.timedge.in host references, routes the cTrader WebSocket bridge to https://timedge-render.onrender.com/ws, and uses actual cTrader bid/ask midpoint updates to draw quote-derived XAUUSD minute candles.
- server.py: current Python/Aiohttp backend source, preserved from the supplied server.py.

Important data limitations:
- XAUUSD quote bars are built from actual cTrader spot quote updates after the page opens. This patch does not fabricate historical candles.
- Genuine XAUUSD trade footprint/aggressor CVD/order-book history is not provided by the current cTrader spot backend; these must not be represented as real cTrader trades/depth until an actual supported source is integrated.
- Historical replay endpoints from the old relay are not implemented by this backend, so those replay panels will not be populated by this patch.

Render backend expected: https://timedge-render.onrender.com
WebSocket: wss://timedge-render.onrender.com/ws
