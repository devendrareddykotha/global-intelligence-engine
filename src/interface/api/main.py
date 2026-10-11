from contextlib import asynccontextmanager
import asyncio
import json
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from src.application.services.live_news_engine import LiveNewsEngine
from src.infrastructure.ingestion.feed_settings import load_feed_settings


@asynccontextmanager
async def _lifespan(application: FastAPI) -> AsyncIterator[None]:
    engine: LiveNewsEngine = application.state.engine
    if application.state.polling_enabled:
        engine.start_worker()
    try:
        yield
    finally:
        await engine.stop_worker()


def create_app(
    engine: LiveNewsEngine | None = None,
    *,
    polling_enabled: bool = True,
) -> FastAPI:
    news_engine = engine or LiveNewsEngine(
        load_feed_settings(),
        fixture_directory=Path(__file__).resolve().parents[3],
    )
    application = FastAPI(lifespan=_lifespan)
    application.state.engine = news_engine
    application.state.polling_enabled = polling_enabled

    @application.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/favicon.ico", include_in_schema=False)
    def favicon() -> Response:
        return Response(status_code=204)

    @application.get("/api/status")
    def api_status() -> dict:
        return news_engine.status()

    @application.post("/api/refresh")
    async def api_refresh() -> dict:
        return await news_engine.poll_once()

    @application.get("/api/current")
    def api_current() -> list[dict]:
        return news_engine.current_signals(limit=10)

    @application.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            while True:
                await websocket.send_text(json.dumps(news_engine.current_signals(limit=10)))
                await asyncio.sleep(10)
        except WebSocketDisconnect:
            return

    @application.get("/", response_class=HTMLResponse)
    def dashboard() -> str:
        return _DASHBOARD_HTML

    return application


app = create_app()


_DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Global Intelligence Dashboard</title>
<style>
body{background:#050b14;color:white;font-family:Arial,sans-serif;margin:0}
header{background:#0f172a;padding:20px;font-size:22px}
.container{padding:20px}
.toolbar{display:flex;align-items:center;gap:12px;margin:18px 20px 0}
.status-banner{flex:1;padding:12px 16px;border-radius:8px;font-size:14px;background:#172554;border-left:5px solid #60a5fa;color:#dbeafe}
.status-banner.live{background:#123524;border-left-color:#34d399;color:#d1fae5}
.status-banner.degraded,.status-banner.stale{background:#451a1a;border-left-color:#f87171;color:#fee2e2}
button{background:#0284c7;color:white;border:0;border-radius:6px;padding:10px 14px;font-weight:600;cursor:pointer}
button:disabled{opacity:.6;cursor:wait}
.card{background:#0f172a;padding:15px;margin-bottom:12px;border-left:5px solid #38bdf8}
.card h2{font-size:18px;margin-top:0}
.card a{color:#38bdf8;text-decoration:none}
.card a:hover{text-decoration:underline}
.metadata{color:#cbd5e1;font-size:14px}
.published{color:#94a3b8;font-size:13px}
#refresh-error{color:#fca5a5;margin:8px 20px 0}
</style>
</head>
<body>
<header>🌍 Global Intelligence Dashboard</header>
<div class="toolbar">
  <div id="data-status" class="status-banner" role="status">Connecting to feeds…</div>
  <button id="refresh-button" type="button">Refresh now</button>
</div>
<div id="refresh-error" role="alert"></div>
<div class="container" id="signals" aria-live="polite"></div>
<script>
const statusElement = document.getElementById("data-status");
const signalsElement = document.getElementById("signals");
const refreshButton = document.getElementById("refresh-button");
const refreshError = document.getElementById("refresh-error");

function localTime(value) {
  return value ? new Date(value).toLocaleString() : "Not available";
}

function createSignalCard(signal, rank) {
  const card = document.createElement("article");
  card.className = "card";
  const heading = document.createElement("h2");
  if (signal.link && /^https?:\/\//i.test(signal.link)) {
    const link = document.createElement("a");
    link.href = signal.link;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    link.textContent = `#${rank} ${signal.title}`;
    heading.appendChild(link);
  } else {
    heading.textContent = `#${rank} ${signal.title}`;
  }
  const metadata = document.createElement("p");
  metadata.className = "metadata";
  metadata.textContent = `Impact ${signal.impact} · ${signal.sources} sources · ${signal.article_count} articles${signal.categories?.length ? ` · ${signal.categories.join(", ")}` : ""}`;
  const summary = document.createElement("p");
  summary.textContent = signal.summary || "No summary available.";
  const published = document.createElement("p");
  published.className = "published";
  published.textContent = `Latest article published: ${localTime(signal.published_at)}`;
  card.append(heading, metadata, summary, published);
  return card;
}

function renderStatus(status) {
  const state = status.status || "DEGRADED";
  const isDegraded = state === "DEGRADED" || status.stale;
  statusElement.className = `status-banner ${isDegraded ? "degraded" : "live"}`;
  const mode = (status.data_mode || "unknown").toUpperCase();
  const feedCounts = `${status.healthy_feeds}/${status.configured_feeds} feeds healthy · ${status.failed_feeds} failed`;
  const feedStates = (status.feeds || []).map(feed => `${feed.source}: ${feed.state}`).join(" · ");
  const freshness = status.stale ? " · STALE CONTENT" : "";
  const success = `Last successful: ${localTime(status.last_successful_poll)}`;
  const next = `Next poll: ${localTime(status.next_poll_at)}`;
  const totals = `${status.new_articles_received} new this refresh · ${status.total_articles} stored articles · ${status.signal_count} signals`;
  statusElement.textContent = `${state} · ${mode}${freshness} · ${feedCounts} · ${status.stale_feeds} stale feeds · ${success} · ${next} · ${totals} · ${feedStates}`;
  if (status.errors?.length) {
    refreshError.textContent = status.errors.join(" | ");
  } else if (!refreshButton.disabled) {
    refreshError.textContent = "";
  }
}

async function refreshDashboard() {
  try {
    const [statusResponse, signalsResponse] = await Promise.all([
      fetch("/api/status", {cache: "no-store"}),
      fetch("/api/current", {cache: "no-store"})
    ]);
    if (!statusResponse.ok || !signalsResponse.ok) {
      throw new Error(`API request failed (${statusResponse.status}/${signalsResponse.status})`);
    }
    const [status, signals] = await Promise.all([
      statusResponse.json(),
      signalsResponse.json()
    ]);
    renderStatus(status);
    signalsElement.replaceChildren(...signals.map((signal, index) => createSignalCard(signal, index + 1)));
  } catch (error) {
    statusElement.className = "status-banner degraded";
    statusElement.textContent = "DEGRADED · Unable to retrieve current feed or signal status.";
    refreshError.textContent = error.message;
  }
}

refreshButton.addEventListener("click", async () => {
  refreshButton.disabled = true;
  refreshButton.textContent = "Refreshing…";
  refreshError.textContent = "";
  try {
    const response = await fetch("/api/refresh", {method: "POST"});
    const result = await response.json();
    if (!response.ok) throw new Error(result.detail || `Refresh failed (${response.status})`);
    if (result.refresh_skipped) refreshError.textContent = result.refresh_skipped;
    await refreshDashboard();
  } catch (error) {
    refreshError.textContent = error.message;
  } finally {
    refreshButton.disabled = false;
    refreshButton.textContent = "Refresh now";
  }
});

refreshDashboard();
setInterval(refreshDashboard, 10000);
</script>
</body>
</html>
"""
