# Global Intelligence Engine

Global Intelligence Engine (GIE) is an offline-testable MVP that polls public RSS feeds, deduplicates and categorizes articles, groups related titles into signals, calculates transparent impact scores, and serves a live dashboard/API. It reads publisher-provided feeds only; it does not scrape paywalled pages.

## Setup

Requires Python 3.10 or newer. From the repository root in PowerShell:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

## Start the live application

From the repository root:

```powershell
python -m uvicorn src.interface.api.main:app --host 127.0.0.1 --port 8000
```

The FastAPI lifespan starts one background feed poller, performs an initial poll, and shuts the worker down on application shutdown. Use one Uvicorn worker for this local MVP. Feed requests run concurrently in worker threads with timeouts, and ordinary dashboard/API requests do not wait for a feed poll. The default polling interval is 60 seconds; publisher updates may be less frequent. The browser independently refreshes dashboard data every 10 seconds.

## Dashboard and API

Open `http://127.0.0.1:8000/` for the dashboard. It displays live/degraded status, live/cached/fixture/unavailable mode, per-feed health, last successful poll, next poll, a stale-content flag (publication data older than 24 hours), article/signal totals, new articles from the latest poll, scores, counts, categories, local publication timestamps, and publisher links. Use **Refresh now** to request a serialized manual refresh.

- `GET /health` — process health.
- `GET /api/status` — data mode, poll timestamps, next scheduled poll, interval, configured/healthy/failed feed counts, per-feed errors/health, new and changed article counts, total stored articles, signal count, newest publication time, and stale flag.
- `GET /api/current` — up to ten active signals ranked by current impact score, including article/source counts, categories, UTC publication timestamp, summary, and source link.
- `POST /api/refresh` — request an immediate poll. Concurrent requests do not start a second simultaneous poll; one receives the in-progress status.
- `WebSocket /ws` — sends current ranked signals every 10 seconds without triggering additional feed requests.
- `GET /favicon.ico` — `204 No Content` (no favicon asset is shipped).

`LIVE` means all configured feeds are healthy on a recent poll. Partial failures report `DEGRADED`; previously stored data is labeled `cached` when a recent live poll has not succeeded. Old publication timestamps are separately labeled stale (over 24 hours), even when feeds are reachable.

## Feed and storage configuration

Default live feeds are configured in `src/infrastructure/ingestion/feed_settings.py`:

- BBC World News
- BBC Business
- BBC Technology
- The New York Times World News

They are public HTTPS RSS endpoints. They can be replaced with a JSON array in `GIE_FEEDS_JSON`, for example:

```powershell
$env:GIE_FEEDS_JSON = '[{"feed_id":"example_world","source":"Example World","category":"world","url":"https://example.org/world.xml"}]'
```

Other environment options:

- `GIE_POLL_INTERVAL_SECONDS` — default `60`; values below 30 are raised to the 30-second minimum.
- `GIE_FEED_TIMEOUT_SECONDS` — default `10`; bounded to 1–60 seconds.
- `GIE_DATA_MODE=fixture` — explicitly use the checked-in XML files; status reports `FIXTURE`, never `LIVE`.
- `GIE_DATABASE_PATH` — SQLite path; default is `data/gie.sqlite3`.

SQLite uses safe `CREATE TABLE IF NOT EXISTS` initialization, WAL mode, transactional article upserts, and unique canonical article keys. Articles, signals, feed health, and poll metadata survive restarts. Generated database files are ignored by Git. The RSS client retains the original publisher URL while using a canonicalized URL/stable identifier for deduplication. Entries without a usable publication timestamp are rejected rather than assigned a fabricated publish time.

## Offline run and tests

The deterministic offline NYT pipeline remains available:

```powershell
python run_pipeline.py
```

Run tests from the repository root:

```powershell
python -m pytest -q
```

Automated HTTP ingestion tests mock the network. The suite also covers parsing, UTC timestamps, canonical URL deduplication, feed failures/rate limits, polling serialization, SQLite restart persistence, API status/refresh, ranking, and fixture mode. Tests do not require internet access.

## Scoring and limitations

Impact uses the current signal metrics and actual UTC publication times:

`2 × source_count + 3 × country_count + 1.5 × velocity + 5 × freshness_factor`

The freshness factor is clamped to $[0, 1]$ and linearly decays to zero over 24 hours. Polling alone does not increase a score; the score is recalculated against the article timestamp and current time. Aggregation is a title-token overlap heuristic (threshold 0.3), not a validated event-resolution model.

- Feed availability, publisher rate limits, and RSS publication cadence determine when new stories appear. Polling every minute cannot guarantee a new story every minute.
- Feed health means the configured endpoint returned parseable RSS recently; it does not certify article accuracy or completeness.
- `source_count` counts distinct feed labels; the same publisher's separate feeds can count separately. `country_count` is currently set to 1 because geographic entity resolution is not implemented.
- `velocity` currently represents grouped article count, not a time-normalized rate. Summaries are shortened descriptions from the latest matching article, not synthesized multi-source intelligence.
- SQLite article history currently has no automatic retention/compaction policy; storage can grow over long runtimes.
- Impact scores are explainable heuristics, not scientifically validated predictions. This remains a local MVP, not a production-ready alerting or intelligence service.
