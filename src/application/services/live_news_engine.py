from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path
from typing import Any

from src.application.use_cases.generate_top_signals import GenerateTopSignalsUseCase
from src.application.use_cases.refresh_signals import RefreshSignalsFromFeedsUseCase
from src.domain.entities.raw_article import RawArticle
from src.domain.services.impact_scoring_service import ImpactScoringService
from src.domain.services.signal_aggregation_service import SignalAggregationService
from src.infrastructure.ingestion.feed_settings import FeedConfig, FeedSettings
from src.infrastructure.ingestion.rss_client import (
    FeedFetchError,
    FeedParseResult,
    RSSClient,
    deduplicate_articles,
)
from src.infrastructure.persistence.sqlite_news_store import SQLiteNewsStore


logger = logging.getLogger(__name__)
STALE_AFTER = timedelta(hours=24)


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        result = datetime.fromisoformat(value)
        return result if result.tzinfo is not None else result.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


class LiveNewsEngine:
    """Fetch, deduplicate, persist, score, and rank public RSS articles."""

    def __init__(
        self,
        settings: FeedSettings,
        *,
        store: SQLiteNewsStore | None = None,
        rss_client: RSSClient | None = None,
        fixture_directory: str | Path | None = None,
    ) -> None:
        self.settings = settings
        self.store = store or SQLiteNewsStore()
        self.rss_client = rss_client or RSSClient(settings.timeout_seconds)
        self.fixture_directory = Path(
            fixture_directory or Path(__file__).resolve().parents[2]
        )
        self.aggregation_service = SignalAggregationService()
        self.scoring_service = ImpactScoringService()
        self.refresh_use_case = RefreshSignalsFromFeedsUseCase(
            self.store,
            self.aggregation_service,
        )
        self.generate_use_case = GenerateTopSignalsUseCase(
            self.store,
            self.scoring_service,
        )
        self._poll_lock = asyncio.Lock()
        self._worker_task: asyncio.Task[None] | None = None

        saved_status = self.store.load_metadata("engine_status", {})
        if not isinstance(saved_status, dict):
            saved_status = {}
        self._last_attempted_poll = _parse_timestamp(saved_status.get("last_attempted_poll"))
        self._last_successful_poll = _parse_timestamp(saved_status.get("last_successful_poll"))
        self._next_poll_at = _parse_timestamp(saved_status.get("next_poll_at"))
        self._new_articles_received = int(saved_status.get("new_articles_received", 0))
        self._changed_articles_received = int(saved_status.get("changed_articles_received", 0))
        self._last_refresh_errors = list(saved_status.get("errors", []))
        self._last_poll_had_success = bool(saved_status.get("last_poll_had_success", False))

        saved_feeds = self.store.load_metadata("feed_states", {})
        if not isinstance(saved_feeds, dict):
            saved_feeds = {}
        self._feed_states: dict[str, dict[str, Any]] = {}
        for feed in settings.feeds:
            state = {
                "state": "pending",
                "last_attempted_at": None,
                "last_successful_at": None,
                "http_status": None,
                "article_count": 0,
                "invalid_entries": 0,
                "error": None,
                "next_retry_at": None,
            }
            state.update(saved_feeds.get(feed.feed_id, {}))
            self._feed_states[feed.feed_id] = state

    async def poll_once(self) -> dict[str, Any]:
        if self._poll_lock.locked():
            snapshot = self.status()
            snapshot["refresh_skipped"] = "A refresh is already in progress."
            return snapshot

        async with self._poll_lock:
            attempted_at = datetime.now(timezone.utc)
            self._last_attempted_poll = attempted_at
            self._new_articles_received = 0
            self._changed_articles_received = 0
            self._last_refresh_errors = []
            successful_responses = 0
            fetched_articles: list[RawArticle] = []
            errors: list[str] = []

            outcomes = await asyncio.gather(
                *(self._fetch_feed(feed, attempted_at) for feed in self.settings.feeds),
                return_exceptions=True,
            )

            for feed, outcome in zip(self.settings.feeds, outcomes):
                state = self._feed_states[feed.feed_id]
                if isinstance(outcome, BaseException):
                    if isinstance(outcome, asyncio.CancelledError):
                        raise outcome
                    state["state"] = (
                        "rate_limited"
                        if isinstance(outcome, FeedFetchError)
                        and outcome.status_code == 429
                        else "failed"
                    )
                    state["error"] = str(outcome)[:300]
                    state["http_status"] = (
                        outcome.status_code if isinstance(outcome, FeedFetchError) else None
                    )
                    state["article_count"] = 0
                    state["invalid_entries"] = 0
                    if isinstance(outcome, FeedFetchError) and outcome.status_code == 429:
                        retry_at = attempted_at + timedelta(
                            seconds=(
                                outcome.retry_after_seconds
                                if outcome.retry_after_seconds is not None
                                else max(60, self.settings.poll_interval_seconds)
                            )
                        )
                        state["next_retry_at"] = retry_at.isoformat()
                    errors.append(f"{feed.feed_id}: {state['error']}")
                    continue

                if outcome is None:
                    continue

                result = outcome
                state["http_status"] = result.http_status
                state["article_count"] = len(result.articles)
                state["invalid_entries"] = result.invalid_entries
                state["last_successful_at"] = attempted_at.isoformat()
                feed_usable = bool(result.articles) or (
                    result.parse_warning is None and result.invalid_entries == 0
                )
                if feed_usable:
                    successful_responses += 1
                if result.parse_warning or result.invalid_entries:
                    state["state"] = "degraded"
                    state["error"] = result.parse_warning or (
                        f"Skipped {result.invalid_entries} malformed entries."
                    )
                    errors.append(f"{feed.feed_id}: {state['error']}")
                else:
                    state["state"] = "healthy"
                    state["error"] = None
                state["next_retry_at"] = None
                fetched_articles.extend(result.articles)

            changed_count = 0
            try:
                unique_articles = deduplicate_articles(fetched_articles)
                self._new_articles_received, changed_count = self.store.upsert_articles(
                    unique_articles
                )
                self._changed_articles_received = changed_count

                if changed_count or self._new_articles_received or not self.store.signal_count():
                    all_articles = self.store.load_articles()
                    signals = self.refresh_use_case.execute(all_articles)
                else:
                    signals = self.store.get_active_signals()

                self.generate_use_case.execute(
                    attempted_at,
                    limit=max(1, len(signals)),
                )
            except Exception as exc:
                logger.exception("Unable to update GIE article/signal state")
                errors.append(f"pipeline: {type(exc).__name__}: {exc}")

            if successful_responses:
                self._last_successful_poll = attempted_at
            self._last_poll_had_success = successful_responses > 0
            self._last_refresh_errors = errors
            self._next_poll_at = datetime.now(timezone.utc) + timedelta(
                seconds=self.settings.poll_interval_seconds
            )
            self._persist_status()
            result = self.status()
            result["refresh_in_progress"] = False
            return result

    async def _fetch_feed(
        self,
        feed: FeedConfig,
        attempted_at: datetime,
    ) -> FeedParseResult | None:
        state = self._feed_states[feed.feed_id]
        retry_at = _parse_timestamp(state.get("next_retry_at"))
        if retry_at is not None and retry_at > attempted_at:
            state["state"] = "rate_limited"
            return None

        state["last_attempted_at"] = attempted_at.isoformat()
        if self.settings.data_mode == "fixture":
            fixture_name = feed.url.removeprefix("fixture://")
            fixture_path = self.fixture_directory / fixture_name
            return await asyncio.to_thread(
                self.rss_client.parse_from_file,
                str(fixture_path),
                feed.source,
                feed.category,
            )
        return await self.rss_client.fetch_from_url(
            feed.url,
            feed.source,
            feed.category,
            self.settings.timeout_seconds,
        )

    async def poll_forever(self) -> None:
        while True:
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("GIE background polling failed")
            await asyncio.sleep(self.settings.poll_interval_seconds)

    def start_worker(self) -> asyncio.Task[None]:
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(
                self.poll_forever(),
                name="gie-rss-poller",
            )
        return self._worker_task

    async def stop_worker(self) -> None:
        task = self._worker_task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._worker_task = None

    def current_signals(self, limit: int = 10) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc)
        signals = self.generate_use_case.execute(now, limit=limit)
        article_by_title = {
            article.title: article for article in self.store.load_articles()
        }
        results = []
        for signal in signals:
            article = article_by_title.get(signal.title)
            results.append(
                {
                    "id": str(signal.id),
                    "title": signal.title,
                    "impact": round(signal.impact_score, 2),
                    "sources": signal.source_count,
                    "article_count": int(signal.velocity),
                    "categories": list(signal.categories),
                    "published_at": signal.last_updated_at.isoformat(),
                    "summary": (
                        article.description[:500]
                        if article is not None and article.description
                        else "No summary available."
                    ),
                    "link": article.url if article is not None else "",
                }
            )
        return results

    def status(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        articles = self.store.load_articles()
        latest_article_at = max(
            (article.published_at for article in articles),
            default=None,
        )
        stale = (
            latest_article_at is None
            or now - latest_article_at > STALE_AFTER
        )
        recent_poll_window = timedelta(
            seconds=max(120, self.settings.poll_interval_seconds * 2)
        )
        feed_values = []
        for feed in self.settings.feeds:
            state = dict(self._feed_states[feed.feed_id])
            last_feed_success = _parse_timestamp(state.get("last_successful_at"))
            if (
                state["state"] == "healthy"
                and (
                    last_feed_success is None
                    or now - last_feed_success > recent_poll_window
                )
            ):
                state["state"] = "stale"
            feed_values.append(
                {
                    "feed_id": feed.feed_id,
                    "source": feed.source,
                    "category": feed.category,
                    **state,
                }
            )

        healthy_feeds = sum(feed["state"] == "healthy" for feed in feed_values)
        failed_feeds = sum(
            feed["state"] in {"failed", "rate_limited", "degraded"}
            for feed in feed_values
        )
        stale_feeds = sum(feed["state"] == "stale" for feed in feed_values)
        if self.settings.data_mode == "fixture":
            overall_status = "FIXTURE" if failed_feeds == 0 and healthy_feeds else "DEGRADED"
            data_mode = "fixture"
        elif self._last_attempted_poll is None:
            overall_status = "STARTING"
            data_mode = "live_pending"
        elif (
            self._last_successful_poll is None
            or now - self._last_successful_poll > recent_poll_window
        ):
            overall_status = "DEGRADED"
            data_mode = "cached" if articles else "unavailable"
        elif failed_feeds or healthy_feeds < len(self.settings.feeds):
            overall_status = "DEGRADED"
            data_mode = "live" if self._last_poll_had_success else (
                "cached" if articles else "unavailable"
            )
        else:
            overall_status = "LIVE"
            data_mode = "live"

        return {
            "status": overall_status,
            "data_mode": data_mode,
            "last_attempted_poll": (
                self._last_attempted_poll.isoformat()
                if self._last_attempted_poll
                else None
            ),
            "last_successful_poll": (
                self._last_successful_poll.isoformat()
                if self._last_successful_poll
                else None
            ),
            "next_poll_at": (
                max(self._next_poll_at, now).isoformat()
                if self._next_poll_at is not None
                else None
            ),
            "poll_interval_seconds": self.settings.poll_interval_seconds,
            "configured_feeds": len(self.settings.feeds),
            "healthy_feeds": healthy_feeds,
            "failed_feeds": failed_feeds,
            "stale_feeds": stale_feeds,
            "feeds": feed_values,
            "new_articles_received": self._new_articles_received,
            "changed_articles_received": self._changed_articles_received,
            "total_articles": self.store.article_count(),
            "signal_count": self.store.signal_count(),
            "latest_article_at": latest_article_at.isoformat() if latest_article_at else None,
            "stale": stale,
            "errors": list(self._last_refresh_errors),
            "refresh_in_progress": self._poll_lock.locked(),
        }

    def _persist_status(self) -> None:
        self.store.save_metadata(
            "engine_status",
            {
                "last_attempted_poll": (
                    self._last_attempted_poll.isoformat()
                    if self._last_attempted_poll
                    else None
                ),
                "last_successful_poll": (
                    self._last_successful_poll.isoformat()
                    if self._last_successful_poll
                    else None
                ),
                "next_poll_at": self._next_poll_at.isoformat() if self._next_poll_at else None,
                "new_articles_received": self._new_articles_received,
                "changed_articles_received": self._changed_articles_received,
                "errors": self._last_refresh_errors,
                "last_poll_had_success": self._last_poll_had_success,
            },
        )
        self.store.save_metadata("feed_states", self._feed_states)
