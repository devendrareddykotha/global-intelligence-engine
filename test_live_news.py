import asyncio
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import httpx

from src.application.services.live_news_engine import LiveNewsEngine
from src.domain.entities.raw_article import RawArticle
from src.infrastructure.ingestion.feed_settings import (
    DEFAULT_FEEDS,
    FIXTURE_FEEDS,
    FeedConfig,
    FeedSettings,
    load_feed_settings,
)
from src.infrastructure.ingestion.rss_client import (
    FeedFetchError,
    FeedParseResult,
    RSSClient,
    canonicalize_url,
    deduplicate_articles,
)
from src.infrastructure.persistence.sqlite_news_store import SQLiteNewsStore
from src.interface.api.main import create_app


SAMPLE_FEED = b"""<?xml version='1.0' encoding='utf-8'?>
<rss version='2.0'><channel><title>Test</title>
<item><title>Global summit begins</title><link>https://news.example/story?utm_source=rss</link>
<guid>story-1</guid><description>Summit details.</description>
<pubDate>Tue, 06 Oct 2026 17:30:00 +0530</pubDate></item>
<item><title>Global summit begins</title><link>https://news.example/story#top</link>
<guid>story-duplicate</guid><description>Summit details.</description>
<pubDate>Tue, 06 Oct 2026 17:30:00 +0530</pubDate></item>
<item><title>Missing date</title><link>https://news.example/no-date</link></item>
</channel></rss>"""


class FakeResponse(BytesIO):
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class FakeRSSClient:
    def __init__(self, articles_by_url, errors_by_url=None):
        self.articles_by_url = articles_by_url
        self.errors_by_url = errors_by_url or {}
        self.calls = {}

    async def fetch_from_url(self, url, source, category, timeout_seconds):
        self.calls[url] = self.calls.get(url, 0) + 1
        await asyncio.sleep(0)
        if url in self.errors_by_url:
            raise self.errors_by_url[url]
        articles = tuple(
            RawArticle(
                title=article.title,
                description=article.description,
                source=source,
                published_at=article.published_at,
                url=article.url,
                category=category,
                identifier=article.identifier,
                canonical_url=article.canonical_url,
            )
            for article in self.articles_by_url[url]
        )
        return FeedParseResult(articles, len(articles), 0, http_status=200)


class BlockingFakeRSSClient(FakeRSSClient):
    def __init__(self, articles_by_url):
        super().__init__(articles_by_url)
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def fetch_from_url(self, url, source, category, timeout_seconds):
        self.calls[url] = self.calls.get(url, 0) + 1
        self.started.set()
        await self.release.wait()
        articles = tuple(
            RawArticle(
                title=article.title,
                description=article.description,
                source=source,
                published_at=article.published_at,
                url=article.url,
                category=category,
                identifier=article.identifier,
                canonical_url=article.canonical_url,
            )
            for article in self.articles_by_url[url]
        )
        return FeedParseResult(articles, len(articles), 0, http_status=200)


def test_rss_http_fetch_is_mocked_and_normalizes_publication_time(monkeypatch):
    seen = {}

    def fake_urlopen(request, timeout):
        seen["url"] = request.full_url
        seen["user_agent"] = request.get_header("User-agent")
        seen["timeout"] = timeout
        return FakeResponse(SAMPLE_FEED)

    monkeypatch.setattr("src.infrastructure.ingestion.rss_client.urlopen", fake_urlopen)
    result = asyncio.run(
        RSSClient().fetch_from_url(
            "https://news.example/rss.xml",
            "Example News",
            "world",
            timeout_seconds=3,
        )
    )

    assert seen == {
        "url": "https://news.example/rss.xml",
        "user_agent": "GlobalIntelligenceEngine/1.0 (public RSS reader)",
        "timeout": 3,
    }
    assert result.http_status == 200
    assert result.entries_seen == 3
    assert result.invalid_entries == 1
    assert len(result.articles) == 1
    assert result.articles[0].published_at == datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
    assert result.articles[0].canonical_url == "https://news.example/story"


def test_canonical_url_dedup_retains_source_and_category_attribution():
    published_at = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
    articles = [
        RawArticle(
            "Global summit begins", "BBC report", "BBC World", published_at,
            "https://news.example/story?utm_source=bbc", "world",
            canonical_url=canonicalize_url("https://news.example/story?utm_source=bbc"),
        ),
        RawArticle(
            "Global summit begins", "Expanded report", "BBC Business", published_at,
            "https://news.example/story#top", "business",
            canonical_url=canonicalize_url("https://news.example/story#top"),
        ),
    ]

    unique = deduplicate_articles(articles)

    assert canonicalize_url(articles[0].url) == canonicalize_url(articles[1].url)
    assert len(unique) == 1
    assert set(unique[0].all_sources) == {"BBC World", "BBC Business"}
    assert set(unique[0].all_categories) == {"world", "business"}
    assert unique[0].description == "Expanded report"


def test_feed_configuration_has_safe_poll_minimum(monkeypatch):
    monkeypatch.setenv("GIE_POLL_INTERVAL_SECONDS", "1")
    monkeypatch.delenv("GIE_FEEDS_JSON", raising=False)
    monkeypatch.delenv("GIE_DATA_MODE", raising=False)

    settings = load_feed_settings()

    assert settings.poll_interval_seconds == 30
    assert settings.feeds == DEFAULT_FEEDS
    assert len(settings.feeds) >= 3


def test_partial_feed_failure_persists_deduplicates_and_scores(tmp_path):
    published_at = datetime.now(timezone.utc) - timedelta(minutes=2)
    article = RawArticle(
        "Global summit response", "Officials described the response.", "",
        published_at, "https://news.example/response",
        canonical_url="https://news.example/response",
    )
    good_url = "https://good.example/rss"
    bad_url = "https://bad.example/rss"
    settings = FeedSettings(
        (
            FeedConfig("good", "Good Source", "world", good_url),
            FeedConfig("bad", "Unavailable Source", "world", bad_url),
        ),
        60,
        2,
        "live",
    )
    client = FakeRSSClient(
        {good_url: [article]},
        {bad_url: FeedFetchError("HTTP 503 from RSS endpoint", status_code=503)},
    )
    database_path = tmp_path / "gie.sqlite3"
    store = SQLiteNewsStore(database_path)
    engine = LiveNewsEngine(settings, store=store, rss_client=client)

    async def run_two_polls():
        first = await engine.poll_once()
        second = await engine.poll_once()
        return first, second

    first, second = asyncio.run(run_two_polls())

    assert first["status"] == "DEGRADED"
    assert first["data_mode"] == "live"
    assert first["configured_feeds"] == 2
    assert first["healthy_feeds"] == 1
    assert first["failed_feeds"] == 1
    assert first["new_articles_received"] == 1
    assert first["total_articles"] == 1
    assert first["signal_count"] == 1
    assert first["errors"]
    assert second["new_articles_received"] == 0
    assert second["total_articles"] == 1
    ranked = engine.current_signals()
    assert ranked[0]["impact"] > 0
    assert ranked[0]["sources"] == 1

    reopened = SQLiteNewsStore(database_path)
    assert reopened.article_count() == 1
    assert reopened.signal_count() == 1
    assert reopened.load_articles()[0].title == article.title
    assert reopened.load_metadata("engine_status")["last_successful_poll"]


def test_manual_refresh_is_serialized_and_new_articles_are_added(tmp_path):
    feed_url = "https://mock.example/rss"
    first_article = RawArticle(
        "Global summit update", "First report.", "Mock News",
        datetime.now(timezone.utc), "https://news.example/first",
        canonical_url="https://news.example/first",
    )
    second_article = RawArticle(
        "Markets respond to policy", "Second report.", "Mock News",
        datetime.now(timezone.utc), "https://news.example/second",
        canonical_url="https://news.example/second",
    )
    settings = FeedSettings(
        (FeedConfig("mock", "Mock News", "world", feed_url),),
        60,
        2,
        "live",
    )

    async def scenario():
        client = BlockingFakeRSSClient({feed_url: [first_article]})
        engine = LiveNewsEngine(
            settings,
            store=SQLiteNewsStore(tmp_path / "concurrent.sqlite3"),
            rss_client=client,
        )
        running_refresh = asyncio.create_task(engine.poll_once())
        await client.started.wait()
        duplicate_refresh = await engine.poll_once()
        assert duplicate_refresh["refresh_skipped"]
        assert client.calls[feed_url] == 1
        client.release.set()
        first_status = await running_refresh
        assert first_status["new_articles_received"] == 1

        client.articles_by_url[feed_url].append(second_article)
        updated_status = await engine.poll_once()
        assert updated_status["new_articles_received"] == 1
        assert updated_status["total_articles"] == 2
        assert updated_status["signal_count"] == 2

    asyncio.run(scenario())


def test_rate_limited_feed_honors_retry_after(tmp_path):
    feed_url = "https://limited.example/rss"
    settings = FeedSettings(
        (FeedConfig("limited", "Limited Source", "world", feed_url),),
        60,
        2,
        "live",
    )
    client = FakeRSSClient(
        {},
        {
            feed_url: FeedFetchError(
                "HTTP 429 from RSS endpoint",
                status_code=429,
                retry_after_seconds=3600,
            )
        },
    )
    engine = LiveNewsEngine(
        settings,
        store=SQLiteNewsStore(tmp_path / "limited.sqlite3"),
        rss_client=client,
    )

    async def run_two_attempts():
        first = await engine.poll_once()
        second = await engine.poll_once()
        return first, second

    first, second = asyncio.run(run_two_attempts())

    assert first["feeds"][0]["state"] == "rate_limited"
    assert first["feeds"][0]["next_retry_at"]
    assert second["feeds"][0]["state"] == "rate_limited"
    assert client.calls[feed_url] == 1


def test_fixture_mode_is_explicit_and_never_claimed_live(tmp_path):
    settings = FeedSettings(FIXTURE_FEEDS, 60, 2, "fixture")
    engine = LiveNewsEngine(
        settings,
        store=SQLiteNewsStore(tmp_path / "fixture.sqlite3"),
        fixture_directory=Path(__file__).resolve().parent,
    )

    status = asyncio.run(engine.poll_once())

    assert status["status"] == "FIXTURE"
    assert status["data_mode"] == "fixture"
    assert status["stale"] is True
    assert status["configured_feeds"] == 2
    assert status["total_articles"] > 0
    assert all(feed["state"] == "healthy" for feed in status["feeds"])


def test_api_status_refresh_and_current_use_mocked_feeds(tmp_path):
    published_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    article = RawArticle(
        "API fixture event", "A test description.", "", published_at,
        "https://news.example/api-event",
        canonical_url="https://news.example/api-event",
    )
    feed_url = "https://mock.example/rss"
    settings = FeedSettings(
        (FeedConfig("mock", "Mock News", "world", feed_url),),
        60,
        2,
        "live",
    )
    engine = LiveNewsEngine(
        settings,
        store=SQLiteNewsStore(tmp_path / "api.sqlite3"),
        rss_client=FakeRSSClient({feed_url: [article]}),
    )
    test_app = create_app(engine, polling_enabled=False)

    async def check_endpoints():
        async with test_app.router.lifespan_context(test_app):
            transport = httpx.ASGITransport(app=test_app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as client:
                assert (await client.get("/health")).json() == {"status": "ok"}
                status = await client.get("/api/status")
                assert status.json()["data_mode"] == "live_pending"
                refreshed = await client.post("/api/refresh")
                assert refreshed.status_code == 200
                assert refreshed.json()["status"] == "LIVE"
                current = await client.get("/api/current")
                assert current.status_code == 200
                assert current.json()[0]["title"] == "API fixture event"
                assert current.json()[0]["impact"] > 0
                assert current.json()[0]["article_count"] == 1
                assert (await client.get("/favicon.ico")).status_code == 204

    asyncio.run(check_endpoints())
