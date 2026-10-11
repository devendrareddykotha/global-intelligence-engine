"""Environment-driven configuration for live RSS polling."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from urllib.parse import urlparse


MIN_POLL_INTERVAL_SECONDS = 30


@dataclass(frozen=True)
class FeedConfig:
    feed_id: str
    source: str
    category: str
    url: str


@dataclass(frozen=True)
class FeedSettings:
    feeds: tuple[FeedConfig, ...]
    poll_interval_seconds: int
    timeout_seconds: float
    data_mode: str


DEFAULT_FEEDS = (
    FeedConfig("bbc_world", "BBC World", "world", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    FeedConfig("bbc_business", "BBC Business", "business", "https://feeds.bbci.co.uk/news/business/rss.xml"),
    FeedConfig("bbc_technology", "BBC Technology", "technology", "https://feeds.bbci.co.uk/news/technology/rss.xml"),
    FeedConfig("nyt_world", "New York Times World", "world", "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"),
)
FIXTURE_FEEDS = (
    FeedConfig("bbc_fixture", "BBC", "world", "fixture://bbc_feed.xml"),
    FeedConfig("nyt_fixture", "New York Times", "world", "fixture://nytimes_feed.xml"),
)


def load_feed_settings() -> FeedSettings:
    interval = max(
        MIN_POLL_INTERVAL_SECONDS,
        int(os.getenv("GIE_POLL_INTERVAL_SECONDS", "60")),
    )
    timeout = min(60.0, max(1.0, float(os.getenv("GIE_FEED_TIMEOUT_SECONDS", "10"))))
    data_mode = os.getenv("GIE_DATA_MODE", "live").strip().lower()
    if data_mode not in {"live", "fixture"}:
        raise ValueError("GIE_DATA_MODE must be either 'live' or 'fixture'.")
    if data_mode == "fixture":
        return FeedSettings(
            FIXTURE_FEEDS,
            interval,
            timeout,
            data_mode,
        )

    raw_feeds = os.getenv("GIE_FEEDS_JSON")
    if not raw_feeds:
        return FeedSettings(DEFAULT_FEEDS, interval, timeout, data_mode)

    try:
        values = json.loads(raw_feeds)
        feeds = tuple(
            FeedConfig(
                feed_id=str(item["feed_id"]),
                source=str(item["source"]),
                category=str(item.get("category", "general")),
                url=str(item["url"]),
            )
            for item in values
        )
    except (TypeError, ValueError, KeyError) as exc:
        raise ValueError("GIE_FEEDS_JSON must be a JSON array of feed definitions.") from exc

    if not feeds:
        raise ValueError("GIE_FEEDS_JSON must contain at least one feed.")
    if any(not feed.feed_id or not feed.source or not feed.url for feed in feeds):
        raise ValueError("Every configured feed requires feed_id, source, and url.")
    if len({feed.feed_id for feed in feeds}) != len(feeds):
        raise ValueError("Configured feed_id values must be unique.")
    if any(urlparse(feed.url).scheme != "https" for feed in feeds):
        raise ValueError("Configured live RSS feeds must use HTTPS.")

    return FeedSettings(feeds, interval, timeout, data_mode)