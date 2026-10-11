import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Iterable, List
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

import feedparser

from src.domain.entities.raw_article import RawArticle


MAX_FEED_BYTES = 5_000_000
USER_AGENT = "GlobalIntelligenceEngine/1.0 (public RSS reader)"
TRACKING_PARAMETERS = {"fbclid", "gclid", "mc_cid", "mc_eid"}


@dataclass(frozen=True)
class FeedParseResult:
    articles: tuple[RawArticle, ...]
    entries_seen: int
    invalid_entries: int
    parse_warning: str | None = None
    http_status: int | None = None


class FeedFetchError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds


def canonicalize_url(url: str) -> str:
    """Remove fragments and common tracking parameters without changing article links."""
    if not url:
        return ""
    try:
        parts = urlsplit(url.strip())
        if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
            return ""

        host = parts.hostname.lower()
        port = parts.port
    except ValueError:
        return ""

    if port and not (parts.scheme.lower() == "https" and port == 443) and not (
        parts.scheme.lower() == "http" and port == 80
    ):
        host = f"{host}:{port}"

    query_items = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if not key.lower().startswith("utm_") and key.lower() not in TRACKING_PARAMETERS
    ]
    query = urlencode(sorted(query_items))
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower(), host, path, query, ""))


def deduplicate_articles(articles: Iterable[RawArticle]) -> list[RawArticle]:
    """Merge duplicate feed records while retaining every feed/category attribution."""
    unique: dict[str, RawArticle] = {}
    for article in articles:
        key = article.deduplication_key
        if not key:
            key = f"{article.source}:{article.title}:{article.published_at.isoformat()}"

        previous = unique.get(key)
        if previous is None:
            unique[key] = article
            continue

        preferred = article if article.published_at >= previous.published_at else previous
        sources = set(previous.all_sources) | set(article.all_sources)
        categories = set(previous.all_categories) | set(article.all_categories)
        unique[key] = RawArticle(
            title=preferred.title or previous.title,
            description=max((previous.description, article.description), key=len),
            source=preferred.source,
            published_at=max(previous.published_at, article.published_at),
            url=preferred.url or previous.url,
            category=preferred.category,
            identifier=preferred.identifier or previous.identifier,
            canonical_url=preferred.canonical_url or previous.canonical_url,
            source_labels=tuple(sorted(sources - {preferred.source})),
            categories=tuple(sorted(categories - {preferred.category})),
        )
    return list(unique.values())


class RSSClient:
    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self.timeout_seconds = timeout_seconds

    def fetch_from_file(
        self,
        file_path: str,
        source: str | None = None,
        *,
        source_name: str | None = None,
        category: str = "general",
    ) -> List[RawArticle]:
        resolved_source = self._resolve_source(source, source_name)
        with open(file_path, "rb") as feed_file:
            return list(
                self._parse_payload(
                    feed_file.read(), resolved_source, category
                ).articles
            )

    def parse_from_file(
        self,
        file_path: str,
        source: str,
        category: str = "general",
    ) -> FeedParseResult:
        with open(file_path, "rb") as feed_file:
            return self._parse_payload(feed_file.read(), source, category)

    async def fetch_from_url(
        self,
        url: str,
        source: str,
        category: str = "general",
        timeout_seconds: float | None = None,
    ) -> FeedParseResult:
        timeout = timeout_seconds or self.timeout_seconds
        return await asyncio.to_thread(self._fetch_url, url, source, category, timeout)

    @staticmethod
    def _resolve_source(source: str | None, source_name: str | None) -> str:
        if source is not None and source_name is not None and source != source_name:
            raise ValueError("source and source_name must match when both are provided.")
        resolved_source = source if source is not None else source_name
        if not resolved_source:
            raise TypeError("fetch_from_file() requires source or source_name.")
        return resolved_source

    def _fetch_url(
        self,
        url: str,
        source: str,
        category: str,
        timeout_seconds: float,
    ) -> FeedParseResult:
        request = Request(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml",
            },
        )
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                payload = response.read(MAX_FEED_BYTES + 1)
                http_status = response.status
        except HTTPError as exc:
            raise FeedFetchError(
                f"HTTP {exc.code} from RSS endpoint",
                status_code=exc.code,
                retry_after_seconds=self._retry_after(exc.headers.get("Retry-After")),
            ) from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise FeedFetchError(f"RSS request failed: {exc}") from exc

        if len(payload) > MAX_FEED_BYTES:
            raise FeedFetchError("RSS response exceeded the 5 MB size limit.")

        result = self._parse_payload(payload, source, category)
        return FeedParseResult(
            articles=result.articles,
            entries_seen=result.entries_seen,
            invalid_entries=result.invalid_entries,
            parse_warning=result.parse_warning,
            http_status=http_status,
        )

    @staticmethod
    def _retry_after(value: str | None) -> float | None:
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                return None

    @staticmethod
    def _parse_payload(payload: bytes, source: str, category: str) -> FeedParseResult:
        parsed = feedparser.parse(payload)
        articles: list[RawArticle] = []
        invalid_entries = 0

        for entry in parsed.entries:
            title = str(entry.get("title", "")).strip()
            description = str(entry.get("summary") or entry.get("description") or "").strip()
            link = str(entry.get("link", "")).strip()
            identifier = str(entry.get("id") or entry.get("guid") or "").strip()
            canonical = canonicalize_url(link)
            published = entry.get("published_parsed") or entry.get("updated_parsed")

            if not title or not published or (not canonical and not identifier):
                invalid_entries += 1
                continue

            try:
                published_at = datetime(*published[:6], tzinfo=timezone.utc)
                articles.append(
                    RawArticle(
                        title=title,
                        description=description,
                        source=source,
                        published_at=published_at,
                        url=link,
                        category=category,
                        identifier=identifier,
                        canonical_url=canonical,
                    )
                )
            except (TypeError, ValueError, OverflowError):
                invalid_entries += 1

        warning = str(parsed.bozo_exception) if parsed.get("bozo") else None
        return FeedParseResult(
            articles=tuple(deduplicate_articles(articles)),
            entries_seen=len(parsed.entries),
            invalid_entries=invalid_entries,
            parse_warning=warning,
        )