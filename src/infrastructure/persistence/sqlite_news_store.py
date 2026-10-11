"""Small, transactional SQLite store for article and signal state."""

from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from pathlib import Path
import sqlite3
import threading
from datetime import datetime
from typing import Iterable, Iterator, List

from src.application.ports.signal_repository import SignalRepository
from src.domain.entities.raw_article import RawArticle
from src.domain.entities.signal import Signal
from src.domain.value_objects.signal_id import SignalId
from src.infrastructure.ingestion.rss_client import deduplicate_articles


class SQLiteNewsStore(SignalRepository):
    def __init__(self, database_path: str | Path | None = None) -> None:
        default_path = Path(__file__).resolve().parents[3] / "data" / "gie.sqlite3"
        self.path = Path(database_path or os.getenv("GIE_DATABASE_PATH", default_path))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._lock, self._connection() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS articles (
                    deduplication_key TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_labels TEXT NOT NULL,
                    category TEXT NOT NULL,
                    categories TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    canonical_url TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    url TEXT NOT NULL,
                    fingerprint TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS signals (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    source_count INTEGER NOT NULL,
                    country_count INTEGER NOT NULL,
                    first_detected_at TEXT NOT NULL,
                    last_updated_at TEXT NOT NULL,
                    velocity REAL NOT NULL,
                    impact_score REAL NOT NULL,
                    is_active INTEGER NOT NULL,
                    categories TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                PRAGMA user_version = 1;
                """
            )

    @staticmethod
    def _article_fingerprint(article: RawArticle) -> str:
        content = json.dumps(
            {
                "title": article.title,
                "description": article.description,
                "sources": article.all_sources,
                "categories": article.all_categories,
                "published_at": article.published_at.isoformat(),
                "url": article.url,
                "identifier": article.identifier,
            },
            sort_keys=True,
        )
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    def upsert_articles(self, articles: Iterable[RawArticle]) -> tuple[int, int]:
        new_count = 0
        changed_count = 0
        unique_articles = deduplicate_articles(articles)
        with self._lock, self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for article in unique_articles:
                key = article.deduplication_key
                existing = connection.execute(
                    "SELECT * FROM articles WHERE deduplication_key = ?",
                    (key,),
                ).fetchone()
                if existing is not None:
                    previous = RawArticle(
                        title=existing["title"],
                        description=existing["description"],
                        source=existing["source"],
                        published_at=datetime.fromisoformat(existing["published_at"]),
                        url=existing["url"],
                        category=existing["category"],
                        identifier=existing["identifier"],
                        canonical_url=existing["canonical_url"],
                        source_labels=tuple(json.loads(existing["source_labels"])),
                        categories=tuple(json.loads(existing["categories"])),
                    )
                    article = deduplicate_articles((previous, article))[0]
                fingerprint = self._article_fingerprint(article)
                if existing is not None and existing["fingerprint"] == fingerprint:
                    continue
                if existing is None:
                    new_count += 1
                else:
                    changed_count += 1
                connection.execute(
                    """
                    INSERT INTO articles (
                        deduplication_key, title, description, source, source_labels,
                        category, categories, identifier, canonical_url, published_at,
                        url, fingerprint
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(deduplication_key) DO UPDATE SET
                        title=excluded.title,
                        description=excluded.description,
                        source=excluded.source,
                        source_labels=excluded.source_labels,
                        category=excluded.category,
                        categories=excluded.categories,
                        identifier=excluded.identifier,
                        canonical_url=excluded.canonical_url,
                        published_at=excluded.published_at,
                        url=excluded.url,
                        fingerprint=excluded.fingerprint
                    """,
                    (
                        key,
                        article.title,
                        article.description,
                        article.source,
                        json.dumps(article.source_labels),
                        article.category,
                        json.dumps(article.categories),
                        article.identifier,
                        article.canonical_url,
                        article.published_at.isoformat(),
                        article.url,
                        fingerprint,
                    ),
                )
        return new_count, changed_count

    def load_articles(self) -> List[RawArticle]:
        with self._lock, self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM articles ORDER BY published_at ASC, deduplication_key ASC"
            ).fetchall()
        return [
            RawArticle(
                title=row["title"],
                description=row["description"],
                source=row["source"],
                published_at=datetime.fromisoformat(row["published_at"]),
                url=row["url"],
                category=row["category"],
                identifier=row["identifier"],
                canonical_url=row["canonical_url"],
                source_labels=tuple(json.loads(row["source_labels"])),
                categories=tuple(json.loads(row["categories"])),
            )
            for row in rows
        ]

    def article_count(self) -> int:
        with self._lock, self._connection() as connection:
            row = connection.execute("SELECT COUNT(*) FROM articles").fetchone()
        return int(row[0])

    def save(self, signal: Signal) -> None:
        values = self._signal_values(signal)
        with self._lock, self._connection() as connection:
            connection.execute(
                """
                INSERT INTO signals (
                    id, title, source_count, country_count, first_detected_at,
                    last_updated_at, velocity, impact_score, is_active, categories
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    title=excluded.title,
                    source_count=excluded.source_count,
                    country_count=excluded.country_count,
                    first_detected_at=excluded.first_detected_at,
                    last_updated_at=excluded.last_updated_at,
                    velocity=excluded.velocity,
                    impact_score=excluded.impact_score,
                    is_active=excluded.is_active,
                    categories=excluded.categories
                """,
                values,
            )

    def save_all(self, signals: List[Signal]) -> None:
        values = [self._signal_values(signal) for signal in signals]
        with self._lock, self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("DELETE FROM signals")
            connection.executemany(
                """
                INSERT INTO signals (
                    id, title, source_count, country_count, first_detected_at,
                    last_updated_at, velocity, impact_score, is_active, categories
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )

    def add_initial(self, signals: List[Signal]) -> None:
        for signal in signals:
            self.save(signal)

    def get_active_signals(self) -> List[Signal]:
        with self._lock, self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM signals WHERE is_active = 1 ORDER BY impact_score DESC"
            ).fetchall()
        return [self._row_to_signal(row) for row in rows]

    def signal_count(self) -> int:
        with self._lock, self._connection() as connection:
            row = connection.execute("SELECT COUNT(*) FROM signals").fetchone()
        return int(row[0])

    def save_metadata(self, key: str, value: object) -> None:
        payload = json.dumps(value, sort_keys=True)
        with self._lock, self._connect() as connection:
            connection.execute(
                "INSERT INTO metadata(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, payload),
            )

    def load_metadata(self, key: str, default: object = None) -> object:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT value FROM metadata WHERE key = ?", (key,)
            ).fetchone()
        return json.loads(row["value"]) if row is not None else default

    @staticmethod
    def _signal_values(signal: Signal) -> tuple[object, ...]:
        return (
            str(signal.id),
            signal.title,
            signal.source_count,
            signal.country_count,
            signal.first_detected_at.isoformat(),
            signal.last_updated_at.isoformat(),
            signal.velocity,
            signal.impact_score,
            int(signal.is_active),
            json.dumps(signal.categories),
        )

    @staticmethod
    def _row_to_signal(row: sqlite3.Row) -> Signal:
        return Signal(
            id=SignalId(row["id"]),
            title=row["title"],
            source_count=row["source_count"],
            country_count=row["country_count"],
            first_detected_at=datetime.fromisoformat(row["first_detected_at"]),
            last_updated_at=datetime.fromisoformat(row["last_updated_at"]),
            velocity=row["velocity"],
            impact_score=row["impact_score"],
            is_active=bool(row["is_active"]),
            categories=tuple(json.loads(row["categories"])),
        )
