# src/domain/entities/raw_article.py

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class RawArticle:
    """
    Represents a single news article from an external source.

    This is NOT yet a Signal.
    It is raw input data.

    Immutable because raw ingestion data
    should not be mutated after creation.
    """

    title: str
    description: str
    source: str
    published_at: datetime
    url: str
    category: str = "general"
    identifier: str = ""
    canonical_url: str = ""
    source_labels: tuple[str, ...] = ()
    categories: tuple[str, ...] = ()

    def __post_init__(self):
        if self.published_at.tzinfo is None:
            raise ValueError("published_at must be timezone-aware (UTC).")

    @property
    def deduplication_key(self) -> str:
        return self.canonical_url or self.url or (
            f"identifier:{self.identifier}" if self.identifier else ""
        )

    @property
    def all_sources(self) -> tuple[str, ...]:
        return tuple(sorted({self.source, *self.source_labels}))

    @property
    def all_categories(self) -> tuple[str, ...]:
        return tuple(sorted({self.category, *self.categories} - {""}))