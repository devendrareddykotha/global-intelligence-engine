from datetime import datetime, timedelta, timezone

import pytest

from src.application.use_cases.generate_top_signals import GenerateTopSignalsUseCase
from src.domain.entities.raw_article import RawArticle
from src.domain.entities.signal import Signal
from src.domain.services.article_summary_service import ArticleSummaryService
from src.domain.services.impact_scoring_service import ImpactScoringService
from src.domain.services.signal_aggregation_service import SignalAggregationService
from src.domain.value_objects.signal_id import SignalId
from src.infrastructure.ingestion.rss_client import RSSClient
from src.infrastructure.persistence.in_memory_signal_repository import (
    InMemorySignalRepository,
)


def test_rss_articles_use_raw_article_fields_and_source_name_alias():
    articles = RSSClient().fetch_from_file(
        "nytimes_feed.xml",
        source_name="NYTimes",
    )

    assert articles
    assert all(isinstance(article, RawArticle) for article in articles)
    assert all(article.source == "NYTimes" for article in articles)
    assert all(article.published_at.tzinfo is not None for article in articles)
    assert all(article.url for article in articles)


def test_aggregation_uses_raw_article_publication_time():
    first_published = datetime(2026, 3, 3, tzinfo=timezone.utc)
    latest_published = first_published + timedelta(hours=1)
    articles = [
        RawArticle("Global climate summit begins", "", "A", first_published, "https://a"),
        RawArticle("Global climate summit concludes", "", "B", latest_published, "https://b"),
    ]

    signals = SignalAggregationService().aggregate(articles)

    assert len(signals) == 1
    assert signals[0].title == articles[1].title
    assert signals[0].source_count == 2
    assert signals[0].velocity == 2
    assert signals[0].first_detected_at == first_published
    assert signals[0].last_updated_at == latest_published


def test_add_initial_seeds_without_clearing_repository():
    repository = InMemorySignalRepository()
    now = datetime.now(timezone.utc)
    initial = Signal(SignalId.new(), "Initial", 1, 1, now, now, 1)
    seeded = Signal(SignalId.new(), "Seeded", 1, 1, now, now, 1)
    repository.save(initial)

    repository.add_initial([seeded])

    assert {signal.title for signal in repository.get_active_signals()} == {
        "Initial",
        "Seeded",
    }


def test_generate_top_signals_uses_repository_contract():
    repository = InMemorySignalRepository()
    now = datetime.now(timezone.utc)
    signals = [
        Signal(
            SignalId.new(),
            f"Signal {index}",
            5 + index,
            2,
            now,
            now - timedelta(hours=index),
            10 + index,
        )
        for index in range(4)
    ]
    repository.add_initial(signals)

    top_signals = GenerateTopSignalsUseCase(
        repository,
        ImpactScoringService(),
    ).execute(now, limit=2)

    assert len(top_signals) == 2
    assert top_signals[0].impact_score >= top_signals[1].impact_score


def test_nyt_feed_runs_through_aggregation_and_ranking():
    articles = RSSClient().fetch_from_file("nytimes_feed.xml", "NYTimes")
    repository = InMemorySignalRepository()
    signals = SignalAggregationService().aggregate(articles)
    repository.add_initial(signals)
    now = datetime.now(timezone.utc)

    top_signals = GenerateTopSignalsUseCase(
        repository,
        ImpactScoringService(),
    ).execute(now, limit=10)

    assert articles
    assert signals
    assert top_signals
    assert all(signal.last_updated_at.tzinfo is not None for signal in top_signals)


def test_signal_still_rejects_naive_timestamps():
    naive = datetime.now()

    with pytest.raises(ValueError, match="timezone-aware"):
        Signal(SignalId.new(), "Invalid", 1, 1, naive, naive, 1)


def test_article_summary_preserves_common_abbreviations():
    description = (
        "Gen. Dan Caine said U.S. forces moved. "
        "Officials confirmed the update. More details follow."
    )

    assert ArticleSummaryService().generate_summary(description) == (
        "Gen. Dan Caine said U.S. forces moved. "
        "Officials confirmed the update."
    )


def test_impact_freshness_decays_deterministically_from_fixed_timestamp():
    published_at = datetime(2026, 3, 3, tzinfo=timezone.utc)
    signal = Signal(
        SignalId.new(),
        "Fixed-time signal",
        source_count=2,
        country_count=1,
        first_detected_at=published_at,
        last_updated_at=published_at,
        velocity=4,
    )
    scorer = ImpactScoringService()

    assert scorer.calculate(signal, published_at) == 18.0
    assert scorer.calculate(signal, published_at + timedelta(hours=12)) == 15.5
    assert scorer.calculate(signal, published_at + timedelta(hours=24)) == 13.0
    assert scorer.calculate(signal, published_at - timedelta(hours=12)) == 18.0