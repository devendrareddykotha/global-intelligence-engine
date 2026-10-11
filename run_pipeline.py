from datetime import datetime, timezone
from pathlib import Path

from src.application.use_cases.generate_top_signals import GenerateTopSignalsUseCase
from src.domain.services.article_summary_service import ArticleSummaryService
from src.domain.services.impact_scoring_service import ImpactScoringService
from src.domain.services.signal_aggregation_service import SignalAggregationService
from src.infrastructure.ingestion.rss_client import RSSClient
from src.infrastructure.persistence.in_memory_signal_repository import (
    InMemorySignalRepository,
)


def main() -> None:
    feed_path = Path(__file__).resolve().parent / "nytimes_feed.xml"
    articles = RSSClient().fetch_from_file(str(feed_path), source_name="NYTimes")
    signals = SignalAggregationService().aggregate(articles)

    repository = InMemorySignalRepository()
    repository.add_initial(signals)
    ranked_signals = GenerateTopSignalsUseCase(
        repository,
        ImpactScoringService(),
    ).execute(datetime.now(timezone.utc), limit=5)

    article_by_title = {article.title: article for article in articles}
    summary_service = ArticleSummaryService()

    print(
        f"NYT feed: {len(articles)} articles parsed; "
        f"{len(signals)} intelligence signals aggregated."
    )
    print("Top 5 ranked signals (summary from the latest matching article):\n")

    for rank, signal in enumerate(ranked_signals, start=1):
        article = article_by_title.get(signal.title)
        description = article.description if article is not None else ""
        summary = summary_service.generate_summary(description)
        print(f"{rank}. {signal.title}")
        print(f"   Impact score: {signal.impact_score:.2f}")
        print(f"   Articles: {int(signal.velocity)} | Sources: {signal.source_count}")
        print(f"   Summary: {summary}")
        print()


if __name__ == "__main__":
    main()