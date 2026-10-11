from src.infrastructure.ingestion.rss_client import RSSClient
from src.domain.services.signal_aggregation_service import SignalAggregationService

client = RSSClient()

bbc_articles = client.fetch_from_file("bbc_feed.xml", "BBC")
nyt_articles = client.fetch_from_file("nytimes_feed.xml", "NYTimes")

all_articles = bbc_articles + nyt_articles

print(f"BBC articles: {len(bbc_articles)}")
print(f"NYT articles: {len(nyt_articles)}")
print(f"Total articles: {len(all_articles)}\n")

aggregator = SignalAggregationService()
signals = aggregator.aggregate(all_articles)

print(f"Generated signals: {len(signals)}\n")

for s in signals[:10]:
    print("Signal:", s.title)
    print("  Sources:", s.source_count)
    print("  Velocity:", s.velocity)
    print()