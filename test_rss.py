from src.infrastructure.ingestion.rss_client import RSSClient

client = RSSClient()

articles = client.fetch_from_file(
    "bbc_feed.xml",
    source_name="BBC"
)

print(f"Fetched {len(articles)} articles\n")

for article in articles[:5]:
    print(article.title)