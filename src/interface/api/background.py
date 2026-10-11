"""Standalone compatibility entry point for the live polling engine.

The normal application starts this worker through FastAPI's lifespan. Do not
run this standalone collector alongside the API process.
"""

from src.application.services.live_news_engine import LiveNewsEngine
from src.infrastructure.ingestion.feed_settings import load_feed_settings


async def minute_collector() -> None:
    engine = LiveNewsEngine(load_feed_settings())
    await engine.poll_forever()
