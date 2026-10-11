# src/domain/services/signal_aggregation_service.py

import re
from typing import List, Set

from src.domain.entities.raw_article import RawArticle
from src.domain.entities.signal import Signal
from src.domain.value_objects.signal_id import SignalId


class SignalAggregationService:

    STOPWORDS = {
        "the", "and", "with", "after", "from", "into",
        "over", "about", "this", "that", "have", "has",
        "been", "will", "says", "said", "amid", "what",
        "know", "more", "your", "their", "than"
    }

    SIMILARITY_THRESHOLD = 0.3  # lowered threshold

    def aggregate(self, articles: List[RawArticle]) -> List[Signal]:

        clusters = []

        for article in articles:
            tokens = self._tokenize(article.title)

            matched_cluster = None

            for cluster in clusters:
                similarity = self._calculate_similarity(
                    tokens, cluster["tokens"]
                )

                if similarity >= self.SIMILARITY_THRESHOLD:
                    matched_cluster = cluster
                    break

            if matched_cluster:
                matched_cluster["articles"].append(article)
                matched_cluster["tokens"].update(tokens)
            else:
                clusters.append({
                    "tokens": set(tokens),
                    "articles": [article]
                })

        signals: List[Signal] = []

        for cluster in clusters:
            grouped_articles = cluster["articles"]

            source_set: Set[str] = {
                source
                for article in grouped_articles
                for source in article.all_sources
            }
            categories = {
                category
                for article in grouped_articles
                for category in article.all_categories
            }

            signal = Signal(
                id=SignalId.new(),
                title=self._derive_title(grouped_articles),
                source_count=len(source_set),
                country_count=1,
                first_detected_at=min(a.published_at for a in grouped_articles),
                last_updated_at=max(a.published_at for a in grouped_articles),
                velocity=len(grouped_articles),
                categories=tuple(sorted(categories)),
            )

            signals.append(signal)

        return signals

    def _tokenize(self, text: str) -> Set[str]:
        words = re.findall(r"\b\w+\b", text.lower())
        return {
            word
            for word in words
            if len(word) > 3 and word not in self.STOPWORDS
        }

    def _calculate_similarity(self, tokens1: Set[str], tokens2: Set[str]) -> float:
        if not tokens1 or not tokens2:
            return 0.0

        intersection = tokens1.intersection(tokens2)

        smaller = min(len(tokens1), len(tokens2))

        return len(intersection) / smaller

    def _derive_title(self, articles: List[RawArticle]) -> str:
        latest = max(articles, key=lambda a: a.published_at)
        return latest.title