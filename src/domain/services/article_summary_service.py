import re


class ArticleSummaryService:

    ABBREVIATIONS = re.compile(
        r"\b(?:U\.S|U\.K|Mr|Mrs|Ms|Dr|Prof|Gen|Sen|Rep)\.",
        re.IGNORECASE,
    )

    def generate_summary(self, description: str):

        if not description:
            return "No summary available."

        protected = self.ABBREVIATIONS.sub(
            lambda match: match.group().replace(".", "\u0000"),
            description.strip(),
        )
        sentences = re.split(r"(?<=[.!?])\s+", protected)

        if len(sentences) > 2:
            summary = " ".join(sentences[:2])
            if not summary.endswith((".", "!", "?")):
                summary += "."
        else:
            summary = " ".join(sentences)

        return summary.replace("\u0000", ".")