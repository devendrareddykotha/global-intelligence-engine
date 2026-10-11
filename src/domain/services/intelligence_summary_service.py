from typing import List


class IntelligenceSummaryService:

    def generate(self, articles: List[dict]) -> str:
        """
        Combine multiple article descriptions into
        a single synthesized intelligence summary.
        """

        if not articles:
            return "No detailed information available."

        descriptions = []

        for a in articles:
            desc = a.get("description")
            if desc:
                descriptions.append(desc.strip())

        if not descriptions:
            return "Limited reporting available at this time."

        # Basic synthesis logic (Phase 1)
        combined = " ".join(descriptions[:3])

        return (
            "Multi-source reporting indicates: "
            + combined
        )