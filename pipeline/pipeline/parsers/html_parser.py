from __future__ import annotations
from typing import Any
from bs4 import BeautifulSoup
from pipeline.adapters.models import ParseConfig


def parse_html(html: str, config: ParseConfig) -> dict[str, Any]:
    """Parse BOE HTML using CSS selectors configured in ParseConfig."""
    soup = BeautifulSoup(html or "", "lxml")

    # Articles — collect heading tag + sibling body paragraphs until next article-level tag
    article_tags = soup.select(config.article_selector)
    # Build a set of article tag ids for quick boundary detection
    article_tag_set = set(id(t) for t in article_tags)

    # Determine which CSS classes are "structural" (articles + provisions + annexes)
    # so we know when a sibling paragraph belongs to a different section
    structural_selectors = (
        [config.article_selector]
        + [f".{cls}" for cls in config.provision_selectors]
        + ([config.annex_selector] if config.annex_selector else [])
    )
    structural_tags = set(
        id(t)
        for sel in structural_selectors
        for t in soup.select(sel)
    )

    articles: list[dict] = []
    for i, tag in enumerate(article_tags, 1):
        span = tag.select_one(config.article_title_selector)
        if not span:
            continue
        article_num = span.get_text(strip=True).rstrip(".")
        # Collect text: start with the article heading tag itself
        parts = [tag.get_text(separator=" ", strip=True)]
        # Walk next siblings and gather body paragraphs until another structural tag
        for sibling in tag.next_siblings:
            if not hasattr(sibling, "select"):
                continue  # skip NavigableString nodes
            if id(sibling) in structural_tags:
                break
            text = sibling.get_text(separator=" ", strip=True)
            if text:
                parts.append(text)
        articles.append({
            "article_id": f"art-{i}",
            "article_num": article_num,
            "text": " ".join(parts),
        })

    # Provisions — grouped by type, numbered per type
    provisions: list[dict] = []
    type_counters: dict[str, int] = {}
    for css_class, prov_type in config.provision_selectors.items():
        for tag in soup.select(f".{css_class}"):
            type_counters[prov_type] = type_counters.get(prov_type, 0) + 1
            provisions.append({
                "type": prov_type,
                "num": str(type_counters[prov_type]),
                "text": tag.get_text(separator=" ", strip=True),
            })

    # Annexes
    annexes: list[dict] = [
        {
            "annex_id": f"annex-{i}",
            "title": tag.get_text(strip=True)[:100],
            "text": tag.get_text(separator=" ", strip=True),
        }
        for i, tag in enumerate(soup.select(config.annex_selector), 1)
    ]

    # Preamble — text from configured selectors
    preamble_parts = [
        tag.get_text(separator=" ", strip=True)
        for sel in config.preamble_selectors
        for tag in soup.select(sel)
        if tag.get_text(strip=True)
    ]
    preamble_text = " ".join(preamble_parts) if preamble_parts else None

    return {
        "preamble_text": preamble_text,
        "articles": articles,
        "provisions": provisions,
        "annexes": annexes,
    }
