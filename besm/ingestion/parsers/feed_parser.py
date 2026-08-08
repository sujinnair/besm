from __future__ import annotations

from dataclasses import dataclass

import feedparser


@dataclass(slots=True)
class FeedEntry:
    title: str
    link: str
    summary: str
    published: str  # raw string from feed; may be empty


def parse_feed(url: str) -> list[FeedEntry]:
    result = feedparser.parse(url)
    entries = []
    for e in result.entries:
        entries.append(
            FeedEntry(
                title=getattr(e, "title", ""),
                link=getattr(e, "link", ""),
                summary=getattr(e, "summary", ""),
                published=getattr(e, "published", ""),
            )
        )
    return entries
