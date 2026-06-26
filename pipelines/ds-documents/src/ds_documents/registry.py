"""
Build `source_registry` rows:
Normalize a raw ingested source into the registry's shape,
deriving reliability/privacy from the source type, hashing the raw content,
and folding in the grounding result (`ds_esports` resolver) when present.

Producers call `build_source_registry_row` and upsert the result on `source_id`;
the row carries everything the privacy and normalization stages need next.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Literal, Optional

from .patch import infer_patch_hint

SourceType = Literal[
    # official Riot Games — not ingested yet
    # "official_patch_notes",
    # "official_dev_blog",
    # "hotfix",
    "transcript",
    "article",
    "forum",
    "guide",
]
ReliabilityTier = Literal["A", "B", "C", "D"]
PrivacyRisk = Literal["low", "medium", "high"]

# Per source type: how much we trust it, and how much PII it tends to carry.
SOURCE_PROPERTIES: dict[SourceType, tuple[ReliabilityTier, PrivacyRisk]] = {
    # educational
    "guide": ("B", "low"),
    "transcript": ("B", "high"),
    # others
    "article": ("C", "medium"),
    "forum": ("D", "high"),
}


def build_source_registry_row(
    *,
    source_id: str,
    origin_uri: str,
    source_type: SourceType,
    content: str,
    platform: Optional[str] = None,
    author_name: Optional[str] = None,
    title: Optional[str] = None,
    published_at: Optional[str] = None,
    grounding: Optional[dict] = None,
) -> dict:
    """Assemble one `source_registry` row from a raw source plus optional grounding.

    - `content` is the raw body — hashed for dedup/lineage and scanned for an in-text
    patch hint, but not stored.
    - `grounding` is the resolver's matched row (or None). When it yields a `match_id`,
    the source is `grounded` and inherits the match's `patch`,
    so the weak in-text `patch_hint` is only computed as a fallback.
    """
    reliability_tier, privacy_risk_level = SOURCE_PROPERTIES[source_type]

    matched = grounding or {}
    match_id = matched.get("match_id")
    grounded = match_id is not None

    return {
        "source_id": source_id,
        "origin_uri": origin_uri,
        "source_type": source_type,
        "platform": platform,
        "author_name": author_name,
        "title": title,
        "published_at": published_at,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "raw_hash": hashlib.sha256(content.encode("utf-8")).hexdigest(),
        "reliability_tier": reliability_tier,
        "privacy_risk_level": privacy_risk_level,
        "processing_status": "grounded" if grounded else "ungrounded",
        "match_id": match_id,
        "patch": matched.get("patch"),
        # Patch-of-record comes from the grounded match;
        # the in-text hint is only a fallback for un-grounded sources
        # (never inferred from publish date).
        "patch_hint": None if grounded else infer_patch_hint(origin_uri, content),
    }
