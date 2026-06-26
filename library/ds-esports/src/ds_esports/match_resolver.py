"""
Resolve arbitrary content (a transcript, an article, a forum post, …)
to the **pro match** it discusses, against a catalog of candidate matches.

The method is *entity linking*, not semantic search:
a match's identity is symbolic (which two teams, which event)
and temporal (which date), so the resolver leans on exact team-pair matching
within a time window.

Two composable steps, each public so a pipeline can cache or swap either:

1. `extract_match_reference` — the LLM does what it is good at, *extraction*:
   it parses messy content into structured slots {teams, event, game_number}.
2. `link_reference` — pure, deterministic resolution: keep candidate rows whose
   two team columns equal the referenced pair within the window. No model.

`resolve_match` composes the two.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Protocol

import polars as pl
from pydantic import BaseModel


class TextGenerator(Protocol):
    """
    A protocol for text generation backends.
    """

    def generate(self, system: str, user: str, max_new_tokens: int = ...) -> str: ...


class MatchReference(BaseModel):
    """
    The structured slots extracted from content, before any catalog lookup.

    `teams` holds 0–2 surface forms exactly as written ("KC", "Karmine Corp");
    they are matched verbatim (case-insensitively) against the candidate team
    columns, so the caller's denormalization decides what resolves.
    An empty `teams` means the content names no specific match,
    and linking abstains.
    """

    teams: list[str] = []
    event: str | None = None
    game_number: int | None = None


# How far to look for candidate matches, relative to the source's timestamp.
# By default, the match is expected to precede its content.
# A timedelta of a single day addresses issues with date granularity.
DEFAULT_WINDOW_BEFORE = timedelta(days=30)
DEFAULT_WINDOW_AFTER = timedelta(days=1)

# Only the head of the content is read — match references should live in titles
# and the first lines, not buried in the body.
DEFAULT_CONTENT_HEAD_CHARS = 800

EXTRACTION_SYSTEM_PROMPT = (
    "You extract structured references to professional League of Legends matches "
    "from a piece of content. Return ONLY a JSON object with keys:\n"
    '  "teams": array of the team names mentioned, exactly as written (0, 1, or 2);\n'
    '  "event": the tournament/league/event name if stated, else null;\n'
    '  "game_number": the game number within a series as an integer if stated, else null.\n\n'
    "Extract names verbatim — do not expand or normalize abbreviations "
    "(write \"KC\", not \"Karmine Corp\"); the caller resolves them. "
    "If the content is about general strategy, solo-queue, or no specific pro "
    'match, return {"teams": []}. Output only the JSON, nothing else.'
)


def _parse_dt(value) -> datetime | None:
    """
    Coerce a candidate's date cell (datetime or ISO string) to aware UTC.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        # Leaguepedia stores naive UTC strings ("2025-08-31 18:42:00"); tolerate a trailing Z.
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "").strip())
        except ValueError:
            return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _parse_extraction(raw: str) -> MatchReference | None:
    """
    Parse the model's JSON reply into a `MatchReference`, tolerating stray prose.

    Returns `None` only when nothing JSON-shaped is found; a well-formed object
    with no teams becomes an empty reference (a deliberate "no match here").
    """
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        return None
    try:
        data = json.loads(match.group(0))
    except (json.JSONDecodeError, TypeError):
        return None

    teams = [str(t).strip() for t in (data.get("teams") or []) if str(t).strip()]
    game_number = data.get("game_number")
    return MatchReference(
        teams=teams,
        event=(data.get("event") or None),
        game_number=int(game_number) if isinstance(game_number, int) else None,
    )


def extract_match_reference(
    *,
    content: str,
    llm: TextGenerator,
    system_prompt: str = EXTRACTION_SYSTEM_PROMPT,
    content_head_chars: int = DEFAULT_CONTENT_HEAD_CHARS,
) -> MatchReference | None:
    """
    Extract the match slots from content via the LLM.

    Anything with a `generate(system, user)` method, such as an LLM,
    satisfies this, but so does any other backend.

    Return `None` on unparseable output.
    """
    raw = llm.generate(system_prompt, content[:content_head_chars], max_new_tokens=128)
    return _parse_extraction(raw)


def link_reference(
    *,
    reference: MatchReference,
    timestamp: datetime,
    candidates: pl.DataFrame,
    team_1_column: str = "team1",
    team_2_column: str = "team2",
    datetime_column: str = "datetime_utc",
    tournament_column: str | None = None,
    window_before: timedelta = DEFAULT_WINDOW_BEFORE,
    window_after: timedelta = DEFAULT_WINDOW_AFTER,
) -> dict | None:
    """
    Deterministically link an extracted reference to one candidate row. No LLM.

    Requires exactly two distinct teams in the reference.
    Considers candidates dated in `[timestamp - window_before, timestamp + window_after]`.
    Among matching pairs,
    an `event` hint narrows by `tournament_column` (when given),
    and ties break to the date nearest the source timestamp.

    `candidates` must already be alias-denormalized (one row per surface form);
    the matched row is returned whole, that is, every additional column is preserved.
    """
    for column in (team_1_column, team_2_column, datetime_column,
                   *([tournament_column] if tournament_column else [])):
        if column not in candidates.columns:
            raise ValueError(f"`candidates` has no column {column!r}")

    pair = {t.strip().lower() for t in reference.teams if t.strip()}
    if len(pair) != 2:
        return None

    ts = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
    low, high = ts - window_before, ts + window_after

    # Walk the catalog once: keep rows inside the window whose two team cells form
    # exactly the referenced pair. Score by date proximity to the source.
    survivors: list[tuple[float, dict]] = []
    for row in candidates.iter_rows(named=True):
        dt = _parse_dt(row.get(datetime_column))
        if dt is None or not (low <= dt <= high):
            continue
        t1 = str(row.get(team_1_column) or "").strip().lower()
        t2 = str(row.get(team_2_column) or "").strip().lower()
        if {t1, t2} != pair:
            continue
        survivors.append((abs((dt - ts).total_seconds()), row))

    if not survivors:
        return None

    # Soft prior: if an event was named and any survivor's tournament matches it,
    # restrict to those — but never let a stray hint discard every candidate.
    if tournament_column and reference.event:
        hint = reference.event.strip().lower()
        on_event = [s for s in survivors if hint in str(s[1].get(tournament_column) or "").lower()]
        if on_event:
            survivors = on_event

    return min(survivors, key=lambda s: s[0])[1]


def resolve_match(
    *,
    timestamp: datetime,
    content: str,
    candidates: pl.DataFrame,
    llm: TextGenerator,
    team_1_column: str = "team1",
    team_2_column: str = "team2",
    datetime_column: str = "datetime_utc",
    tournament_column: str | None = None,
    window_before: timedelta = DEFAULT_WINDOW_BEFORE,
    window_after: timedelta = DEFAULT_WINDOW_AFTER,
    extraction_system_prompt: str = EXTRACTION_SYSTEM_PROMPT,
    content_head_chars: int = DEFAULT_CONTENT_HEAD_CHARS,
) -> dict | None:
    """
    Best-effort grounding of `content` (published at `timestamp`) to a pro match.

    Composes the two steps:
    1. LLM extraction of {teams, event, game_number};
    2. Deterministic linking against the (alias-denormalized) `candidates`.

    Returns the matched row whole, or `None` for un-grounded content.
    """
    reference = extract_match_reference(
        content=content,
        llm=llm,
        system_prompt=extraction_system_prompt,
        content_head_chars=content_head_chars,
    )
    if reference is None or not reference.teams:
        return None

    return link_reference(
        reference=reference,
        timestamp=timestamp,
        candidates=candidates,
        team_1_column=team_1_column,
        team_2_column=team_2_column,
        datetime_column=datetime_column,
        tournament_column=tournament_column,
        window_before=window_before,
        window_after=window_after,
    )
