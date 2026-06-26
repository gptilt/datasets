import aiohttp
import asyncio
import dagster as dg
import random


BASE_URL_REGION = lambda region: f"https://{region}.api.riotgames.com"
BASE_URL_PLATFORM = lambda platform: f"https://{platform}.api.riotgames.com"
ENDPOINTS = {
    'players': lambda platform:
        f"{BASE_URL_PLATFORM(platform)}/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5",

    'league_entries': lambda platform, tier, division, page:
        f"{BASE_URL_PLATFORM(platform)}/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}",

    'league_entries_elite': lambda platform, elite_tier:
        f"{BASE_URL_PLATFORM(platform)}/lol/league/v4/{elite_tier.lower()}leagues/by-queue/RANKED_SOLO_5x5",

    'player_riot_account': lambda region, puuid:
        f"{BASE_URL_REGION(region)}/riot/account/v1/accounts/by-puuid/{puuid}",

    'player_match_ids': lambda region, puuid, queue, type, count, start_time, end_time:
        f"{BASE_URL_REGION(region)}/lol/match/v5/matches/by-puuid/{puuid}/ids"
        f"?startTime={start_time}&endTime={end_time}"
        f"&start=0&queue={queue}&type={type}&count={count}",

    'match_info': lambda region, match_id:
        f"{BASE_URL_REGION(region)}/lol/match/v5/matches/{match_id}",

    'match_timeline': lambda region, match_id:
        f"{BASE_URL_REGION(region)}/lol/match/v5/matches/{match_id}/timeline",
}
RIOT_API_KEY = dg.EnvVar('RIOT_API_KEY').get_value()

# Transient 5xx responses worth retrying, mapped to a human-readable label.
# 429 is handled separately because it carries a Retry-After header.
RETRYABLE_STATUSES = {
    500: "INTERNAL SERVER ERROR",
    502: "BAD GATEWAY",
    503: "SERVICE UNAVAILABLE",
    504: "GATEWAY TIMEOUT",
}
MAX_ATTEMPTS = 7
DEFAULT_RETRY_AFTER = 30  # seconds, when a 429 omits Retry-After
BACKOFF_BASE = 2          # seconds; exponential backoff for transient failures
BACKOFF_CAP = 60          # seconds; ceiling for a single backoff wait
# Per-request ceiling. Without this a hung socket would block the worker forever.
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)


class RiotAPIError(Exception):
    """Raised when the Riot API returns a non-retryable error or retries are exhausted."""


def _backoff(attempt: int) -> float:
    """Exponential backoff with jitter, capped at BACKOFF_CAP."""
    wait = min(BACKOFF_BASE * 2 ** (attempt - 1), BACKOFF_CAP)
    # Full jitter avoids a thundering herd of concurrent workers retrying in lockstep.
    return random.uniform(0, wait)


async def fetch_with_rate_limit(
    context: dg.AssetExecutionContext,
    endpoint: str,
    session: aiohttp.ClientSession = None,
    **kwargs
) -> dict:
    """
    Fetches data from the Riot API.

    Retries on rate limits (429), transient 5xx responses, and connection-level failures
    (DNS errors, dropped/refused connections, timeouts) with exponential backoff.
    Non-retryable HTTP errors and exhausted retries raise RiotAPIError.
    """
    assert RIOT_API_KEY, "RIOT_API_KEY environment variable is not set"

    flag_cleanup = False
    if session is None:
        session = aiohttp.ClientSession()
        flag_cleanup = True

    try:
        url = ENDPOINTS[endpoint](**kwargs)
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                async with session.get(
                    url,
                    headers={"X-Riot-Token": RIOT_API_KEY},
                    timeout=REQUEST_TIMEOUT,
                ) as response:
                    if response.status == 200:
                        return await response.json()

                    if response.status == 429:
                        retry_after = int(
                            response.headers.get("Retry-After", DEFAULT_RETRY_AFTER)
                        )
                        context.log.warning(
                            f"[RATE LIMIT] {endpoint} - waiting {retry_after}s "
                            f"(attempt {attempt}/{MAX_ATTEMPTS})"
                        )
                        await asyncio.sleep(retry_after)
                        continue

                    if response.status in RETRYABLE_STATUSES:
                        wait = _backoff(attempt)
                        context.log.warning(
                            f"[{RETRYABLE_STATUSES[response.status]}] {endpoint} "
                            f"({response.status}) - retrying in {wait:.1f}s "
                            f"(attempt {attempt}/{MAX_ATTEMPTS})"
                        )
                        await asyncio.sleep(wait)
                        continue

                    # Non-retryable HTTP error: surface the body to aid debugging.
                    body = await response.text()
                    raise RiotAPIError(f"Error {response.status} on {url}: {body}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                # Connection-level failures:
                # - DNS/refused (ClientConnectorError);
                # - Dropped sockets (ServerDisconnectedError);
                # - Payload errors;
                # - Request timeouts.
                # These are transient — back off and retry.
                if attempt == MAX_ATTEMPTS:
                    raise RiotAPIError(
                        f"Connection error on {url} after {MAX_ATTEMPTS} attempts: {exc!r}"
                    ) from exc
                wait = _backoff(attempt)
                context.log.warning(
                    f"[CONNECTION ERROR] {endpoint} - {exc!r}; retrying in {wait:.1f}s "
                    f"(attempt {attempt}/{MAX_ATTEMPTS})"
                )
                await asyncio.sleep(wait)

        raise RiotAPIError(f"Too many retries for {url}")
    finally:
        if flag_cleanup:
            await session.close()
