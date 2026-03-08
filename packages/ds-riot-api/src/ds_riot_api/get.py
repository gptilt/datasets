import aiohttp
import asyncio
import dagster as dg


BASE_URL_REGION = lambda region: f"https://{region}.api.riotgames.com"
BASE_URL_PLATFORM = lambda platform: f"https://{platform}.api.riotgames.com"
ENDPOINTS = {
    'players': lambda platform:
        f"{BASE_URL_PLATFORM(platform)}/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5",

    'league_entries': lambda platform, tier, division, page:
        f"{BASE_URL_PLATFORM(platform)}/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}",

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


async def fetch_with_rate_limit(
    context: dg.AssetExecutionContext,
    endpoint: str,
    session: aiohttp.ClientSession = None,
    **kwargs
) -> dict:
    """
    Fetches data from the Riot API.
    Waits for rate limit if exceeded.
    """
    assert RIOT_API_KEY, "RIOT_API_KEY environment variable is not set"

    flag_cleanup = False
    if session is None:
        session = aiohttp.ClientSession()
        flag_cleanup = True

    try:
        url = ENDPOINTS[endpoint](**kwargs)
        for _attempt in range(6):
            async with session.get(url, headers={"X-Riot-Token": RIOT_API_KEY}) as response:
                if response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", 30))
                    context.log.info(f"[RATE LIMIT] {endpoint} - waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    continue
                elif response.status == 502:
                    context.log.info(f"[BAD GATEWAY] {endpoint} - retrying...")
                    await asyncio.sleep(5)
                    continue
                elif response.status == 503:
                    context.log.info(f"[SERVER UNAVAILABLE] {endpoint} - retrying...")
                    await asyncio.sleep(5)
                    continue
                elif response.status == 504:
                    context.log.info(f"[GATEWAY TIMEOUT] {endpoint} - retrying...")
                    await asyncio.sleep(5)
                    continue
                elif response.status != 200:
                    raise Exception(f"Error {response.status} on {url}")
                return await response.json()
        raise Exception(f"Too many retries for {url}")
    finally:
        if flag_cleanup:
            await session.close()
