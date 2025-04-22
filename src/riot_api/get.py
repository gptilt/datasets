import aiohttp
import asyncio
import os
from tqdm import tqdm


BASE_URL_REGION = lambda region: f"https://{region}.api.riotgames.com"
BASE_URL_PLATFORM = lambda platform: f"https://{platform}.api.riotgames.com"
ENDPOINTS = {
    'players': lambda platform:
        f"{BASE_URL_PLATFORM(platform)}/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5",

    'player_match_ids': lambda region, puuid, queue, type, count:
        f"{BASE_URL_REGION(region)}/lol/match/v5/matches/by-puuid/{puuid}/ids?start=0&queue={queue}&type={type}&count={count}",

    'match_info': lambda region, match_id:
        f"{BASE_URL_REGION(region)}/lol/match/v5/matches/{match_id}",

    'match_timeline': lambda region, match_id:
        f"{BASE_URL_REGION(region)}/lol/match/v5/matches/{match_id}/timeline",
}


async def fetch_with_rate_limit(endpoint: str, session: aiohttp.ClientSession = None, **kwargs) -> dict:
    """
    Fetches data from the Riot API.
    Waits for rate limit if exceeded.
    """
    assert os.environ.get("RIOT_API_KEY"), "RIOT_API_KEY environment variable is not set"

    flag_cleanup = False
    if session is None:
        session = aiohttp.ClientSession()
        flag_cleanup = True

    try:
        url = ENDPOINTS[endpoint](**kwargs)
        for attempt in range(6):
            async with session.get(url, headers={"X-Riot-Token": os.getenv("RIOT_API_KEY")}) as response:
                if response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", 30))
                    tqdm.write(f"[RATE LIMIT] {endpoint} - waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    continue
                elif response.status != 200:
                    raise Exception(f"Error {response.status} on {url}")
                return await response.json()
        raise Exception(f"Too many retries for {url}")
    finally:
        if flag_cleanup:
            await session.close()
