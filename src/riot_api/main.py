import asyncio
from concurrent.futures import ThreadPoolExecutor
from riot_api import get, worker


REGIONS_AND_PLATFORMS = {
    "americas": {
        "br1",
        "la1",
        "la2",
        "na1",
    },
    "asia": {
        "jp1",
        "kr",
        "vn2",
    },
    "europe": {
        "eun1",
        "euw1",
        "tr1",
    },
}


async def async_main():
    list_of_platforms = [
        (region, platform)
        for region, platforms in REGIONS_AND_PLATFORMS.items()
        for platform in platforms
    ]

    regions = REGIONS_AND_PLATFORMS.keys()
    dict_of_player_uuids = {
        region: []
        for region in regions
    }

    # Collect player UUIDs per region/platform pair
    for region, platform in list_of_platforms:
        print(f"Fetching challenger players from {region}/{platform}...")
        players = await get.fetch_with_rate_limit('players', platform=platform)

        list_of_players_uuids = [
            entry['puuid'] for entry in players["entries"]
        ]
        
        print(f"Got {len(list_of_players_uuids)} players from {region}/{platform}.")
        dict_of_player_uuids[region].extend(list_of_players_uuids)
    
    # Run a region_worker per region
    with ThreadPoolExecutor(max_workers=len(regions)) as executor:
        futures = [executor.submit(
            lambda region, puuids: asyncio.run(worker.region_worker(region, puuids)),
            region,
            dict_of_player_uuids[region]
        ) for region in regions]

        # Wait for all threads to finish
        for future in futures:
            future.result()
            

def main():
    asyncio.run(async_main())
