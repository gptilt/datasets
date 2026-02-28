DATASET_NAME = 'riot_api'

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
REGIONS = list(REGIONS_AND_PLATFORMS.keys())
SERVERS = [server for servers in REGIONS_AND_PLATFORMS.values() for server in servers]
REGION_PER_SERVER = {server: region for region, servers in REGIONS_AND_PLATFORMS.items() for server in servers}

TIERS = {
    "IRON",
    "BRONZE",
    "SILVER",
    "GOLD",
    "PLATINUM",
    "DIAMOND"
}

DIVISIONS = {1, 2, 3, 4}