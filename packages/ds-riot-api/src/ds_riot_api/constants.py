DATASET_NAME = 'riot_api'

REGIONS_AND_PLATFORMS = {
    "americas": {
        "br1",  # Brazil 1
        "la1",  # Latin America North
        "la2",  # Latin America South
        "na1",  # North America 1
    },
    "asia": {
        "jp1",  # Japan 1
        "kr",   # Korea
        "oc1",  # Oceania 1
        "sg2",  # Southeast Asia 2
        "tw2",  # Taiwan 2
        "vn2",  # Vietnam 2
    },
    "europe": {
        "eun1",  # Europe North 1
        "euw1",  # Europe West 1
        "me1",   # Middle East 1
        "ru",    # Russia
        "tr1",   # Turkey 1
    },
}
REGIONS = list(REGIONS_AND_PLATFORMS.keys())
SERVERS = [server for servers in REGIONS_AND_PLATFORMS.values() for server in servers]
REGION_PER_SERVER = {server: region for region, servers in REGIONS_AND_PLATFORMS.items() for server in servers}

TIERS = [
    "IRON",
    "BRONZE",
    "SILVER",
    "GOLD",
    "PLATINUM",
    "EMERALD",
    "DIAMOND"
]
DIVISIONS = ["I", "II", "III", "IV"]
ELITE_TIERS = [
    "MASTER",
    "GRANDMASTER",
    "CHALLENGER"
]
