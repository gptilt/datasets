import polars as pl

PL_CHAMPION_STATS = pl.Struct([
    pl.Field("abilityHaste", pl.Int16),
    pl.Field("abilityPower", pl.Int16),
    pl.Field("armor", pl.Int16),
    pl.Field("armorPen", pl.Int16),
    pl.Field("armorPenPercent", pl.Int16),
    pl.Field("attackDamage", pl.Int16),
    pl.Field("attackSpeed", pl.Int16),
    pl.Field("bonusArmorPenPercent", pl.Int16),
    pl.Field("bonusMagicPenPercent", pl.Int16),
    pl.Field("ccReduction", pl.Int16),
    pl.Field("cooldownReduction", pl.Int16),
    pl.Field("health", pl.Int32),
    pl.Field("healthMax", pl.Int32),
    pl.Field("healthRegen", pl.Int32),
    pl.Field("lifesteal", pl.Int16),
    pl.Field("magicPen", pl.Int16),
    pl.Field("magicPenPercent", pl.Int16),
    pl.Field("magicResist", pl.Int16),
    pl.Field("movementSpeed", pl.Int32),
    pl.Field("omnivamp", pl.Int16),
    pl.Field("physicalVamp", pl.Int16),
    pl.Field("power", pl.Int16),
    pl.Field("powerMax", pl.Int16),
    pl.Field("powerRegen", pl.Int16),
    pl.Field("spellVamp", pl.Int16),
])

PL_DAMAGE_STATS = pl.Struct([
    pl.Field("magicDamageDone", pl.Int32),
    pl.Field("magicDamageDoneToChampions", pl.Int32),
    pl.Field("magicDamageTaken", pl.Int32),
    pl.Field("physicalDamageDone", pl.Int32),
    pl.Field("physicalDamageDoneToChampions", pl.Int32),
    pl.Field("physicalDamageTaken", pl.Int32),
    pl.Field("totalDamageDone", pl.Int32),
    pl.Field("totalDamageDoneToChampions", pl.Int32),
    pl.Field("totalDamageTaken", pl.Int32),
    pl.Field("trueDamageDone", pl.Int32),
    pl.Field("trueDamageDoneToChampions", pl.Int32),
    pl.Field("trueDamageTaken", pl.Int32)
])

PL_DAMAGE_DEALT_INNER_STRUCT = pl.Struct([
    pl.Field("basic", pl.Boolean),
    pl.Field("magicDamage", pl.Int32),
    pl.Field("name", pl.String),
    pl.Field("participantId", pl.Int16),
    pl.Field("physicalDamage", pl.Int32),
    pl.Field("spellName", pl.String),
    pl.Field("spellSlot", pl.Int32),
    pl.Field("trueDamage", pl.Int32),
    pl.Field("type", pl.String)
])

PL_DAMAGE_DEALT = pl.List(PL_DAMAGE_DEALT_INNER_STRUCT)

EVENTS = {
    'matchId': pl.String,
    'eventId': pl.Int16,
    'positionX': pl.Int32,
    'positionY': pl.Int32,
    'teamId': pl.Int16,
    'timestamp': pl.Int32,
    'type': pl.String,
    'winningTeam': pl.Int16,
    # Feats of Strength
    'featType': pl.Int16,
    'featValue': pl.Int16,
    # Gold
    'currentGold': pl.Int32,
    'goldGain': pl.Int16,
    'goldPerSecond': pl.Int16,
    'shutdownBounty': pl.Int16,
    'totalGold': pl.Int32,
    # Items
    'afterId': pl.Int32,
    'beforeId': pl.Int32,
    'inventory': pl.List(pl.Int32),
    'itemId': pl.Int32,
    # Jungle
    'jungleMinionsKilled': pl.Int16,
    'monsterSubType': pl.String,
    'monsterType': pl.String,
    'name': pl.String,
    # Kills
    'bounty': pl.Int16,
    'killerId': pl.Int16,
    'killerTeamId': pl.Int16,
    'killStreakLength': pl.Int16,
    'killType': pl.String,
    'multiKillLength': pl.Int16,
    'numberOfAssists': pl.Int8,
    'victimDamageDealt': PL_DAMAGE_DEALT,
    'victimDamageReceived': PL_DAMAGE_DEALT,
    'victimId': pl.Int16,
    # Lanes
    'buildingType': pl.String,
    'laneType': pl.String,
    'towerType': pl.String,
    # Level
    'level': pl.Int16,
    'levelUpType': pl.String,
    'skillSlot': pl.Int16,
    'xp': pl.Int32,
    # Player
    # 'assistingParticipantIds': pl.List(pl.Int16), # Uncomment if needed
    'championStats': PL_CHAMPION_STATS,
    'damageStats': PL_DAMAGE_STATS,
    'minionsKilled': pl.Int16,
    'participantId': pl.Int16,
    'timeEnemySpentControlled': pl.Int32,
    # Wards
    # 'creatorId': pl.Int16, # Uncomment if needed
    'wardType': pl.String,
}