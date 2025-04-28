import pyarrow as pa


CHAMPION_STATS = pa.struct([
    ("abilityHaste", pa.int16()),
    ("abilityPower", pa.int16()),
    ("armor", pa.int16()),
    ("armorPen", pa.int16()),
    ("armorPenPercent", pa.int16()),
    ("attackDamage", pa.int16()),
    ("attackSpeed", pa.int16()),
    ("bonusArmorPenPercent", pa.int16()),
    ("bonusMagicPenPercent", pa.int16()),
    ("ccReduction", pa.int16()),
    ("cooldownReduction", pa.int16()),
    ("health", pa.int32()),
    ("healthMax", pa.int32()),
    ("healthRegen", pa.int32()),
    ("lifesteal", pa.int16()),
    ("magicPen", pa.int16()),
    ("magicPenPercent", pa.int16()),
    ("magicResist", pa.int16()),
    ("movementSpeed", pa.int32()),
    ("omnivamp", pa.int16()),
    ("physicalVamp", pa.int16()),
    ("power", pa.int16()),
    ("powerMax", pa.int16()),
    ("powerRegen", pa.int16()),
    ("spellVamp", pa.int16()),
])

DAMAGE_STATS = pa.struct([
    ("magicDamageDone", pa.int32()),
    ("magicDamageDoneToChampions", pa.int32()),
    ("magicDamageTaken", pa.int32()),
    ("physicalDamageDone", pa.int32()),
    ("physicalDamageDoneToChampions", pa.int32()),
    ("physicalDamageTaken", pa.int32()),
    ("totalDamageDone", pa.int32()),
    ("totalDamageDoneToChampions", pa.int32()),
    ("totalDamageTaken", pa.int32()),
    ("trueDamageDone", pa.int32()),
    ("trueDamageDoneToChampions", pa.int32()),
    ("trueDamageTaken", pa.int32())
])

DAMAGE_DEALT = pa.list_(pa.struct([
    ("basic", pa.bool_()),
    ("magicDamage", pa.int32()),
    ("name", pa.string()),
    ("participantId", pa.int16()),
    ("physicalDamage", pa.int32()),
    ("spellName", pa.string()),
    ("spellSlot", pa.int32()),
    ("trueDamage", pa.int32()),
    ("type", pa.string())
]))

EVENTS = pa.schema([
    ('matchId', pa.string()),
    ('positionX', pa.int32()),
    ('positionY', pa.int32()),
    ('teamId', pa.int16()),
    ('timestamp', pa.int32()),
    ('type', pa.string()),
    ('winningTeam', pa.int16()),
    # Feats of Strength
    ('featType', pa.int16()),
    ('featValue', pa.int16()),
    # Gold
    ('bounty', pa.int16()),
    ('currentGold', pa.int32()),
    ('goldGain', pa.int16()),
    ('goldPerSecond', pa.int16()),
    ('shutdownBounty', pa.int16()),
    ('totalGold', pa.int32()),
    # Item Ids
    ('afterId', pa.int32()),
    ('beforeId', pa.int32()),
    ('itemId', pa.int32()),
    # Jungle
    ('jungleMinionsKilled', pa.int16()),
    ('monsterSubType', pa.string()),
    ('monsterType', pa.string()),
    ('name', pa.string()),
    # Kills
    ('killerId', pa.int16()),
    ('killerTeamId', pa.int16()),
    ('killStreakLength', pa.int16()),
    ('killType', pa.string()),
    ('multiKillLength', pa.int16()),
    ('victimDamageDealt', DAMAGE_DEALT),
    ('victimDamageReceived', DAMAGE_DEALT),
    ('victimId', pa.int16()),
    # Lanes
    ('buildingType', pa.string()),
    ('laneType', pa.string()),
    ('towerType', pa.string()),
    # Level
    ('level', pa.int16()),
    ('levelUpType', pa.string()),
    ('skillSlot', pa.int16()),
    ('xp', pa.int32()),
    # Player
    ('assistingParticipantIds', pa.list_(pa.int16())),
    ('championStats', CHAMPION_STATS),
    ('damageStats', DAMAGE_STATS),
    ('minionsKilled', pa.int16()),
    ('participantId', pa.int16()),
    ('timeEnemySpentControlled', pa.int32()),
    # Wards
    ('creatorId', pa.int16()),
    ('wardType', pa.string()),
])
