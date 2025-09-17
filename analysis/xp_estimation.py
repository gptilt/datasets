from dorans import xp
from matplotlib.colors import ListedColormap
import matplotlib.pyplot as plt
import polars as pl
import polars.selectors as cs
import seaborn as sns
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import root_mean_squared_error
from sklearn.model_selection import train_test_split
from storage import StoragePartition
import huggingface_hub as hub
import datasets

# ds = datasets.load_dataset("gptilt/lol-basic-matches-challenger-10k")
# "parquet", "matches", "/mnt/ai/gptilt/datasets/basic/matches/matches") #
# print(ds)

# Delete a specific file from a dataset repo
# api = hub.HfApi()
# api.delete_files(
#     repo_id="gptilt/lol-ultimate-events-challenger-10m",
#     delete_patterns=[f"events/region*.parquet"],
#     repo_type="dataset"
# )

# raise

# from pathlib import Path
# import json
# json_files = Path("/mnt/ai/gptilt/datasets/raw/riot_api/match_info").rglob("*.json")

# count = 0
# for file in json_files:
#     try:
#         with open(file, 'r', encoding='utf-8') as f:
#             if 'queueId": 420' not in f.read():
#                 count += 1
#                 file.unlink()  # delete the file
#                 Path(str(file).replace('match_info', 'match_timeline')).unlink()
#     except Exception as e:
#         print(f"Error processing {file}: {e}")

# print(count)
# raise

storage_basic = StoragePartition(
    root="/mnt/ai/gptilt/datasets",
    schema="basic",
    dataset="matches",
    tables=["matches", "participants", "events"],
    partition_col="region",
    partition_val="europe"
)

df = storage_basic.load_to_polars(
    "matches",
    # eventId=[1, 2, 100, 300]
)

print(df.count())
print(df.select(pl.col("patch").value_counts()))

raise

storage_gold = StoragePartition(
    root="/mnt/ai/gptilt/datasets",
    schema="ultimate",
    dataset="events",
    tables=["events"],
)

df_events = storage_gold.load_to_polars("events")

# Add XP gain column when events impact XP
def compute_xp_gain(row):
    level_from_id = lambda participantId: row[f"level_{participantId}"]

    match row["type"], row["monsterType"], row["monsterSubType"]:
        case "CHAMPION_KILL", _, _:
            if row["participantId"] == 0:
                return 0.0
            return xp.from_kill(
                level_from_id(row["participantId"]),
                level_from_id(row["victimId"]),
                row["numberOfAssists"],
            )
        case "CHAMPION_ASSIST", _, _:
            return xp.from_kill(
                level_from_id(row["participantId"]),
                level_from_id(row["victimId"]),
                row["numberOfAssists"],
            )
        case "ELITE_MONSTER_KILL", "DRAGON", "ELDER_DRAGON":
            return xp.from_elder_dragon(
                elder_dragon_level=12,
            ) / (1 + row["numberOfAssists"])
        case "ELITE_MONSTER_KILL", "DRAGON", _:
            return xp.from_dragon(
                dragon_level=12,
            ) / (1 + row["numberOfAssists"])
        case "ELITE_MONSTER_KILL", "HORDE", _:
            return xp.from_grub(
                grub_level=6,
            ) / (1 + row["numberOfAssists"])
        case "ELITE_MONSTER_KILL", "RIFTHERALD", _:
            return xp.from_rift_herald(
                rift_herald_level=14,
            ) / (1 + row["numberOfAssists"])
        case "ELITE_MONSTER_KILL", "BARON_NASHOR", _:
            return xp.from_baron(is_within_2000_units=True)
        case "WARD_KILL", _, _:
            return xp.from_control_ward() if row["wardType"] == "CONTROL_WARD" else 0.0
        case _:
            return 0.0
        
df_events_with_xp_gain = df_events.with_columns([
    pl.struct([
        "matchId",
        "type",
        "participantId",
        "victimId",
        "numberOfAssists",
        "monsterType",
        "monsterSubType",
        "wardType",
        *(f"level_{i}" for i in range(1, 11)),
    ]).map_elements(compute_xp_gain).alias("xp_gain")
])
print(df_events_with_xp_gain.filter(
    (pl.col("matchId") == "TR1_1609012740")
    & (pl.col("xp_gain") > 0)
).sort("timestamp")[[
    "timestamp", "type", "xp_gain", "participantId", "victimId", "numberOfAssists", "monsterType"
]].filter(pl.col("type") == "ELITE_MONSTER_KILL"))

####################################################################################################################################
####################################################################################################################################
####################################################################################################################################
df_participant_frames = df_events.filter(df_events["type"] == "PARTICIPANT_FRAME")

# Shift the xp column to create current_xp and previous_xp columns
df_samples = (
    df_participant_frames
    .sort(["matchId", "participantId", "timestamp"])
    .group_by(["matchId", "participantId", "region"], maintain_order=True)
    .agg([
        pl.col("timestamp").alias("previous_timestamp"),
        pl.col("timestamp").shift(-1).alias("current_timestamp"),
        pl.col("xp").alias("previous_xp"),
        pl.col("xp").shift(-1).alias("current_xp"),
    ])
    .explode(["previous_timestamp", "current_timestamp", "previous_xp", "current_xp"])  # unpack lists into rows
    .drop_nans(["current_xp"])  # drop rows with NaN future_xp (last frame of each match)
)

# One-hot encode participantId
df_samples = df_samples.with_columns(
    df_samples["participantId"].to_dummies().cast(pl.UInt8)
).rename({
    # Pad participantId with leading zero
    f"participantId_{i}": f"participantId_{i:02}"
    for i in range(1, 11)
})

# One-hot encode region
df_samples = df_samples.with_columns(
    df_samples["region"].to_dummies().cast(pl.UInt8)
)

# Add team
df_samples = df_samples.with_columns(
    (pl.col("participantId") > 5).cast(pl.UInt8).alias("team")
)

# Compute the number of events per interval
# Select only events that impact XP
df_xp_events = df_events.select([
    "matchId",
    "participantId",
    "timestamp",
    "type",
    "wardType",
    "monsterType",
    "numberOfAssists",
]).sort([
    "matchId", "participantId", "timestamp"
]).filter(pl.col("type").is_in([
    "PARTICIPANT_FRAME",
    "WARD_KILL",
    "CHAMPION_KILL",
    "CHAMPION_KILLED",
    "CHAMPION_ASSIST",
    "ELITE_MONSTER_KILL",
    "ELITE_MONSTER_ASSIST",
])).filter(
    (pl.col("wardType") == "CONTROL_WARD")
    | (pl.col("wardType").is_null())
).drop("wardType")

# One-hot encode monsterType
df_xp_events = df_xp_events.with_columns(
    df_xp_events["monsterType"].to_dummies().cast(pl.UInt8)
).drop("monsterType_null")

# Create an interval_id column that tracks PARTICIPANT_FRAME events
df_xp_events = df_xp_events.with_columns([
    (pl.col("type") == "PARTICIPANT_FRAME")
    .cum_sum()
    .alias("interval_id")
])

# Count the number of events of each type per interval
df_xp_events_counts = (
    df_xp_events.group_by(["matchId", "participantId", "interval_id", "type"])
    .len()
    .sort(["matchId", "participantId", "interval_id", "type"])
)
df_xp_events_counts_pivoted = df_xp_events_counts.pivot(
    on=["type"],
    values="len",
    index=["interval_id", "matchId", "participantId"],
).fill_null(0)

# Join back to df_samples
df_xp_events_counts_pivoted = df_xp_events_counts_pivoted.join(
    df_xp_events,
    on=["matchId", "participantId", "interval_id"],
    how="left",
).drop(["type", "monsterType"])

df_samples = df_samples.join(
    df_xp_events_counts_pivoted,
    left_on=["matchId", "participantId", "previous_timestamp"],
    right_on=["matchId", "participantId", "timestamp"],
    how="left",
)

# Get unique summoner spell IDs from both columns
# One-hot encode Summoner Spell IDs
set_of_summoner_spell_ids = (
    df_participants["summoner1Id"].unique()
).union(
    df_participants["summoner2Id"].unique()
)
# Add the one-hot encoded columns to the dataframe
df_participants = df_participants.with_columns([
    ((
        df_participants["summoner1Id"] == spell_id
    ) | (
        df_participants["summoner2Id"] == spell_id
    )).cast(pl.UInt8).alias(f"summoner_spell_{spell_id:02}")  # Pad with leading zero
    for spell_id in set_of_summoner_spell_ids
])
df_samples = df_samples.select(df_samples.columns + [
    col
    for col in df_participants.columns
    if col.startswith("summoner_spell_")
    or col == "championId"
])

# Remove data points from the first minute of the game
df_participant_frames = df_participant_frames.filter(pl.col("timestamp") > 180000)
print(f"Size of df_participant_frames: {df_participant_frames.shape}")

### Visualize the data
# Convert participantId to numeric values if it's not already
participant_roles = ((df_samples["participantId"] - 1) % 5)

# # Custom 5-color map (picked manually or from tab10)
custom_colors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red', 'tab:purple']
cmap = ListedColormap(custom_colors)

# Scatter plot
plt.figure(figsize=(10, 6))
plt.scatter(
    df_samples["previous_timestamp"],
    df_samples["previous_xp"], 
    c=participant_roles,
    cmap=cmap,
    alpha=0.7
)
cbar = plt.colorbar(ticks=range(5), label="Role")
cbar.ax.set_yticklabels([
    "Top",
    "Jungle",
    "Mid",
    "ADC",
    "Support",
])
plt.xlabel("Timestamp")
plt.ylabel("XP")
plt.title("XP over Time colored by Role")
plt.savefig("output/xp_over_time.png")

# Add time_delta and xp_delta columns
df_samples = df_samples.with_columns([
    (pl.col("current_timestamp") - pl.col("previous_timestamp")).alias("time_delta"),
    (pl.col("current_xp") - pl.col("previous_xp")).alias("xp_delta"),
])

# Add nonlinear terms
df_samples = df_samples.with_columns([
    (pl.col("time_delta") ** 2).alias("time_delta_squared"),
    (pl.col("previous_xp") ** 2).alias("previous_xp_squared"),
    (pl.col("previous_xp") * pl.col("previous_timestamp")).alias("previous_xp_time_interaction"),
    (pl.col("current_timestamp") ** 2).alias("current_timestamp_squared"),
    (pl.col("previous_xp") * pl.col("CHAMPION_KILL")).alias("champion_kill_X_xp"),
    (pl.col("previous_xp") * pl.col("CHAMPION_ASSIST") / pl.col("numberOfAssists")).alias("champion_assist_X_xp"),
    (pl.col("previous_xp") * pl.col("ELITE_MONSTER_KILL")).alias("monster_kill_X_xp"),
    *((
        pl.col("previous_timestamp")
        * pl.col("ELITE_MONSTER_KILL")
        * pl.col(col)
    ).alias(f"monster_kill_{col}_X_previous_timestamp")
        for col in df_samples.columns
        if col.startswith("monsterType_")
    )
])

# Root transformation
df_samples = df_samples.with_columns(
    df_samples["xp_delta"].sqrt().alias("xp_delta_sqrt")
)

# Target variable skewness
from scipy.stats import skew

plt.figure(figsize=(10, 6))
sns.histplot(df_samples['xp_delta_sqrt'], bins=100)
plt.title("Distribution of xp_delta_sqrt")
plt.savefig("output/xp_delta_histogram.png")

print("Skewness:", skew(df_samples['xp_delta_sqrt']))

feature_cols = [
    col
    for col in df_samples.columns
    if col not in (
        "matchId",
        "participantId",
        "region",
        "type",
        "interval_id",
        # "previous_xp",
        # "previous_timestamp",
        "current_xp", "xp_delta", "xp_delta_sqrt",  # target variable
        # "current_timestamp",
    )
]
print(f"Feature columns: {feature_cols}")

# Normalize columns
# df_samples = df_samples.with_columns([
#     (pl.col(col) - pl.col(col).mean()) / pl.col(col).std()
#     for col in feature_cols
# ])
print(df_samples.filter(
    (pl.col("matchId") == "TR1_1609358800")
    & (pl.col("participantId") == 1)
).sort("current_timestamp").select([
    "previous_timestamp",
    "current_timestamp",
    "previous_xp",
    "current_xp",
    "participantId",
    "CHAMPION_KILL",
]).tail(10))

X = df_samples.select(feature_cols)
y = df_samples["xp_delta_sqrt"]

# Train-test split
print("Splitting data into train and test sets...")
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
print("Analysing feature importance with Gradient Boosting...")
model = GradientBoostingRegressor(
    n_estimators=30,
    learning_rate=0.05,
    max_depth=7,
    random_state=42,
    max_features="log2",
)
model.fit(X_train, y_train)

# Pair feature names with importances
feature_names = X.columns if hasattr(X, "columns") else [f"feat_{i}" for i in range(X.shape[1])]
feature_importance = list(zip(feature_names, model.feature_importances_))

# Sort alphabetically by feature name
feature_importance.sort(key=lambda x: x[0])  # sort by feature name
filtered_feature_importance = [
    (name, importance)
    for name, importance
    in feature_importance
    if name not in (
        # "previous_xp", "previous_timestamp",
        # "current_xp", "current_timestamp"
    )
]

# Unpack
sorted_features, sorted_importances = zip(*filtered_feature_importance)

# Plot
plt.figure(figsize=(10, 6))
sns.barplot(x=list(sorted_importances), y=list(sorted_features))
plt.title("Gradient Boost Feature Importances")
plt.xlabel("Importance")
plt.ylabel("Feature")
plt.tight_layout()
plt.savefig("output/feature_importances.png")
plt.show()

# Predict
y_pred = model.predict(X_test)

# Evaluate properly
r2_score = model.score(X_test, y_test)
rmse = root_mean_squared_error(y_test, y_pred)

print(f"R²: {r2_score:.4f}")
print(f"RMSE: {rmse:.4f}")
print(f"Predictions: {X_test[:5]} \n {y_pred[:5]}")

# Train model
import statsmodels.api as sm

X_const = sm.add_constant(X_train.to_pandas())
ols_model = sm.OLS(y_train.to_pandas(), X_const).fit()
print(ols_model.summary())
