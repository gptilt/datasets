from sklearn.metrics import root_mean_squared_error
from sklearn.model_selection import train_test_split
import polars as pl
from storage import StorageParquet

storage_pq = StorageParquet(
    root="/mnt/ai/gptilt/datasets",
    dataset="tables",
    schema="base",
    tables=["matches", "participants", "events"],
)

region="asia"


df_matches = storage_pq.load_to_polars("matches") #, region=region)

df_participants = storage_pq.load_to_polars("participants") #, region=region)
# One-hot encode Summoner Spell IDs
# Get unique summoner spell IDs from both columns
set_of_summoner_spell_ids = set(
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
    )).cast(pl.UInt8).alias(f"summoner_spell_{spell_id}")
    for spell_id in set_of_summoner_spell_ids
])

df_events = storage_pq.load_to_polars("events") #, region=region)
df_participant_frames = df_events.filter(df_events["type"] == "PARTICIPANT_FRAME")

# Shift the xp column to create future_xp
df_samples = (
    df_participant_frames
    .sort(["matchId", "participantId", "timestamp"])
    .group_by(["matchId", "participantId"], maintain_order=True)
    .agg([
        pl.col("timestamp"),
        pl.col("timestamp").shift(-1).alias("future_timestamp"),
        pl.col("xp"),
        pl.col("xp").shift(-1).alias("future_xp"),
    ])
    .explode(["timestamp", "future_timestamp", "xp", "future_xp"])  # unpack lists into rows
    .drop_nans(["future_xp"])  # drop rows with NaN future_xp
)

# One-hot encode participantId
df_samples = df_samples.with_columns(
    df_samples["participantId"].to_dummies().cast(pl.UInt8)
)

# Add team
# df_samples = df_samples.with_columns(
#     (pl.col("participantId") > 5).cast(pl.UInt8).alias("team")
# )

# Join with participants to get summoner spell columns
# df_samples = df_samples.join(
#     df_participants,
#     on=["matchId", "participantId"],
#     how="inner",
# ).select(df_samples.columns + [col for col in df_participants.columns if col.startswith("summoner_spell_")])
print(df_samples.columns)

### Visualize the data
import matplotlib.pyplot as plt

# Let's say you want to plot XP vs timestamp
# Convert participantId to numeric values if it's not already
participant_ids = df_samples["participantId"].to_numpy()
participant_groups = ((participant_ids - 1) % 5)
timestamps = df_samples["timestamp"].to_numpy()

from matplotlib.colors import ListedColormap

# Custom 5-color map (picked manually or from tab10)
custom_colors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red', 'tab:purple']
cmap = ListedColormap(custom_colors)

# Scatter plot
plt.scatter(
    df_samples["xp"], 
    df_samples["future_xp"] / df_samples["xp"], 
    c=timestamps, 
    cmap=cmap,
    alpha=0.7
)
plt.ylim(1, 1.2)
plt.colorbar(ticks=range(5), label="Participant Group")
plt.xlabel("Timestamp")
plt.ylabel("XP")
plt.title("XP over Time colored by Participant")
plt.savefig("xp_over_time.png")

feature_cols = [
    col
    for col in df_samples.columns
    if col not in ("matchId", "participantId", "future_xp")
]
X = df_samples.select(feature_cols).to_numpy()
y = df_samples.select(["future_xp"]).to_numpy().ravel()  # .ravel() to flatten y to 1D

print(X.shape, y.shape)

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
from sklearn.ensemble import GradientBoostingRegressor
model = GradientBoostingRegressor(
    n_estimators=100,
    learning_rate=0.1,
    random_state=42,
    max_depth=10,
    max_features="sqrt",
)
from sklearn.linear_model import LinearRegression
model = LinearRegression()
model.fit(X_train, y_train)

# Predict
y_pred = model.predict(X_test)

# Evaluate properly
r2_score = model.score(X_test, y_test)
rmse = root_mean_squared_error(y_test, y_pred)

print(f"R²: {r2_score:.4f}")
print(f"RMSE: {rmse:.4f}")
print("Predictions:", y_pred[:5])
