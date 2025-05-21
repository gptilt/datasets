import polars as pl
import pytest
import storage


@pytest.fixture
def storage_partition(tmp_path):
    schema = "basic"
    dataset = "matches"
    tables = ["games", "players"]
    return storage.StoragePartition(
        tmp_path,
        schema,
        dataset,
        tables,
        partition_col="region",
        partition_val="europe"
    )


def test_initialization_defaults(storage_partition):
    assert storage_partition.file_extension == "parquet"
    assert storage_partition.target_batch_size == 2_000_000_000
    for table in storage_partition.tables:
        assert storage_partition.table_path(table).exists()


def test_has_record_false_when_no_dir(storage_partition):
    result = storage_partition.has_record("nonexistent_table", "some_column", "value")
    assert result is False


def test_has_record_false_when_column_missing(storage_partition):
    # Create table with unrelated column
    df = pl.DataFrame({"game_id": [1, 2, 3]})
    storage_partition._flush_table("games", df)

    result = storage_partition.has_record("games", "nonexistent_column", 123)
    assert result is False


def test_has_record_true_when_value_exists(storage_partition):
    df = pl.DataFrame({"summoner_id": ["a", "b", "c"]})
    storage_partition._flush_table("players", df)

    assert storage_partition.has_record("players", "summoner_id", "b") is True
    assert storage_partition.has_record("players", "summoner_id", "z") is False


def test_has_records_in_all_tables(storage_partition):
    for table_name in storage_partition.tables:
        table_path = storage_partition.table_path(table_name)
        df = pl.DataFrame({"game_id": ["abc", "def"]})
        df.write_parquet(table_path / "file.parquet")

    assert storage_partition.has_records_in_all_tables(game_id="abc") is True
    assert storage_partition.has_records_in_all_tables(game_id="xyz") is False


def test_flush_table_creates_file(storage_partition):
    # Create a small table
    data = [{"id": i, "value": i * 10, "region": "europe"} for i in range(5)]
    df = storage.to_polars(data)

    storage_partition._flush_table("games", df)
       
    partition_dir = storage_partition.table_path("games")
    output_files = list(partition_dir.glob("*.parquet"))
    assert len(output_files) == 1
    assert pl.read_parquet(partition_dir).to_dicts() == data


def test_store_batch_buffers_and_writes(storage_partition):
    storage_partition.target_batch_size = 0  # Force write regardless of size

    data = [{"name": f"Player{i}", "score": i} for i in range(1000)]
    storage_partition.store_batch("players", data)

    partition_dir = storage_partition.table_path("players")
    output_files = list(partition_dir.glob("*.parquet"))
    assert len(output_files) == 1

    df = pl.read_parquet(partition_dir)
    assert len(df) == 1000
    assert df["name"].to_list()[0] == "Player0"


def test_flush_writes_and_clears_buffer(storage_partition, tmp_path):
    # Don't include 'region' in data, let partitioning logic add it
    storage_partition.buffer["games"] = pl.from_dicts([{"gameId": 1}, {"gameId": 2}])
    storage_partition.buffer["players"] = pl.from_dicts([{"playerId": "abc"}])

    # Flush with region partition
    nbytes = storage_partition.flush()

    assert nbytes > 0
    assert "games" not in storage_partition.buffer
    assert "players" not in storage_partition.buffer

    # Validate file creation and content
    games_path = tmp_path / "basic/matches/games"
    players_path = tmp_path / "basic/matches/players"

    assert any(f.suffix == ".parquet" for f in games_path.iterdir())
    assert any(f.suffix == ".parquet" for f in players_path.iterdir())

    df = pl.read_parquet(games_path)
    assert len(df) == 2
    assert set(df.columns) == {"gameId", "region"}
    assert df["region"].to_list() == ["europe", "europe"]


def test_load_to_polars(storage_partition):
    # Create dummy data
    europe_df = pl.DataFrame({
        "id": [1, 2],
        "name": ["Alice", "Bob"],
        "region": ["europe", "europe"]
    })

    # Save as partitioned Parquet
    europe_df.write_parquet(storage_partition.table_path("players") / "region_europe-00000.parquet")

    # Load only europe region
    df = storage_partition.load_to_polars("players", ["name"])

    assert df.shape[0] == 2
    assert "name" in df.columns
    assert df["name"].to_list() == ["Alice", "Bob"]
