import polars as pl
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from storage import StorageParquet


@pytest.fixture
def sample_partitioned_parquet(tmp_path):
    """Create a small hive-partitioned Parquet dataset."""
    base_path = tmp_path / "gptilt" / "match_v5" / "players"
    europe_path = base_path / "region=europe"
    europe_path.mkdir(parents=True)

    # Create dummy data
    europe_df = pl.DataFrame({
        "id": [1, 2],
        "name": ["Alice", "Bob"]
    })

    # Save as partitioned Parquet
    europe_df.write_parquet(europe_path / "data.parquet")

    return tmp_path


@pytest.fixture
def storage_parquet(sample_partitioned_parquet):
    dataset = "gptilt"
    schema = "match_v5"
    tables = ["games", "players"]
    return StorageParquet(sample_partitioned_parquet, dataset, schema, tables)


def test_initialization_defaults(storage_parquet):
    assert storage_parquet.file_extension == "parquet"
    assert storage_parquet.target_batch_size == 1_000_000_000
    for table in storage_parquet.tables:
        assert storage_parquet.buffer[table] == []
        assert storage_parquet.table_path(table).exists()


def test_has_record_false_when_no_dir(storage_parquet):
    result = storage_parquet.has_record("nonexistent_table", "some_column", "value")
    assert result is False


def test_has_record_false_when_column_missing(storage_parquet):
    # Create table with unrelated column
    table_path = storage_parquet.table_path("games")
    table = pa.table({"game_id": [1, 2, 3]})
    pq.write_table(table, table_path / "dummy.parquet")

    result = storage_parquet.has_record("games", "nonexistent_column", 123)
    assert result is False


def test_has_record_true_when_value_exists(storage_parquet):
    table_path = storage_parquet.table_path("players")
    table = pa.table({"summoner_id": ["a", "b", "c"]})
    pq.write_table(table, table_path / "dummy.parquet")

    assert storage_parquet.has_record("players", "summoner_id", "b") is True
    assert storage_parquet.has_record("players", "summoner_id", "z") is False


def test_has_records_in_all_tables(storage_parquet):
    for table_name in storage_parquet.tables:
        table_path = storage_parquet.table_path(table_name)
        table = pa.table({"game_id": ["abc", "def"]})
        pq.write_table(table, table_path / "file.parquet")

    assert storage_parquet.has_records_in_all_tables(game_id="abc") is True
    assert storage_parquet.has_records_in_all_tables(game_id="xyz") is False


def test_write_to_dataset_creates_file(storage_parquet):
    # Create a small table
    data = [{"id": i, "value": i * 10} for i in range(5)]
    table = pa.Table.from_pylist(data)

    storage_parquet.write_to_dataset("games", table)

    output_files = list(storage_parquet.table_path("games").glob("part*.parquet"))
    assert len(output_files) == 1
    assert pq.read_table(output_files[0]).to_pylist() == data


def test_store_batch_buffers_and_writes(storage_parquet):
    storage_parquet.target_batch_size = 0  # Force write regardless of size

    data = [{"name": f"Player{i}", "score": i} for i in range(1000)]
    storage_parquet.store_batch("players", data)

    output_files = list(storage_parquet.table_path("players").glob("part*.parquet"))
    assert len(output_files) == 1

    table = pq.read_table(output_files[0])
    assert len(table) == 1000
    assert table.column("name").to_pylist()[0] == "Player0"


def test_flush_writes_and_clears_buffer(storage_parquet, tmp_path):
    # Don't include 'region' in data, let partitioning logic add it
    storage_parquet.buffer["games"] = [{"gameId": 1}, {"gameId": 2}]
    storage_parquet.buffer["players"] = [{"playerId": "abc"}]

    # Flush with region partition
    nbytes = storage_parquet.flush(region="euw")

    assert nbytes > 0
    assert storage_parquet.buffer["games"] == []
    assert storage_parquet.buffer["players"] == []

    # Validate file creation and content
    games_path = tmp_path / "gptilt/match_v5/games/region=euw"
    players_path = tmp_path / "gptilt/match_v5/players/region=euw"

    assert any(f.suffix == ".parquet" for f in games_path.iterdir())
    assert any(f.suffix == ".parquet" for f in players_path.iterdir())

    table = pq.read_table(next(games_path.glob("*.parquet")))
    assert table.num_rows == 2
    assert set(table.column_names) == {"gameId", "region"}
    assert table.column("region").to_pylist() == ["euw", "euw"]


def test_load_to_polars_with_partition_filter(storage_parquet):
    # Load only europe region
    df = storage_parquet.load_to_polars("players", ["name"], region="europe")

    assert df.shape[0] == 2
    assert "name" in df.columns
    assert df["name"].to_list() == ["Alice", "Bob"]


def test_load_to_polars_with_nonexistent_partition(storage_parquet):
    # Try to load a region that doesn't exist
    df = storage_parquet.load_to_polars("players", region="asia")

    assert df.is_empty(), "Expected an empty DataFrame for nonexistent partition."


def test_store_polars_writes_partitioned_file(storage_parquet):
    df = pl.DataFrame({
        "summoner_id": ["x1", "x2"],
        "kills": [5, 6],
    })

    nbytes = storage_parquet.store_polars("players", df, region="euw")

    assert nbytes > 0

    partition_dir = storage_parquet.table_path("players") / "region=euw"
    assert partition_dir.exists(), "Expected partition directory to exist"

    output_files = list(partition_dir.glob("*.parquet"))
    assert len(output_files) == 1

    table = pq.read_table(output_files[0])
    assert table.column("region").to_pylist() == ["euw", "euw"]
