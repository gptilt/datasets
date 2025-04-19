import os
import tempfile
import pytest
from storage import Storage

@pytest.fixture
def temp_env_root():
    with tempfile.TemporaryDirectory() as tmpdir:
        os.environ["DIR_ROOT"] = tmpdir
        yield tmpdir

@pytest.fixture
def storage_instance(temp_env_root):
    dataset = "my_dataset"
    schema = "my_schema"
    region = "euw1"
    tables = ["table1", "table2"]
    return Storage(dataset, schema, region, tables)

def test_directories_created(storage_instance):
    for table in storage_instance.tables:
        assert storage_instance.table_path(table).exists()
        assert storage_instance.table_path(table).is_dir()

def test_file_extension_default(storage_instance):
    assert storage_instance.file_extension == "json"

def test_total_size_in_gb(storage_instance):
    dummy_file = storage_instance.table_path("table1") / "test.json"
    dummy_file.write_text("a" * 1024 * 1024)  # ~1MB
    size_gb = storage_instance.total_size_in_gb()
    assert 0.0009 < size_gb < 0.0011  # Allow for minor float variance
