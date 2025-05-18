from datasets import GeneratorBasedBuilder, DatasetInfo, SplitGenerator, Split
import polars as pl


class PolarsDatasetBuilder(GeneratorBasedBuilder):
    BUILDER_CONFIGS = []  # to add configs dynamically

    def __init__(self, *args, tables: dict[pl.DataFrame], **kwargs):
        super().__init__(*args, **kwargs)
        # polars_tables is a dict of name -> polars.DataFrame
        self.polars_tables = tables

    def _info(self):
        # Infer schema
        return DatasetInfo()

    def _split_generators(self, dl_manager):
        # A split for each table
        return [SplitGenerator(name=name, gen_kwargs={"name": name}) for name in self.polars_tables]

    def _generate_examples(self, table_name):
        # 'table_name' here is passed from gen_kwargs and should match keys in self.polars_tables
        # It also implicitly means we are generating for the configuration matching this table_name.

        polars_df = self.polars_tables[table_name]
        arrow_table = polars_df.to_arrow()

        idx = 0
        # Iterate over Arrow record batches
        for batch in arrow_table.to_batches():
            # Convert the Arrow RecordBatch to a list of Python dicts
            # This is what GeneratorBasedBuilder expects _generate_examples to yield
            # The key for each example should be unique
            batch_as_pylist = batch.to_pylist()
            for record_dict in batch_as_pylist:
                yield idx, record_dict
                idx += 1
