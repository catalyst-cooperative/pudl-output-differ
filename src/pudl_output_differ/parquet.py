"""Module for comparing contents of parquet files."""

import logging
from typing import Iterator

import pyarrow.parquet as pq
from pydantic_settings import BaseSettings, SettingsConfigDict

from pudl_output_differ.types import (
    Analyzer,
    Result,
    TaskQueueInterface,
    TypeDef,
)

logger = logging.getLogger(__name__)


class ParquetSettings(BaseSettings):
    """Default configuration for the parquet analysis."""
    model_config = SettingsConfigDict(env_prefix="diff_")


class ParquetFile(TypeDef):
    """Represents parquet file."""
    name: str

    def __str__(self):
        return f"ParquetFile({self.name})"


class ParquetAnalyzer(Analyzer):
    name: str
    left_path: str
    right_path: str
    # TODO(rousik): add settings once we know how to tune this

    def execute(self, task_queue: TaskQueueInterface) -> Iterator[Result]:
        lmeta = pq.read_metadata(self.left_path)
        rmeta = pq.read_metadata(self.right_path)
        if not lmeta.schema.equals(rmeta.schema):
            yield Result(markdown=" * parquet schemas are different.\n")
            # TODO(rousik): add comparison of schema columns

        # TODO(rousik): try loading the contents of the parquet files
        # for comparison. This will be similar to sqlite and may reuse
        # the same pandas machinery.

# logger = logging.getLogger(__name__)


# class ParquetEvaluator(DiffEvaluatorBase):
#     left_path: str
#     right_path: str


#     def get_columns(self, schema: pq.ParquetSchema)-> list[str]:
#         """Return list containing column_name::column_type."""
#         ret = []
#         for i in len(schema.names):
#             ret.append(
#                 schema.column(i).name + "::" + schema.column(i).logical_type.type
#             )
#         return ret
    
#     def execute(self, task_queue: TaskQueue) -> list[DiffTreeNode]:
#         """Compare two parquet files."""
#         diffs = []
#         # lfs, lpath = fsspec.core.url_to_fs(self.left_path)
#         # rfs, rpath = fsspec.open(self.right_path)
        
#         lmeta = pq.read_metadata(self.left_path)
#         rmeta = pq.read_metadata(self.right_path)
#         if not lmeta.schema.equals(rmeta.schema):
#             logger.info("Parquet schemas are different.")
#             diffs.append(
#                 self.parent_node.add_child(
#                     DiffTreeNode(
#                         name="ParquetSchema",
#                         diff=KeySetDiff.from_sets(
#                             set(self.get_columns(lmeta.schema)),
#                             set(self.get_columns(rmeta.schema)),
#                             entity="columns",
#                         )
#                     )
#                 )
#             )
#         # Now, go on to compare the metadata more broadly.
#         if not lmeta.equals(rmeta):
#             logger.info("Parquet metadata are different.")
#             logger.info(f"Left metadata: {lmeta}")
#             logger.info(f"Right metadata: {rmeta}")
#             if lmeta.num_rows != rmeta.num_rows:
#                 logger.info("Number of rows are different.")
#                 diffs.append(
#                     self.parent_node.add_child(
#                         DiffTreeNode(
#                             name="ParquetNumRows",
#                             diff=RowCountDiff(
#                                 left_rows=lmeta.num_rows,
#                                 right_rows=rmeta.num_rows)
#                         )
#                     )
#                 )
#         return diffs