"""Module for comparing contents of parquet files."""

import logging
from pudl_output_differ.sqlite import RowCountDiff
from pudl_output_differ.types import DiffEvaluatorBase, DiffTreeNode, KeySetDiff, TaskQueue
import pyarrow.parquet as pq


logger = logging.getLogger(__name__)


class ParquetEvaluator(DiffEvaluatorBase):
    left_path: str
    right_path: str


    def get_columns(self, schema: pq.ParquetSchema)-> list[str]:
        """Return list containing column_name::column_type."""
        ret = []
        for i in len(schema.names):
            ret.append(
                schema.column(i).name + "::" + schema.column(i).logical_type.type
            )
        return ret
    
    def execute(self, task_queue: TaskQueue) -> list[DiffTreeNode]:
        """Compare two parquet files."""
        diffs = []
        # lfs, lpath = fsspec.core.url_to_fs(self.left_path)
        # rfs, rpath = fsspec.open(self.right_path)
        
        lmeta = pq.read_metadata(self.left_path)
        rmeta = pq.read_metadata(self.right_path)
        if not lmeta.schema.equals(rmeta.schema):
            logger.info("Parquet schemas are different.")
            diffs.append(
                self.parent_node.add_child(
                    DiffTreeNode(
                        name="ParquetSchema",
                        diff=KeySetDiff.from_sets(
                            set(self.get_columns(lmeta.schema)),
                            set(self.get_columns(rmeta.schema))
                        )
                    )
                )
            )
        # Now, go on to compare the metadata more broadly.
        if not lmeta.equals(rmeta):
            logger.info("Parquet metadata are different.")
            logger.info(f"Left metadata: {lmeta}")
            logger.info(f"Right metadata: {rmeta}")
            if lmeta.num_rows != rmeta.num_rows:
                logger.info("Number of rows are different.")
                diffs.append(
                    self.parent_node.add_child(
                        DiffTreeNode(
                            name="ParquetNumRows",
                            diff=RowCountDiff(
                                left_rows=lmeta.num_rows,
                                right_rows=rmeta.num_rows)
                        )
                    )
                )
        return diffs