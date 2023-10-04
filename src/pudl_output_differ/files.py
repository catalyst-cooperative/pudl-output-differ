"""Generic utilities for diffing PUDL_OUTPUT directories."""

from pathlib import Path
import logging
import re
from opentelemetry import trace

import fsspec
from pudl_output_differ.parquet import ParquetEvaluator
from pudl_output_differ.sqlite import SQLiteDBEvaluator

from pudl_output_differ.types import (
    DiffEvaluatorBase, DiffTreeNode, KeySetDiff, TaskQueue
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class OutputDirectoryEvaluator(DiffEvaluatorBase):
    """Represents diff between two directories."""
    left_path: str
    right_path: str
    local_cache_root: str
    filename_filter: str = ""

    def get_files(self, root_path: str) -> dict[str, str]:
        """Returns list of files in the output directory.

        The files are returned in a dictionary where keys are
        relative paths to the files and values are fully qualified
        paths.
        """
        # TODO(rousik): check if root_path is an actual directory.
        out: dict[str, str] = {}

        # FIXME(rousik): glob doesn't actually do recursive search here,
        # at least not for local files.
        fs, fs_base_path = fsspec.core.url_to_fs(root_path)        
        for fpath in fs.glob(fs_base_path + "/**"):
            rel_path = Path(fpath).relative_to(fs_base_path).as_posix()
            if self.filename_filter:
                if not re.match(self.filename_filter, rel_path):
                    continue
            out[rel_path] = fs.unstrip_protocol(fpath)
        logger.debug(f"Found {len(out)} files in {root_path}.")
        return out

    # TODO(rousik): passing parents this way is a bit clunky, but acceptable.
    @tracer.start_as_current_span(name="OutputDirectoryEvaluator.execute")
    def execute(self, task_queue: TaskQueue) -> list[DiffTreeNode]:
        """Computes diff between two output directories.

        Files on the left and right are compared for presence, children
        are deeper-layer analyzers for specific file types that are supported.
        """
        sp = trace.get_current_span()
        sp.set_attribute("left_path", self.left_path)
        sp.set_attribute("right_path", self.right_path)
        lfs = self.get_files(self.left_path)
        rfs = self.get_files(self.right_path)

        files_node = self.parent_node.add_child(
            DiffTreeNode(
                name="Files",
                diff=KeySetDiff.from_sets(set(lfs), set(rfs)),
            )
        )
        for shared_file in files_node.diff.shared:
            if shared_file.endswith(".sqlite"):
                task_queue.put(
                    SQLiteDBEvaluator(
                        db_name=shared_file,
                        left_db_path=lfs[shared_file],
                        right_db_path=rfs[shared_file],
                        parent_node = files_node,
                    )
                )
            elif shared_file.endswith(".parquet"):
                task_queue.put(
                    ParquetEvaluator(
                        left_path=lfs[shared_file],
                        right_path=rfs[shared_file],
                        parent_node=files_node,
                    )
                )
        return [files_node]