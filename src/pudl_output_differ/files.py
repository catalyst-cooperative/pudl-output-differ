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
        logger.info(f"Found {len(out)} files in {root_path}.")
        logger.info(f"These are: {out}")
        return out

    def maybe_cache_remote(self, url: str, local_path: str) -> str:
        """Downloads remote file to local cache."""
        fs, fs_path = fsspec.core.url_to_fs(url)
        if fs.protocol == "file":
            return fs_path
        lp = Path(self.local_cache_root) / local_path
        logger.warn(f"Need to cache remote file {url} to {lp}.")
        fs.get(fs_path, lp.as_posix())
        return lp.as_posix()

    # TODO(rousik): passing parents this way is a bit clunky, but acceptable.
    @tracer.start_as_current_span(name="OutputDirectoryEvaluator.execute")
    def execute(self, task_queue: TaskQueue) -> list[DiffTreeNode]:
        """Computes diff between two output directories.

        Files on the left and right are compared for presence, children
        are deeper-layer analyzers for specific file types that are supported.
        """
        sp = tracer.get_current_span()
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
                # Download remote files to local cache 
                left_path = self.maybe_cache_remote(
                    lfs[shared_file], f"left/{shared_file}")
                right_path = self.maybe_cache_remote(
                    rfs[shared_file], f"right/{shared_file}")

                task_queue.put(
                    SQLiteDBEvaluator(
                        db_name=shared_file,
                        left_db_path=left_path,
                        right_db_path=right_path,
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