"""Generic utilities for diffing PUDL_OUTPUT directories."""

from pathlib import Path
from queue import Queue
from typing import Iterator
import logging

import fsspec
from pudl_output_differ.sqlite import SQLiteDBEvaluator

from pudl_output_differ.types import (
    DiffEvaluator, DiffEvaluatorBase, DiffTreeNode, KeySetDiff
)

logger = logging.getLogger(__name__)


class OutputDirectoryEvaluator(DiffEvaluatorBase):
    """Represents diff between two directories."""
    left_path: str
    right_path: str
    local_cache_root: str

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
        for fpath in fs.glob(f"{fs_base_path}/**"):
            rel_path = Path(fpath).relative_to(fs_base_path).as_posix()
            out[rel_path] = fs.unstrip_protocol(fpath)
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
    def execute(self, task_queue: Queue[DiffEvaluator]) -> Iterator[DiffTreeNode]:
        """Computes diff between two output directories.

        Files on the left and right are compared for presence, children
        are deeper-layer analyzers for specific file types that are supported.
        """
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
        yield files_node