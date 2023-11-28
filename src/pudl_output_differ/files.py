"""Generic utilities for diffing PUDL_OUTPUT directories."""

from pathlib import Path
import logging
import re
from typing import Counter, Iterator
from opentelemetry import trace

import fsspec
from pudl_output_differ.parquet import ParquetAnalyzer, ParquetFile
from pudl_output_differ.sqlite import Database, SQLiteAnalyzer

from pudl_output_differ.types import (
    Result, Analyzer, KeySetDiff, TaskQueueInterface
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


def is_remote(dir: str) -> bool:
    """Returns true if the directory points to a remote fs."""
    fs, fs_base_path = fsspec.core.url_to_fs(dir)
    # TODO(rousik): this doesn't support s3 or other remote fs
    # yet, but that's okay as we don't work with those.
    return "gcs" in fs.protocol


class DirectoryAnalyzer(Analyzer):
    """Compares two directories for files."""
    left_path: str
    right_path: str

    # TODO(rousik): The following should be runtime
    # settings for this analyzer, ideally loaded from
    # aconfig file or env variables.
    local_cache_root: str | None = None
    filename_filter: str = ""

    def get_title(self) -> str:
        return "## Files"
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

    def retrieve_remote(self, full_path: str) -> str:
        """Translates remote paths to local file paths.
        
        Uses fsspec filecache feature to retrieve remote files.
        """
        fs, fspath = fsspec.core.url_to_fs(full_path)
        if "gcs" not in fs.protocol:
            return fspath
        
        if not self.local_cache_root:
            raise RuntimeError("Local cache root is not set.")
        fs = fsspec.filesystem(
            "filecache",
            target_protocol="gcs",
            cache_storage=self.local_cache_root,
        )
        f = fs.open(f"filecache://{fspath}")
        logger.info(f"Remote file {full_path} cached as {f.name}")
        # TODO(rousik): perhaps the best approach would be to instantiate
        # filecache once and pass it to SQLiteAnalyzer, with the full
        # paths rewritten. That way, we can use provided filecache
        # fs to detrmine local path only when needed.
        return f.name

    def execute(self, task_queue: TaskQueueInterface) -> Iterator[Result]:
        """Computes diff between two output directories.

        Files on the left and right are compared for presence, children
        are deeper-layer analyzers for specific file types that are supported.
        """
        trace.get_current_span().set_attributes(
            {
                "left_path": self.left_path,
                "right_path": self.right_path,
            }
        )

        lfs = self.get_files(self.left_path)
        rfs = self.get_files(self.right_path)

        file_diff = KeySetDiff.from_sets(set(lfs), set(rfs), entity="files")

        unsupported_formats = Counter()
        for shared_file in file_diff.shared:
            if shared_file.endswith(".sqlite"):
                # Cache remote files locally
                left_path = self.retrieve_remote(lfs[shared_file])
                right_path = self.retrieve_remote(rfs[shared_file])

                task_queue.put(
                    SQLiteAnalyzer(
                        object_path=self.object_path.extend(Database(name=shared_file)),
                        db_name=shared_file,
                        left_db_path=left_path,
                        right_db_path=right_path,
                    )
                )
            elif shared_file.endswith(".parquet"):
                task_queue.put(
                    ParquetAnalyzer(
                        object_path=self.object_path.extend(ParquetFile(name=shared_file)),
                        name=shared_file,
                        left_path=lfs[shared_file],
                        right_path=rfs[shared_file],
                    )
                )
            else:
                fmt = Path(shared_file).suffix.lstrip(".").strip()
                if fmt:
                    unsupported_formats.update([fmt])
            # TODO(rousik): add support for yml (perhaps sensitive to kinds of yml files we have)
            # TODO(rousik): add support for json, csv, etc..
        if unsupported_formats:
            for fmt, count in unsupported_formats.items():
                logger.warning(f"Unsupported file format {fmt} found {count} times.")

        if file_diff.has_diff():
            yield Result(markdown=file_diff.markdown(long_format=True))