"""Script for comparing two PUDL output directories.

This script scans the baseline and experiment directories, finds files that are missing
in one or the other side and for files that are present in both it will analyze the
contents, based on their type.

For SQLite databases, it will compare the tables, schemas and individual rows.

For other files, file checksums are calculated and compared instead.
"""
import argparse
import sys
import tempfile

from pudl_output_differ.files import OutputDirectoryEvaluator
from pudl_output_differ.types import DiffTreeNode, TaskQueue


def parse_command_line(argv) -> argparse.Namespace:
    """Parse command line arguments. See the -h option.

    Args:
        argv (str): Command line arguments, including caller filename.

    Returns:
        dict: Dictionary of command line arguments and their parsed values.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "left", type=str, help="path containing left side of outputs.")
    parser.add_argument(
        "right", type=str, help="path containing right side of outputs.")
    parser.add_argument(
        "--cache-into", type=str, help="Directory where remote files should be cached."
    )
    parser.add_argument(
        "--max-workers", type=int, default=4, 
        help="Number of worker threads to use."
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def main() -> int:
    """Run differ on two directories."""
    args = parse_command_line(sys.argv)

    cache_path = args.cache_into
    if not args.cache_into:
        cache_path = tempfile.mkdtemp()

    task_queue = TaskQueue(max_workers=args.max_workers)
    task_queue.put(OutputDirectoryEvaluator(
        parent_node=DiffTreeNode(name="Root"),
        left_path=args.left,
        right_path=args.right,
        local_cache_root=cache_path,
    ))
    has_diff = False
    for diff in task_queue.get_diffs():
        if diff.has_diff():
            has_diff = True
            print(diff)

    if has_diff:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())