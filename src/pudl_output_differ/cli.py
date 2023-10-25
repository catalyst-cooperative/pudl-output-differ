"""Script for comparing two PUDL output directories.

This script scans the baseline and experiment directories, finds files that are missing
in one or the other side and for files that are present in both it will analyze the
contents, based on their type.

For SQLite databases, it will compare the tables, schemas and individual rows.

For other files, file checksums are calculated and compared instead.
"""
import argparse
import atexit
import logging
import os
import shutil
import sys
import tempfile

from pudl_output_differ.files import DirectoryAnalyzer, OutputDirectoryEvaluator
from pudl_output_differ.types import DiffTreeNode, TaskQueue
from github import Github

logger = logging.getLogger(__name__)


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
        "--cache-dir", type=str, help="Directory where remote files should be cached."
    )
    parser.add_argument(
        "--filename-filter",
        type=str, default="",
        help="If specified, only look at files that match this regex filter."
    )
    parser.add_argument(
        "--max-workers", type=int, default=4, 
        help="Number of worker threads to use."
    )
    parser.add_argument(
        "--github-repo", type=str, default="",
        help="Name of the github repository where comments should be posted."
    )
    parser.add_argument(
        "--github-pr", type=int, default=0,
        help="If supplied, diff will be published as a comment to the github PR."
    )
    parser.add_argument(
        "--trace-backend-otel",
        default="http://localhost:4317",
        help="Address of the OTEL compatible trace backend."
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def main() -> int:
    """Run differ on two directories."""
    args = parse_command_line(sys.argv)
    
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    # provider = TracerProvider()
    # TODO(rousik): add support for other trace backends.
    # processor = BatchSpanProcessor(ConsoleSpanExporter())
    # provider.add_span_processor(processor)
    # trace.set_tracer_provider(provider)

    if not args.cache_dir:
        args.cache_dir = tempfile.mkdtemp()
        atexit.register(shutil.rmtree, args.cache_dir)

    lpath = args.left
    rpath = args.right
        
    task_queue = TaskQueue(max_workers=args.max_workers)

    task_queue.put(
        DirectoryAnalyzer(
            left_path=lpath,
            right_path=rpath,
            local_cache_root=args.cache_dir, 
        )
    )
    # Debug diagnostic
    task_queue.wait()

    # if args.github_pr and args.github_repo:
    #     gh = Github(os.environ["GITHUB_TOKEN"])
    #     gh.get_repo(args.github_repo).get_pull(args.github_pr)
    #     task_queue.wait()
    #     # Iterate diff tree in a BFS manner and generate PR comment. 
    # else:
    #     for diff in task_queue.get_diffs():
    #         if diff.has_diff():
    #             has_diff = True
    #             print(diff)

    # if args.github_pr and args.github_repo:
    #     gh = Github(os.environ["GITHUB_TOKEN"])
    #     gh.get_repo(args.github_repo).get_pull(args.github_pr).create_issue_comment(
    #         body=diff.to_github_comment()
    #     )

    # if has_diff:
    #     return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())