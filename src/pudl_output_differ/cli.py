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
import shutil
import sys
import tempfile

import markdown

from pudl_output_differ.files import DirectoryAnalyzer, is_remote
from pudl_output_differ.types import TaskQueue
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry import trace
from mdx_gfm import GithubFlavoredMarkdownExtension

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


def parse_command_line(argv) -> argparse.Namespace:
    """Parse command line arguments. See the -h option.

    Args:
        argv (str): Command line arguments, including caller filename.

    Returns:
        dict: Dictionary of command line arguments and their parsed values.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("left", type=str, help="path containing left side of outputs.")
    parser.add_argument(
        "right", type=str, help="path containing right side of outputs."
    )
    parser.add_argument(
        "--cache-dir", type=str, help="Directory where remote files should be cached."
    )
    parser.add_argument(
        "--filename-filter",
        type=str,
        default="",
        help="If specified, only look at files that match this regex filter.",
    )
    parser.add_argument(
        "--max-workers", type=int, default=1, help="Number of worker threads to use."
    )
    parser.add_argument(
        "--catch-exceptions",
        type=bool,
        default=True,
        help="""If True, runtime exceptions produced by analyzers will
        be rendered as markdown and included in the report.
        If False, these exceptions will be re-raised and the program will
        abort.
        """,
    )
    parser.add_argument(
        "--html-report",
        type=str,
        default="",
        help="""If set, write html markdown report into this file.""",
    )

    parser.add_argument(
        "--github-repo",
        type=str,
        default="",
        help="Name of the github repository where comments should be posted.",
    )
    parser.add_argument(
        "--github-pr",
        type=int,
        default=0,
        help="If supplied, diff will be published as a comment to the github PR.",
    )
    parser.add_argument(
        "--trace-backend",
        default="",
        # default="http://localhost:4317/",
        help="Address of the OTEL compatible trace backend.",
    )
    parser.add_argument(
        "--loglevel",
        type=str,
        default="DEBUG",
        # default="INFO",
        help="Controls the severity of logging.",
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def main() -> int:
    """Run differ on two directories."""
    args = parse_command_line(sys.argv)

    logging.basicConfig(stream=sys.stdout, level=args.loglevel)

    if args.trace_backend:
        logger.info(f"Configuring tracing to OTEL backend {args.trace_backend}")
        provider = TracerProvider(
            resource=Resource(
                attributes={
                    SERVICE_NAME: "pudl-output-differ",
                },
            ),
        )
        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=args.trace_backend))
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

    # provider = TracerProvider()
    # TODO(rousik): add support for other trace backends.
    # processor = BatchSpanProcessor(ConsoleSpanExporter())
    # provider.add_span_processor(processor)
    # trace.set_tracer_provider(provider)

    if not args.cache_dir and any(is_remote(p) for p in [args.left, args.right]):
        args.cache_dir = tempfile.mkdtemp()
        logger.info(f"Created temporary cache directory {args.cache_dir}")
        atexit.register(shutil.rmtree, args.cache_dir)

    lpath = args.left
    rpath = args.right

    with tracer.start_as_current_span(name="main"):
        task_queue = TaskQueue(max_workers=args.max_workers)

        task_queue.put(
            DirectoryAnalyzer(
                object_path=[],
                left_path=lpath,
                right_path=rpath,
                local_cache_root=args.cache_dir,
                filename_filter=args.filename_filter,
            )
        )
        reports = 0
        nonempty_reports = 0
        for analysis in task_queue.iter_analyses(
            catch_exceptions=args.catch_exceptions
        ):
            reports += 1
            # TODO(rousik): it would be good if AnalysisReport contained metadata
            # identifyng the analyzer that produced it. Perhaps we could use
            # wrapper that will contain both the analysis, as well as the analyzer
            # metadata, e.g.:
            # - object_path
            # - instance that produced it (config)
            # - possible runtime exception information (so that we can distinguish)
            # Analysis itself could have severity (ERROR, WARNING) to indicate
            # whether the problem is serious or not.
            if analysis.markdown:
                nonempty_reports += 1
                print(analysis.title)
                print(analysis.markdown)
                print()
        logger.info(f"Total {reports} reports, with {nonempty_reports} nonempty.")

    if args.html_report:
        md = task_queue.to_markdown(catch_exceptions=True)
        with open(args.html_report, "w") as f:
            f.write(markdown.markdown(md, extensions=[GithubFlavoredMarkdownExtension()]))
        with open(args.html_report + ".markdown", "w") as f:
            f.write(md)

    # TODO(rousik): for the proper output, sort the
    # analyses by their object_path and construct the
    # title depth automatically (by skipping empty analyses
    # or by calculating depth based on the object_path).

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
