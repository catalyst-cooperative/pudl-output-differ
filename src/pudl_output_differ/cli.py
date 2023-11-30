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

import fsspec
import markdown
from mdx_gfm import GithubFlavoredMarkdownExtension
from opentelemetry import trace
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from pudl_output_differ.files import DirectoryAnalyzer, is_remote
from pudl_output_differ.task_queue import TaskQueue
from pudl_output_differ.types import ObjectPath

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

# Removed the following from style:
# max-width: 980px;

MARKDOWN_CSS_STYLE = """
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="github-markdown-light.css">
<style>
	.markdown-body {
		box-sizing: border-box;
		min-width: 200px;
		margin: 0 auto;
		padding: 45px;
	}

	@media (max-width: 767px) {
		.markdown-body {
			padding: 15px;
		}
	}
</style>
"""

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
        "--gcp-cloud-trace",
        type=bool,
        default=False,
        help="If True, publish traces to GCP Cloud Trace service.",
    )
    parser.add_argument(
        "--otel-trace-backend",
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


def setup_tracing(args: argparse.Namespace) -> None:
    """Configures tracing based on the command line arguments."""
    provider = TracerProvider(
        resource=Resource(
            attributes={
                SERVICE_NAME: "pudl-output-differ",
            },
        ),
    )
    if args.otel_trace_backend:
        logger.info(f"Publishing traces to OTEL backend {args.otel_trace_backend}")
        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=args.trace_backend))
        provider.add_span_processor(processor)

    if args.gcp_cloud_trace:
        logger.info("Publishing traces to Google Cloud Trace service.")
        provider.add_span_processor(
            BatchSpanProcessor(CloudTraceSpanExporter())
        )        
    trace.set_tracer_provider(provider)


def main() -> int:
    """Run differ on two directories."""
    args = parse_command_line(sys.argv)

    logging.basicConfig(stream=sys.stdout, level=args.loglevel)
    setup_tracing(args)

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
                object_path=ObjectPath(),
                left_path=lpath,
                right_path=rpath,
                local_cache_root=args.cache_dir,
                filename_filter=args.filename_filter,
            )
        )
        task_queue.run()
        task_queue.wait()

    if args.html_report:
        md = task_queue.to_markdown()
        fs, report_path = fsspec.core.url_to_fs(args.html_report.removesuffix(".html"))
        with fs.open(f"{report_path}.html", "w") as f:
            f.write(MARKDOWN_CSS_STYLE)
            f.write('<article class="markdown-body">')
            f.write(markdown.markdown(md, extensions=[GithubFlavoredMarkdownExtension()]))
            f.write('</article>')
        with fs.open(f"{report_path}.markdown", "w") as f:
            f.write(markdown.markdown(md, extensions=[GithubFlavoredMarkdownExtension()]))

    # TODO(rousik): add suopport for publishing comments to github PRs/analyses.
    return 0


if __name__ == "__main__":
    sys.exit(main())
