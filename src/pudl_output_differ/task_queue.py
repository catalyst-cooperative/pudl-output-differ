"""
Implementation of the TaskQueue class that handles parallel execution
of the outstanding analyses.
"""

from asyncio import ALL_COMPLETED
from io import StringIO
import logging
import threading
import concurrent.futures
import traceback
from typing import Iterator

from pudl_output_differ.types import AnalysisReport, Analyzer, ObjectPath, ReportSeverity

from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry import context


logger = logging.getLogger(__name__)



class AnalysisMetadata:
    """Represents the analysis that should be run."""

    object_path: ObjectPath
    analyzer: Analyzer




class TaskQueue:
    """Thread pool backed executor for diff evaluation."""
    def __init__(self, max_workers: int = 1, no_threadpool: bool = False):
        # TODO(rousik): when dealing with sqlite tables, we could consider
        # estimating their payload size and assigning cost to each task
        # to ensure that we do not overload the worker with memory
        # pressure.
        # For now, using single worker (slower but safer) is a reasonable
        # workaround or initial strategy.
        # We could also indicate which tables are possibly expensive
        # in the differ configuration, which will eliminate the need
        # for dynamically estimating the cost.

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.Lock()
        self.analyses: dict[concurrent.futures.Future, Analyzer] = {}
        self.trace_carrier = {}
        TraceContextTextMapPropagator().inject(self.trace_carrier)

    def run_analysis(self) -> AnalysisReport:
        """Picks analysis to run and executes it."""
        # TODO(rousik): Implement this, this can contain the exception catching code
        # once and for all. Resulting analyses can be put in the output queue.
        # Perhaps instead of using ThreadpoolExecutor, we can simply launch bunch
        # of these runners in parallel based on max_workers flag.
        ...

    def put(self, analyzer: Analyzer):
        """Add evaluator to the execution queue."""
        with self._lock:
            # TODO(rousik): here we should decide on how to handle
            # big tasks that need to be run in isolation, e.g. 
            # analyis of very large sql tables.
            def traced_execute():
                if self.trace_carrier:
                    ctx = TraceContextTextMapPropagator().extract(carrier=self.trace_carrier)
                    token = context.attach(ctx)
                    try:
                        logger.debug(f"Executing {analyzer.__class__.__name__} with configuration: {analyzer}")
                        return analyzer.execute(self)
                    finally:
                        context.detach(token)
                else:
                    return analyzer.execute(self)

            fut = self.executor.submit(traced_execute)
            self.analyses[fut] = analyzer
            # TODO(rousik): perhaps we can have a better way to associate
            # reports with the analyzer metadata and other info.

    def iter_analyses(self, catch_exceptions:bool=True) -> Iterator[AnalysisReport]:        
        keys_seen = set()
        while True:
            remaining_futures = set(self.analyses.keys()).difference(keys_seen)
            if not remaining_futures:
                return
            keys_seen.update(remaining_futures)
            for fut in concurrent.futures.as_completed(remaining_futures):
                analyzer = self.analyses[fut]
                try:
                    yield fut.result()
                except Exception as e:
                    error_title = f"{analyzer.__class__.__name__} failed on {analyzer.object_path}"
                    if not catch_exceptions:
                        raise RuntimeError(error_title) from e
                    logger.error(f"Analyzer {analyzer.__class__.__name__} failed on {analyzer.object_path}: {repr(e)}")

                    # Otherwise, render exception as markdown.
                    yield AnalysisReport(
                        object_path=analyzer.object_path,
                        title=f"## {error_title}",
                        markdown=f"\n```\n{traceback.format_exc()}\n```\n",
                        severity=ReportSeverity.EXCEPTION,
                    )
     
    def wait(self):
        """Waits until all tasks are done."""
        concurrent.futures.wait(self.analyses.keys(), return_when=ALL_COMPLETED)

    def to_markdown(self, catch_exceptions: bool = True) -> str:
        reports = list(self.iter_analyses(catch_exceptions=catch_exceptions))
        reports.sort(key=lambda r: r.object_path)
        md = StringIO()
        for rep in reports:
            if rep.has_changes():
                md.write(f"\n{rep.title}\n")
                md.write(rep.markdown)
        return md.getvalue()
     
    def get_analyses(self, catch_exceptions:bool = True) -> list[AnalysisReport]:
        """Retrieve all analysis reports.

        This is expected to be called only after wait().
        
        """
        self.wait()
        return list(self.iter_analyses(catch_exceptions=catch_exceptions))