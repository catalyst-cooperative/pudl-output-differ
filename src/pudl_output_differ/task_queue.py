"""
Implementation of the TaskQueue class that handles parallel execution
of the outstanding analyses.
"""

from collections import Counter
from time import sleep
from enum import IntEnum
from io import StringIO
import logging
import threading
import concurrent.futures
import traceback
from uuid import uuid1

from pydantic import UUID1, BaseModel, Field
from pudl_output_differ.sqlite import Database

from pudl_output_differ.types import AnalysisReport, Analyzer, ObjectPath, ReportSeverity

from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry import context, trace


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class ExecutionState(IntEnum):
    """Encodes possible state of Analysis in the queue.
    
    Each analysis will start as PENDING, once it is chosen to be executed,
    it moves to RUNNING and once it is completed, it can be either COMPLETED
    or FAILED.
    """
    PENDING = 0 
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3

class AnalysisMetadata(BaseModel):
    """Represents the analysis that should be run."""

    id: UUID1 = Field(default_factory=uuid1)
    object_path: ObjectPath
    analyzer: Analyzer
    report: AnalysisReport | None = None
    state: ExecutionState = ExecutionState.PENDING
    # TODO(rousik): perhaps add field for the traceback of the exception
    # or the exception itself (?); perhaps embedding this in the report is
    # okay also.


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
        self.max_workers = max_workers
        self.executor = concurrent.futures.ThreadPoolExecutor()
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self.pending_tasks: dict[UUID1, AnalysisMetadata] = {}
        self.inflight_tasks: dict[UUID1, AnalysisMetadata] = {}
        self.completed_tasks: dict[UUID1, AnalysisMetadata] = {}
        self.runners: list[concurrent.futures.Future] = []
        self.trace_carrier = {}
        self.max_db_concurrency = 1
        TraceContextTextMapPropagator().inject(self.trace_carrier)

    def has_pending_tasks(self):
        """Returns if any tasks are pending execution."""
        with self._lock:
            return len(self.pending_tasks) > 0
        
    def get_next_task(self) -> AnalysisMetadata | None:
        """Returns next pending analysis to run.
        
        Returns None if there's no task to run. This could mean the queue
        is empty, or maybe that all tasks have constraints that can't be 
        currently met.
        """
        with self._lock:
            if not self.pending_tasks:
                return None
            
            # TODO(rousik): pick next task such there's only single RUNNING
            # task that has specific Database() instance in its path.

            db_open_count: Counter = Counter()
            for am in self.inflight_tasks.values():
                db: Database | None = am.object_path.get_first(Database) # type: ignore
                if db is None:
                    continue
                db_open_count.update(db.name)

            for am in self.pending_tasks.values():
                db: Database | None = am.object_path.get_first(Database) # type: ignore
                if db is not None and db_open_count.get(db.name, 0) >= self.max_db_concurrency:
                    # This task is currently not viable for scheduling.
                    continue
                
                # This task can be scheduled, move it to in-flight, change status and return.
                am.state = ExecutionState.RUNNING
                del self.pending_tasks[am.id]
                self.inflight_tasks[am.id] = am
                return am
        
            return None
    
    def analysis_runner(self) -> None:
        """Runs the analyses in the queue until there are no more pending tasks.

        This method is expected to be run multiple times in parallel and process
        the analysis queue until it is empty. It will also collect reports and
        possible runtime exceptions.
        """

        # TODO(rousik): Implement this, this can contain the exception catching code
        # once and for all. Resulting analyses can be put in the output queue.
        # Perhaps instead of using ThreadpoolExecutor, we can simply launch bunch
        # of these runners in parallel based on max_workers flag.
        token = None
        if self.trace_carrier:
            ctx = TraceContextTextMapPropagator().extract(carrier=self.trace_carrier)
            token = context.attach(ctx)
        try:
            # TODO(rousik): wrap this in spans to make it work well.
            while self.has_pending_tasks():
                cur_task = self.get_next_task()
                if cur_task is None:
                    logger.debug("No tasks to run, waiting for more...")
                    # We should ideally wait for when any of the in-flight tasks
                    # complete and then recalculate. But waiting for 1s and eagerly
                    # trying again is a reasonable workaround.
                    sleep(1)
                    continue

                analyzer_name = cur_task.analyzer.__class__.__name__
                with tracer.start_as_current_span(f"{analyzer_name}.execute") as sp:
                    sp.set_attribute("object_path", str(cur_task.object_path))
                    try:
                        cur_task.report = cur_task.analyzer.execute(self)
                        cur_task.state = ExecutionState.COMPLETED
                    except Exception:
                        cur_task.state = ExecutionState.FAILED
                        # Umph, the analyzer failed; log the problem and make synthetic report.
                        error_title = f"{cur_task.analyzer.__class__.__name__} failed on {next_analysis.object_path}"
                        cur_task.report = AnalysisReport(
                            object_path=cur_task.object_path,
                            title=f"## {error_title}",
                            markdown=f"\n```\n{traceback.format_exc()}\n```\n",
                            severity=ReportSeverity.EXCEPTION,
                        )
                # Move the analysis to completed state.
                with self._lock:
                    self.completed_tasks[cur_task.id] = cur_task
                    del self.inflight_tasks[cur_task.id]

                    # The following print-as-they-become available should be a debug feature.
                    # TODO(rousik): It should be possible to turn this functionality off.
                with self._lock:
                    if cur_task.report and cur_task.report.has_changes():
                        print(cur_task.report.title)
                        print(cur_task.report.markdown)

            logger.info("Queue has no more pending tasks, terminating this runner.")
        finally:
            if token is not None:
                context.detach(token)

    def run(self):
        """Kicks off analysis_runners in the thread pool."""
        for i in range(self.max_workers):
            self.runners.append(self.executor.submit(self.analysis_runner))

    def put(self, analyzer: Analyzer):
        """Add evaluator to the execution queue."""
        with self._lock:
            am = AnalysisMetadata(
                object_path=analyzer.object_path,
                analyzer=analyzer,
            )
            self.pending_tasks[am.id] = am
            # Do we need to kick of analyzer threads/tasks?
    
    def wait(self):
        """Awaits until all tasks are completed."""
        concurrent.futures.wait(self.runners, return_when="ALL_COMPLETED")        

    def to_markdown(self) -> str:
        """Convert all reports to markdown, sorting them by their object_paths."""
        assert len(self.pending_tasks) + len(self.inflight_tasks) == 0

        md = StringIO()
        for task in sorted(self.completed_tasks.values(), key=lambda t: t.object_path):
            if task.report and task.report.has_changes():
                md.write(f"\n{task.report.title}\n")
                md.write(task.report.markdown)
        return md.getvalue()