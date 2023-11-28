"""
Implementation of the TaskQueue class that handles parallel execution
of the outstanding analyses.
"""

import concurrent.futures
import logging
import threading
import traceback
from collections import Counter
from enum import IntEnum
from io import StringIO
from time import sleep
from uuid import uuid1

from opentelemetry import trace
from progress.bar import Bar
from pydantic import UUID1, BaseModel, Field

from pudl_output_differ.sqlite import Database
from pudl_output_differ.types import (
    Analyzer,
    ObjectPath,
    ReportSeverity,
    Result,
)

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
    results: list[Result] = []
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
        self.max_db_concurrency = 2
        self.progress_bar = Bar(max=100)
        
    def has_unfinished_tasks(self):
        """Returns true if any tasks are pending or in-flight."""
        with self._lock:
            return len(self.pending_tasks) + len(self.inflight_tasks) > 0
        
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
    
    @tracer.start_as_current_span("TaskQueue.analysis_runner")
    def analysis_runner(self, runner_id: str = "") -> None:
        """Runs the analyses in the queue until there are no more pending tasks.

        This method is expected to be run multiple times in parallel and process
        the analysis queue until it is empty. It will also collect reports and
        possible runtime exceptions.
        """
        tasks_processed = 0
        trace.get_current_span().set_attribute("runner_id", runner_id)
        # TODO(rousik): wrap this in spans to make it work well.
        while self.has_unfinished_tasks():
            with tracer.start_as_current_span("select_next_task"):
                cur_task = self.get_next_task()
                if cur_task is None:
                    logger.debug("No tasks to run, waiting for more...")
                    # We should ideally wait for when any of the in-flight tasks
                    # complete and then recalculate. But waiting for 1s and eagerly
                    # trying again is a reasonable workaround.
                    sleep(1)
                    continue

            analyzer_name = cur_task.analyzer.__class__.__name__
            try:
                tasks_processed += 1
                with tracer.start_as_current_span(f"{analyzer_name}.execute") as sp:
                    sp.set_attribute("object_path", str(cur_task.object_path))
                    cur_task.results = cur_task.analyzer.execute_sync(self)
                    cur_task.state = ExecutionState.COMPLETED
            except Exception:
                cur_task.state = ExecutionState.FAILED
                cur_task.results.append(
                    Result(
                        severity=ReportSeverity.EXCEPTION,
                        markdown=f"\n```\n{traceback.format_exc()}\n```\n",    
                    )
                )
            # Move the analysis to completed state.
            with self._lock:
                self.progress_bar.max = (
                    len(self.pending_tasks) + 
                    len(self.inflight_tasks) + 
                    len(self.completed_tasks)
                )
                self.progress_bar.next()
                self.completed_tasks[cur_task.id] = cur_task
                del self.inflight_tasks[cur_task.id]

            # The following print-as-they-become available should be a debug feature.
            # TODO(rousik): It should be possible to turn this functionality off.
            # with self._lock:
            #     if cur_task.results:
            #         print(cur_task.analyzer.get_title())
            #         for res in cur_task.results:
            #             print(res.markdown)
        logger.info(f"Runner {runner_id} terminating after {tasks_processed} tasks.")

    def run(self, wait: bool=True):
        """Kicks off analysis_runners in the thread pool.

        Args:
            wait: if True, this method will block until all tasks are completed.

        """
        for i in range(self.max_workers):
            self.runners.append(self.executor.submit(self.analysis_runner, runner_id=str(i)))
        if wait:
            self.wait()

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
        """Awaits until all tasks are completed.
        """
        done_tasks, _ = concurrent.futures.wait(self.runners, return_when="ALL_COMPLETED")
        for dt in done_tasks:
            # If any of the runners failed, the following will raise the exception that
            # caused the failure.
            # We do not gracefully deal with worker failures so lets just fail hard.
            dt.result()

    def to_markdown(self) -> str:
        """Convert all reports to markdown, sorting them by their object_paths."""
        assert len(self.pending_tasks) + len(self.inflight_tasks) == 0

        md = StringIO()
        md.write("# Summary\n")
        sorted_tasks = sorted(self.completed_tasks.values(), key=lambda t: t.object_path)

        # Count number of objects and then number of results by severity.
        by_severity = Counter(
            [res.severity.name for task in sorted_tasks for res in task.results]
        )
        if by_severity:
            md.write(f"Processed {len(sorted_tasks)} objects. Got {by_severity.total()} results:\n\n")
            md.write("| Severity | Count |\n")
            md.write("| -------- | ----- |\n")
            for sev, cnt in by_severity.most_common():
                md.write(f"| {sev} | {cnt} |\n")

        # TODO(rousik): We might want to add a summary here, with the following
        # information:
        # 1. number of results by type
        # 2. number of exceptions (if any)
        # 3. generated TOC for quick navigation (we may use hash of object_path as href)
        # 4. evaluation stats (e.g. number of workers, total time elapsed, ...)
        for task in sorted_tasks:
            if task.results:
                md.write(f"\n{task.analyzer.get_title()}\n")
            for res in task.results:
                # TODO(rousik): We may add some indication of severity here.
                md.write(res.markdown.rstrip() + "\n")
        return md.getvalue()