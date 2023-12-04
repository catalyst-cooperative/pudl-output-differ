"""
Implementation of the TaskQueue class that handles parallel execution
of the outstanding analyses.
"""

import concurrent.futures
import logging
from re import L
import threading
import traceback
from collections import Counter
from datetime import datetime, timedelta
from enum import IntEnum
from io import StringIO
from time import sleep
from uuid import uuid1

import progressbar
import prometheus_client as prom
from opentelemetry import context, trace
from pydantic import UUID1, BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from pudl_output_differ.sqlite import Database
from pudl_output_differ.types import (
    Analyzer,
    ObjectPath,
    ReportSeverity,
    Result,
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class TaskQueueSettings(BaseSettings):
    """Settings for the task queue."""
    model_config = SettingsConfigDict(env_prefix="diff_")

    max_workers: int = 1
    max_db_concurrency: int = 4
    task_duration_warning_threshold: timedelta = timedelta(minutes=5)


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
    start_time: datetime | None = None
    end_time: datetime | None = None
    # TODO(rousik): perhaps add field for the traceback of the exception
    # or the exception itself (?); perhaps embedding this in the report is
    # okay also.


class TaskQueue:
    """Thread pool backed executor for diff evaluation."""
    def __init__(self, settings = TaskQueueSettings()):
        # TODO(rousik): when dealing with sqlite tables, we could consider
        # estimating their payload size and assigning cost to each task
        # to ensure that we do not overload the worker with memory
        # pressure.
        # For now, using single worker (slower but safer) is a reasonable
        # workaround or initial strategy.
        # We could also indicate which tables are possibly expensive
        # in the differ configuration, which will eliminate the need
        # for dynamically estimating the cost.
        self.settings = settings
        self.executor = concurrent.futures.ThreadPoolExecutor()
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self.pending_tasks: dict[UUID1, AnalysisMetadata] = {}
        self.inflight_tasks: dict[UUID1, AnalysisMetadata] = {}
        self.completed_tasks: dict[UUID1, AnalysisMetadata] = {}
        self.runners: list[concurrent.futures.Future] = []
        self.progress_bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength)
        self.prom_tasks_pending = prom.Gauge("tasks_pending", "Number of tasks pending")
        self.prom_tasks_inflight = prom.Gauge("tasks_inflight", "Number of tasks in-flight")
        self.prom_tasks_done = prom.Gauge("tasks_done", "Number of tasks completed")
        self.prom_tasks_processed = prom.Counter("tasks_processed", "Number of tasks processed by the differ", ["runner_id"])
        self.prom_wait_for_work = prom.Summary("wait_for_work_seconds", "Time spent waiting for work", ["runner_id"])

    def has_unfinished_tasks(self):
        """Returns true if any tasks are pending or in-flight."""
        with self._lock:
            return len(self.pending_tasks) + len(self.inflight_tasks) > 0

    def update_task_stats(self):
        """Updates prometheus metrics for task stats."""
        pending = len(self.pending_tasks)
        inflight = len(self.inflight_tasks)
        completed = len(self.completed_tasks)
        self.prom_tasks_pending.set(pending)
        self.prom_tasks_inflight.set(inflight)
        self.prom_tasks_done.set(completed)
        self.progress_bar.max_value = pending + inflight + completed
        self.progress_bar.update(completed)

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
                if db is not None:
                    if db_open_count.get(db.name, 0) >= self.settings.max_db_concurrency:
                        # Task not schedulable due to db concurrency limits.
                        continue

                # This task can be scheduled, move it to in-flight, change status and return.
                am.state = ExecutionState.RUNNING
                am.start_time = datetime.now()
                del self.pending_tasks[am.id]
                self.inflight_tasks[am.id] = am
                self.update_task_stats()
                return am

            return None

    def slow_task_tracker(self) -> None:
        """This method monitors in-flight tasks and emits warning if any tasks take too long."""
        already_warned = set()
        while self.has_unfinished_tasks():
            now = datetime.now()
            with self._lock:
                for am in self.inflight_tasks.values():
                    if am.start_time is None:
                        continue
                    runs_for = now - am.start_time
                    if runs_for > self.settings.task_duration_warning_threshold:
                        if am.id in already_warned:
                            continue
                        already_warned.add(am.id)
                        logger.warning(
                            f"{am.analyzer.get_title()}: slow! Running for {runs_for}."
                        )
            sleep(5)

    @tracer.start_as_current_span("TaskQueue.analysis_runner")
    def analysis_runner(self, runner_id: str = "", context: None | context.Context = None) -> None:
        """Runs the analyses in the queue until there are no more pending tasks.

        This method is expected to be run multiple times in parallel and process
        the analysis queue until it is empty. It will also collect reports and
        possible runtime exceptions.
        """
        tasks_processed = 0
        with tracer.start_as_current_span("TaskQueue.analysis_runner", context=context) as sp:
            sp.set_attribute("runner_id", runner_id)
            # TODO(rousik): wrap this in spans to make it work well.
            while self.has_unfinished_tasks():
                with self.prom_wait_for_work.labels(runner_id).time():
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
                        cur_task.end_time = datetime.now()
                except Exception:
                    cur_task.state = ExecutionState.FAILED
                    cur_task.end_time = datetime.now()

                    cur_task.results.append(
                        Result(
                            severity=ReportSeverity.EXCEPTION,
                            markdown=f"\n```\n{traceback.format_exc()}\n```\n",
                        )
                    )

                # Move the analysis to completed state.
                with self._lock:
                    self.prom_tasks_processed.labels(runner_id).inc()
                    self.completed_tasks[cur_task.id] = cur_task
                    del self.inflight_tasks[cur_task.id]
                    self.update_task_stats()

            logger.info(f"Runner {runner_id} terminating after {tasks_processed} tasks.")

    @tracer.start_as_current_span("TaskQueue.run")
    def run(self, wait: bool=True):
        """Kicks off analysis_runners in the thread pool.

        Args:
            wait: if True, this method will block until all tasks are completed.

        """
        ctx = context.get_current()
        for i in range(self.settings.max_workers):
            self.runners.append(self.executor.submit(self.analysis_runner, runner_id=str(i), context=ctx))
        self.runners.append(self.executor.submit(self.slow_task_tracker))
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
            self.update_task_stats()
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
        md.write("## Summary\n")
        sorted_tasks = sorted(self.completed_tasks.values(), key=lambda t: t.object_path)

        # Count number of objects and then number of results by severity.
        by_severity = Counter(
            [res.severity.name for task in sorted_tasks for res in task.results]
        )
        failed_tasks = sum(1 for t in sorted_tasks if t.state == ExecutionState.FAILED)
        if by_severity:
            md.write("\n")
            md.write(f"* Ran {len(sorted_tasks)} analyses, got {by_severity.total()} results:\n")
            md.write(f"* Encountered {failed_tasks} failures.\n")

            md.write("\n")
            md.write("Number of results by severity:\n\n")
            md.write("| Severity | Count |\n")
            md.write("| -------- | ----- |\n")
            for sev, cnt in by_severity.most_common():
                md.write(f"| {sev} | {cnt} |\n")
            md.write("\n")

            # List slow tasks (if any)
            slow_tasks = []
            for t in sorted_tasks:
                if t.start_time is None or t.end_time is None:
                    continue
                if t.end_time - t.start_time > self.settings.task_duration_warning_threshold:
                    slow_tasks.append(t)

            if slow_tasks:
                slow_tasks = sorted(slow_tasks, key=lambda t: t.end_time - t.start_time, reverse=True)
                md.write("\n")
                md.write(f"{len(slow_tasks)} slow tasks:\n\n")
                md.write("| Analysis | Duration |\n")
                md.write("| -------- | -------- |\n")
                for st in slow_tasks:
                    md.write(f"| {st.analyzer.get_title()} | {st.end_time - st.start_time} |\n")
                md.write("\n")
                # TODO(rousik): it might make sense to let analyzers construct their own fully qualified
                # names with the relevant path components, e.g.
                # TableAnalyzer(pudl.sqlite/ferc1_respondent_id) or something like that.

        # TODO(rousik): Other things we may want to add to the summary:
        # - left and right paths
        # - total time for evaulation
        # - runtime stats (e.g. number of workers, settings, ...)
        # - slowest 5 eval tasks
        # - generate TOC for quick navigation

        exceptions: list[tuple[AnalysisMetadata, list[Result]]] = []
        has_exceptions = False
        md.write("\n\n## Results\n")
        for task in sorted_tasks:
            if task.state == ExecutionState.FAILED:
                has_exceptions = True
                continue
            if task.results:
                md.write(f"\n### {task.analyzer.get_title()}\n")
                for res in task.results:
                    md.write(res.markdown.rstrip() + "\n")

        if has_exceptions:
            md.write("\n## Exceptions\n")
            for task in sorted_tasks:
                if task.state != ExecutionState.FAILED:
                    continue
                if task.results:
                    md.write(f"\n### {task.analyzer.get_title()}\n")
                    for res in task.results:
                        md.write(res.markdown.rstrip() + "\n")

        return md.getvalue()