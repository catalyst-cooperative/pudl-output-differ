"""Generic types used in output diffing."""
from asyncio import ALL_COMPLETED
from functools import total_ordering
from io import StringIO
import logging
import threading
import traceback
from typing import Iterator, Protocol

from pydantic import BaseModel
import concurrent.futures
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry import trace, context


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@total_ordering
class TypeDef(BaseModel):
    """Base class for type definitions.
    
    This makes all Types orderable, first by name, then
    by the value of their field (processed in the alphabetical order).
    """
    def __lt__(self, other) -> bool:
        if self.__class__.__name__ != other.__class__.__name__:
            return self.__class__.__name__ < other.__class__.__name__
        self_model = self.model_dump()
        other_model = other.model_dump()

        for k, v in self_model.items():
            if k not in other_model:
                raise RuntimeError(f"Field {k} not found in {other_model}")
            if v != other_model[k]:
                return v < other_model[k]
        # TODO(rousik): we should never get here, as that would be the 
        # case for __eq__.
        return False


# TODO(rousik): add the following, when useful.
# class ReportSeverity(IntEnum):
#     """Indicates the severity of a given report."""
#     WARNING = 1
#     ERROR = 2


# class ReportBlock(BaseModel):
#     """Represents single block of data that is part of the report."""
#     severity: ReportSeverity = ReportSeverity.ERROR
#     content: str = ""


class AnalysisReport(BaseModel):
    """Holds the results of the analysis."""
    object_path: list[TypeDef]
    title: str = ""
    markdown: str = ""
    # TODO(rousik): analysis should be associated with object_path. Unclear
    # whether this should be part of the report, or attached to the object
    # by the TaskQueue.

    def has_changes(self) -> bool:
        """Returns true if the report is non-empty."""
        return bool(self.markdown)


class Analyzer(Protocol):
    """Defines API for analyzers."""
    def execute(task_queue: "TaskQueue") -> AnalysisReport:
        """Runs the analysis and returns the report.
        
        Args:
          task_queue: task queue to push sub-analyses into.

        """
        ...

class GenericAnalyzer(BaseModel):
    """Represents the common ancestor for the analyzers.
    
    Every analyzer instance is associated with the object
    identified by its object_path, which is chain of rich
    types, that are derived from TypeDef
    """
    object_path: list[TypeDef]

    def extend_path(self, child: TypeDef) -> list[TypeDef]:
        """Returns object path extended by the `child`."""
        return self.object_path + [child]


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
                    str_path = "/".join(repr(p) for p in analyzer.object_path)
                    error_title = f"{analyzer.__class__.__name__} failed on {str_path}"
                    if not catch_exceptions:
                        raise RuntimeError(error_title) from e
                    # Otherwise, render exception as markdown.
                    yield AnalysisReport(
                        object_path=analyzer.object_path,
                        title=f"## {error_title}",
                        markdown=f"```\n{traceback.format_exc()}\n```",
                    )
     
    def wait(self):
        """Waits until all tasks are done."""
        concurrent.futures.wait(self.analyses.keys(), return_when=ALL_COMPLETED)

    def get_analyses(self, catch_exceptions:bool = True) -> list[AnalysisReport]:
        """Retrieve all analysis reports.

        This is expected to be called only after wait().
        
        """
        self.wait()
        return list(self.iter_analyses(catch_exceptions=catch_exceptions))

class KeySetDiff(BaseModel):
    """Represents two-way diff between two sets of keys."""
    entity: str
    left_only: set[str]
    right_only: set[str]
    shared: set[str] = set()

    # * {object} added: abc, def, ...
    # * {object} removed: abc, def, ...

    def write_items(self, out: StringIO, stuff: set[str], verb: str, long_format: bool) -> None:
        """Writes information about items in the `stuff` set."""
        if not stuff:
            return 
        n = len(stuff)
        if long_format:
            out.write(f"* {n} {self.entity} {verb}:\n")
            out.writelines([f"  * {k}\n" for k in sorted(stuff)])
        else:
            out.write(f"* {n} {self.entity} {verb}: {sorted(stuff)}\n")
    
    def markdown(self, long_format: bool = False) -> str:
        out = StringIO()
        self.write_items(out, self.left_only, verb="removed", long_format=long_format)
        self.write_items(out, self.right_only, verb="added", long_format=long_format)
        return out.getvalue()
    
    def has_diff(self):
        """Returns true if the diff is non-empty."""
        return bool(self.left_only or self.right_only)
    
    @staticmethod
    def from_sets(left: set[str], right: set[str],
                  entity: str,
                  keep_shared: bool = True) -> "KeySetDiff":
        """Returns diff between two sets."""
        left_only = set(left).difference(right)
        right_only = set(right).difference(left)
        shared = set()
        if keep_shared:
            shared = set(left).intersection(right)
        return KeySetDiff(
            entity=entity,
            left_only=left_only,
            right_only=right_only,
            shared=shared
        )