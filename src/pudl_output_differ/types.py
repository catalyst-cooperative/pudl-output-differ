"""Generic types used in output diffing."""
from asyncio import FIRST_COMPLETED, ALL_COMPLETED
from functools import total_ordering
from io import StringIO
import logging
import threading
from typing import Iterator, Optional, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict
import concurrent.futures


logger = logging.getLogger(__name__)


@total_ordering
class TypeDef(BaseModel):
    """Base class for type definitions.
    
    This makes all Types orderable, first by name, then
    by the value of their field (processed in the alphabetical order).
    """
    def __lt__(self, other) -> bool
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


class AnalysisReport(BaseModel):
    """Holds the results of the analysis."""
    object_path: list[TypeDef]
    title: str = ""
    markdown: str = ""
    # TODO(rousik): analysis should be associated with object_path. Uncleaer
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

    def extend_path(self, child: TypeDef):
        """Returns object path extended by the `child`."""
        return self.object_path + [child]


# class DiffTreeNode(BaseModel):
#     """Represents a node in the structure diff tree."""
#     model_config = ConfigDict(arbitrary_types_allowed=True)

#     name: str
#     parent: Optional["DiffTreeNode"] = None
#     diff: Optional[GenericDiff] = None
#     children: list["DiffTreeNode"] = []
#     _lock: threading.Lock
#     # _lock: threading.Lock = Field(exclude=True, default_factory=threading.Lock)

#     def __init__(self, **data):
#         """Initializes the node and instantiates the lock."""
#         super().__init__(**data)
#         self._lock = threading.Lock()

#     def get_full_name(self, delimiter="/") -> str:
#         """Returns concatenated full path of the node."""
#         name_components = []
#         node = self
#         while node is not None:
#             name_components.append(node.name)
#             node = node.parent
#         name_components.reverse()
#         return delimiter.join(name_components)
    
#     def add_child(self, child: "DiffTreeNode") -> "DiffTreeNode":
#         """Appends node as a child and returns it."""
#         with self._lock and child._lock:
#             self.children.append(child)
#             child.parent = self
#         return child
    
#     def has_diff(self, recursive=False) -> bool:
#         """Returns true if the diff is non-empty.
        
#         Args:
#             recursive: if True, look for diffs recursively in the subtree.
#         """
#         if self.diff is not None and self.diff.has_diff():
#             return True
#         if recursive:
#             return any(c.has_diff(recursive=True) for c in self.children)
#         return False
    
#     def __str__(self) -> str:
#         """Returns human readable string represenation of the diff."""
#         if not self.has_diff():
#             return ""
#         return f"{self.get_full_name()}:\n{self.diff}"
    
    

class TaskQueue:
    """Thread pool backed executor for diff evaluation."""
    def __init__(self, max_workers: int = 1):
        # TODO(rousik): raise max_worker to something higher,
        # but then also consider high-priority tasks that should
        # not be run among other high-priority tasks.
        # Perhaps put() method should indicate priority?
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.Lock()
        self.futures: list[concurrent.futures.Future] = []
        self.completed: list[concurrent.futures.Future] = []
        self.finished = False

    def put(self, analyzer: Analyzer):
        """Add evaluator to the execution queue."""
        with self._lock:
            if self.finished:
                raise RuntimeError("Can't add tasks to finished queue.")
            # TODO(rousik): here we should decide on how to handle
            # big tasks that need to be run in isolation, e.g. 
            # analyis of very large sql tables.
            fut = self.executor.submit(analyzer.execute, self)
            self.futures.append(fut)            

    def iter_analyses(self) -> Iterator[AnalysisReport]:
        """Retrieves analyses as soon as they're available."""
        while self.futures:
            done, _ = concurrent.futures.wait(self.futures, return_when=FIRST_COMPLETED)
            for analysis_future in done:
                with self._lock:
                    self.futures.remove(analysis_future)
                    self.completed.append(analysis_future)
                yield analysis_future.result()
            
    def wait(self):
        """Waits until all tasks are done."""
        done, not_done = concurrent.futures.wait(self.futures, return_when=ALL_COMPLETED)
        with self._lock:
            if not_done:
                raise RuntimeError("Not all tasks are completed.")
            self.completed = done
            self.futures = not_done
            self.finished = True

    def get_analyses(self) -> list[AnalysisReport]:
        """Retrieve all analysis reports.

        This is expected to be called only after wait().
        
        """
        with self._lock:
            if not self.finished:
                raise RuntimeError("Need to call wait() on the queue first.")
            if self.futures:
                raise RuntimeError("Not all tasks are completed.")
            return [f.result() for f in self.completed]


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
        if long_format:
            out.write(f"* {self.entity} {verb}:\n")
            out.writelines([f"  * {k}\n" for k in sorted(stuff)])
        else:
            out.write(f"* {self.entity} {verb}: {sorted(stuff)}\n")
    
    def markdown(self, long_format: bool = False) -> str:
        out = StringIO()
        self.write_items(out, self.left_only, verb="removed", long_format=long_format)
        self.write_items(out, self.right_only, verb="added", long_format=long_format)
    
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
            left_only=left_only,
            right_only=right_only,
            shared=shared
        )