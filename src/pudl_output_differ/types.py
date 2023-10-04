"""Generic types used in output diffing."""
from asyncio import FIRST_COMPLETED
import logging
import threading
from typing import Iterator, Optional, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict
import concurrent.futures


logger = logging.getLogger(__name__)


@runtime_checkable
class GenericDiff(Protocol):
    """Interface for classes implementing diff."""

    def has_diff(self) -> bool:
        """Returns true if the diff is non-empty."""


class DiffTreeNode(BaseModel):
    """Represents a node in the structure diff tree."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    parent: Optional["DiffTreeNode"] = None
    diff: Optional[GenericDiff] = None
    children: list["DiffTreeNode"] = []
    _lock: threading.Lock
    # _lock: threading.Lock = Field(exclude=True, default_factory=threading.Lock)

    def __init__(self, **data):
        """Initializes the node and instantiates the lock."""
        super().__init__(**data)
        self._lock = threading.Lock()

    def get_full_name(self, delimiter="/") -> str:
        """Returns concatenated full path of the node."""
        name_components = []
        node = self
        while node is not None:
            name_components.append(node.name)
            node = node.parent
        name_components.reverse()
        return delimiter.join(name_components)
    
    def add_child(self, child: "DiffTreeNode") -> "DiffTreeNode":
        """Appends node as a child and returns it."""
        with self._lock and child._lock:
            self.children.append(child)
            child.parent = self
        return child
    
    def has_diff(self) -> bool:
        """Returns true if the diff is non-empty."""
        return self.diff is not None and self.diff.has_diff()
    
    def __str__(self) -> str:
        """Returns human readable string represenation of the diff."""
        if not self.has_diff():
            return ""
        return f"{self.get_full_name()}:\n{self.diff}"
    

class TaskQueue:
    """Thread pool backed executor for diff evaluation."""
    def __init__(self, max_workers: int = 4):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.Lock()
        self.futures: list[concurrent.futures.Future] = []
        self.completed: list[concurrent.futures.Future] = []

    def put(self, evaluator: "DiffEvaluator"):
        """Add evaluator to the execution queue."""
        with self._lock:
            fut = self.executor.submit(evaluator.execute, self)
            self.futures.append(fut)            
            #self.futures.append(self.executor.submit(evaluator.execute, self))

    def get_diffs(self) -> Iterator[DiffTreeNode]:
        """Retrieves diffs as soon as they're available."""
        while self.futures:
            done, _ = concurrent.futures.wait(self.futures, return_when=FIRST_COMPLETED)
            for diff_future in done:
                with self._lock:
                    self.futures.remove(diff_future)
                    self.completed.append(diff_future)
                for diff in diff_future.result():
                    yield diff
            
    def wait(self):
        """Waits until all tasks are done."""
        concurrent.futures.wait(self.futures)
        with self._lock:
            self.completed = self.futures
            self.futures = []


@runtime_checkable
class DiffEvaluator(Protocol):
    """Interface for classes implementing diff evaluation.

    DiffEvaluators should be instantiated with the relevant data
    to run the evaluation.
    """

    def execute(task_queue: TaskQueue) -> list[DiffTreeNode]:
        """Runs the evaulation.
        
        This is expected to generate list of diffs found at this level and push
        lower level evaluations into the task_queue.
        """


class KeySetDiff(BaseModel):
    """Represents two-way diff between two sets of keys."""
    left_only: set[str]
    right_only: set[str]
    shared: set[str] = set()

    def __str__(self):
        """Prints the + and - lines for left/side only elements."""
        pieces = [(k, "-") for k in self.left_only]
        pieces += [(k, "+") for k in self.right_only]
        return "\n".join(f"{sgn} {k}" for (k, sgn) in sorted(pieces))

    def has_diff(self):
        """Returns true if the diff is non-empty."""
        return bool(self.left_only or self.right_only)

    @staticmethod
    def from_sets(left: set[str], right: set[str],
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

class DiffEvaluatorBase(BaseModel):
    """Base class for diff evaluators."""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    parent_node: DiffTreeNode