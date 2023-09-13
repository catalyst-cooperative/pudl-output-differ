"""Generic types used in output diffing."""
from queue import Queue
from typing import Iterator, Optional, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field


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
    

@runtime_checkable
class DiffEvaluator(Protocol):
    """Interface for classes implementing diff evaluation.

    DiffEvaluators should be instantiated with the relevant data
    to run the evaluation.
    """

    def execute(task_queue: Queue["DiffEvaluator"]) -> Iterator[DiffTreeNode]:
        """Runs the evaulation.
        
        This is expected to generate list of diffs found at this level and push
        lower level evaluations into the task_queue.
        """

class DiffEvaluatorBase(BaseModel):
    """Base class for diff evaluators."""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    parent_node: DiffTreeNode


class DiffTreeExecutor(BaseModel):
    """Holds the diff tree and executes the evaluation."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    root_node: DiffTreeNode = DiffTreeNode(name="Root")
    task_queue: Queue[DiffEvaluator] = Field(exclude=True, default_factory=Queue)
    has_diff: bool = False

    def evaluate_and_print(self) -> bool:
        """Runs tree evaluation and prints the diffs continuously."""
        while self.task_queue.qsize() > 0:
            evaluator = self.task_queue.get_nowait()
            for diff in evaluator.execute(self.task_queue):
                if diff.has_diff():
                    self.has_diff = True
                    print(diff)


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
