"""Generic types used in output diffing."""
from enum import IntEnum
import logging
from abc import ABC, abstractmethod
from functools import total_ordering
from io import StringIO
from typing import Iterator, Protocol

from opentelemetry import trace
from pydantic import BaseModel

# from pudl_output_differ.task_queue import TaskQueue


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
    
    
@total_ordering
class ObjectPath(BaseModel):
    """Represents structural path to the object that is being analyzed.
    
    E.g. this could represent Partition(key=foo) that is part of the 
    Table(name=boilers) which is part of the Database(name=pudl.sqlite).

    This hierarchy would be represent with the the following object path:
    [ 
        Database(name="pudl.sqlite"),
        Table(name="boilers"),
        Partition(key="foo"),
    ]

    Because TypeDefs are orderable, the object path is also orderable.
    """
    path: list[TypeDef] = []

    def __lt__(self, other) -> bool:
        """Returns true if self is less than other using TypeDef sorting."""
        if self.__class__.__name__ != other.__class__.__name__:
            return self.__class__.__name__ < other.__class__.__name__
        return self.path < other.path
    
    def extend(self, node: TypeDef) -> "ObjectPath":
        """Returns new path extended by the `node`."""
        return ObjectPath(path=self.path + [node])
    
    def __str__(self):
        """Returns object path represented as a string."""
        return "/".join(str(p) for p in self.path)
    
    def get_first(self, cls: type[TypeDef]) -> TypeDef | None:
        """Retrieves first node of the given type from the path."""
        for node in self.path:
            if isinstance(node, cls):
                return node
        return None
    
    
    @staticmethod
    def from_nodes(*nodes: TypeDef) -> "ObjectPath":
        """Converts list of TypeDef instances to a path instance."""
        return ObjectPath(path=list(nodes))


# TODO(rousik): add the following, when useful.
class ReportSeverity(IntEnum):
     """Indicates the severity of a given result."""
     INFO = 0
     WARNING = 1
     ERROR = 2
     DIAGNOSTIC = 3
     EXCEPTION = 4


# We can modify execute() method to take both the task_queue
# and report buffer as arguments; it can then publish issues
# into the report buffer, one result at a time.
# This might be a bit easier than single return value that
# is constructed bit by bit. However, we may also want to 
# be able to append to the result buffer for convenience
# and set its severity afterwards, to allow for changing
# conditions.

class Result(BaseModel):
    """Represents single analysis result."""
    severity: ReportSeverity = ReportSeverity.ERROR
    markdown: str = ""


class TaskQueueInterface(Protocol):
    """Represents the interface for the task queue.
    
    Analysis instances can be put on the queue, that's all.
    """
    def put(self, analyzer: "Analyzer") -> None:
        ...


class Analyzer(BaseModel, ABC):
    """Represents the common ancestor for the analyzers.
    
    Every analyzer instance is associated with the object
    identified by its object_path, which is chain of rich
    types, that are derived from TypeDef
    """
    object_path: ObjectPath
    
    @abstractmethod
    def execute(self, task_queue: TaskQueueInterface) -> Iterator[Result]:
        """Runs the analysis and returns results, one at a time."""

    def execute_sync(self, task_queue: TaskQueueInterface) -> list[Result]:
        """Runs execute and converts all results to a list."""
        return list(self.execute(task_queue))

    def get_title(self):
        """Returns the title of the analysis."""
        return f"## {self.object_path}"


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
            out.write(f" * {n} {self.entity} {verb}:\n")
            out.writelines([f"  * {k}\n" for k in sorted(stuff)])
        else:
            out.write(f" * {n} {self.entity} {verb}: {sorted(stuff)}\n")
    
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