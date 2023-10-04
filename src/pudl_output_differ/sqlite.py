"""Utilities for comparing sqlite databases."""

import logging
import apsw
from queue import Queue
import fsspec
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings
from opentelemetry import trace

from pudl_output_differ.gcs_vfs import FSSpecVFS
from pudl_output_differ.types import (
    DiffEvaluator, DiffEvaluatorBase, DiffTreeNode, KeySetDiff, TaskQueue
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class SQLiteEvaluationSettings(BaseSettings):
    """Holds settings for SQLite evaluation."""
    count_rows: bool = False
    compare_rows: bool = False
    unique_rows_sample: int = 5


class SQLiteDBEvaluator(DiffEvaluatorBase):
    db_name: str
    left_db_path: str
    right_db_path: str

    def get_table_schemas(self, db: apsw.Connection) -> dict[str, set[str]]:
        """Returns dictionary of table schemas."""
        out = {}
        for table_name in [r[1] for r in db.pragma("table_list")]:
            # TODO(rousik): we might want to consider skipping special tables such
            # as alembic table and so on.
            tb_info = db.pragma(f"table_info({table_name})")
            if isinstance(tb_info, tuple):
                # It's unclear why pragma returns tuple when there's only
                # single row. This is a stupid workaround but what can
                # we do here.
                tb_info = [tb_info]
            out[table_name] = [
                "::".join([col[1], col[2]])
                for col in tb_info
            ]
        return out
    
    def connect_db(self, fullpath: str) -> apsw.Connection:
        """Connect sqlite database, perhaps using custom VFS."""
        fs, path = fsspec.core.url_to_fs(fullpath)
        open_flags = apsw.SQLITE_OPEN_READONLY | apsw.SQLITE_OPEN_URI
        if "gcs" in fs.protocol:
            vfs_instance = FSSpecVFS(fs)
            return apsw.Connection(
                f"file:/{path}?immutable=1",
                flags=open_flags,
                vfs=vfs_instance.vfs_name,
            )
        if fs.protocol == "file":
            return apsw.Connection(f"file:{path}", flags=open_flags)
        raise ValueError(f"Unsupported protocol {fs.protocol}")
 
    @tracer.start_as_current_span(name="SQLiteDBEvaluator.execute")
    def execute(self, task_queue: TaskQueue) -> list[DiffTreeNode]:
        """Analyze tables and their schemas."""
        sp = trace.get_current_span()
        sp.set_attribute("db_name", self.db_name)
        sp.set_attribute("left_db_path", self.left_db_path)
        sp.set_attribute("right_db_path", self.right_db_path)

        diffs = []
        ldb = self.connect_db(self.left_db_path)
        rdb = self.connect_db(self.right_db_path)
        
        lschema = self.get_table_schemas(ldb)
        rschema = self.get_table_schemas(rdb)
        
        # All database diffs will be children of this node.
        db_node = self.parent_node.add_child(
            DiffTreeNode(name=f"SQLiteDB({self.db_name})")
        )
        diffs.append(db_node)

        # Tables are compared by name.
        tables = db_node.add_child(DiffTreeNode(
            name="Tables",
            diff=KeySetDiff.from_sets(set(lschema), set(rschema)),
        ))
        diffs.append(tables)

        for table_name in tables.diff.shared:
            table_node = DiffTreeNode(name=f"Table({table_name})", parent=tables)
            columns_node = DiffTreeNode(
                name="Columns",
                parent=table_node,
                diff=KeySetDiff.from_sets(lschema[table_name], rschema[table_name]),
            )
            diffs.append(columns_node)
            # TODO(rousik): perhaps we might want to do row comparisons even
            # if the schemas differ?
            if not columns_node.diff.has_diff():
                task_queue.put(
                    RowEvaluator(
                        parent_node=table_node,
                        db_name=self.db_name,
                        table_name=table_name,
                        left_connection=ldb,
                        right_connection=rdb,
                    )
                )
        return diffs

class RowCountDiff(BaseModel):
    left_rows: int
    right_rows: int

    def has_diff(self):
        """Returns true if the diff is non-empty."""
        return self.left_rows != self.right_rows
    
    def __str__(self) -> str:
        if self.has_diff():
            d = self.right_rows - self.left_rows
            return f"~ left={self.left_rows} right={self.right_rows}, diff={d}"
        return ""
    

class RowSampleDiff(BaseModel):
    """Represents sample of rows that are unique to either side."""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    left_only_rows: pd.DataFrame
    right_only_rows: pd.DataFrame
    left_total_count: int
    right_total_count: int

    def has_diff(self):
        """Returns true if either side has unique rows."""
        return len(self.left_only_rows) > 0 or len(self.right_only_rows) > 0

    def __str__(self):
        """Prints row samples."""
        if not self.has_diff():
            return ""
        parts = []
        if len(self.left_only_rows):
            parts.append(f"* left_only_rows ({self.left_total_count} total)")
            parts.append(self.left_only_rows.to_string())
        if len(self.right_only_rows):
            parts.append(f"* right_only_rows ({self.right_total_count} total)")
            parts.append(self.right_only_rows.to_string())
        return "\n".join(parts)


class RowEvaluator(DiffEvaluatorBase):
    db_name: str
    table_name: str
    left_connection: apsw.Connection = Field(exclude=True)
    right_connection: apsw.Connection = Field(exclude=True)

    def _count_rows(self, conn: apsw.Connection) -> int:
        """Returns number of rows in a table."""
        total = 0
        for r in conn.execute(f"SELECT COUNT(*) FROM {self.table_name}"):
            total += int(r[0])
        return total

    @tracer.start_as_current_span(name="SQLite.RowEvaluator.execute")
    def execute(self, task_queue: Queue[DiffEvaluator]) -> list[DiffTreeNode]:
        """Analyze rows of a given table."""
        sp = trace.get_current_span()
        sp.set_attribute("db_name", self.db_name)
        sp.set_attribute("table_name", self.table_name)
        diffs = []
        
        if SQLiteEvaluationSettings().count_rows:
            lrows = self._count_rows(self.left_connection)
            rrows = self._count_rows(self.right_connection)
            diffs.append(self.parent_node.add_child(
                DiffTreeNode(
                    name="RowCount",
                    diff=RowCountDiff(left_rows=lrows, right_rows=rrows),
                )
            ))

            if lrows != rrows and SQLiteEvaluationSettings().compare_rows:
                ldf = pd.read_sql_table(self.table_name, self.left_connection)
                rdf = pd.read_sql_table(self.table_name, self.right_connection)
                merged = ldf.merge(rdf, how="outer", indicator=True)
                lo = merged[merged["_merge"] == "left_only"]
                ro = merged[merged["_merge"] == "right_only"]
                diffs.append(self.parent_node.add_child(
                    DiffTreeNode(
                        name="RowsFullComparison",
                        diff=RowSampleDiff(
                            left_only_rows=lo.head(n=SQLiteEvaluationSettings().unique_rows_sample),
                            right_only_rows=ro.head(n=SQLiteEvaluationSettings().unique_rows_sample),
                            left_total_count=len(lo),
                            right_total_count=len(ro),
                        )
                    )
                ))
        return diffs