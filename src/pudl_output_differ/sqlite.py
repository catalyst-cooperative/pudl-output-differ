"""Utilities for comparing sqlite databases."""

import logging
from typing import Any
from queue import Queue
import pandas as pd
import sqlite3
from pydantic import BaseModel, ConfigDict
from opentelemetry import trace

from pudl_output_differ.types import (
    AnalysisReport, DiffEvaluator, DiffEvaluatorBase, DiffTreeNode, GenericAnalyzer, KeySetDiff, TaskQueue, TypeDef
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

# TODO(rousik): we need a single caching mechanism for 
# all backends. Having this local to SQLiteAnalyzer seems
# counter-intuitive.


class Database(TypeDef):
    """Represents a database."""
    name: str

class Table(TypeDef):
    """Represents a table in a database."""
    name: str

class SQLiteAnalyzer(GenericAnalyzer):
    db_name: str
    left_db_path: str
    right_db_path: str
    # TODO(rousik): we might want to get access to filecache fs to handle
    # remote files. Right now, they come translated as local paths.

    def get_table_schemas(self, db: sqlite3.Connection) -> dict[str, dict[str, Any]]:
        """Returns dictionary of table schemas.
        
        Keys are table names, values is dict mapping column names to column types.
        """
        out = {}
        tdf = pd.read_sql_query("PRAGMA table_list", db)
        for table_name in tdf[tdf.schema == "main"]["name"]:
            sdf = pd.read_sql_query(f"PRAGMA table_info({table_name})", db)
            out[table_name] = {
                r["name"]: r["type"] for _, r in sdf.iterrows()
            }
            # Note that r["pk"] is an interesting indication of whether
            # table has primary keys and which columns are part of it.
        return out
    
    def list_tables(self, db: sqlite3.Connection) -> set[str]:
        """Returns set of table names in the database."""
        tables_df = pd.read_sql_query("PRAGMA table_list", db)
        return set(tables_df[tables_df.schema == "main"]["name"])

    @tracer.start_as_current_span(name="SQLiteDBEvaluator.execute")
    def execute(self, task_queue: TaskQueue) -> list[DiffTreeNode]:
        """Analyze tables and their schemas."""
        sp = trace.get_current_span()
        sp.set_attribute("db_name", self.db_name)

        ldb = sqlite3.connect("sqlite://{self.left_db_path}")
        rdb = sqlite3.connect("sqlite://{self.right_db_path}")

        tables_diff = KeySetDiff.from_sets(
            left=self.list_tables(ldb),
            right=self.list_tables(rdb),
            entity="tables",
        )
        for table_name in tables_diff.shared:
            task_queue.put(
                TableAnalyzer(
                    object_path=self.exend_path(Table(name=table_name)),
                    left_db_path=self.left_db_path,
                    right_db_path=self.right_db_path,
                    db_name=self.db_name,
                    table_name=table_name,
                )
            )
        return AnalysisReport(
            object_path=self.object_path,
            title="## SQLite database {db_name}",
            markdown=tables_diff.markdown(),
        )

        
class TableAnalyzer(GenericAnalyzer):
    db_name: str
    left_db_path: str
    right_db_path: str
    table_name: str

    def get_columns(self, db: sqlite3.Connection, pk_only: bool = False) -> dict[str, str]:
        """Returns dictionary with columns and their types."""
        ti_df = pd.read_sql_query(f"PRAGMA table_info({self.table_name})", db)
        if pk_only:
            ti_df = ti_df[ti_df.pk == 1]
        return {r["name"]: r["type"] for _, r in ti_df.iterrows()}        

    @tracer.start_as_current_span(name="SQLiteDBEvaluator.execute")
    def execute(self, task_queue: TaskQueue) -> AnalysisReport:
        """Analyze tables and their schemas."""
        # TODO(rousik): First, look for schema discrepancies.
        ldb = sqlite3.connect(self.left_db_path)
        rdb = sqlite3.connect(self.right_db_path)

        # TODO(rousik): recover from runtime errors; if we can't handle it,
        # crashing future execution should be okay and the report should simply
        # be the exception.
        l_pk = self.get_columns(ldb, pk_only=True)
        r_pk = self.get_columns(rdb, pk_only=True)

        if l_pk != r_pk:
            raise RuntimeError(f"Primary key columns differ for table {self.table_name}")
        if not l_pk:
            logger.warning(f"Table {self.table_name} has no primary key columns.")
        if l_pk:
            # Compare rows by primary key columns only.
            cols_fetch = ", ".join(sorted(l_pk))

            sql = f"SELECT {cols_fetch} FROM {self.table_name}",
            l_df = pd.read_sql_query(sql, ldb)
            r_df = pd.read_sql_query(sql, rdb)
            merged = l_df.merge(r_df, how="outer", indicator=True)

            # TODO(rousik): as a first pass, we will calculate the 
            # magnitued of this change. Of N records x percent 
            # were added/removed/modified.

            # TODO(rousik): analyzing modifying files with pandas
            # frame comparison assertion, but we will have to load
            # all of those into memory.
            # It's unclear what would be the most edffective way
            # to load this.
        

        # TODO(rousik): Then figure out which rows were added/removed
        # TODO(rousik): Then figure out which rows were modified, to
        # do that, we need to do comparison using primary key columns,
        # and compare the results.
        # pandas.testing.assert_frame_equal() should be good tool to 
        # compare records common in the two dataframes.
        



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

            # For ease of use, let's collapse col_name => col_type lschema dict into
            # string representations of columms
            lcols = [":".join(it) for it in lschema[table_name].items()]
            rcols = [":".join(it) for it in rschema[table_name].items()]
            columns_node = DiffTreeNode(
                name="Columns",
                parent=table_node,
                diff=KeySetDiff.from_sets(lcols, rcols)
            )
            diffs.append(columns_node)

            # Figure out which columns share type and are present on both sides.
            overlap_cols = [
                col for col in lschema[table_name].keys()
                if lschema[table_name][col] == rschema[table_name].get(col)
            ]
            if not columns_node.has_diff():
                # TODO(rousik): Some strange ferc tables have "index" column which
                # seems to break SQL for loading the stuff. We can probably work around
                # this by using SQLAlchemy with read_sql_table(...).
                # That would be a better choice anyways.
                overlap_cols = ["*"]

            if overlap_cols:
                task_queue.put(
                    RowEvaluator(
                        parent_node=table_node,
                        db_name=self.db_name,
                        table_name=table_name,
                        columns=overlap_cols,
                        left_db_path=self.get_local_db_path(self.left_db_path),
                        right_db_path=self.get_local_db_path(self.right_db_path),
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
    columns: list[str] = ["*"]
    left_db_path: str 
    right_db_path: str

    def _count_rows(self, cx: sqlite3.Connection) -> int:
        """Returns number of rows in a table."""
        return cx.cursor().execute(
            f"SELECT COUNT(*) FROM {self.table_name}"
        ).fetchall()[0][0]

    @tracer.start_as_current_span(name="outer_join_table")
    def outer_join_table(self, ldb: sqlite3.Connection, rdb: sqlite3.Connection) -> (pd.DataFrame, pd.DataFrame):
        """Runs outer join over the table contents.
        
        Returns two data-frames representing rows only found on the left side
        and rows only found on the right side.
        """
        cols = ",".join(self.columns)
        sql = f"SELECT {cols} FROM {self.table_name}"
        
        ldf = pd.read_sql_query(sql, ldb)
        rdf = pd.read_sql_query(sql, rdb)
        # TODO(rousik): We might improve effectiveness of join by setting indices
        # to match primary key columns. This might not, however, detect row
        # discrepancies among rows that differ on non-PK columns.
        # For those records, we could simply compare row-by-row matching on the 
        # primary keys from the two dfs (i.e. avoid outer joins).
        
        merged = ldf.merge(rdf, how="outer", indicator=True)
        lo = merged[merged["_merge"] == "left_only"]
        ro = merged[merged["_merge"] == "right_only"]
        return lo, ro

    @tracer.start_as_current_span(name="SQLite.RowEvaluator.execute")
    def execute(self, task_queue: Queue[DiffEvaluator]) -> list[DiffTreeNode]:
        """Analyze rows of a given table."""
        sp = trace.get_current_span()
        sp.set_attribute("db_name", self.db_name)
        sp.set_attribute("table_name", self.table_name)
        diffs = []

        ldb = sqlite3.connect(self.left_db_path)
        rdb = sqlite3.connect(self.right_db_path)

        if SQLiteEvaluationSettings().count_rows:
            lrows = self._count_rows(ldb)
            rrows = self._count_rows(rdb)
            diffs.append(self.parent_node.add_child(
                DiffTreeNode(
                    name="RowCount",
                    diff=RowCountDiff(left_rows=lrows, right_rows=rrows),
                )
            ))
            # Running full outer join on the table may be memory intensive
            # and slow, but let's try going with this naive approach before
            # we find out this is not applicable.
            
            # Very large datasets may prove to be problematic here.

            # There's also a possibility that some column types may be unstable
            # and we might need to employ fuzzy-matching (e.g. FLOATS)
        if SQLiteEvaluationSettings().compare_rows:
            lo, ro = self.outer_join_table(ldb, rdb)
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