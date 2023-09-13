"""Utilities for comparing sqlite databases."""

from queue import Queue
from typing import Iterator
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings

import sqlalchemy as sa
from pudl_output_differ.types import (
    DiffEvaluator, DiffEvaluatorBase, DiffTreeNode, KeySetDiff
)

class SQLiteEvaluationSettings(BaseSettings):
    """Holds settings for SQLite evaluation."""
    count_rows: bool = True
    compare_rows: bool = True
    unique_rows_sample: int = 5


class SQLiteDBEvaluator(DiffEvaluatorBase):
    db_name: str
    left_db_path: str
    right_db_path: str

    def get_table_schemas(self, engine: sa.engine.Engine) -> dict[str, set[str]]:
        """Returns dictionary of table schemas."""
        out = {}
        inspector = sa.inspect(engine)
        for table_name in inspector.get_table_names():
            out[table_name] = [
                "::".join([col["name"], str(col["type"])])
                for col in inspector.get_columns(table_name)
            ]
        return out

    def execute(self, task_queue: Queue[DiffEvaluator]) -> Iterator[DiffTreeNode]:
        """Analyze tables and their schemas."""
        left_engine = sa.create_engine(f"sqlite:///{self.left_db_path}")
        right_engine = sa.create_engine(f"sqlite:///{self.right_db_path}")

        lschema = self.get_table_schemas(left_engine)
        rschema = self.get_table_schemas(right_engine)
        
        # All database diffs will be children of this node.
        db_node = self.parent_node.add_child(
            DiffTreeNode(name=f"SQLiteDB({self.db_name})")
        )
        yield db_node

        # Tables are compared by name.
        tables = db_node.add_child(DiffTreeNode(
            name="Tables",
            diff=KeySetDiff.from_sets(set(lschema), set(rschema)),
        ))
        yield tables

        left_connection = left_engine.connect()
        right_connection = right_engine.connect()
        for table_name in tables.diff.shared:
            table_node = DiffTreeNode(name=f"Table({table_name})", parent=tables)
            columns_node = DiffTreeNode(
                name="Columns",
                parent=table_node,
                diff=KeySetDiff.from_sets(lschema[table_name], rschema[table_name]),
            )
            yield columns_node
            # TODO(rousik): perhaps we might want to do row comparisons even
            # if the schemas differ?
            if not columns_node.diff.has_diff():
                task_queue.put(
                    RowEvaluator(
                        parent_node=table_node,
                        table_name=table_name,
                        left_connection=left_connection,
                        right_connection=right_connection,
                    )
                )

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
    table_name: str
    left_connection: sa.engine.Connection = Field(exclude=True)
    right_connection: sa.engine.Connection = Field(exclude=True)

    def execute(self, task_queue: Queue[DiffEvaluator]) -> Iterator[DiffTreeNode]:
        """Analyze rows of a given table."""
        
        if SQLiteEvaluationSettings().count_rows:
            lrows = self.left_connection.execute(
                sa.text(f"SELECT COUNT(*) FROM {self.table_name}")
            ).scalar()
            rrows = self.right_connection.execute(
                sa.text(f"SELECT COUNT(*) FROM {self.table_name}")
            ).scalar()
            yield self.parent_node.add_child(
                DiffTreeNode(
                    name="RowCount",
                    diff=RowCountDiff(left_rows=lrows, right_rows=rrows),
                )
            )

            if lrows != rrows and SQLiteEvaluationSettings().compare_rows:
                ldf = pd.read_sql_table(self.table_name, self.left_connection)
                rdf = pd.read_sql_table(self.table_name, self.right_connection)
                merged = ldf.merge(rdf, how="outer", indicator=True)
                lo = merged[merged["_merge"] == "left_only"]
                ro = merged[merged["_merge"] == "right_only"]
                yield self.parent_node.add_child(
                    DiffTreeNode(
                        name="RowsFullComparison",
                        diff=RowSampleDiff(
                            left_only_rows=lo.head(n=SQLiteEvaluationSettings().unique_rows_sample),
                            right_only_rows=ro.head(n=SQLiteEvaluationSettings().unique_rows_sample),
                            left_total_count=len(lo),
                            right_total_count=len(ro),
                        )
                    )
                )