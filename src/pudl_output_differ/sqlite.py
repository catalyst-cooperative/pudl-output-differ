"""Utilities for comparing sqlite databases."""

from io import StringIO
import logging
from typing import Any
import pandas as pd
import sqlite3
from opentelemetry import trace
from sqlalchemy import Connection, create_engine

from pudl_output_differ.types import (
    AnalysisReport, GenericAnalyzer, KeySetDiff, TaskQueue, TypeDef
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


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

    @tracer.start_as_current_span(name="SQLiteAnalyzer.execute")
    def execute(self, task_queue: TaskQueue) -> AnalysisReport:
        """Analyze tables and their schemas."""
        logger.debug(f"Starting analysis of a database {self.db_name}")
        sp = trace.get_current_span()
        sp.set_attribute("db_name", self.db_name)
        ldb = sqlite3.connect(self.left_db_path)
        rdb = sqlite3.connect(self.right_db_path)

        ltables = self.list_tables(ldb)
        rtables = self.list_tables(rdb)
        tables_diff = KeySetDiff.from_sets(
            left=ltables,
            right=rtables,
            entity="tables",
        )
        for table_name in sorted(tables_diff.shared):
            task_queue.put(
                TableAnalyzer(
                    object_path=self.extend_path(Table(name=table_name)),
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

    def get_columns(self, db: Connection, pk_only: bool = False) -> dict[str, str]:
        """Returns dictionary with columns and their types."""
        ti_df = pd.read_sql_query(f"PRAGMA table_info({self.table_name})", db)
        if pk_only:
            ti_df = ti_df[ti_df.pk == 1]
        return {r["name"]: r["type"] for _, r in ti_df.iterrows()}        

    @tracer.start_as_current_span(name="TableAnalyzer.execute")
    def execute(self, task_queue: TaskQueue) -> AnalysisReport:
        """Analyze tables and their schemas."""
        logger.debug(f"Starting analysis of table {self.db_name}/{self.table_name}")
        # TODO(rousik): First, look for schema discrepancies.
        logger.info(f"Opening db: {self.left_db_path}")
        l_db_engine = create_engine(f"sqlite:///{self.left_db_path}")
        r_db_engine = create_engine(f"sqlite:///{self.right_db_path}")

        lconn = l_db_engine.connect()
        rconn = r_db_engine.connect()

        # TODO(rousik): recover from runtime errors; if we can't handle it,
        # crashing future execution should be okay and the report should simply
        # be the exception.
        l_pk = self.get_columns(lconn, pk_only=True)
        r_pk = self.get_columns(rconn, pk_only=True)


        if l_pk != r_pk:
            raise RuntimeError(f"Primary key columns for {self.table_name} do not match.")
        if l_pk:
            logger.debug(f"{self.table_name}: primary columns: {','.join(sorted(l_pk))}")
            return self.compare_pk_tables(lconn, rconn, sorted(l_pk))
        logger.debug(f"{self.table_name}: no primary key columns, comparing all rows.")
        return self.compare_raw_tables(lconn, rconn)
        
    def compare_raw_tables(self, ldb: Connection, rdb: Connection) -> AnalysisReport:
        """Compare two tables that do not have primary key columns."""
        ldf = pd.read_sql_table(self.table_name, ldb)
        rdf = pd.read_sql_table(self.table_name, rdb)
        merged = ldf.merge(rdf, how="outer", indicator=True)
        orig_row_count = len(ldf.index)
        rows_added = len(merged[merged._merge == "right_only"].index)
        rows_removed = len(merged[merged._merge == "left_only"].index)

        md = StringIO()
        if rows_added: 
            pct_change = float(rows_added) * 100 / orig_row_count 
            md.write(f" * added {rows_added} rows ({pct_change:.2f}% change)")
        if rows_removed:
            pct_change = float(rows_removed) * 100 / orig_row_count 
            md.write(f" * removed {rows_removed} rows ({pct_change:.2f}% change)")

        return AnalysisReport(
            object_path=self.object_path,
            title=f"## {self.db_name}/{self.table_name} rows",
            markdown=md.getvalue(),
        )
    
    def compare_pk_tables(
              self,
              ldb: Connection,
              rdb: Connection,
              pk_cols: list[str],
    ) -> AnalysisReport:
            """Compare two tables with primary key columns.
            
            Args:
                ldb: connection to the left database
                rdb: connection to the right database
                pk_cols: primary key columns
            """
            ldf = pd.read_sql_table(self.table_name, ldb, index_col=pk_cols, columns=pk_cols)
            rdf = pd.read_sql_table(self.table_name, rdb, index_col=pk_cols, columns=pk_cols)
            idx_merge = ldf.merge(rdf, how="outer", indicator=True, left_index=True, right_index=True)
    
            orig_row_count = len(ldf.index)
            rows_added = len(idx_merge[idx_merge._merge == "right_only"].index)
            rows_removed = len(idx_merge[idx_merge._merge == "left_only"].index)

            # TODO(rousik): if a single report can have more ReportBlocks, we could
            # also indicate severity of added/removed rows.
            md = StringIO()
            if rows_added:
                pct_change = float(rows_added) * 100 / orig_row_count
                md.write(f"* added {rows_added} rows ({pct_change:.2f}% change)\n")
            if rows_removed:
                pct_change = float(rows_removed) * 100 / orig_row_count
                md.write(f"* removed {rows_removed} rows ({pct_change:.2f}% change)\n")

            # Now, we need to compare rows that are present on both sides.
            # We will load dataframes from the database, and filter them
            # by the rows that are present on both sided.
            overlap_df = idx_merge[idx_merge._merge == "both"]
            l_overlap_df = self.load_and_filter_rows(ldb, pk_cols, overlap_df)
            r_overlap_df = self.load_and_filter_rows(rdb, pk_cols, overlap_df)

            diff_rows_df = l_overlap_df.compare(r_overlap_df, result_names=("left", "right"), align_axis=0)
            # TODO(rousik): possibly apply numeric tolerances here to filter out
            # close enough rows.
            rows_changed = len(diff_rows_df.index)

            if rows_changed:
                pct_change = float(rows_changed) * 100 / orig_row_count
                md.write(f"* changed {rows_changed} rows ({pct_change:.2f}% change)\n")
                # TODO(rousik): optionally add examples of the affected rows; tune
                # this output.
                md.write("\n")
                md.write("### Examples of rows with changes:\n")
                diff_rows_df.head(5).to_markdown(md)
                
            return AnalysisReport(
                object_path=self.object_path,
                title=f"## {self.db_name}/{self.table_name} rows",
                markdown=md.getvalue(),
            )

    def load_and_filter_rows(
            self,
            db: Connection,
            pk_cols: list[str],
            filter_df: pd.DataFrame) -> pd.DataFrame:
        """Load rows from the database, filter by filter_df.
        
        Args:
            db: Open connection to the database to load from.
            pk_cols: columns that are part of the primary key, these
                columns will be turned into the index.
            filter_df: dataframe with the primary key columns as the
                index.

        Returns:
            rows from the table, where primary key matches records from
            the filter_df    
        """
        df = pd.read_sql_table(self.table_name, db, index_col=pk_cols)
        return df[df.index.isin(filter_df.index)]