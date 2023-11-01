"""Utilities for comparing sqlite databases."""

from io import StringIO
import logging
from typing import Any, Optional
import pandas as pd
from opentelemetry import trace
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Connection, Engine, create_engine, inspect, text

from pudl_output_differ.types import (
    AnalysisReport, GenericAnalyzer, KeySetDiff, TaskQueue, TypeDef
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


REPORT_YEAR_PARTITION = "substr(report_date, 1, 4)"


class SQLiteSettings(BaseSettings):
    """Default configuration for this module."""
    model_config = SettingsConfigDict(env_prefix="diff_")
    sqlite_tables_only: list[str] = []
    sqlite_tables_exclude: list[str] = []
    partitioning_schema: dict[str, str] = {
        "mcoe_monthly": REPORT_YEAR_PARTITION,
        "mcoe_generators_monthly": REPORT_YEAR_PARTITION,
        "generation_fuel_by_generator_energy_source_monthly_eia923": REPORT_YEAR_PARTITION,
        "demand_hourly_pa_ferc714": "report_date",
    }


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
    
    def list_tables(self, engine: Engine) -> set[str]:
        """Returns set of table names in the database."""
        return set(inspect(engine).get_table_names())

    @tracer.start_as_current_span(name="SQLiteAnalyzer.execute")
    def execute(self, task_queue: TaskQueue) -> AnalysisReport:
        """Analyze tables and their schemas."""
        sp = trace.get_current_span()
        sp.set_attribute("db_name", self.db_name)
        ldb = create_engine(f"sqlite:///{self.left_db_path}")
        rdb = create_engine(f"sqlite:///{self.right_db_path}")

        ltables = self.list_tables(ldb)
        rtables = self.list_tables(rdb)
        tables_diff = KeySetDiff.from_sets(
            left=ltables,
            right=rtables,
            entity="tables",
        )
        for table_name in sorted(tables_diff.shared):
            settings = SQLiteSettings()
            if settings.sqlite_tables_exclude and table_name in settings.sqlite_tables_exclude:
                logger.debug(f"Table {table_name} skipped due to config exclusion via sqlite_tables_exclude.")
                continue
            if settings.sqlite_tables_only and table_name not in settings.sqlite_tables_only:
                logger.debug(f"Table {table_name} skipped due to not in the list of sqlite_tables_only.")
                continue

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
    """Analyzes contents of a single sqlite table."""
    db_name: str
    left_db_path: str
    right_db_path: str
    table_name: str
    partition_key: str = ""

    def get_pk_columns(self, engine: Engine) -> list[str]:
        """Returns list of primary key columns."""
        return list(inspect(engine).get_pk_constraint(self.table_name)["constrained_columns"])
    
    def get_partition_func(self) -> Optional[str]:
        """Retrieves partition function for this table.
        
        Returns the partition function, or None if this table is not partitioned.
        """
        return SQLiteSettings().partitioning_schema.get(self.table_name, None)
    
    def is_partitioned_table(self) -> bool:
        """Returns true if this table should be partitioned."""
        return self.get_partition_func() is not None
        
    def get_partitions(self, conn: Connection) -> dict[str, int]:
        """Returns table partitions, dict maps partition_keys to number of rows.
        
        Partition keys are calculated by the table's partition_func retrieved from
        the partitioning_schema. Values are assumed to be strings.
        """
        if self.get_partition_func() is None:
            raise RuntimeError(f"Partitions not supported for table {self.table_name}")
        
        part_func = self.get_partition_func()
        df = pd.read_sql_query(
            f"SELECT {part_func} AS partition_key, COUNT(*) AS num_rows FROM {self.table_name} GROUP BY partition_key",
            conn,
        )
        partitions = { r["partition_key"]: r["num_rows"] for _, r in df.iterrows() }
        if "" in partitions:
            raise AssertionError(f"Empty partition key found for table {self.table_name}")
        return partitions
    
    @tracer.start_as_current_span("split_to_partitioned_tasks")
    def split_to_partitioned_tasks(self, task_queue: TaskQueue, lconn: Connection, rconn: Connection) -> AnalysisReport:
        """Splits table analysis into partitioned tasks.

        Partition keys are calculated, new task is then submitted for each
        partition. Additionally, report outlining which partitions were
        added or removed (with row counts) will be returned.
        """
        lparts = self.get_partitions(lconn)
        rparts = self.get_partitions(rconn)

        if len(lparts) == len(rparts) == 0:
            raise RuntimeError(f"Table {self.table_name} is partitioned, but has no partitions.")

        if len(lparts) > 100 or len(rparts) > 100:
            logger.warn(
                f"""Table {self.table_name} has too many partitions ({len(lparts)}, {len(rparts)}).
                Consider better partition_func than: {self.get_partition_func()}"""
            )
        avg_rows = sum(lparts.values()) / len(lparts)
        logger.debug(f"Table {self.table_name} splits into {len(lparts)} partitions, with ~{avg_rows} rows per partition.")

        logger.debug(f"partitions: {lparts}, {rparts}")
        partition_diff = KeySetDiff.from_sets(
            left=set(lparts),
            right=set(rparts),
            entity="partitions",
        )

        for partition_key in partition_diff.shared:
            logger.debug(f"Submitting task for partition of {self.table_name}: {partition_key}")
            task_queue.put(
                TableAnalyzer(
                    object_path=self.object_path,
                    left_db_path=self.left_db_path,
                    right_db_path=self.right_db_path,
                    db_name=self.db_name,
                    table_name=self.table_name,
                    partition_key=partition_key,
                )
            )
        md = StringIO()
        part_func = self.get_partition_func()
        if partition_diff.has_diff():
            md.write(f"Change in partitions (using partition function: {part_func}):\n")
        for pk in partition_diff.left_only:
            md.write(f"* partition {pk} removed ({lparts[pk]} rows)\n")
        for pk in partition_diff.right_only:
            md.write(f"* partition {pk} added ({rparts[pk]} rows)\n")

        return AnalysisReport(
            object_path=self.object_path,
            title="## Table {self.db_name}/{self.table_name} partitioning",
            markdown=md.getvalue(),
        )
    
    @tracer.start_as_current_span(name="TableAnalyzer.execute")
    def execute(self, task_queue: TaskQueue) -> AnalysisReport:
        """Analyze tables and their schemas."""
        sp = trace.get_current_span()
        sp.set_attribute("db_name", self.db_name)
        sp.set_attribute("table_name", self.table_name)
        if self.partition_key:
            sp.set_attribute("partition_key", self.partition_key)

        l_db_engine = create_engine(f"sqlite:///{self.left_db_path}")
        r_db_engine = create_engine(f"sqlite:///{self.right_db_path}")

        lconn = l_db_engine.connect()
        rconn = r_db_engine.connect()
        
        # TODO(rousik): test for schema discrepancies here.
        l_pk = self.get_pk_columns(l_db_engine)
        r_pk = self.get_pk_columns(r_db_engine)

        if l_pk != r_pk:
            raise RuntimeError(f"Primary key columns for {self.table_name} do not match.")

        if not self.partition_key and self.is_partitioned_table():
            return self.split_to_partitioned_tasks(task_queue, lconn, rconn)
                
        if not l_pk:
            return self.compare_raw_tables(lconn, rconn)
        else:
            logger.debug(f"{self.table_name}: primary columns: {','.join(sorted(l_pk))}")
            return self.compare_pk_tables(lconn, rconn, sorted(l_pk))
        
    @tracer.start_as_current_span(name="compare_raw_tables")
    def compare_raw_tables(self, ldb: Connection, rdb: Connection) -> AnalysisReport:
        """Compare two tables that do not have primary key columns.
        
        For now, simply assess the rate of change in number of rows without digging
        deeper into these tables. 
        """
        lrows = int(ldb.execute(text("SELECT COUNT(*) FROM :table"), parameters={"table": self.table_name}).scalar())
        rrows = int(rdb.execute(text("SELECT COUNT(*) FROM :table"), parameters={"table": self.table_name}).scalar())
        pct_change = float(abs(lrows - rrows) * 100) / lrows

        if lrows > rrows:
            return AnalysisReport(
                object_path=self.object_path,
                title=f"## {self.db_name}/{self.table_name} rows",
                markdown=f" * removed {lrows - rrows} rows ({pct_change:.2f}% change)",
            )
        elif rrows > lrows:
            return AnalysisReport(
                object_path=self.object_path,
                title=f"## {self.db_name}/{self.table_name} rows",
                markdown=f" * added {rrows - lrows} rows ({pct_change:.2f}% change)",
            )
        return AnalysisReport(object_path=self.object_path)

    def get_records(
            self, 
            conn: Connection,
            columns:list[str] = [],
            index_columns: list[str] = []) -> pd.DataFrame:
        """Retrieve records from this table.
        
        If partition_key is set on this instance, it will use it to filter out the records
        that will be loaded.
        """
        if not self.is_partitioned_table():
            return pd.read_sql_table(self.table_name, conn, columns=columns, index_col=index_columns)
        
        selector = "*"
        if columns:
            selector = ", ".join(columns)
    

        sql = text(f"SELECT {selector} FROM {self.table_name} WHERE {self.get_partition_func()} = :partition_key")
        df = pd.read_sql_query(sql, conn, params={"partition_key": self.partition_key})
        if index_columns:
            df = df.set_index(index_columns)
        return df
    
    @tracer.start_as_current_span(name="compare_pk_tables")
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
            md = StringIO()
            overlap_index = None
            with tracer.start_as_current_span("compare_primary_keys"):
                # TODO(rousik): fetch the primary key columns, filtered by the partition if present.
                ldf = self.get_records(ldb, columns=pk_cols, index_columns=pk_cols)
                rdf = self.get_records(rdb, columns=pk_cols, index_columns=pk_cols)
                logger.debug(f"{self.table_name}:{self.partition_key} left {len(ldf.index)} rows, right {len(rdf.index)} rows")

                idx_merge = ldf.merge(rdf, how="outer", indicator=True, left_index=True, right_index=True)
                overlap_index = idx_merge[idx_merge._merge == "both"].index

                orig_row_count = len(ldf.index)
                rows_added = len(idx_merge[idx_merge._merge == "right_only"].index)
                rows_removed = len(idx_merge[idx_merge._merge == "left_only"].index)

                # TODO(rousik): we could take samples of the added/removed rows and emit markdown
                # table to make it easier to compare.
                if rows_added:
                    pct_change = float(rows_added) * 100 / orig_row_count
                    md.write(f"* added {rows_added} rows ({pct_change:.2f}% change)\n")
                if rows_removed:
                    pct_change = float(rows_removed) * 100 / orig_row_count
                    md.write(f"* removed {rows_removed} rows ({pct_change:.2f}% change)\n")

            with tracer.start_as_current_span("compare_overlapping_rows") as sp:
                sp.set_attribute("num_rows", len(overlap_index))
                ldf = self.get_records(ldb, index_columns=pk_cols)
                ldf = ldf[ldf.index.isin(overlap_index)]

                rdf = self.get_records(rdb, index_columns=pk_cols)
                rdf = rdf[rdf.index.isin(overlap_index)]

                # Note that if we use align_axis=0, diff_rows.index will double
                # count changed records due to the index being expanded with
                # left/right fields.
                diff_rows = ldf.compare(rdf, result_names=("left", "right"))
                
                # TODO(rousik): Do something useful with the detected discrepancies, e.g. 
                # identify columns that hold the changes and emit those in the report.
                
                # TODO(rousik): overlapping but changed rows should be dumped into some
                # database where it can be further analyzed.
                rows_changed = len(diff_rows.index)
                if rows_changed:
                    pct_change = float(rows_changed) * 100 / orig_row_count
                    md.write(f"* changed {rows_changed} rows ({pct_change:.2f}% change)\n")

            title=f"## Table {self.db_name}/{self.table_name} rows"
            if self.partition_key:
                title += f" (partition {self.get_partition_func()}={self.partition_key})"

            return AnalysisReport(
                object_path=self.object_path,
                title=title,
                markdown=md.getvalue(),
            )

    def load_and_filter_rows(
            self,
            db: Connection,
            pk_cols: list[str],
            filter_df: pd.DataFrame,
            partition: dict[str, Any] | None = None,
            ) -> pd.DataFrame:
        """Load rows from the database, filter by filter_df.
        
        Args:
            db: Open connection to the database to load from.
            pk_cols: columns that are part of the primary key, these
                columns will be turned into the index.
            filter_df: dataframe with the primary key columns as the
                index.
            partition: may be tuple of values that should be used
                to retrieve partitioned data.

        Returns:
            rows from the table, where primary key matches records from
            the filter_df    
        """
        if partition is None:
            df = pd.read_sql_table(self.table_name, db, index_col=pk_cols)
        else:
            sql = f"SELECT * FROM {self.table_name} WHERE {constraints}"
            df = pd.read_sql_query(

            )
        return df[df.index.isin(filter_df.index)]