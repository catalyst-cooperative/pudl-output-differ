"""Utilities for comparing sqlite databases."""

import logging
from io import StringIO
from typing import Iterator, Optional

import backoff
import pandas as pd
from opentelemetry import trace
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Connection, Engine, create_engine, inspect, text
from sqlalchemy.exc import OperationalError

from pudl_output_differ.types import (
    Analyzer,
    KeySetDiff,
    ReportSeverity,
    Result,
    TaskQueueInterface,
    TypeDef,
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


REPORT_YEAR_PARTITION = "substr(report_date, 1, 4)"


# TODO(rousik): for the sake of unit-testing, we should be passing 
# these as `settings` to the individual analyzers. 
class SQLiteSettings(BaseSettings):
    """Default configuration for this module."""

    model_config = SettingsConfigDict(env_prefix="diff_")
    sqlite_tables_only: list[str] = []
    sqlite_tables_exclude: list[str] = ["alembic_version"]
    partitioning_schema: dict[str, str] = {
        "mcoe_monthly": REPORT_YEAR_PARTITION,
        "mcoe_generators_monthly": REPORT_YEAR_PARTITION,
        "generation_fuel_by_generator_energy_source_monthly_eia923": REPORT_YEAR_PARTITION,
        "demand_hourly_pa_ferc714": "report_date",
    }
    # If set to true, we will only fetch the data from tables once; all matching
    # columns. Otherwise we will fetch PK only and then only fetch overlapping
    # rows for the matching PKs.
    single_pass_pk_comparison: bool = False


class Database(TypeDef):
    """Represents a database."""
    name: str

    def __str__(self):
        return f"Database({self.name})"


class Table(TypeDef):
    """Represents a table in a database."""

    name: str

    def __str__(self):
        return f"Table({self.name})"


class Partition(TypeDef):
    """Represents partition of a table."""
    pk: str

    def __str__(self):
        return f"Partition(key:{self.pk})"


class SQLiteAnalyzer(Analyzer):
    db_name: str
    left_db_path: str
    right_db_path: str
    settings: SQLiteSettings = SQLiteSettings()

    def list_tables(self, engine: Engine) -> set[str]:
        """Returns set of table names in the database."""
        return set(inspect(engine).get_table_names())

    def should_process_table(self, table_name) -> bool:
        """Returns True if table should be processed."""
        if self.settings.sqlite_tables_exclude and table_name in self.settings.sqlite_tables_exclude:
            return False
        if not self.settings.sqlite_tables_only:
            return True
        return table_name in self.settings.sqlite_tables_only

    def execute(self, task_queue: TaskQueueInterface) -> Iterator[Result]:
        """Analyze tables and their schemas."""
        trace.get_current_span().set_attribute("db_name", self.db_name)

        ldb = create_engine(f"sqlite:///{self.left_db_path}")
        rdb = create_engine(f"sqlite:///{self.right_db_path}")

        ltables = self.list_tables(ldb)
        rtables = self.list_tables(rdb)
        tables_diff = KeySetDiff.from_sets(
            left=ltables,
            right=rtables,
            entity="tables",
        )
        if tables_diff.has_diff():
            yield Result(markdown=tables_diff.markdown())

        for table_name in sorted(tables_diff.shared):
            if not self.should_process_table(table_name):
                logger.debug(
                    f"Table {table_name} skipped due to config exclusion."
                )
                continue

            task_queue.put(
                TableAnalyzer(
                    object_path=self.object_path.extend(Table(name=table_name)),
                    left_db_path=self.left_db_path,
                    right_db_path=self.right_db_path,
                    db_name=self.db_name,
                    table_name=table_name,
                )
            )


class TableAnalyzer(Analyzer):
    """Analyzes contents of a single sqlite table."""

    db_name: str
    left_db_path: str
    right_db_path: str
    table_name: str
    partition_key: str = ""
    settings: SQLiteSettings = SQLiteSettings()


    def get_title(self) -> str:
        """Returns the title of the analysis."""
        title = f"## Table {self.db_name}/{self.table_name}"
        if self.partition_key:
            title += f" (partition {self.get_partition_func()}=`{self.partition_key}`)"
        return title

    def get_pk_columns(self, engine: Engine) -> list[str]:
        """Returns list of primary key columns."""
        return list(
            inspect(engine).get_pk_constraint(self.table_name)["constrained_columns"]
        )

    def get_columns(self, engine: Engine) -> dict[str, str]:
        """Returns dictionary mapping column names to column types."""
        return {
            col["name"]: str(col["type"])
            for col in inspect(engine).get_columns(self.table_name)
        }

    def get_partition_func(self) -> Optional[str]:
        """Retrieves partition function for this table.

        Returns the partition function, or None if this table is not partitioned.
        """
        return self.settings.partitioning_schema.get(self.table_name, None)

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
        raw_partitions = {r["partition_key"]: r["num_rows"] for _, r in df.iterrows()}
        if None in raw_partitions:
            logger.warning(f"Empty partition key found for table {self.db_name}/{self.table_name}")

        return {str(pk): num_rows for pk, num_rows in raw_partitions.items() if pk is not None}

    @tracer.start_as_current_span("split_to_partitioned_tasks")
    def split_to_partitioned_tasks(
        self, task_queue: TaskQueueInterface, lconn: Connection, rconn: Connection
    ) -> Iterator[Result]:
        """Splits table analysis into partitioned tasks.

        Partition keys are calculated, new task is then submitted for each
        partition. Additionally, report outlining which partitions were
        added or removed (with row counts) will be returned.
        """
        lparts = self.get_partitions(lconn)
        rparts = self.get_partitions(rconn)

        if len(lparts) == len(rparts) == 0:
            raise RuntimeError(
                f"Table {self.table_name} is partitioned, but has no partitions."
            )

        if len(lparts) > 100 or len(rparts) > 100:
            logger.warn(
                f"""Table {self.table_name} has too many partitions ({len(lparts)}, {len(rparts)}).
                Consider better partition_func than: {self.get_partition_func()}"""
            )
        avg_rows = (sum(rparts.values()) + sum(lparts.values())) / (
            len(lparts) + len(lparts)
        )
        logger.debug(
            f"Table {self.table_name} splits into {len(lparts)} partitions, with ~{avg_rows} rows per partition."
        )

        logger.debug(f"partitions: {lparts}, {rparts}")
        partition_diff = KeySetDiff.from_sets(
            left=set(lparts),
            right=set(rparts),
            entity="partitions",
        )

        for partition_key in partition_diff.shared:
            task_queue.put(
                TableAnalyzer(
                    object_path=self.object_path.extend(Partition(pk=partition_key)),
                    left_db_path=self.left_db_path,
                    right_db_path=self.right_db_path,
                    db_name=self.db_name,
                    table_name=self.table_name,
                    partition_key=partition_key,
                    settings=self.settings,
                )
            )
        md = StringIO()
        part_func = self.get_partition_func()
        if partition_diff.has_diff():
            md.write(f"Change in partitions (using partition function: `{part_func}`):\n")
        for pk in partition_diff.left_only:
            md.write(f" * partition {pk} removed ({lparts[pk]} rows)\n")
        for pk in partition_diff.right_only:
            md.write(f" * partition {pk} added ({rparts[pk]} rows)\n")

        if md.tell() > 0:
            yield Result(markdown=md.getvalue())
    
    @backoff.on_exception(backoff.expo, OperationalError, max_tries=4)
    def retry_connect(self, engine: Engine) -> Connection:
        """Connects to the database, retrying on OperationalError."""
        with tracer.start_as_current_span("connect_sqlite") as sp:
            sp.set_attribute("db_path", engine.url.render_as_string())
            return engine.connect()

    def execute(self, task_queue: TaskQueueInterface) -> Iterator[Result]:
        """Analyze tables and their schemas."""
        sp = trace.get_current_span()
        sp.set_attribute("db_name", self.db_name)
        sp.set_attribute("table_name", self.table_name)
        if self.partition_key:
            sp.set_attribute("partition_key", self.partition_key)

        l_db_engine = create_engine(f"sqlite:///{self.left_db_path}")
        r_db_engine = create_engine(f"sqlite:///{self.right_db_path}")

        lconn = self.retry_connect(l_db_engine)
        rconn = self.retry_connect(r_db_engine)

        # TODO(rousik): test for schema discrepancies here.
        l_pk = self.get_pk_columns(l_db_engine)
        r_pk = self.get_pk_columns(r_db_engine)

        if l_pk != r_pk:
            raise AssertionError(
                f"Primary key columns for {self.table_name} do not match."
            )

        if not self.partition_key and self.is_partitioned_table():
            for res in self.split_to_partitioned_tasks(task_queue, lconn, rconn):
                yield res

        if not l_pk:
            for res in self.compare_raw_tables(lconn, rconn):
                yield res
        else:
            for res in self.compare_pk_tables(lconn, rconn, sorted(l_pk)):
                yield res

    @tracer.start_as_current_span(name="compare_raw_tables")
    def compare_raw_tables(self, ldb: Connection, rdb: Connection) -> Iterator[Result]:
        """Compare two tables that do not have primary key columns.

        For now, simply assess the rate of change in number of rows without digging
        deeper into these tables.
        """
        sql = f"SELECT COUNT(*) FROM {self.table_name}"
        lrows = int(ldb.execute(text(sql)).scalar())
        rrows = int(rdb.execute(text(sql)).scalar())

        if lrows > rrows:
            pct_change = float(abs(lrows - rrows) * 100) / lrows
            yield Result(markdown=f" * removed {lrows - rrows} rows ({pct_change:.2f}% change)\n")
        elif rrows > lrows:
            if lrows > 0:
                pct_change = float(abs(lrows - rrows) * 100) / lrows
                yield Result(markdown=f" * added {rrows - lrows} rows ({pct_change:.2f}% change)\n")
            else:
                yield Result(markdown=f" * added {rrows} rows (no rows previously)\n")

        # TODO(rousik): By doing df.merge() we should be able to tell how many rows were added/removed/changed.
        # If there's change to any column content, this will be marked as both left_only and right_only
        # and there's no way to easily tell that this is "changed" row, and this row will be double counted.

    @tracer.start_as_current_span("get_records")
    def get_records(
        self, conn: Connection, columns: list[str] = [], index_columns: list[str] = []
    ) -> pd.DataFrame:
        """Retrieve records from this table.

        If partition_key is set on this instance, it will use it to filter out the records
        that will be loaded.
        """
        trace.get_current_span().set_attributes(
            {
                "db_name": self.db_name,
                "table_name": self.table_name,
                "columns": sorted(columns),
                "index_columns": sorted(index_columns),
            }
        )

        constraint = ""
        query_params = {}
        if self.is_partitioned_table():
            constraint = f"WHERE {self.get_partition_func()} = :partition_key"
            query_params["partition_key"] = self.partition_key

        selector = "*"
        if columns:
            selector = ", ".join(columns)

        sql = text(
            f"SELECT {selector} FROM {self.table_name} {constraint}"
        )
        df = pd.read_sql_query(sql, conn, params=query_params)
        if index_columns:
            df = df.set_index(index_columns)
        return df

    # TODO(rousik): many of these operations could be simplified if we construct
    # wrapper helper classes for handling pairs of objects, e.g. database connections
    # or even pair of data-frames that could be compared for discrepancies.
    # For now, this will be implemented in these methods and that's okay as a first pass.

    @tracer.start_as_current_span(name="compare_pk_tables")
    def compare_pk_tables(
        self,
        ldb: Connection,
        rdb: Connection,
        pk_cols: list[str],
    ) -> Iterator[Result]:
        """Compare two tables with primary key columns.

        Args:
            ldb: connection to the left database
            rdb: connection to the right database
            pk_cols: primary key columns
        """
        overlap_index = None
        ldf = pd.DataFrame()
        rdf = pd.DataFrame()

        # TODO(rousik): following column magic should be extracted to standalone method.
        # What we need in the end is a list of shared and shared pk columns.
        lcols = self.get_columns(ldb.engine)
        rcols = self.get_columns(rdb.engine)

        cols_removed = set(lcols) - set(rcols)
        if cols_removed:
            cstr = ", ".join(sorted(cols_removed))
            yield Result(markdown=f" * {len(cols_removed)} columns removed: {cstr}\n")
        cols_added = set(rcols) - set(lcols)
        if cols_added:
            cstr = ", ".join(sorted(cols_added))
            yield Result(markdown=f" * {len(cols_added)} columns added: {cstr}\n")

        cols_intact = []
        cols_changed = []
        for col in set(lcols) & set(rcols):
            if lcols[col] == rcols[col]:
                cols_intact.append(col)
            else:
                cols_changed.append(col)
        if cols_changed:
            cstr = ", ".join(
                "{c}({lcols[c]}->{rcols[c]})" for c in sorted(cols_changed)
            )
            yield Result(markdown=f" * {len(cols_changed)} columns changed type: {cstr}\n")

        # pk_cols should be filtered so that only cols_intact are considered
        pk_cols_filtered = [c for c in pk_cols if c in cols_intact]
        if len(pk_cols_filtered) != len(pk_cols):
            pk_cols_dropped = sorted(set(pk_cols) - set(pk_cols_filtered))
            yield Result(
                markdown=f" * {len(pk_cols_dropped)} primary key columns need to be dropped: {', '.join(pk_cols_dropped)}\n"
            )
            pk_cols = pk_cols_filtered

        ldf: pd.DataFrame = pd.DataFrame()
        rdf: pd.DataFrame = pd.DataFrame()

        if self.settings.single_pass_pk_comparison:
            ldf = self.get_records(ldb, columns=cols_intact, index_columns=pk_cols)
            rdf = self.get_records(rdb, columns=cols_intact, index_columns=pk_cols)

        with tracer.start_as_current_span("compare_primary_keys"):
            # TODO(rousik): fetch the primary key columns, filtered by the partition if present.
            if not self.settings.single_pass_pk_comparison:
                with tracer.start_as_current_span("load_pk_dataframes"):
                    ldf = self.get_records(ldb, columns=pk_cols, index_columns=pk_cols)
                    rdf = self.get_records(rdb, columns=pk_cols, index_columns=pk_cols)

            mrg = ldf.merge(rdf, how="outer", indicator=True, left_index=True, right_index=True, validate="one_to_one")
            overlap_index = mrg[mrg._merge == "both"].index

            orig_row_count = len(ldf)
            rows_added = len(mrg[mrg._merge == "right_only"])
            rows_removed = len(mrg[mrg._merge == "left_only"])

            # TODO(rousik): we could take samples of the added/removed rows and emit markdown
            # table to make it easier to compare.
            if rows_added:
                if orig_row_count > 0:
                    pct_change = (
                        f"({float(rows_added) * 100 / orig_row_count :.2f}% change)"
                    )
                else:
                    pct_change = "(no rows previously)"
                yield Result(markdown=f" * added {rows_added} rows {pct_change}\n")
            if rows_removed:
                pct_change = (
                    f"({float(rows_removed) * 100 / orig_row_count :.2f}% change)"
                )
                yield Result(markdown=f" * removed {rows_removed} rows {pct_change}\n")

        with tracer.start_as_current_span("compare_overlapping_rows") as sp:
            sp.set_attribute("num_rows", len(overlap_index))
            if self.settings.single_pass_pk_comparison:
                ldf = ldf.loc[overlap_index]
                rdf = rdf.loc[overlap_index]
            else:
                with tracer.start_as_current_span("load_overlapping_rows"):
                    ldf = self.get_records(ldb, columns=cols_intact, index_columns=pk_cols)
                    ldf = ldf.loc[overlap_index]

                    rdf = self.get_records(rdb, columns=cols_intact, index_columns=pk_cols)
                    rdf = rdf.loc[overlap_index]

            diff_rows = ldf.compare(rdf, result_names=("left", "right"))
            rows_changed = len(diff_rows)
            if rows_changed:
                pct_change = float(rows_changed) * 100 / orig_row_count
                yield Result(markdown=f" * changed {rows_changed} rows ({pct_change:.2f}% change)\n")

                # calculate number of rows that have changes in a particular column
                changes_per_col = (~diff_rows.T.isna()).groupby(level=0).any().T.sum()
                changes_per_col = changes_per_col.to_frame().reset_index()
                changes_per_col.columns = ["column_name", "num_rows"]

                # TODO(rousik): assign column names: column_name, rows_changed
                # TODO(rousik): This could be severity DIAGNOSTIC.

                cc = StringIO()
                cc.write("\nNumber of changes found per column:\n\n")
                cc.write(changes_per_col.to_markdown())
                yield Result(severity=ReportSeverity.DIAGNOSTIC, markdown=cc.getvalue())