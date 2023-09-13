"""Utilities for comparing sqlite databases."""

from queue import Queue
from typing import Iterator
from pydantic import BaseModel, Field

import sqlalchemy as sa
from pudl_output_differ.types import (
    DiffEvaluator, DiffEvaluatorBase, DiffTreeNode, KeySetDiff
)

class SQLiteDBEvaluator(DiffEvaluatorBase):
    db_name: str
    left_db_path: str
    right_db_path: str

    def  get_table_schema(self, db_path: str) -> dict[str, set[str]]:
        """Returns dictionary of table schemas."""
        engine = sa.create_engine(f"sqlite:///{db_path}")
        inspector = sa.inspect(engine)
        out = {}
        for table_name in inspector.get_table_names():
            out[table_name] = [
                "::".join([col["name"], str(col["type"])])
                for col in inspector.get_columns(table_name)
            ]
        return out

    def execute(self, task_queue: Queue[DiffEvaluator]) -> Iterator[DiffTreeNode]:
        """Analyze tables and their schemas."""
        lschema = self.get_table_schema(self.left_db_path)
        rschema = self.get_table_schema(self.right_db_path)
        
        # All database diffs will be children of this node.
        db_node = DiffTreeNode(
            name=f"SQLiteDB({self.db_name})",
            parent=self.parent_node,
        )
        yield db_node

        # Tables are compared by name.
        tables = DiffTreeNode(
            name="Tables",
            parent=db_node,
            diff=KeySetDiff.from_sets(set(lschema), set(rschema)),
        )
        yield tables

        left_engine = sa.create_engine(f"sqlite:///{self.left_db_path}")
        right_engine = sa.create_engine(f"sqlite:///{self.right_db_path}")
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
                        left_db_engine=left_engine,
                        right_db_engine=right_engine,
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
            return f"~ RowCount: left={self.left_rows} right={self.right_rows}, diff={d}"
        return ""
    
class RowEvaluator(DiffEvaluatorBase):
    table_name: str
    left_db_engine: sa.engine.Engine = Field(exclude=True)
    right_db_engine: sa.engine.Engine = Field(exclude=True)

    def execute(self, task_queue: Queue[DiffEvaluator]) -> Iterator[DiffTreeNode]:
        """Analyze rows of a given table."""

        lrows = self.left_db_engine.connect().execute(
            sa.text(f"SELECT COUNT(*) FROM {self.table_name}")
        ).scalar()
        rrows = self.right_db_engine.connect().execute(
            sa.text(f"SELECT COUNT(*) FROM {self.table_name}")
        ).scalar()
        rc = DiffTreeNode(
            name="RowCount",
            parent=self.parent_node,
            diff=RowCountDiff(left_rows=lrows, right_rows=rrows),
        )
        yield rc
        
        # TODO(rousik): even if the rows are different, we might want to inspect
        # which rows are missing or different for better insights into what's going
        # on here.

        # If number or rows are the same, we can consider them to be equal by default,
        # unless in-depth analysis is requested.

#     def compare_table_rows(self, fname: str, table_name: str) -> RowSampleDiff:
#         """Compares individual rows within given file and table."""
#         # TODO(rousik): this may be very memory/resource expensive for tables
#         # with many rows. We might want to switch this off for very large tables
#         # or switch to some cheaper approach, e.g. statistical sampling or
#         # comparing row hashes.
#         bdf = self.baseline.get_rows_as_df(fname, table_name)
#         edf = self.experiment.get_rows_as_df(fname, table_name)

#         merged = bdf.merge(edf, how="outer", indicator=True)
#         b_only = merged[merged["_merge"] == "left_only"]
#         e_only = merged[merged["_merge"] == "right_only"]
#         shared = merged[merged["_merge"] == "both"]

#         # For the sake of compact result, simply count distinct records.
#         # Later on, samples of differing rows, or even all differing rows
#         # could be stored in left/right.
#         return RowSampleDiff(
#             category=f"Rows({fname}/{table_name})",
#             left_unique=len(b_only),
#             right_unique=len(e_only),
#             shared_rows=len(shared),
#             left=[],
#             right=[],
#         )



# class RowSampleDiff(ListDiff):
#     """Diff for row-by-row comparison."""

#     left_unique: int
#     right_unique: int
#     shared_rows: int
#     left: list[Any] = []
#     right: list[Any] = []
#     both: list[Any] = []

#     def sides_equal(self):
#         """Returns true if sides are equal."""
#         return self.left_unique == 0 and self.right_unique == 0

#     def print_diff(self):
#         """Prints human friendly representation of this diff."""
#         if not self.sides_equal():
#             print(
#                 f"{self.category}: -{self.left_unique} +{self.right_unique} ={self.shared_rows}"
#             )
#             for l_item in self.left:
#                 print(f"- {l_item}")
#             for r_item in self.right:
#                 print(f"+ {r_item}")

# # PudlOutputDirectory allows for listing, and possibly retrieiving and caching files
# # locally before accessing them.

# class PudlOutputDirectory(BaseModel):
#     """Represents single pudl output directory.

#     This could be either local or remote and faciliates access to the files and type-
#     aware accessors (e.g. sqlite_engine).
#     """
#     root_path: str
#     file_types: list[str] = ["json", "sqlite"]


#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         if self.root_path.startswith("gs://"):


#         """Create new instance pointed at root_path."""
#         self.root_path = root_path
#         if root_path.startswith("gs://"):
#             self.fs = fsspec.filesystem("gcs", project=gcs_project)
#         else:
#             self.fs = fsspec.filesystem("file")

#     def list_files(self) -> dict[str, str]:
#         """Returns dict mapping from basenames to full paths."""
#         # TODO(rousik): check if root_path is an actual directory.
#         if self.root_path.endswith(".sqlite"):
#             return {"sqlite": self.root_path}
#         return {
#             Path(fpath).name: fpath
#             for fpath in self.fs.glob(self.root_path + "/*")
#             if self.match_filetype(fpath)
#         }

#     def match_filetype(self, fpath: str) -> bool:
#         """Returns true if file should be considered for comparison."""
#         return any(fpath.endswith(ft) for ft in self.FILE_TYPES)

#     def get_engine(self, fname: str):
#         """Returns sqlalchemy engine for reading contents of fname."""
#         return create_engine_remote(self.fs, f"{self.root_path}/{fname}")

#     def get_inspector(self, fname: str):
#         """Returns sqlalchemy inspector for analyzing contents of fname."""
#         return inspect(self.get_engine(fname))

#     def get_row_count(self, fname: str, table_name: str) -> int:
#         """Returns number of rows contained within the table."""
#         if not re.compile(r"^[a-zA-Z0-9_]+$").match(table_name):
#             raise ValueError("table_name is not SQL safe")
#         # Note that because of the above check, CWE-89 SQL injection issue
#         # is no longer possible below.
#         # Because we get the table names from sqlite metadata, these table
#         # names should be safe by definition also.
#         return (
#             self.get_engine(fname)
#             .execute(text(f"SELECT COUNT(*) FROM {table_name}"))  # nosec
#             .scalar()
#         )

#     def get_rows_as_df(self, fname: str, table_name: str) -> pd.DataFrame:
#         """Returns sqlite table contents as pandas DataFrame."""
#         con = self.get_engine(fname)
#         return pd.concat(
#             list(pd.read_sql_table(table_name, con, chunksize=100_000))
#         )
