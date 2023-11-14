import sqlite3
import tempfile
from textwrap import dedent
from typing import Any
import unittest

from sqlalchemy import create_engine
from pudl_output_differ import sqlite
from pudl_output_differ.task_queue import TaskQueue
from pudl_output_differ.types import ObjectPath

# TODO(rousik): we could set up temporary sqlite databases for testing as
# part of the test cases.


def make_temp_db(sql_script: str) -> Any:
    """Creates a temporary sqlite database.

    It invokes the sql_script to bootstrap the database
    and returns the file-like object that represents
    the database. This can be opened by using its .name
    property to get the path to the file.
    """
    temp_file = tempfile.NamedTemporaryFile(suffix=".sqlite")
    conn = sqlite3.connect(temp_file.name)
    conn.executescript(sql_script)
    conn.commit()
    conn.close()
    return temp_file


class LiveTest(unittest.TestCase):
    TABLES_TO_TEST = [
        "boiler_fuel_eia923",
        "denorm_boiler_fuel_eia923",
        "denorm_boiler_fuel_monthly_eia923",
        "denorm_boiler_fuel_yearly_eia923",
        "denorm_generation_fuel_combined_monthly_eia923",
        "denorm_generation_fuel_combined_yearly_eia923",
        "denorm_generation_monthly_eia923",
        "denorm_generation_yearly_eia923",
        "denorm_plants_utilities_eia",
    ]
    def setUp(self):
        """Show all diffs."""
        self.skipTest("This test is too slow for CI or non local development.")

        self.maxDiff = None
        self.left_db_path = "/Users/rousik/pudl-data/fast-samples/left/pudl.sqlite"
        self.right_db_path = "/Users/rousik/pudl-data/fast-samples/right/pudl.sqlite"
        self.left_db = create_engine("sqlite:////Users/rousik/pudl-data/fast-samples/left/pudl.sqlite")
        self.right_db = create_engine("sqlite:////Users/rousik/pudl-data/fast-samples/right/pudl.sqlite")
        self.obj_path = ObjectPath().extend(sqlite.Database(name="pudl.sqlite"))
    # TODO(rousik): Further tests here could involve
    # 1. testing sharding of the tables (submitting TableEvaluation tasks with the right
    # partition keys)
    # 2. testing various scenarios of pk/full comparison in one/two way mode.

    # TODO(rousik): we could invoke compare_pk_table() directly to see
    # why there are differences in the two runs.

    def test_two_evaluation_modes(self):
        """Make sure that running one-way and two-way comparison yields same results."""
        two_way = sqlite.TableAnalyzer(
            object_path=self.obj_path,
            db_name="pudl.sqlite",
            table_name="coalmine_eia923",
            left_db_path=self.left_db_path,
            right_db_path=self.right_db_path,
            settings=sqlite.SQLiteSettings(single_pass_pk_comparison=False),
        )
        one_way = sqlite.TableAnalyzer(
            object_path=self.obj_path,
            db_name="pudl.sqlite",
            table_name="coalmine_eia923",
            left_db_path=self.left_db_path,
            right_db_path=self.right_db_path,
            settings=sqlite.SQLiteSettings(single_pass_pk_comparison=True),
        )
        lconn = self.left_db.connect()
        rconn = self.right_db.connect()
        pk_cols = two_way.get_pk_columns(self.left_db)
        first_report = two_way.compare_pk_tables(lconn, rconn, pk_cols)
        second_report = one_way.compare_pk_tables(lconn, rconn, pk_cols)
        self.assertEqual(first_report, second_report)

    def test_live_data(self):
        for table_name in self.TABLES_TO_TEST:
            task_queue = TaskQueue(max_workers=1)
            task_queue.put(
                sqlite.TableAnalyzer(
                    object_path=self.obj_path,
                    db_name="pudl.sqlite",
                    table_name=table_name,
                    left_db_path="/Users/rousik/pudl-data/fast-samples/left/pudl.sqlite",
                    right_db_path="/Users/rousik/pudl-data/fast-samples/right/pudl.sqlite",
                    settings=sqlite.SQLiteSettings(
                        single_pass_pk_comparison=False,
                    )
                )
            )
            self.assertEqual("", task_queue.to_markdown())
            # print(task_queue.to_markdown())
            # TODO(rousik): consider bunch of test cases that have left.sqlite, right.sqlite and report.md
            # files. Those will then be iterated over, executed and compared.

            # Perhaps a tool could be built to extract some table from existing sqlite files for generating
            # these test cases.

class TestSQLiteAnalyzer(unittest.TestCase):
    def test_compare_pk_tables(self):
        left = make_temp_db(
            """
            CREATE TABLE foo (
                year INTEGER,
                state STRING,
                quantity INTEGER,
                CONSTRAINT pk_foo PRIMARY KEY (year, state)
            );
            INSERT INTO foo VALUES (1900, "HI", 1);
            INSERT INTO foo VALUES (2019, "CA", 2);
            INSERT INTO foo VALUES (2020, "NY", 3);
            INSERT INTO foo VALUES (2021, "CO", 4);
            """
        )
        right = make_temp_db(
            """
            CREATE TABLE foo (
                year INTEGER,
                state STRING,
                quantity INTEGER,
                CONSTRAINT pk_foo PRIMARY KEY (year, state)
            );
            INSERT INTO foo VALUES (1900, "HI", 1);
            INSERT INTO foo VALUES (2019, "CA", 200);
            INSERT INTO foo VALUES (2020, "NY", 3);
            INSERT INTO foo VALUES (1984, "NH", 40);
            """
        )
        # In the above table, 2019/CA has changed, 2021/CO is deleted and 1984/NH is added.
        task_queue = TaskQueue(max_workers=1)
        task_queue.put(
            sqlite.SQLiteAnalyzer(
                object_path=ObjectPath(),
                db_name="test",
                left_db_path=left.name,
                right_db_path=right.name,
            )
        )
        self.assertEqual(
            dedent(
                """
            ## Table test/foo rows
            * added 1 rows (25.00% change)
            * removed 1 rows (25.00% change)
            * changed 1 rows (25.00% change)

            Number of changes detected per column:

            |    | column_name   |   num_rows |
            |---:|:--------------|-----------:|
            |  0 | quantity      |          1 |
            """
            ),
            dedent(task_queue.to_markdown()),
        )


if __name__ == "__main__":
    unittest.main()
