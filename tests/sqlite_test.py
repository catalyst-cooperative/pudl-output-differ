import sqlite3
import tempfile
from textwrap import dedent
from typing import Any
import unittest
from pudl_output_differ import sqlite
from pudl_output_differ.types import TaskQueue

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
    def test_live_data(self):
        task_queue = TaskQueue(max_workers=1)
        task_queue.put(
            sqlite.TableAnalyzer(
                object_path=[],
                db_name="pudl.sqlite",
                table_name="mcoe_generators_yearly",
                left_db_path="/Users/rousik/pudl-data/fast-samples/left/pudl.sqlite",
                right_db_path="/Users/rousik/pudl-data/fast-samples/right/pudl.sqlite",
            )
        )
        print(task_queue.to_markdown())


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
                object_path=[],
                db_name="test",
                left_db_path=left.name,
                right_db_path=right.name,
            )
        )
        self.assertEqual(
            dedent(
                """\
            ## Table test/foo rows
            * added 1 rows (25.00% change)
            * removed 1 rows (25.00% change)
            * changed 1 rows (25.00% change)
            """
            ),
            task_queue.to_markdown(),
        )


if __name__ == "__main__":
    unittest.main()
