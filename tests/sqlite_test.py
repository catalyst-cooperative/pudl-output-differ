import unittest
import os
from pudl_output_differ import sqlite
from pudl_output_differ.types import AnalysisReport, Analyzer

class DirectExecutor:
    def __init__(self):
        self.reports: list[AnalysisReport] = []
        
    def put(self, analyzer: Analyzer): 
        self.reports.append(analyzer.execute(self))

class TestSQLiteAnalyzer(unittest.TestCase):
    # def setUp(self):
    #     """Sets up the """
    #     pass

    # def tearDown(self):
    #     # Clean up any test data or variables
    #     pass

    def test_compare_sample_database(self):
        tq = DirectExecutor()
        this_file = os.path.dirname(os.path.abspath(__file__))
        tq.put(
            sqlite.SQLiteAnalyzer(
                object_path = [],
                db_name="db.sqlite",
                left_db_path=os.path.join(this_file, "left/db.sqlite"),
                right_db_path=os.path.join(this_file, "right/db.sqlite"),
            )
        )
        for rep in tq.reports:
            if rep.has_changes():
                print(rep.title)
                print(rep.markdown)


if __name__ == '__main__':
    unittest.main()