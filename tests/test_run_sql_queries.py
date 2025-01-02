import os
import sqlite3
import tempfile
import unittest
import logging
import io
from sqlite_manager.run_sql_queries import run_sql_queries


class TestRunSQLQueries(unittest.TestCase):
    """
    Unit test for the `run_sql_queries` function.

    This test validates:
    - The SQLite database is queried successfully.
    - The CSV output is generated correctly.
    - The test environment is cleaned up after execution.
    """

    def setUp(self):
        """
        Set up a temporary test environment.
        """
        # Create an in-memory log stream
        self.log_stream = io.StringIO()
        self.log_handler = logging.StreamHandler(self.log_stream)

        # Set up the logger to use the in-memory handler
        self.logger = logging.getLogger()
        self.logger.handlers = []  # Clear existing handlers
        self.logger.addHandler(self.log_handler)
        self.logger.setLevel(logging.INFO)

        # Create a temporary directory for the test
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_file = os.path.join(self.temp_dir.name, "test_database.db")
        self.query_dir = os.path.join(self.temp_dir.name, "queries")
        self.output_dir = os.path.join(self.temp_dir.name, "output")

        os.makedirs(self.query_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

        # Create a sample SQLite database
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        # Create a sample table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                age INTEGER
            )
        """)

        # Insert some test data
        cursor.executemany("""
            INSERT INTO test_table (name, age) VALUES (?, ?)
        """, [
            ("Alice", 25),
            ("Bob", 30),
            ("Charlie", 35)
        ])
        conn.commit()
        conn.close()

        # Create a sample query file
        query_file = os.path.join(self.query_dir, "sample_query.sql")
        with open(query_file, "w") as f:
            f.write("SELECT * FROM test_table;")

    def tearDown(self):
        """
        Clean up after each test.
        """
        # Remove the in-memory log handler
        self.logger.removeHandler(self.log_handler)
        self.log_handler.close()

        # Clean up the temporary test environment
        self.temp_dir.cleanup()

    def test_run_sql_queries(self):
        """
        Test the `run_sql_queries` function.
        """
        # Run the function
        run_sql_queries(
            query_dir=self.query_dir,
            db_file=self.db_file,
            output_dir=self.output_dir,
            rerun_all=True,
            log_dir=self.temp_dir.name
        )

        # Check the output file
        output_file = os.path.join(self.output_dir, "sample_query.csv")
        self.assertTrue(
            os.path.exists(output_file),
            "Output file was not created."
        )

        # Validate the contents of the CSV file
        with open(output_file, "r") as f:
            lines = f.readlines()
        self.assertEqual(
            len(lines),
            4,
            "CSV file does not have the expected number of rows."
        )
        self.assertIn(
            "Alice,25",
            lines[1],
            "CSV content does not match expected data."
        )

        # Validate the logs
        logs = self.log_stream.getvalue()
        self.assertIn("Connected to database", logs)
        self.assertIn("Executing query", logs)
        self.assertIn("Query executed successfully", logs)


if __name__ == "__main__":
    unittest.main()
