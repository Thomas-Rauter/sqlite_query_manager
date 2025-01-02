import os
import sqlite3
import tempfile
import unittest
import logging
import io
import pandas as pd
from sqlite_manager.create_sqlite_db import create_sqlite_db


class TestCreateSQLiteDB(unittest.TestCase):
    """
    Unit test for the `create_sqlite_db` function.

    This test validates:
    - The SQLite database is created or updated correctly.
    - The schema from the SQL file is applied.
    - The data from the pandas DataFrame is inserted into the database.
    - Logs are written to a temporary directory and cleaned up after the test.
    """

    def setUp(self):
        """
        Set up a temporary test environment and in-memory logging.
        """
        # Create an in-memory log stream
        self.log_stream = io.StringIO()
        self.log_handler = logging.StreamHandler(self.log_stream)

        # Set up the logger to use the in-memory handler
        self.logger = logging.getLogger()
        self.logger.handlers = []  # Clear any existing handlers
        self.logger.addHandler(self.log_handler)
        self.logger.setLevel(logging.INFO)

        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.schema_file = os.path.join(self.temp_dir.name, "schema.sql")
        self.db_file = os.path.join(self.temp_dir.name, "test_database.db")
        self.log_dir = self.temp_dir.name  # Use the temporary directory for logs
        self.table_name = "TestTable"

        # Create a small schema file
        schema_content = """
        CREATE TABLE IF NOT EXISTS TestTable (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER
        );
        """
        with open(self.schema_file, "w") as f:
            f.write(schema_content)

        # Create a test DataFrame
        self.data = {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35]
        }
        self.df = pd.DataFrame(self.data)

    def tearDown(self):
        """
        Clean up the in-memory logger and temporary files.
        """
        # Remove the in-memory log handler
        self.logger.removeHandler(self.log_handler)
        self.log_handler.close()

        # Clean up the temporary test environment
        self.temp_dir.cleanup()

    def test_create_sqlite_db(self):
        """
        Test the `create_sqlite_db` function for:
        - Database and schema creation.
        - Data insertion into the SQLite database.
        """
        # Call the function being tested
        create_sqlite_db(
            df=self.df,
            schema_file=self.schema_file,
            db_file=self.db_file,
            table_name=self.table_name,
            log_dir=self.log_dir,  # Pass the temporary directory for logs
        )

        # Validate that the database file exists
        self.assertTrue(
            os.path.exists(self.db_file),
            "Database file was not created."
        )

        # Connect to the database and validate the schema and data
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        # Check if the table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='TestTable';"
        )
        table_exists = cursor.fetchone()
        self.assertIsNotNone(
            table_exists,
            "The table 'TestTable' does not exist in the database."
        )

        # Validate the data in the table
        cursor.execute("SELECT * FROM TestTable;")
        rows = cursor.fetchall()
        self.assertEqual(
            len(rows),
            len(self.df),
            f"Expected {len(self.df)} rows, but got {len(rows)}."
        )
        self.assertEqual(
            rows[0],
            (1, "Alice", 25),
            "Row 1 data does not match the expected value."
        )

        conn.close()

        # Validate log output
        logs = self.log_stream.getvalue()
        self.assertIn("Schema applied successfully.", logs)
        self.assertIn("Inserted 3 rows into table 'TestTable'.", logs)


if __name__ == "__main__":
    unittest.main()
