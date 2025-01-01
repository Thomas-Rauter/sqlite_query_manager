from sqlite_manager.create_sqlite_db import create_sqlite_db
import os
import sqlite3
import tempfile
import unittest
import pandas as pd


class TestCreateSQLiteDB(unittest.TestCase):
    """
    Unit test for the `create_sqlite_db` function.

    This test validates the following:
    - The SQLite database is created correctly.
    - The schema from the SQL file is applied.
    - The data from the pandas DataFrame is inserted into the database.
    """

    def setUp(self):
        """
        Set up a temporary directory and test inputs for the unit test.
        """
        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.schema_file = os.path.join(self.temp_dir.name, "schema.sql")
        self.db_file = os.path.join(self.temp_dir.name, "test_database.db")

        # Create a small schema file
        schema_content = """
        CREATE TABLE TestTable (
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
        Clean up temporary files and directories after the test.
        """
        self.temp_dir.cleanup()

    def test_create_sqlite_db(self):
        """
        Test the `create_sqlite_db` function for the following:
        - Database and schema creation.
        - Data insertion into the SQLite database.
        """
        # Call the function being tested
        create_sqlite_db(
            self.df,
            self.schema_file,
            self.db_file
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


if __name__ == "__main__":
    unittest.main()
