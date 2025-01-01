import sqlite3
import os
from sqlite_query_manager.run_sql_queries import run_sql_queries

# Paths for testing
TEST_DIR = "test_sql"
DB_FILE = os.path.join(TEST_DIR, "test_database.db")
QUERY_DIR = os.path.join(TEST_DIR, "queries")
OUTPUT_DIR = os.path.join(TEST_DIR, "output")


def create_test_environment():
    """
    Creates a minimal SQLite database, test query, and directory structure.
    """
    os.makedirs(QUERY_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Create a sample SQLite database
    conn = sqlite3.connect(DB_FILE)
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
    with open(os.path.join(QUERY_DIR, "sample_query.sql"), "w") as f:
        f.write("SELECT * FROM test_table;")


def test_run_sql_queries():
    """
    Tests the run_sql_queries function with the created test environment.
    """
    run_sql_queries(
        query_dir=QUERY_DIR,
        db_file=DB_FILE,
        output_dir=OUTPUT_DIR,
        rerun_all=True
    )

    # Check the output
    output_file = os.path.join(OUTPUT_DIR, "sample_query.csv")
    if os.path.exists(output_file):
        print("Test passed: Output file created.")
    else:
        print("Test failed: Output file not found.")


if __name__ == "__main__":
    create_test_environment()
    test_run_sql_queries()
