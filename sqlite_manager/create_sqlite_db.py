import pandas as pd
import sqlite3
import os
import logging
from typing import Union


def create_sqlite_db(
        df: pd.DataFrame,
        schema_file: Union[str, os.PathLike],
        db_file: Union[str, os.PathLike],
        table_name: str,
        log_dir: Union[str, os.PathLike] = None
) -> None:
    """
    Create or update an SQLite database using a schema file and load data into
    a specified table from a pandas DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The data to be loaded into the database.

    schema_file : Union[str, os.PathLike]
        Path to the SQL file containing the schema definition.

    db_file : Union[str, os.PathLike]
        Path to the SQLite database file to be created or updated.

    table_name : str
        Name of the table to insert the DataFrame into.

    log_dir : Union[str, os.PathLike], optional
        Directory where the log file is written. Default is the current
        working directory.

    Raises
    ------
    FileNotFoundError
        If the schema file does not exist.

    ValueError
        If the specified table name is not found in the schema or the schema
        does not match the DataFrame structure.
    Examples
    --------
    .. code-block:: python

        import pandas as pd
        from sqlite_manager import create_sqlite_db

        # Define the DataFrame to insert
        data = {
            "InvoiceNo": ["A001", "A002", "A003"],
            "StockCode": ["P001", "P002", "P003"],
            "Description": ["Product 1", "Product 2", "Product 3"],
            "Quantity": [10, 5, 20],
            "InvoiceDate": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "UnitPrice": [12.5, 8.0, 15.0],
            "CustomerID": ["C001", "C002", "C003"],
            "Country": ["USA", "UK", "Germany"]
        }
        df = pd.DataFrame(data)

        # Schema file (SQL file defining the database schema)
        schema_file = "schema.sql"
        # Contents of schema.sql:
        # CREATE TABLE IF NOT EXISTS OnlineRetail (
        #     InvoiceNo TEXT NOT NULL,
        #     StockCode TEXT NOT NULL,
        #     Description TEXT,
        #     Quantity INTEGER NOT NULL,
        #     InvoiceDate TEXT NOT NULL,
        #     UnitPrice REAL NOT NULL,
        #     CustomerID TEXT,
        #     Country TEXT
        # );

        # SQLite database file to create or update
        db_file = "data/online_retail.db"

        # Create or update the database and insert data into the table
        create_sqlite_db(
            df=df,
            schema_file=schema_file,
            db_file=db_file,
            table_name="OnlineRetail",
            log_dir="."  # Optional
        )
    """
    if log_dir is None:
        log_dir = os.getcwd()

    os.makedirs(log_dir, exist_ok=True)

    # Configure the logging
    log_file = os.path.join(log_dir, "create_sqlite_db.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    # Validate input files
    if not os.path.exists(schema_file):
        raise FileNotFoundError(f"Schema file '{schema_file}' not found.")

    # Read and validate schema
    with open(schema_file, 'r') as file:
        schema = file.read()

    if (f"CREATE TABLE {table_name}" not in schema
            and f"CREATE TABLE IF NOT EXISTS {table_name}" not in schema):
        logging.error(
            f"Table '{table_name}' is not defined in the schema file.")
        raise ValueError(
            f"Table '{table_name}' is not defined in the schema file.")

    # Check if the database already exists
    db_exists = os.path.exists(db_file)

    # Connect to SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    try:
        if not db_exists:
            logging.info(
                f"Database does not exist. Creating new database at: {db_file}")
            conn.executescript(schema)
            conn.commit()
            logging.info("Schema applied successfully.")
        else:
            logging.info(f"Using existing database at: {db_file}")
            # Apply the schema in case new tables are defined
            conn.executescript(schema)
            conn.commit()
            logging.info(
                "Schema re-applied to ensure all definitions are current.")

            # Check if the specified table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            existing_tables = [row[0] for row in cursor.fetchall()]
            if table_name in existing_tables:
                logging.info(
                    f"Table '{table_name}' already exists."
                    f" Data will be appended."
                )
            else:
                logging.info(
                    f"Table '{table_name}' was created from the schema.")

        # Validate table schema against DataFrame columns
        cursor.execute(f"PRAGMA table_info({table_name});")
        schema_columns = [row[1] for row in cursor.fetchall()]
        if not schema_columns:
            raise ValueError(
                f"Table '{table_name}' does not exist after applying"
                f" the schema."
            )

        missing_columns = [col for col in df.columns if
                           col not in schema_columns]
        if missing_columns:
            logging.error(
                f"Columns in DataFrame not found in table schema:"
                f" {missing_columns}"
            )
            raise ValueError(
                f"Table schema is missing required columns: {missing_columns}")

        # Insert DataFrame into the specified table
        df.to_sql(table_name, conn, if_exists='append', index=False)
        logging.info(f"Inserted {len(df)} rows into table '{table_name}'.")

    except sqlite3.Error as e:
        logging.error(f"SQLite Error: {e}")
        raise
    except ValueError as ve:
        logging.error(f"Validation Error: {ve}")
        raise
    finally:
        conn.close()
        logging.info(f"Database connection closed: {db_file}")
