import sqlite3
import pandas as pd
import os
from typing import Union


def create_sqlite_db(
        df: pd.DataFrame,
        schema_file: Union[str, os.PathLike] = os.path.abspath(
            "./db_schema.sql"
        ),
        db_file: Union[str, os.PathLike] = os.path.abspath("./database.db")
) -> None:
    """
    Create an SQLite database using a schema file and load data from a pandas
    DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The data to be loaded into the database.

    schema_file : Union[str, os.PathLike]
        Path to the SQL file containing the schema definition. Default is
        'db_schema.sql'.

    db_file : Union[str, os.PathLike], optional
        Path to the SQLite database file to be created.
        Defaults to './database.db'.
    """
    # Check if the database file already exists
    if os.path.exists(db_file):
        raise FileExistsError(
            f"The database file '{db_file}' already exists."
            f" Please specify a different path or remove the existing file."
        )

    # Create the database and apply the schema
    conn = sqlite3.connect(db_file)
    try:
        with open(schema_file, 'r') as file:
            schema = file.read()
        cursor = conn.cursor()
        cursor.executescript(schema)
        conn.commit()
        print(f"Database and schema created at: {db_file}")

        # Load the DataFrame into the database
        table_name = schema.split("CREATE TABLE")[1].split("(")[0].strip()
        df.to_sql(
            table_name,
            conn,
            if_exists='append',
            index=False
        )
        print(f"Data inserted into table: {table_name}")

    except sqlite3.Error as e:
        print(f"SQLite Error: {e}")
        raise

    finally:
        conn.close()
        print(f"Database connection closed: {db_file}")
