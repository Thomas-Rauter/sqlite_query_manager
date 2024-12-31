import os
import sqlite3
import pandas as pd
import logging
from pathlib import Path
from typing import Union, List


def run_sql_queries(
        query_dir: Union[str, os.PathLike],
        db_file: Union[str, os.PathLike],
        output_dir: Union[str, os.PathLike],
        rerun_all: bool = False,
        rerun_queries: List[str] = None
) -> None:
    """
    Execute all SQL queries in a directory (including subdirectories) on a
    SQLite database. Output results as CSV files mirroring the SQL directory
    structure.

    Parameters
    ----------
    query_dir : str
        Path to the directory containing SQL query files.
    db_file : str
        Path to the SQLite database file.
    output_dir : str
        Path to the output directory for CSV files.
    rerun_all : bool, optional
        Whether to rerun queries whose output already exists (default is False).
    rerun_queries : list of str, optional
        List of specific query filenames to rerun, regardless of existing
         output.

    Returns
    -------
    None
    """
    validate_inputs(
        query_dir=query_dir,
        db_file=db_file,
        output_dir=output_dir,
        rerun_all=rerun_all,
        rerun_queries=rerun_queries
    )

    # Normalize rerun_queries for comparison
    rerun_queries = set(rerun_queries or [])

    # Configure logging
    logging.basicConfig(
        filename='query_manager.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger()

    logger.info("Starting SQL query execution process.")

    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file)
        logger.info(f"Connected to database: {db_file}")
    except Exception as e:
        logger.error(f"Failed to connect to database: {db_file}. Error: {e}")
        return

    # Walk through the SQL directory structure
    for root, _, files in os.walk(query_dir):
        relative_path = os.path.relpath(root, query_dir)
        target_output_dir = os.path.join(output_dir, relative_path)

        # Ensure output subdirectory exists
        os.makedirs(target_output_dir, exist_ok=True)

        for file_name in files:
            if file_name.endswith('.sql'):  # Process only .sql files
                sql_file_path = os.path.join(root, file_name)
                output_file_path = os.path.join(
                    target_output_dir,
                    f"{Path(file_name).stem}.csv"
                )

                # Skip if output already exists and rerun_all is False,
                # unless in rerun_queries
                if (os.path.exists(output_file_path)
                        and not rerun_all
                        and file_name not in rerun_queries):
                    logger.info(
                        f"Skipping query (output exists):"
                        f" {sql_file_path}"
                    )
                    continue

                # Read and execute the SQL query
                try:
                    with open(sql_file_path, 'r') as query_file:
                        query = query_file.read()

                    logger.info(f"Executing query: {sql_file_path}")
                    df = pd.read_sql_query(query, conn)

                    # Save result to CSV
                    df.to_csv(output_file_path, index=False)
                    logger.info(
                        f"Query executed successfully. Output saved to:"
                        f" {output_file_path}"
                    )

                except Exception as e:
                    logger.error(
                        f"Error executing query:"
                        f" {sql_file_path}. Error: {e}"
                    )

    # Close the database connection
    conn.close()
    logger.info("SQL query execution process completed.")


def validate_inputs(
        query_dir: Union[str, os.PathLike],
        db_file: Union[str, os.PathLike],
        output_dir: Union[str, os.PathLike],
        rerun_all: bool,
        rerun_queries: List[str] = None
) -> None:
    """
    Validate the inputs to ensure they match the expected formats.

    Parameters
    ----------
    query_dir : Union[str, os.PathLike]
        Path to the directory containing SQL query files.
    db_file : Union[str, os.PathLike]
        Path to the SQLite database file.
    output_dir : Union[str, os.PathLike]
        Path to the directory where query results will be stored.
    rerun_all : bool
        Whether to rerun queries if their outputs already exist.
    rerun_queries : list of str, optional
        List of specific query filenames to rerun, regardless of existing
        output.

    Raises
    ------
    TypeError
        If any input does not match the expected type.
    ValueError
        If paths do not exist or are not valid directories/files.
    """
    # Validate query_dir
    if not isinstance(query_dir, (str, os.PathLike)):
        raise TypeError(
            f"'query_dir' must be a directory path (str or PathLike). "
            f"Got {type(query_dir).__name__}."
        )

    if not os.path.isdir(query_dir):
        raise ValueError(
            f"'query_dir' must be an existing directory. Path provided:"
            f" {query_dir}"
        )

    # Validate db_file
    if not isinstance(db_file, (str, os.PathLike)):
        raise TypeError(
            f"'db_file' must be a file path (str or PathLike)."
            f" Got {type(db_file).__name__}."
        )

    if not os.path.isfile(db_file):
        raise ValueError(
            f"'db_file' must be an existing file."
            f" Path provided: {db_file}"
        )

    # Validate output_dir
    if not isinstance(output_dir, (str, os.PathLike)):
        raise TypeError(
            f"'output_dir' must be a directory path (str or PathLike)."
            f" Got {type(output_dir).__name__}."
        )

    # No need to check if output_dir exists, as it may be created later

    # Validate force_rerun
    if not isinstance(rerun_all, bool):
        raise TypeError(
            f"'force_rerun' must be a boolean."
            f" Got {type(rerun_all).__name__}."
        )

    # Validate rerun_queries
    if rerun_queries is not None:
        if not isinstance(rerun_queries, list):
            raise TypeError(
                f"'rerun_queries' must be a list of strings."
                f" Got {type(rerun_queries).__name__}."
            )

        if not all(isinstance(query, str) for query in rerun_queries):
            raise TypeError(
                "All elements in 'rerun_queries' must be strings."
            )
