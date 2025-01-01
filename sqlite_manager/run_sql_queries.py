from typing import Union, List
import os
import sqlite3
import pandas as pd
import logging
from pathlib import Path
from tqdm import tqdm
import time


def run_sql_queries(
    db_file: Union[str, os.PathLike],
    query_dir: Union[str, os.PathLike] = os.path.abspath("./sql_queries"),
    output_dir: Union[str, os.PathLike] = os.path.abspath("./query_results"),
    rerun_all: bool = False,
    rerun_queries: List[str] = None
) -> None:
    """
    Execute all SQL queries in a directory (including subdirectories) on a
    SQLite database. Output results as CSV files mirroring the SQL directory
    structure.

    Parameters
    ----------
    db_file : str
        Path to the SQLite database file.

    query_dir : str or Path, optional
        Path to the directory containing SQL query files. Default is
        os.path.abspath("./sql_queries")

    output_dir : str or Path, optional
        Path to the output directory for CSV files. Default is
        os.path.abspath("./query_results")

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

    rerun_queries = set(rerun_queries or [])
    logging.basicConfig(
        filename='query_manager.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger()
    logger.info("Starting SQL query execution process.")

    # Collect all SQL query files
    query_files = []
    for root, _, files in os.walk(query_dir):
        for file_name in files:
            if file_name.endswith('.sql'):
                sql_file_path = os.path.join(
                    root,
                    file_name
                )
                query_files.append(sql_file_path)

    # Filter queries based on rerun criteria
    queries_to_execute = []
    for sql_file_path in query_files:
        relative_path = os.path.relpath(
            os.path.dirname(sql_file_path),
            query_dir
        )

        target_output_dir = os.path.join(
            output_dir,
            relative_path
        )

        os.makedirs(
            target_output_dir,
            exist_ok=True
        )
        output_file_path = os.path.join(
            target_output_dir,
            f"{Path(sql_file_path).stem}.csv"
        )
        if (
            not os.path.exists(output_file_path) or
            rerun_all or
            Path(sql_file_path).name in rerun_queries
        ):
            queries_to_execute.append((
                sql_file_path,
                output_file_path
            ))

    if not queries_to_execute:
        logger.info("No queries to execute. Exiting.")
        print("No queries to execute.")
        return

    # Connect to the SQLite database
    try:
        conn = sqlite3.connect(db_file)
        logger.info(f"Connected to database: {db_file}")
    except Exception as e:
        logger.error(f"Failed to connect to database: {db_file}. Error: {e}")
        return

    # Progress bar setup
    progress = tqdm(
        total=len(queries_to_execute),
        desc="Running queries",
        unit=" query",
        unit_scale=True
    )

    execution_times = []

    for sql_file_path, output_file_path in queries_to_execute:
        start_query_time = time.time()
        try:
            with open(sql_file_path, 'r') as query_file:
                query = query_file.read()

            logger.info(f"Executing query: {sql_file_path}")

            df = pd.read_sql_query(
                query,
                conn
            )

            # Ensure the result is tabular and suitable for CSV
            if df.empty or not isinstance(df, pd.DataFrame):
                logger.warning(
                    f"Query executed but returned no results or invalid "
                    f"structure (must be suitable for .csv file):"
                    f" {sql_file_path}"
                )
            else:
                df.to_csv(
                    output_file_path,
                    index=False
                )
                logger.info(
                    f"Query executed successfully. Output saved to:"
                    f" {output_file_path}"
                )

        except Exception as e:
            logger.error(f"Error executing query: {sql_file_path}. Error: {e}")

        # Update progress and estimate remaining time
        query_duration = time.time() - start_query_time
        execution_times.append(query_duration)
        avg_time_per_query = sum(execution_times) / len(execution_times)
        remaining_queries = len(queries_to_execute) - progress.n - 1
        estimated_time_left = avg_time_per_query * remaining_queries
        progress.set_postfix_str(f"ETA: {estimated_time_left:.2f}s")
        progress.update(1)

    progress.close()
    conn.close()
    logger.info("SQL query execution process completed.")
    print("SQL query execution process completed.")


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

    # Validate rerun_all
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
