import os
import pandas as pd
import importlib.util
import inspect
from tqdm import tqdm
import logging
from typing import Dict
from pathlib import Path
from datetime import datetime


# Exported function definition -------------------------------------------------


def run_plot_functions(
        query_results_dir: str | Path,
        plot_functions_dir: str | Path,
        output_dir: str | Path,
        log_dir: str | Path = None,
        rerun_all: bool = False,
        rerun_functions: list[str] | None = None
) -> None:
    """
    Run plotting functions based on query results and instructions.

    Parameters
    ----------
    query_results_dir : str or Path
        The path to the directory containing .csv results from SQL queries.

    plot_functions_dir : str or Path
        The path to the directory containing Python files with plotting
        functions.

    output_dir : str or Path
        The path to the directory where the plots should be saved.

    log_dir : str or Path
        The directory where the log file should be saved. A timestamped log
        file will be created automatically. Default is the current working dir.

    rerun_all : bool, optional
        Force rerun of all plotting functions, by default False.

    rerun_functions : list of str, optional
        List of specific plotting functions to rerun, by default None.

    Examples
    --------
    Basic usage:

    Assuming you have a directory structure like this:

    .. code-block:: text

        project/
        ├── query_results/
        │   └── data.csv
        ├── plot_functions/
        │   └── plot_example.py
        └── output/
        └── log.txt

    You can call the function as follows:

    .. code-block:: python

        from pathlib import Path
        from sqlite_manager.run_plot_functions import run_plot_functions

        run_plot_functions(
            query_results_dir=Path("project/query_results"),
            plot_functions_dir=Path("project/plot_functions"),
            output_dir=Path("project/output"),
            log_file=Path("project/log.txt"),
            rerun_all=True
        )

    This will:
    - Load all `.csv` files from the `query_results/` directory.
    - Execute all plotting functions in `plot_functions/`.
    - Save plots to the `output/` directory.
    - Log messages to `log.txt`.
    """
    if log_dir is None:     # Set default value
        log_dir = Path.cwd()

    # Convert string paths to pathlib.Path objects
    query_results_dir = Path(query_results_dir)
    plot_functions_dir = Path(plot_functions_dir)
    output_dir = Path(output_dir)
    log_dir = Path(log_dir)

    os.makedirs(        # Create output directory if it doesn't exist
        output_dir,
        exist_ok=True
    )

    os.makedirs(        # Create log directory if it doesn't exist
        log_dir,
        exist_ok=True
    )

    # Create a timestamped log file name
    timestamp = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
    log_file = log_dir / f"plot_functions_{timestamp}.log"

    logger = configure_logging(log_file=log_file)

    # Load all CSV files as DataFrames into a dictionary
    dataframes = get_dataframes(query_results_dir)
    if not dataframes:
        logger.error(
            f"No SQL results in CSV file format found in {query_results_dir}."
        )
        return

    plotting_functions = load_plotting_functions(
        plot_functions_dir=plot_functions_dir,
        logger=logger
    )

    rerun_functions = rerun_functions or []

    # Prepare the list of functions to run
    functions_to_run = []
    for func in plotting_functions:
        func_name = func.__name__
        if rerun_all or func_name in rerun_functions or should_run_function(
                func_name,
                output_dir
        ):
            functions_to_run.append(func)

    if not functions_to_run:
        logger.info("No functions were run as all outputs already exist.")
        return

    process_functions(
        functions_to_run=functions_to_run,
        dataframes=dataframes,
        output_dir=output_dir,
        logger=logger
    )

    logger.info("Completed running the plotting functions.")


# Level 1 function definitions -------------------------------------------------


def configure_logging(log_file: Path) -> logging.Logger:
    """Set up logging with file and console handlers."""
    logger = logging.getLogger("PlotManager")
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_file)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def load_plotting_functions(
        plot_functions_dir: Path,
        logger: logging.Logger
) -> list[callable]:
    """Load plotting functions from Python files in a directory."""
    plot_functions_files = list(plot_functions_dir.rglob("*.py"))
    if not plot_functions_files:
        logger.error(
            "No Python files found in the plot instructions directory."
        )
        return []

    all_functions = []
    for plot_function_file in plot_functions_files:
        module_name = plot_function_file.stem
        spec = importlib.util.spec_from_file_location(
            module_name,
            plot_function_file
        )
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as e:
            logger.error(f"Error executing module {module_name}: {e}")
            continue
        plot_functions = [
            func for name, func in inspect.getmembers(
                module,
                inspect.isfunction
            )
            if func.__module__ == module_name
        ]
        all_functions.extend(plot_functions)

    return all_functions


def should_run_function(
        func_name: str,
        output_dir: Path
) -> bool:
    """
    Determine if a plotting function should run based on existing outputs.

    Parameters
    ----------
    func_name : str
        The name of the plotting function (e.g., 'plot_country_revenue').
    output_dir : Path
        The path to the directory where plot outputs are stored.

    Returns
    -------
    bool
        True if the function should run (i.e., output does not exist),
        False otherwise.
    """
    base_func_name = func_name[len("plot_"):]  # Strip 'plot_' prefix

    # Iterate through files in the output directory
    for file_path in output_dir.glob(f"{base_func_name}*.png"):
        if file_path.is_file():
            return False  # Found a matching file, no need to rerun

    return True  # No matching file found, function should run


def get_dataframes(base_dir: Path) -> Dict[str, pd.DataFrame]:
    """
    Load all CSV files from a directory into a dictionary of DataFrames.

    Parameters
    ----------
    base_dir : Path
        The path to the base directory containing CSV files.

    Returns
    -------
    Dict[str, pd.DataFrame]
        A dictionary where keys are filenames (without extensions) and values
        are DataFrames loaded from the corresponding CSV files.
    """
    dataframes = {}

    # Iterate through CSV files in the directory and subdirectories
    for file_path in base_dir.rglob("*.csv"):
        key = file_path.stem  # Get the filename without the extension
        dataframes[key] = pd.read_csv(file_path)

    return dataframes


def process_functions(
        functions_to_run: list[callable],
        dataframes: dict[str, pd.DataFrame],
        output_dir: Path,
        logger: logging.Logger
) -> None:
    """
    Process and execute selected functions with their required arguments.

    Parameters
    ----------
    functions_to_run : list[callable]
        List of functions to be executed.
    dataframes : dict[str, pd.DataFrame]
        Dictionary of DataFrames keyed by their names.
    output_dir : Path
        Directory where the output files should be saved.
    logger : logging.Logger
        The logger to log errors and information.

    Returns
    -------
    None
    """
    with tqdm(
            total=len(functions_to_run),
            desc="Processing functions"
    ) as pbar:
        for func in functions_to_run:
            func_name = func.__name__
            try:
                # Prepare arguments including output_dir
                func_args = inspect.signature(func).parameters.keys()

                # Prepare the arguments
                args = {}
                for arg in func_args:
                    if arg in dataframes:  # Match argument with a DataFrame
                        args[arg] = dataframes[arg]
                    elif arg == "output_dir":  # Handle the output directory
                        args[arg] = output_dir

                # Check if all required arguments are satisfied
                missing_args = [arg for arg in func_args if
                                arg not in args]
                if missing_args:
                    logger.error(
                        f"Missing arguments for {func_name}: {missing_args}"
                    )
                else:  # Call the function
                    func(**args)
            except Exception as e:
                logger.error(f"Error running {func_name}: {e}")

            pbar.update(1)
