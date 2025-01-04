import tempfile
import shutil
import unittest
from pathlib import Path
import pandas as pd
import logging
from sqlite_manager.run_plot_functions import run_plot_functions


class TestRunPlotFunctions(unittest.TestCase):
    def setUp(self):
        """Set up temporary directories and resources for testing."""
        # Create a temporary directory structure
        self.temp_dir = tempfile.mkdtemp()
        self.query_results_dir = Path(self.temp_dir) / "query_results"
        self.plot_functions_dir = Path(self.temp_dir) / "plot_functions"
        self.output_dir = Path(self.temp_dir) / "output"
        self.log_dir = Path(self.temp_dir) / "logs"

        self.query_results_dir.mkdir(parents=True, exist_ok=True)
        self.plot_functions_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Create a mock CSV file in the query_results directory
        csv_path = self.query_results_dir / "mock_data.csv"
        pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]}).to_csv(
            csv_path,
            index=False
        )

        # Create a mock Python file for a plotting function in the
        # plot_functions directory
        plot_file_path = self.plot_functions_dir / "mock_plot.py"
        with open(plot_file_path, "w") as f:
            f.write("""
def plot_mock_data(mock_data, output_dir):
    output_file = output_dir / "mock_plot.png"
    with open(output_file, "w") as f:
        f.write("This is a mock plot.")
            """)

    def tearDown(self):
        """Clean up temporary directories."""
        # Remove all handlers associated with the logger
        logger = logging.getLogger("PlotManager")
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

        # Delete the temporary directory
        shutil.rmtree(self.temp_dir)

    def test_run_plot_functions(self):
        """Test that run_plot_functions executes without errors."""
        try:
            run_plot_functions(
                query_results_dir=self.query_results_dir,
                plot_functions_dir=self.plot_functions_dir,
                output_dir=self.output_dir,
                log_dir=self.log_dir,  # Updated argument
                rerun_all=True
            )
        except Exception as e:
            self.fail(f"run_plot_functions raised an exception: {e}")

        # Verify that the log directory contains the expected log file
        log_files = list(self.log_dir.glob("*.log"))
        self.assertTrue(
            len(log_files) > 0,
            "No log file was created in the log directory."
        )
        self.assertTrue(
            any("plot_functions_" in log_file.name for log_file in log_files),
            "Log file with the expected timestamped name was not created."
        )

        # Verify that the output file is created
        output_files = list(self.output_dir.glob("*.png"))
        self.assertTrue(
            len(output_files) > 0,
            "No plot files were created."
        )
        self.assertTrue(
            (self.output_dir / "mock_plot.png").exists(),
            "Mock plot file was not created."
        )


if __name__ == "__main__":
    unittest.main()
