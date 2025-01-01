# SQLite Query Manager

`sqlite_manager` is a Python package designed to efficiently manage and 
execute SQL queries on SQLite databases. The package supports organizing SQL queries in directories, running them sequentially or selectively, and exporting the results as CSV files. It also includes flexible options to rerun all queries, specific queries, or skip those with existing outputs.

## Table of Contents

- [Introduction](#introduction)
- [Usage](#usage)
  - [Directory Layouts](#directory-layouts)
- [Installation](#installation)

---

## Introduction

The `sqlite_manager` package simplifies SQL query execution workflows by:
- Recursively processing SQL queries from directories.
- Mirroring directory structures for outputs.
- Avoiding redundant execution of queries unless explicitly specified.
- Logging all execution details and errors.

This makes it ideal for projects involving multiple interdependent queries, such as data pipelines and analytics workflows.

---

## Usage

### `create_sqlite_db` function:

```python
# Import necessary modules
import pandas as pd
from sqlite_manager import create_sqlite_db

# Example DataFrame
data = {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
}
df = pd.DataFrame(data)

# Path to schema file (must contain the table definition)
schema_file = "db_schema.sql"

# Example schema (contents of db_schema.sql)
# CREATE TABLE ExampleTable (
#     id INTEGER PRIMARY KEY,
#     name TEXT,
#     age INTEGER
# );

# Path to SQLite database file
db_file = "example_database.db"

# Create SQLite database from the DataFrame
create_sqlite_db(
  df,
  schema_file,
  db_file
)

# Verify: The database is created, and data is inserted into the ExampleTable.
print(f"Database '{db_file}' created successfully with data from the DataFrame.")
```

### `run_sql_queries` function:

```python
from sqlite_manager import run_sql_queries 

query_dir = "sql_queries"          # Directory containing SQL files
db_file = "data/online_retail.db"  # SQLite database file
output_dir = "output"              # Directory to store query results

# Run all queries, skipping those with existing outputs
run_sql_queries (
  query_dir,
  db_file,
  output_dir
)

# Rerun all queries regardless of existing outputs
run_sql_queries (
  query_dir,
  db_file,
  output_dir,
  rerun_all=True
)

# Rerun specific queries
run_sql_queries (
  query_dir,
  db_file,
  output_dir,
  rerun_queries=["query1.sql", "query2.sql"]
)
```

### Directory Layouts

##### Input Directory (SQL Queries):
```text
sql_queries/
├── stage1/
│   ├── query1.sql
│   ├── query2.sql
├── stage2/
│   ├── query3.sql
│   └── query4.sql

# Output Directory (Query Results):
output/
├── stage1/
│   ├── query1.csv
│   ├── query2.csv
├── stage2/
│   ├── query3.csv
│   └── query4.csv
```

The `run_sql_queries` mirrors the structure of the input directory for outputs, 
ensuring a clean and organized workflow.

## Installation

Install directly from GitHub:

```bash
pip install git+https://github.com/Thomas-Rauter/sqlite_manager.git@v0.1.0
```

## When facing problems

For any issues or feature requests, please open an issue on the [GitHub 
repository](https://github.com/Thomas-Rauter/sqlite_manager).
