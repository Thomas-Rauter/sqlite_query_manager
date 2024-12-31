# SQLite Query Manager

`sqlite_query_manager` is a Python package designed to efficiently manage and 
execute SQL queries on SQLite databases. The package supports organizing SQL queries in directories, running them sequentially or selectively, and exporting the results as CSV files. It also includes flexible options to rerun all queries, specific queries, or skip those with existing outputs.

## Table of Contents

- [Introduction](#introduction)
- [Usage](#usage)
  - [Basic Example](#basic-example)
  - [Directory Layouts](#directory-layouts)
  - [Rerun Options](#rerun-options)
- [Installation](#installation)

---

## Introduction

The `sqlite_query_manager` package simplifies SQL query execution workflows by:
- Recursively processing SQL queries from directories.
- Mirroring directory structures for outputs.
- Avoiding redundant execution of queries unless explicitly specified.
- Logging all execution details and errors.

This makes it ideal for projects involving multiple interdependent queries, such as data pipelines and analytics workflows.

---

## Usage

### Basic Example

Here’s a minimal example of how to use the `run_sql_queries` function:

```python
from sqlite_query_manager import run_sql_queries 

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
pip install git+https://github.com/Thomas-Rauter/sqlite_query_manager.git@v0.1.0
```

## When facing problems

For any issues or feature requests, please open an issue on the [GitHub 
repository](https://github.com/Thomas-Rauter/sqlite_query_manager).
