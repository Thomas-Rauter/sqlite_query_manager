# SQLite Manager

![Version](https://img.shields.io/badge/version-0.1.0-blue)
![Unit Test Status](https://github.com/Thomas-Rauter/sqlite_manager/actions/workflows/test.yml/badge.svg)


`sqlite_manager` is a Python package designed to streamline SQLite database 
creation from dataframes and query execution workflows.

## Table of Contents

- [Introduction](#introduction)
- [Documentation](#documentation)
- [Installation](#installation)

## Introduction

The `sqlite_manager` package streamlines SQLite database workflows by offering two primary functionalities:

1. **Database Creation**:
   - Generate an SQLite database directly from a pandas DataFrame and a schema file.
   - Automate schema creation and data population, minimizing manual effort.

2. **Query Execution**:
   - Recursively execute SQL queries stored in directories on the database.
   - Automatically mirror directory structures for query results, outputting them as CSV files.
   - Avoid redundant query execution unless explicitly requested, saving time and computational resources.
   - Log all execution details and errors for easy debugging and traceability.

## Documentation

Check out the [webpage](https://thomas-rauter.github.io/sqlite_manager/) for the
documentation.

## Installation

Install directly from GitHub:

```bash
pip install git+https://github.com/Thomas-Rauter/sqlite_manager.git@v0.1.0
```
