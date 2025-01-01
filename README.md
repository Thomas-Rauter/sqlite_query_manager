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



## Installation

Install directly from GitHub:

```bash
pip install git+https://github.com/Thomas-Rauter/sqlite_manager.git@v0.1.0
```
