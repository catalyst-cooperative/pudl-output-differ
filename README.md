# PUDL Output Differ

This standalone tool is designed for comparing contents
of two directories. The goal is to give clear and concise
report about what are the difference between the two.

File-type specific evaluations, primarily designed to
be used with databases, will be executed as well.

## Installation

This program uses poetry to manage its dependencies, so you should
[install that first](https://python-poetry.org/docs/#installation).

Once poetry is installed, you can set up environment and all dependencies with:
```
poetry install
```

Alternatively, you can rely on docker builds to run this tool.

## Usage