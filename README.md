# AI-Assisted Development Demo

## Purpose

This repository supports a classroom demonstration of AI-assisted software development using Git, GitHub, VS Code, Codex, and Databricks.

The objective is to demonstrate how an AI coding assistant can help a developer:

* understand an unfamiliar dataset
* generate and modify code
* implement data-quality checks
* develop analytical logic
* create tests
* review and improve code
* work within an existing repository structure
* use Git to track and manage changes

AI is used as a development assistant. The developer remains responsible for defining requirements, reviewing generated code, validating results, and deciding whether proposed changes are appropriate.

## Data Source

The demonstration uses the NAPA 5K `player_master` dataset stored as a Parquet file in a Databricks Unity Catalog volume.

Databricks volume path:

```text
/Volumes/ai_dev_demo/demo/demo_files/player_master.parquet
```

The data file is intentionally not stored in this Git repository.

## Repository Structure

```text
dsb6000-ai-dev-demo/
├── README.md
├── .gitignore
├── requirements.txt
│
├── src/
│   └── player_analysis.py
│
├── tests/
│   └── test_player_analysis.py
│
└── notebooks/
    └── player_analysis.py
```

### `src/`

Contains reusable Python and PySpark analysis and transformation logic.

### `tests/`

Contains tests used to validate the implementation.

### `notebooks/`

Contains Databricks notebook code used to access the dataset, execute the analysis, and display results.

## Initial Problem

A Parquet file containing approximately 5,000 player records has been provided.

The initial task is to explore the dataset and determine what information it contains before deciding how it should be analyzed.

Development should proceed incrementally. Requirements may be clarified or changed as the analysis progresses.

## Development Principles

During the exercise:

* inspect the data before making assumptions about it
* make business rules explicit
* do not silently correct questionable data
* review AI-generated code before accepting it
* test important logic
* validate analytical results
* use Git to preserve a history of changes
* prefer understandable code over unnecessary complexity

## Git Workflow

Changes developed during the demonstration should be committed incrementally so that the evolution of the solution can be reviewed.

A Git tag named `demo-start` identifies the original starting state of the repository and can be used to restore the repository after a demonstration.
