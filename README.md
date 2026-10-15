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
/Volumes/ai_dev_class_demo/ai_dev_schema/ai_dev_raw
```

The data file is intentionally not stored in this Git repository.

## Player Master Bronze Asset Bundle

The bundle ingests `player_master.parquet` files from the raw volume, including
files in nested release folders, into the managed Delta table
`ai_dev_class_demo.bronze_layer.player_master`. Other datasets are excluded.
Raw files are retained. The job creates the destination schema and a managed
`ingestion_state` volume if they do not exist; it does not create the catalog.

The source contract is documented in
[`docs/player_master_data_dictionary.md`](docs/player_master_data_dictionary.md).
All 20 source columns remain in published order. Integer columns remain longs,
dates remain strings, and decimal precision is widened to 38 with the published
scale preserved. Nullability and business rules are not enforced in Bronze;
intentionally tainted values are retained.

Additional Bronze columns are `_source_file`, `_source_file_modified_at`,
`_ingested_at`, and `_rescued_data`. Auto Loader rescues later schema drift and
type mismatches for investigation. Initial inferred schema must match the
dictionary after decimal precision normalization. A first run with no matching
files cannot infer the schema; populate the source volume before running.

Baseline and monthly incremental files are appended as release records. A
player can appear multiple times across releases. This is not a current-state
player table: downstream processing must apply release ordering and merge rules.
Manifests, checksums, and relationship checks against regions are outside this
ingestion job's scope. Clean and tainted variants should use separate source
subdirectories, destination tables, and checkpoints to avoid mixing variants.

### Deploy and run

Install the Databricks CLI and authenticate to the intended workspace, for
example with `databricks auth login --host <workspace-url> --profile demo`.
The default job uses serverless notebook compute, which must be available in
the workspace. To use an existing Unity Catalog compatible cluster, add
`existing_cluster_id` to the task in `resources/player_master_bronze.job.yml`.

The run identity needs `USE CATALOG`, `USE SCHEMA`, and `READ VOLUME` on the
source. On the destination it needs `USE CATALOG`, `CREATE SCHEMA` if the schema
is absent, and `USE SCHEMA`, `CREATE TABLE`, and `CREATE VOLUME` within the
schema, plus `SELECT`/`MODIFY` on an existing destination table and
`READ VOLUME`/`WRITE VOLUME` on the state volume. Administrators can precreate
the schema and volume and grant the narrower required access.

From the repository root:

```powershell
databricks bundle validate -t dev --profile demo
databricks bundle deploy -t dev --profile demo
databricks bundle run -t dev --profile demo player_master_bronze
```

Override bundle variables consistently for validation, deployment, and runs:

```powershell
databricks bundle validate -t dev --profile demo --var "source_path=/Volumes/ai_dev_class_demo/ai_dev_schema/ai_dev_raw/clean,table=player_master_clean"
```

The persistent state is stored at
`/Volumes/<catalog>/<schema>/<state_volume>/<table>/schema` and `/checkpoint`.
Auto Loader processes files available at each run and then terminates. With the
same checkpoint, unchanged files are not reloaded. Source files must be immutable;
publish corrections under a new release directory instead of overwriting a file.
Do not delete the checkpoint or recreate the destination table independently:
losing the checkpoint can duplicate rows, and losing the table while retaining
the checkpoint can omit previously loaded rows.

The job limits concurrent runs to one. Multiple bundle deployments must not
write concurrently to the same destination/checkpoint; use distinct table names
for isolated development. The target defaults to `dev`, but the default table
is the requested shared Bronze table.

### Validation

Run local contract and configuration tests:

```powershell
python -m pip install -r requirements.txt
python -m pytest -q
```

On Databricks, run the ingestion job twice against unchanged files and verify
the table row count is unchanged. Then add a new release directory containing
`player_master.parquet` and verify only its rows are added. Inspect rescued rows:

```sql
SELECT _source_file, count(*) AS rows, count(_rescued_data) AS rescued_rows
FROM ai_dev_class_demo.bronze_layer.player_master
GROUP BY _source_file;
```

Local tests do not prove Auto Loader or Delta checkpoint semantics; these checks
require access to the actual Databricks source and compute.

## Repository Structure

```text
AI_Dev_Demo/
├── README.md
├── .gitignore
├── requirements.txt
├── databricks.yml
├── resources/
│   └── player_master_bronze.job.yml
├── docs/
│   └── player_master_data_dictionary.md
│
├── src/
│   └── player_master_bronze.py
│
├── tests/
│   └── test_player_master_bronze.py
│
└── notebooks/
    └── ingest_player_master_bronze.py
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
