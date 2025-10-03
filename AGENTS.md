# Repository Guidelines

## Project Structure & Module Organization
This repository tracks the Informatica to Databricks migration. Analysis sits in `docs/assessment`, target designs in `docs/architecture`, with parity notes under `docs/parity`. Raw PowerCenter exports remain in `legacy/mappings`. PySpark helpers live in `pipelines/lib`; runnable jobs such as `pipelines/jobs/disable_user_age18` ship their transforms and README alongside configs in `configs/jobs`. Tests split into `tests/unit`, `tests/integration`, and `tests/fixtures`; treat `spark-warehouse` and `tmp` as disposable scratch space.

## Build, Test, and Development Commands
- Create a virtual env via `python -m venv .venv` and activate with `.\.venv\Scripts\activate`; install core tooling using `pip install pyspark chispa pyyaml pytest` until shared requirements arrive.
- `pytest` runs everything; use `pytest tests/unit` for fast iteration and `pytest tests/integration` before opening a PR.
- `python -m pipelines.jobs.disable_user_age18.job --p_config configs/jobs/disable_user_age18.yaml` (or the analogous path for other jobs) executes a pipeline locally against your Spark session.

## Coding Style & Naming Conventions
Follow PEP 8 with a 120 character ceiling and type hints on public helpers. Group imports stdlib ? third-party ? Spark ? local with a single `from pyspark.sql import functions as F, types as T`. Use snake_case for modules, functions, DataFrame columns, and variables; PascalCase for classes; UPPER_SNAKE_CASE for constants. Widget and job parameters start with `p_` plus help text. Enforce Unity Catalog three-part names for tables.

## Testing Guidelines
Pytest plus Chispa is mandatory. Keep pure-function cases in `tests/unit`, data wiring in `tests/integration`, and Spark fixtures under `tests/fixtures`. Name tests `test_<behavior>__<condition>__<expected>` and follow Arrange-Act-Assert. Target at least 70% coverage for new helpers, assert schema and row counts, and cap fixtures at 50 deterministic rows.

## Commit & Pull Request Guidelines
Write imperative, scoped commit messages like `feat: add disable_user_age18 transforms`. PRs must link the source Informatica mapping, summarize contract changes, list test commands, and attach table counts or screenshots when data outputs shift. Ensure notebooks or jobs retain header cells, parameter widgets, contract asserts, and an end-of-run summary; document any exception in the PR body.

## Security & Data Governance
Do not commit secrets. Resolve credentials through `dbutils.secrets.get` or environment variables referenced in config YAML. Mask PII with TODO stubs carrying owner and due date. Every table, view, and storage path needs a Unity Catalog three-part name, and pipelines must log `input_rows`, `output_rows`, `rejects`, and `dq_violations` for observability.
