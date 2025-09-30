# Repository Guidelines

## Project
Migration of Informatica PowerCenter ETL jobs → Databricks PySpark (Delta Lake, Unity Catalog).

## Security
- Never output secrets or credentials.
- Use Unity Catalog 3-part naming for all tables.
- Handle PII with masking stubs (TODO marked).

## Observability
- Emit metrics: input_rows, output_rows, rejects, dq_violations.
- Structured logging with error taxonomy.

## Output Requirements
- Always include: code, tests (pytest+chispa), README.md with usage.
- If ambiguous, output a “Known Gaps” section.

## Project Structure & Module Organization
The repo centers on the Databricks migration assessment. `docs/assessment` holds current-state notes for each Informatica pipeline, while `docs/architecture` captures trade-off matrices and target designs. Use `docs/parity` for side-by-side legacy vs Databricks comparisons. Store raw Informatica exports under `legacy/mappings`, and keep reusable PySpark helpers in `pipelines/lib` with orchestrated jobs in `pipelines/jobs`. Tests live in `tests/unit`, `tests/integration`, and shared fixtures under `tests/fixtures`.

## Naming Conventions (enforce/auto-fix)

### General
- `snake_case` → variables, functions, modules, columns  
- `PascalCase` → classes  
- `UPPER_SNAKE_CASE` → constants  
- Avoid non-standard abbreviations

### Notebooks / Modules / Jobs
- Notebook file: `verb_object_purpose` (e.g., `load_customer_dim`)  
- Module file: `area_topic.py` (e.g., `customers_cleaning.py`)  
- Job name: `env | domain | pipeline | task` (e.g., `prod | growth | emkt | load_clicks`)

### Parameters (widgets/job params)
- Names start with `p_` (e.g., `p_env`, `p_run_date`)  
- Provide defaults in widgets and describe them

```python
dbutils.widgets.text("p_env", "dev", "Environment (dev|stg|prod)")
ENV = dbutils.widgets.get("p_env")
```

### DataFrames / Views / Tables / Columns
- DataFrame var: `df_<noun>[_stage]` (e.g., `df_orders_raw`, `df_orders_clean`)  
- Temp view: `vw_<domain>_<topic>`  
- Table names: snake_case, referenced as `catalog.schema.table`  
- Column patterns:
  - IDs: `*_id`  
  - Dates: `*_dt`  
  - Timestamps: `*_ts`  
  - Booleans: `is_*`, `has_*`

Agent auto-fixes:
- Suggest/preview rename diffs (e.g., `CustomerID → customer_id`)  
- Generate a `rename_map` snippet when many columns need normalization

---

## Structure & Cell Order
Required order:
1) Header (markdown)  
2) Imports (stdlib → third-party → Databricks/Spark → local `/src`)  
3) Parameters (widgets/job params)  
4) Constants & schemas  
5) Helpers (small functions)  
6) Main flow  
7) Validation & outputs (asserts, counts)  
8) Summary (printed artifacts)

Agent must flag:
- Missing parameter cell  
- Imports scattered across the notebook  
- No final summary

---

## Imports & Code Style
- Group and sort imports; no unused imports  
- Prefer `from pyspark.sql import functions as F, types as T` once  
- Python: PEP 8-ish, 120-char max, type annotations, docstrings on public helpers

Template the agent can inject:
```python
# stdlib
from datetime import date
import json
# third-party
# databricks/spark
from pyspark.sql import functions as F, types as T
# local
# from src.package.module import function
```

---

## Comments & Docstrings
- Comment **why**, not what.  
- TODO format: `# TODO(@owner, YYYY-MM-DD): description`

Reject if:
- Large blocks without context  
- TODOs lack owner/date

---

## Parameters & Secrets
- All runtime values come via **widgets/job params** or config file  
- Secrets via `dbutils.secrets.get(scope, key)` only

Agent flags:
- Hard-coded tokens/keys/URLs marked as secrets  
- Missing description for each widget

---

## Minimal Logging Block
Provide or enforce a tiny logging helper:

```python
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("nb")

def log_df(df, name: str):
    log.info("%s: rows=%s cols=%s", name, df.count(), len(df.columns))
```

At minimum, expect:
- `run_start` log with env/params  
- One log after major stage  
- End summary (artifacts & counts)

---

## SQL Style (in `%sql` or strings)
- Keywords UPPERCASE; identifiers snake_case  
- Always alias tables; avoid `SELECT *` in prod code  
- CTEs for readability; one clause per line

Agent autocorrect suggestions:
- Replace `SELECT *` with explicit column list (generate from schema if available)

---

## Data Contract Asserts (pre-write)
Expect at least one explicit contract assert per write:

```python
REQUIRED_COLS = {"customer_id","email","created_ts"}
missing = REQUIRED_COLS - set(df.columns)
assert not missing, f"Contract failed: missing {missing}"
```

Agent flags:
- Writes without any schema/column assertion  
- Non-deterministic writes with ambiguous columns

---

## Notebook Summary (required last cell)
Print the artifacts & quick stats:

```python
print("=== Outputs ===")
print(f"table: {OUTPUT_TABLE}")
print(f"path:  {OUTPUT_PATH}")
print(f"rows:  {spark.table(OUTPUT_TABLE).count() if OUTPUT_TABLE else df_out.count()}")
```

---

## PR Review Checklist (enforced by agent)
- [ ] Header cell complete (purpose, IO, params, owner, reviewed date)  
- [ ] Imports grouped; parameters widgetized; secrets via `dbutils.secrets`  
- [ ] Names follow conventions (vars, DataFrames, columns, views, tables)  
- [ ] No `SELECT *` in production cells  
- [ ] Comments explain **why**; TODOs have owner/date  
- [ ] Pre-write contract asserts present  
- [ ] End-of-run summary present and accurate  
- [ ] Notebook runs top→bottom with defaults

---

## Auto-Fix Suggestions (agent can propose patches)
- Insert missing header/summary cells  
- Normalize column names (generate `rename_map`)  
- Group & sort imports  
- Scaffold parameter widgets from detected constants  
- Generate contract assert from detected output schema  
- Convert `SELECT *` to explicit columns (introspect upstream schema)

---

## Examples (snippets the agent may inject)

**Widget block**
```python
dbutils.widgets.text("p_run_date", "", "Run date (YYYY-MM-DD)")
RUN_DATE = dbutils.widgets.get("p_run_date") or F.date_format(F.current_date(), "yyyy-MM-dd")
```

**Rename map**
```python
rename_map = {
  "CustomerID": "customer_id",
  "EmailAddress": "email",
  "CreatedAt": "created_ts",
}
df_std = df_raw.select([F.col(c).alias(rename_map.get(c, c.lower())) for c in df_raw.columns])
```

**Notebook skeleton**
```python
# 1) Imports
# 2) Params
# 3) Constants/Schemas
# 4) Helpers
# 5) Main flow
# 6) Validations & outputs
# 7) Summary
```

---

## Governance
- This agent.md is authoritative for code-style reviews.  
- Deviations require a short justification in the PR description and a follow-up task to align.


---

## Testing Best Practices

> Conventions for writing and organizing tests for Databricks code (coding hygiene only).

### 1) Test layout & naming

```
/tests/
  unit/                # pure functions & small transforms in /src
  integration/         # IO wiring, joins between 2–3 datasets
  notebooks/           # smoke tests for key notebooks (happy path)
  fixtures/            # tiny CSV/JSON + schema JSON (no PII)
  utils/               # shared helpers (df factory, asserts)
```

**Naming**
- Files: `test_<module_or_feature>.py`
- Test funcs: `test_<behavior>__<condition>__<expected>()`
- Use AAA (Arrange–Act–Assert). Keep asserts specific.

### 2) Fixtures & factories

- Keep fixtures **small and explicit** (≤50 rows). Prefer CSV/JSON checked into `fixtures/`.
- Provide a single factory for tiny DataFrames:

```python
# /tests/utils/df_factory.py
from pyspark.sql import Row
def df_from(list_of_dicts, spark, schema=None):
    rows = [Row(**r) for r in list_of_dicts]
    return spark.createDataFrame(rows, schema=schema)
```

### 3) Unit tests (/tests/unit)

- Focus on **pure functions** (parsers, formatters, mappers) and small DF transforms.
- No external I/O; mock clients or pass canned inputs.
- Make tests deterministic (seed dates/randomness).

```python
def test_normalize_email__trims_and_lowercases():
    assert normalize_email("  A@B.COM ") == "a@b.com"
    assert normalize_email("") is None
```

### 4) Integration tests (/tests/integration)

- Cover **parameters → IO → contract**:
  - Params resolved, inputs read
  - Outputs exist (tables/paths)
  - Contract holds (schema/keys/counts)
- Use temp paths (e.g., `dbfs:/tmp/tests/<uuid>`). Clean up.

```python
def test_pipeline_end_to_end__writes_expected_columns(spark, tmp_path):
    out = f"{tmp_path}/customers_clean"
    run_pipeline(input_path="fixtures/customers.csv", output_path=out)
    df = spark.read.format("delta").load(out)
    assert set(df.columns) >= {"customer_id","email","created_ts"}
    assert df.count() == 10
```

### 5) Notebook smoke tests (/tests/notebooks)

- One **happy-path** test per critical notebook:
  - Set default widgets
  - Execute top→bottom on small data
  - Verify exactly one artifact (table/path) exists and is non-empty
- Keep under ~3 minutes.

### 6) Shared asserts (/tests/utils/asserts.py)

```python
def assert_columns(df, expected: set[str]):
    missing = expected - set(df.columns)
    assert not missing, f"Missing columns: {missing}"

def assert_schema_equals(df, expected_dtypes: dict[str, str]):
    actual = {c: str(t) for c, t in df.dtypes}
    for col, dtype in expected_dtypes.items():
        assert actual.get(col) == dtype, f"{col}: {actual.get(col)} != {dtype}"
```

### 7) Test hygiene

- One behavior per test; short tests over large multi-assert blocks.
- No hidden global state: set widgets/params inside tests.
- Lint tests; same formatter as source. Avoid sleeps/retries unless asserting them.

### 8) CI expectations

- On PR: run unit + integration + one notebook smoke test.
- Fail the PR if any test fails or coverage for `/src` < threshold (e.g., 70%).
- Publish test reports/artifacts (e.g., JUnit XML, coverage HTML).

### 9) Review checklist (tests)

- [ ] Unit tests cover edge cases of pure functions
- [ ] Integration tests verify IO contracts and joins
- [ ] Notebook smoke test runs with defaults
- [ ] Fixtures are tiny, readable, versioned
- [ ] Deterministic (seeded) tests; no external dependencies
- [ ] CI executes tests and enforces coverage threshold

