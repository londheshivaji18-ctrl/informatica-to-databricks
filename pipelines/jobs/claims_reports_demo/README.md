# Claims Reports Demo (Databricks PySpark)

- Source: `legacy/mappings/wf_mp_Claims_Reports_Demo.XML`
- Outputs:
  - `dev.insurance.total_claim_ff_full` — joined claims/policy/member snapshot
  - `dev.insurance.fruad_smoker_by_state` — counts by incident state and type for fraud+smoker

## Run Locally

- Create venv and install deps:
  - `python -m venv .venv && source .venv/bin/activate` (PowerShell: `.\.venv\\Scripts\\Activate.ps1`)
  - `python -m pip install pyspark chispa pytest pyyaml black ruff`
- Execute job (writes Delta tables to your configured metastore):
  - `python pipelines/jobs/claims_reports_demo/job.py --config configs/jobs/claims_reports_demo.yaml`

## Config

`configs/jobs/claims_reports_demo.yaml`

- `inputs.*` — CSV paths for Claim, Policy, Member (config-driven)
- `outputs.*` — Unity Catalog 3-part names
- `write_mode` — `overwrite` or `append`
- `shuffle_partitions` — default `200` (set low locally)

## Observability

- Structured JSON logging
- Metrics emitted: `input_rows`, `output_rows_total_claim`, `output_rows_fraud_by_state`, `rejects`, `dq_violations`

## Tests

- Unit: `python -m pytest tests/unit`
- Integration: `python -m pytest tests/integration`

## Known Gaps

- PII masking is a stub (`mask_pii_columns`) and logs a TODO; implement per data privacy policy.
- The Informatica filter `FIL_Fruad_Smoker` is implemented as `fraud_reported = 'Y' AND smoker = 'Y'` based on naming intent. If stricter parity is required, confirm exact fields/values in the XML and adjust `filter_fraud_smoker`.
- Session/target file writer options from Informatica are mapped to Delta table writes; tune write modes and clustering as needed.

