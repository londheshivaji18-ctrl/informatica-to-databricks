# disable_user_age18

Migrated orchestration for Informatica mapping `m_DISBALE_USER_AGE18_LD`. The job identifies portal users aged 18 or older, calls the disable API, and writes audit views mirroring the original flat-file outputs.

## Flow Overview

1. Read contract snapshots (`t_agre_obj_id_bnd`, `t_agre_party_role_snapshot`, `t_party_snapshot`) and user profiles (`T_USER_PROFILES`) via Unity Catalog or parameterised SQL.
2. Reproduce the mapping joins and age calculation (`AGE = floor(|months_between(today, birth_dt)| / 12)`).
3. Split records into `<18` (rejects) and `=18` cohorts; attach API headers using resolved secrets.
4. Invoke the disable API (`DELETE {base_url_prefix}/{email}{base_url_suffix}`) and capture per-call responses.
5. Emit three Delta tables: API responses, eligible audit log, under-age rejects. Metrics (`input_rows`, `output_rows`, `underage_rows`, `rejects`, `dq_violations`) are logged in JSON.

## Configuration

All runtime settings live in `configs/jobs/disable_user_age18.yaml`:

```yaml
spark:
  shuffle_partitions: 8
sources:
  agreements_table: "sandbox.compliance.t_agre_obj_id_bnd"
  party_role_table: "sandbox.compliance.t_agre_party_role_snapshot"
  party_snapshot_table: "sandbox.compliance.t_party_snapshot"
  user_profiles_table: "sandbox.identity.t_user_profiles"
job:
  age_threshold: 18
  request_prefix: "ETL_DISABLE"
auth:
  bearer_token:
    env: "DISABLE_API_BEARER_TOKEN"
  headers:
    x_ibm_client_id:
      env: "DISABLE_API_CLIENT_ID"
    x_ibm_client_secret:
      env: "DISABLE_API_CLIENT_SECRET"
http:
  base_url_prefix: "https://example.api/users"
  base_url_suffix: "/disable"
outputs:
  disable_response:
    catalog: "sandbox"
    schema: "compliance"
    table: "user_disable_response"
  eligible_audit:
    catalog: "sandbox"
    schema: "compliance"
    table: "user_disable_candidates"
  underage_audit:
    catalog: "sandbox"
    schema: "compliance"
    table: "user_underage_audit"
```

Secrets may be resolved from Databricks scopes (`scope`/`key`), environment variables (`env`), or literal placeholders (`value`) for local testing.

## Running the Job

```bash
python -m pipelines.jobs.disable_user_age18.job \
  --p_config configs/jobs/disable_user_age18.yaml \
  --p_env dev \
  --p_run_date 2025-09-30
```

When executed inside a Databricks notebook, create widgets named `p_config`, `p_env`, and `p_run_date` (matching the CLI flags). The job logs structured events (`run_start`, `write_delta`, `run_complete`) for observability.

## Tests

- Unit tests (`tests/unit/test_disable_user_age18_transforms.py`) cover age derivation, header enrichment, and join semantics using `chispa`.
- Integration test (`tests/integration/test_disable_user_age18_pipeline.py`) exercises the end-to-end request preparation with a stubbed HTTP client.

Run everything with:

```bash
pytest tests/unit/test_disable_user_age18_transforms.py tests/integration/test_disable_user_age18_pipeline.py
```

## Known Gaps

- The exact SQL used by `$$M_SRC_SQL1`/`$$M_SRC_SQL2` is not available. Update `configs/jobs/disable_user_age18.yaml` with production-grade SQL or table references once the legacy queries are exported.
- API response parsing assumes JSON bodies with `systemMessage`/`itemMessage` keys. Adjust `DisableApiClient` if the disable API returns a different payload.
- `mask_pii_stub` currently hashes PII columns but still retains raw values. Harden masking (see TODO) before exposing the audit tables to downstream consumers.
