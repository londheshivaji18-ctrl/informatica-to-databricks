# Workflow Migration Plan
Generated from analyzer/summary reports. Tags: #pattern #antipattern #tradeoff

## Categorization Summary
- Category legend: Mostly Automatable; Hybrid (some manual redesign); Manual-leaning (significant redesign)
- wf_GBS_CONTROL_ACCOUNTING_One_Time_Extraction (MetLife_p2_CSC_GroupLife) ? Hybrid (some manual redesign) [Unconnected Lookup=0, Java=0, XML=0, DynamicSQL=3]
- wf_STARTRAK_Member_Conditions_Delta_Data_Extraction (MetLife_p2_CSC_GroupLife) ? Hybrid (some manual redesign) [Unconnected Lookup=33, Java=0, XML=0, DynamicSQL=12]
- wf_STARTRAK_Member_Diary_Delta_Data_Extraction (MetLife_p2_CSC_GroupLife) ? Hybrid (some manual redesign) [Unconnected Lookup=33, Java=0, XML=0, DynamicSQL=12]
- wkf_load_ods_1203 (MetLife_p2_IB_OPAS_UDX) ? Manual-leaning (significant redesign) [Unconnected Lookup=66, Java=23, XML=0, DynamicSQL=39]
- wkf_m_load_all_accord (MetLife_p2_IB_OPAS_UDX) ? Hybrid (some manual redesign) [Unconnected Lookup=64, Java=0, XML=0, DynamicSQL=141]
- wkf_s_m_load_T_SCHED_VAL_CVR_EX (MetLife_p2_IB_OPAS_UDX) ? Hybrid (some manual redesign) [Unconnected Lookup=33, Java=0, XML=0, DynamicSQL=3]
- wf_m_LENOX_GVUL_ACCORD_XML (MetLife_p2_RAD_APP11538_GVWB_LNOX) ? Hybrid (some manual redesign) [Unconnected Lookup=0, Java=0, XML=17, DynamicSQL=3]
- wf_m_LENOX_IDI_FTP (MetLife_p2_RAD_APP11538_GVWB_LNOX) ? Hybrid (some manual redesign) [Unconnected Lookup=0, Java=0, XML=7, DynamicSQL=0]
- wf_m_LENOX_token_creation_for_getplatform (MetLife_p2_RAD_APP11538_GVWB_LNOX) ? Hybrid (some manual redesign) [Unconnected Lookup=0, Java=0, XML=0, DynamicSQL=3]

## wf_GBS_CONTROL_ACCOUNTING_One_Time_Extraction (MetLife_p2_CSC_GroupLife)
### Overview
- Task Types: Assignment, Session
- Sessions/Maps: 2 ? m_GBS_CONTROL_ACCOUNTING_One_Time_Extraction, m_Transfer_SOR_MF_HDFS_TRIGGER
### Patterns #pattern
- Mostly automatable: Expressions: 4
### Antipatterns #antipattern
- Dynamic SQL in SQ (maps: m_Transfer_SOR_MF_HDFS_TRIGGER)
### Approach
- Ingestion: land sources to ADLS (Auto Loader for files; JDBC for RDBMS) and register Delta bronze tables.
- Transform: reimplement mappings in PySpark DataFrames or Spark SQL; modularize by business domains.
- Dynamic SQL: use parameterized Spark SQL with validated templates; avoid string concatenation.
- Parameterization: manage with Databricks Jobs params, widgets, and secret scopes; maintain per-env config files.
- Storage: write curated datasets as Delta; partition by date/keys; enforce expectations (DQ checks).
- Orchestration: Databricks Workflows/ADF; implement retries, SLAs, lineage capture.
### Performance #tradeoff
- Optimize Delta (OPTIMIZE/ZORDER); set Auto Optimize and compaction for small files.

## wf_STARTRAK_Member_Conditions_Delta_Data_Extraction (MetLife_p2_CSC_GroupLife)
### Overview
- Task Types: Assignment, Session
- Sessions/Maps: 3 ? m_STARTRAK_Member_Conditions_Active_One_Time_Extraction, m_STARTRAK_Member_Conditions_Delta_Data_Extraction, m_Transfer_SOR_MF_HDFS_TRIGGER
### Patterns #pattern
- Mostly automatable: Expressions: 12, Joins: 13, Sorters: 16 (review necessity)
### Antipatterns #antipattern
- Unconnected Lookups: 33; Dynamic SQL in SQ (maps: m_STARTRAK_Member_Conditions_Active_One_Time_Extraction, m_STARTRAK_Member_Conditions_Delta_Data_Extraction, m_Transfer_SOR_MF_HDFS_TRIGGER)
### Approach
- Ingestion: land sources to ADLS (Auto Loader for files; JDBC for RDBMS) and register Delta bronze tables.
- Transform: reimplement mappings in PySpark DataFrames or Spark SQL; modularize by business domains.
- Unconnected lookups: refactor to reusable join helpers or precomputed reference tables; avoid per-row UDF calls.
- Sorters: remove redundant sorts; use orderBy only when determinism is required.
- Dynamic SQL: use parameterized Spark SQL with validated templates; avoid string concatenation.
- Parameterization: manage with Databricks Jobs params, widgets, and secret scopes; maintain per-env config files.
- Storage: write curated datasets as Delta; partition by date/keys; enforce expectations (DQ checks).
- Orchestration: Databricks Workflows/ADF; implement retries, SLAs, lineage capture.
### Performance #tradeoff
- Joins: enable AQE, broadcast hints; check skew and salt where needed.
- Avoid full dataset sorts; leverage partitioning and window order.
- Optimize Delta (OPTIMIZE/ZORDER); set Auto Optimize and compaction for small files.

## wf_STARTRAK_Member_Diary_Delta_Data_Extraction (MetLife_p2_CSC_GroupLife)
### Overview
- Task Types: Assignment, Session
- Sessions/Maps: 3 ? m_STARTRAK_Member_Diary_Active_One_Time_Extraction, m_STARTRAK_Member_Diary_Delta_Data_Extraction, m_Transfer_SOR_MF_HDFS_TRIGGER
### Patterns #pattern
- Mostly automatable: Expressions: 10, Joins: 13, Sorters: 16 (review necessity)
### Antipatterns #antipattern
- Unconnected Lookups: 33; Dynamic SQL in SQ (maps: m_STARTRAK_Member_Diary_Active_One_Time_Extraction, m_STARTRAK_Member_Diary_Delta_Data_Extraction, m_Transfer_SOR_MF_HDFS_TRIGGER)
### Approach
- Ingestion: land sources to ADLS (Auto Loader for files; JDBC for RDBMS) and register Delta bronze tables.
- Transform: reimplement mappings in PySpark DataFrames or Spark SQL; modularize by business domains.
- Unconnected lookups: refactor to reusable join helpers or precomputed reference tables; avoid per-row UDF calls.
- Sorters: remove redundant sorts; use orderBy only when determinism is required.
- Dynamic SQL: use parameterized Spark SQL with validated templates; avoid string concatenation.
- Parameterization: manage with Databricks Jobs params, widgets, and secret scopes; maintain per-env config files.
- Storage: write curated datasets as Delta; partition by date/keys; enforce expectations (DQ checks).
- Orchestration: Databricks Workflows/ADF; implement retries, SLAs, lineage capture.
### Performance #tradeoff
- Joins: enable AQE, broadcast hints; check skew and salt where needed.
- Avoid full dataset sorts; leverage partitioning and window order.
- Optimize Delta (OPTIMIZE/ZORDER); set Auto Optimize and compaction for small files.

## wkf_load_ods_1203 (MetLife_p2_IB_OPAS_UDX)
### Overview
- Task Types: Session
- Sessions/Maps: 5 ? m_delete_ods_acord_1203, m_load_ods_acord_1203, m_load_opas_input, m_load_opas_output, m_update_opas_1203
### Patterns #pattern
- Mostly automatable: Expressions: 24, Joins: 13, Sorters: 24 (review necessity)
### Antipatterns #antipattern
- Unconnected Lookups: 66; Connected Lookups: 66 (optimize via broadcast, caching); Java transformations: 23; Dynamic SQL in SQ (maps: m_delete_ods_acord_1203, m_load_ods_acord_1203, m_load_opas_input, m_load_opas_output, m_update_opas_1203)
### Approach
- Ingestion: land sources to ADLS (Auto Loader for files; JDBC for RDBMS) and register Delta bronze tables.
- Transform: reimplement mappings in PySpark DataFrames or Spark SQL; modularize by business domains.
- Lookups: replace with joins; broadcast small dims; maintain surrogate keys via window functions.
- Unconnected lookups: refactor to reusable join helpers or precomputed reference tables; avoid per-row UDF calls.
- Sorters: remove redundant sorts; use orderBy only when determinism is required.
- Java transforms: port to PySpark/Scala UDFs or external services; validate performance and vectorize.
- Dynamic SQL: use parameterized Spark SQL with validated templates; avoid string concatenation.
- Parameterization: manage with Databricks Jobs params, widgets, and secret scopes; maintain per-env config files.
- Storage: write curated datasets as Delta; partition by date/keys; enforce expectations (DQ checks).
- Orchestration: Databricks Workflows/ADF; implement retries, SLAs, lineage capture.
### Performance #tradeoff
- Joins: enable AQE, broadcast hints; check skew and salt where needed.
- Avoid full dataset sorts; leverage partitioning and window order.
- Optimize Delta (OPTIMIZE/ZORDER); set Auto Optimize and compaction for small files.

## wkf_m_load_all_accord (MetLife_p2_IB_OPAS_UDX)
### Overview
- Task Types: Session
- Sessions/Maps: 3 ? m_T_INS_POL_TRAN_Rejected_Records, m_load_ods_all_accord, m_update_opas_all_accord
### Patterns #pattern
- Mostly automatable: Expressions: 172, Joins: 13, Sorters: 72 (review necessity)
### Antipatterns #antipattern
- Unconnected Lookups: 64; Connected Lookups: 165 (optimize via broadcast, caching); Dynamic SQL in SQ (maps: m_T_INS_POL_TRAN_Rejected_Records, m_load_ods_all_accord, m_update_opas_all_accord)
### Approach
- Ingestion: land sources to ADLS (Auto Loader for files; JDBC for RDBMS) and register Delta bronze tables.
- Transform: reimplement mappings in PySpark DataFrames or Spark SQL; modularize by business domains.
- Lookups: replace with joins; broadcast small dims; maintain surrogate keys via window functions.
- Unconnected lookups: refactor to reusable join helpers or precomputed reference tables; avoid per-row UDF calls.
- Sorters: remove redundant sorts; use orderBy only when determinism is required.
- Dynamic SQL: use parameterized Spark SQL with validated templates; avoid string concatenation.
- Parameterization: manage with Databricks Jobs params, widgets, and secret scopes; maintain per-env config files.
- Storage: write curated datasets as Delta; partition by date/keys; enforce expectations (DQ checks).
- Orchestration: Databricks Workflows/ADF; implement retries, SLAs, lineage capture.
### Performance #tradeoff
- Joins: enable AQE, broadcast hints; check skew and salt where needed.
- Avoid full dataset sorts; leverage partitioning and window order.
- Optimize Delta (OPTIMIZE/ZORDER); set Auto Optimize and compaction for small files.

## wkf_s_m_load_T_SCHED_VAL_CVR_EX (MetLife_p2_IB_OPAS_UDX)
### Overview
- Task Types: Session
- Sessions/Maps: 1 ? m_load_T_SCHED_VAL_CVR_EX_TBL_Load_Error_Report
### Patterns #pattern
- Mostly automatable: Expressions: 8, Sorters: 8 (review necessity)
### Antipatterns #antipattern
- Unconnected Lookups: 33; Dynamic SQL in SQ (maps: m_load_T_SCHED_VAL_CVR_EX_TBL_Load_Error_Report)
### Approach
- Ingestion: land sources to ADLS (Auto Loader for files; JDBC for RDBMS) and register Delta bronze tables.
- Transform: reimplement mappings in PySpark DataFrames or Spark SQL; modularize by business domains.
- Unconnected lookups: refactor to reusable join helpers or precomputed reference tables; avoid per-row UDF calls.
- Sorters: remove redundant sorts; use orderBy only when determinism is required.
- Dynamic SQL: use parameterized Spark SQL with validated templates; avoid string concatenation.
- Parameterization: manage with Databricks Jobs params, widgets, and secret scopes; maintain per-env config files.
- Storage: write curated datasets as Delta; partition by date/keys; enforce expectations (DQ checks).
- Orchestration: Databricks Workflows/ADF; implement retries, SLAs, lineage capture.
### Performance #tradeoff
- Avoid full dataset sorts; leverage partitioning and window order.
- Optimize Delta (OPTIMIZE/ZORDER); set Auto Optimize and compaction for small files.

## wf_m_LENOX_GVUL_ACCORD_XML (MetLife_p2_RAD_APP11538_GVWB_LNOX)
### Overview
- Task Types: Session
- Sessions/Maps: 1 ? m_LENOX_GVUL_ACCORD_XML
### Patterns #pattern
- Mostly automatable: Expressions: 4
### Antipatterns #antipattern
- XML processing refs: 17; Dynamic SQL in SQ (maps: m_LENOX_GVUL_ACCORD_XML)
### Approach
- Ingestion: land sources to ADLS (Auto Loader for files; JDBC for RDBMS) and register Delta bronze tables.
- Transform: reimplement mappings in PySpark DataFrames or Spark SQL; modularize by business domains.
- XML: ingest via spark-xml with explicit schema; avoid row-wise parsing; flatten to normalized columns.
- Dynamic SQL: use parameterized Spark SQL with validated templates; avoid string concatenation.
- Parameterization: manage with Databricks Jobs params, widgets, and secret scopes; maintain per-env config files.
- Storage: write curated datasets as Delta; partition by date/keys; enforce expectations (DQ checks).
- Orchestration: Databricks Workflows/ADF; implement retries, SLAs, lineage capture.
### Performance #tradeoff
- Optimize Delta (OPTIMIZE/ZORDER); set Auto Optimize and compaction for small files.

## wf_m_LENOX_IDI_FTP (MetLife_p2_RAD_APP11538_GVWB_LNOX)
### Overview
- Task Types: Session
- Sessions/Maps: 1 ? m_LENOX_IDI_FTP
### Patterns #pattern
- Mostly automatable: General projections/filters/aggregations
### Antipatterns #antipattern
- XML processing refs: 7
### Approach
- Ingestion: land sources to ADLS (Auto Loader for files; JDBC for RDBMS) and register Delta bronze tables.
- Transform: reimplement mappings in PySpark DataFrames or Spark SQL; modularize by business domains.
- XML: ingest via spark-xml with explicit schema; avoid row-wise parsing; flatten to normalized columns.
- Storage: write curated datasets as Delta; partition by date/keys; enforce expectations (DQ checks).
- Orchestration: Databricks Workflows/ADF; implement retries, SLAs, lineage capture.
### Performance #tradeoff
- Optimize Delta (OPTIMIZE/ZORDER); set Auto Optimize and compaction for small files.

## wf_m_LENOX_token_creation_for_getplatform (MetLife_p2_RAD_APP11538_GVWB_LNOX)
### Overview
- Task Types: Session
- Sessions/Maps: 1 ? m_LENOX_token_creation_for_getplatform
### Patterns #pattern
- Mostly automatable: Expressions: 4
### Antipatterns #antipattern
- Dynamic SQL in SQ (maps: m_LENOX_token_creation_for_getplatform)
### Approach
- Ingestion: land sources to ADLS (Auto Loader for files; JDBC for RDBMS) and register Delta bronze tables.
- Transform: reimplement mappings in PySpark DataFrames or Spark SQL; modularize by business domains.
- Dynamic SQL: use parameterized Spark SQL with validated templates; avoid string concatenation.
- Parameterization: manage with Databricks Jobs params, widgets, and secret scopes; maintain per-env config files.
- Storage: write curated datasets as Delta; partition by date/keys; enforce expectations (DQ checks).
- Orchestration: Databricks Workflows/ADF; implement retries, SLAs, lineage capture.
### Performance #tradeoff
- Optimize Delta (OPTIMIZE/ZORDER); set Auto Optimize and compaction for small files.