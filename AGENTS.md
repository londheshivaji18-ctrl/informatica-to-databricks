# AGENTS.md
> AI Agent Guide for Assessment & Migration Architecture Phase

---

## 0) Context

- **Objective:** Assess legacy Informatica ETL pipelines and propose target migration architectures on Azure Databricks.
- **Focus Areas:**  
  1. Architecture assessment of current pipelines.  
  2. Identifying architectural **patterns** (good practices to retain) and **antipatterns** (issues to eliminate).  
  3. Exploring trade-offs (cost, performance, maintainability, security).  
  4. Producing migration strategy documents (docs/architecture/*.md).

---

## 1) Agents & Responsibilities

### A1. Legacy Analyst
- **Purpose:** Summarize existing Informatica pipelines, data flows, and dependencies.
- **Inputs:** Exported mappings, SQL, lineage diagrams, metadata.
- **Outputs:** `docs/assessment/[pipeline]_current.md` (current-state diagrams & notes).

### A2. Pattern Detector
- **Purpose:** Highlight repeatable design **patterns** (e.g., SCD Type 2, star-schema loads, reusable lookup tables).
- **Outputs:** `docs/assessment/patterns.md` (catalog of reusable migration patterns).

### A3. Antipattern Finder
- **Purpose:** Flag issues in legacy design (tight coupling, excessive staging, hard-coded paths, no error handling).
- **Outputs:** `docs/assessment/antipatterns.md` (list with remediation suggestions).

### A4. Tradeoff Analyst
- **Purpose:** Propose architectural options (e.g., Spark SQL vs DataFrame API, Delta Lake vs Parquet staging, ADF vs native scheduling).
- **Outputs:** `docs/architecture/tradeoffs.md` with a **pros/cons matrix** for each decision.

### A5. Migration Architect
- **Purpose:** Draft target-state reference architectures for Databricks.
- **Outputs:** `docs/architecture/target-[domain].md` diagrams + notes.

---

## 2) Guardrails

- **Data Privacy:** Never expose real production data. Use schemas or masked examples only.
- **Neutral Assessment:** Describe patterns and issues factually; defer recommendations to tradeoff analysis.
- **Evidence-based:** Link every architectural recommendation to a source (performance metric, cost model, Databricks best practice).
- **Consistency:** Use common template for assessment docs: **Overview → Flows → Patterns → Issues → Recommendations**.

---

## 3) Workflows

### W1. Assessment Report
**Prompt template:**
- *“Analyze `legacy/mappings/[pipeline].xml`. Summarize sources, targets, and main transformations. Identify risks.”*

### W2. Pattern & Antipattern Extraction
**Prompt template:**
- *“From assessment of `orders_pipeline`, list architectural patterns (reusable joins, lookups) and antipatterns (tight coupling, hard-coded values).”*

### W3. Tradeoff Analysis
**Prompt template:**
- *“Compare option A (Spark SQL) vs option B (DataFrame API) for [pipeline step]. List pros, cons, cost/performance implications.”*

### W4. Target Architecture Draft
**Prompt template:**
- *“Draft high-level architecture for migrating [domain] pipeline to Databricks. Include ingestion, transformations, storage, orchestration. Save as `docs/architecture/[pipeline].md`.”*

---

## 4) Output Conventions

- **Docs:** Markdown, with diagrams in PlantUML or Mermaid where possible.
- **Tables:** Use for tradeoffs (criteria, option A, option B, recommendation).
- **Tags:** Add `#pattern`, `#antipattern`, `#tradeoff` to sections for quick search.

---

## 5) Example Deliverables

- `docs/assessment/customers_current.md` → Current-state pipeline notes.
- `docs/assessment/patterns.md` → e.g., *Reusable dimension load pattern*.
- `docs/assessment/antipatterns.md` → e.g., *Multiple staging layers causing latency*.
- `docs/architecture/tradeoffs.md` → Spark SQL vs DataFrame API matrix.
- `docs/architecture/target-customers.md` → Target-state architecture with Delta tables.

---

## 6) Ownership

- **Lead Architect:** @[you/role]
- **Supporting SMEs:** [team handles]
- **Reviewers:** Data Engineering Review Board
