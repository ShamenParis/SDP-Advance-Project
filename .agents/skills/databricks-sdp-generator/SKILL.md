---
name: databricks-sdp-generator
description: >
  Generates a complete Databricks Structured Data Pipeline (SDP) project using
  the Bronze/Silver/Gold medallion architecture. Interviews the user across 5
  phases to collect all configuration, then produces:
    1. A ready-to-paste Genie Code instruction file for use inside Databricks workspace
    2. A sdp_config.json master config (user-maintained)
    3. All SDP source files with the correct ETL pipeline folder structure
  Supports file ingestion via Auto Loader (cloudFiles) and Lakeflow Connect.
---

# Databricks SDP Generator Skill

## How This Skill Works

This skill generates everything you need to **create and run an SDP pipeline inside Databricks**. It is designed for two environments:

| Environment | How to use |
|---|---|
| **Databricks Genie Code** (inside workspace) | Paste the generated `genie_instructions.md` into workspace-level custom instructions. Then chat with Genie Code to scaffold and generate each file directly in the workspace. |
| **Antigravity / Local Agent** | Run this skill directly — it interviews the user and generates all SDP files. |

### What is Genie Code?
**Genie Code** is the AI coding assistant built into the Databricks workspace. It is guided by workspace-level **custom instruction files** — markdown documents that tell Genie how your project is structured and what patterns to follow.

This skill generates those instruction files, pre-populated with your project's config, so Genie Code knows exactly how to generate each SDP layer inside the workspace.

> This skill focuses on **creating the SDP pipeline files**. Deployment, DAB bundles, and CI/CD are out of scope.

---

## Prerequisites

Read these reference files before starting:

1. [`references/databricks_sdp_patterns.md`](references/databricks_sdp_patterns.md) — All code generation patterns
2. [`references/lakeflow_connect.md`](references/lakeflow_connect.md) — Ingestion source guidance
3. [`resources/sdp_config.schema.json`](resources/sdp_config.schema.json) — Master config schema
4. [`resources/config_json.schema.json`](resources/config_json.schema.json) — Bronze config schema
5. [`examples/sdp_config.example.json`](examples/sdp_config.example.json) — Generic reference example (no real values)

> **IMPORTANT**: Never put real catalog names, workspace URLs, email addresses, or
> volume paths in skill files, examples, or reference docs. Always use `<placeholder>` notation.

---

## Role

You are an expert Databricks data engineer. Your job is to:
1. **Interview** the user through 5 structured phases
2. **Build** a complete `sdp_config.json` from their answers
3. **Generate** all SDP source files with the correct ETL pipeline folder structure
4. **Produce** a `genie_instructions.md` — ready to paste into Databricks Genie Code

Be conversational but efficient. After each phase, **summarise** what you've captured and confirm before moving on.

---

## Phase 0 — Welcome & Orientation

Start with this message (adapt naturally):

> **Databricks SDP Generator**
>
> I'll guide you through building a complete Bronze → Silver → Gold SDP pipeline.
> We'll go through **5 short phases** — I'll ask questions, you provide the details,
> and I'll generate all the SDP code files plus a ready-to-use Genie Code instruction file.
>
> **Quick note on data ingestion:**
> - **Files** (CSV, JSON, Parquet, etc.) in a Volume or External Location
>   → I'll generate Auto Loader Bronze ingestion code.
> - **Connected systems** (Salesforce, databases, Kafka, etc.) via **Lakeflow Connect**
>   → Bronze is managed by the connector; your SDP starts at Silver.
>
> See: https://docs.databricks.com/aws/en/ingestion/overview
>
> Ready? Let's start with Phase 1.

---

## Phase 1 — Ingestion Source

```
📥 Phase 1 of 5 — Ingestion Source

What is your data source type?
  a) Files — CSV, JSON, Parquet etc. in a Databricks Volume or External Location
  b) Lakeflow Connect — SaaS apps, databases, Kafka, etc. (connector-managed Bronze)

If (a), I'll need details for each source table.
If (b), I'll need just the raw table names that the connector will populate.
```

### If source_type = "files"
For **each source table**, collect:
- Short table name (e.g. `orders`)
- Full raw table name (`<catalog>.<raw_schema>.<table_name>`)
- Schema string in Spark DDL format (e.g. `order_id STRING, amount STRING, insert_date TIMESTAMP`)
  - **Always remind**: `insert_date TIMESTAMP` must be the last column — added automatically at ingest time
- Source path (Volume path or external location URI)
- File format (`csv`, `json`, `parquet`, `avro`, `orc`)
- For CSV: header (`true`/`false`), delimiter character

Ask if there are more tables. Loop until done.

### If source_type = "lakeflow_connect"
For **each source table**, collect:
- Short table name
- Full raw table name (where the connector writes to)

Set `"tables": []` in the ingestion section (connector manages it).

---

## Phase 2 — Project Identity

```
🏗️ Phase 2 of 5 — Project Identity

1. Project name? (snake_case, e.g. retail_sdp)
2. Unity Catalog name? (e.g. my_company_catalog)
3. Raw (Bronze) schema name? (default: raw)
4. Clean (Silver) schema name? (default: clean)
5. Gold schema name? (default: datawarehouse)
```

After collecting, show a summary and confirm:

```
📋 Project Summary:
  Name:       <project_name>
  Catalog:    <catalog>
  Schemas:    raw=<raw> | clean=<clean> | gold=<gold>

Is this correct? (yes / correct anything)
```

---

## Phase 3 — Clean Rules (Silver Layer)

For **each source table** from Phase 1, collect clean rules.

```
🧹 Phase 3 of 5 — Clean Rules (Silver Layer)

For each table, I need to know:
  1. Primary key column(s) — used for SCD2 deduplication
  2. SCD Type:
       • Type 2 — slowly-changing dimensions (keeps full history)
       • Type 1 / Streaming — append-only (facts and events)
  3. Data quality expectations:
       • expect_or_fail   → BLOCKS pipeline (critical NOT NULL checks)
       • expect           → WARNS only (logged, pipeline continues)
       • expect_or_drop   → DROPS bad rows silently
       • expect_all       → batch WARN rules (dict form)
       • expect_all_or_drop → batch DROP rules (dict form)
  4. Type casts — columns needing conversion from STRING
     (e.g. amount → decimal(10,2), quantity → int)
```

For each table, show collected rules as a formatted block and confirm before moving on.

### SCD type guidance:
- **SCD Type 2**: customers, products, employees, warehouses, categories
  (slowly-changing dimensional data — history matters)
- **SCD Type 1 + streaming**: orders, sales, events, transactions
  (high-volume append-only facts)

### Expectation rules reference:

| Rule Type | When to use | Generated code |
|---|---|---|
| `expect_or_fail` | Critical keys that must NEVER be null | `@dp.expect_or_fail("name", "col IS NOT NULL")` |
| `expect` | Soft quality checks, auditing | `@dp.expect("name", "col IS NOT NULL")` |
| `expect_or_drop` | Optional enrichment columns | `@dp.expect_or_drop("name", "col IS NOT NULL")` |
| `expect_all` | Multiple warn rules in one dict | `@dp.expect_all(rules_dict)` |
| `expect_all_or_drop` | Multiple drop rules in one dict | `@dp.expect_all_or_drop(drop_dict)` |

---

## Phase 4 — Star Schema Mapping (Gold Layer)

```
⭐ Phase 4 of 5 — Star Schema (Gold Layer)

DIMENSIONS — one entry per dimension table:
  1. Which clean table does it read from?
  2. Is the source SCD Type 2? (yes → I'll add __END_AT IS NULL filter)
  3. What columns should the dimension expose?
  4. Natural key (business key) and surrogate key name?
  5. Does it join to any other dimension for enrichment?
     (e.g. products → product_category)

FACTS — one entry per fact table:
  1. Which clean table is the fact grain?
  2. Natural key and measure columns?
  3. Which dimensions does it join to?
     For each join:
       • Dimension gold table
       • FK column in the fact (or bridge table if FK is missing from fact)
       • Natural key in the dimension
       • Surrogate key to bring in
```

After collecting, produce a visual mapping summary:

```
Dimensions:
  ✅ dim_customer        ← clean.<customers_table> (SCD2)
  ✅ dim_warehouse       ← clean.<warehouse_table> (SCD2)
  ✅ dim_product_category← raw.<product_category_table>
  ✅ dim_product         ← clean.<products_scd_table> (SCD2) → joins dim_product_category

Facts:
  ✅ fct_orders  ← clean.<orders_table> → dim_customer, dim_warehouse, dim_product
  ✅ fct_sales   ← clean.<sales_table>  → dim_customer (via orders), ...

Confirm? (yes / adjust)
```

---

## Phase 5 — Confirm & Generate

```
🚀 Phase 5 of 5 — Generate SDP Files

Here's what I'm about to generate:

  <project_name>/
  ├── sdp_config.json                   ← YOUR master config (maintain this)
  └── src/
      ├── 01_bronze/
      │   └── datalake_files/
      │       ├── config.json           ← derived from sdp_config.json
      │       └── raw_ingestion_batch.py
      ├── 02_silver/
      │   └── datalake_clean.py
      └── 03_gold/
          ├── dimensions.py
          └── facts.py

ALSO generating:
  genie_instructions.md                 ← Paste into Databricks Genie Code

Ready to generate? (yes)
```

Once confirmed, generate ALL files **and** the `genie_instructions.md`.

---

## Generation Rules

### File: `sdp_config.json`

Build from all collected interview data. Validate mentally against
`resources/sdp_config.schema.json` before writing.
This is the **user-maintained** master config.

> **NEVER** put the user's real values into skill examples, pattern references,
> or any committed skill file. The `sdp_config.json` is generated INTO the user's
> project folder — that's the only place real values live.

### File: `src/01_bronze/datalake_files/config.json`

Derive from `sdp_config.ingestion.tables`. Only generate when `source_type == "files"`.
Path is always `src/01_bronze/datalake_files/config.json` — not directly in `01_bronze/`.

When `source_type == "lakeflow_connect"`:
- Skip `config.json` and `raw_ingestion_batch.py`
- Create `src/01_bronze/README.md` explaining that Bronze is managed by the Lakeflow connector

### File: `src/01_bronze/datalake_files/raw_ingestion_batch.py`

Use the **exact pattern** from `references/databricks_sdp_patterns.md § Bronze Layer`.
Do not deviate. Place in `src/01_bronze/datalake_files/`.

### File: `src/02_silver/datalake_clean.py`

For each table in `sdp_config.clean.tables`:

1. **Check `scd_type` and `streaming` flags**:
   - `scd_type == "2"` → Pattern A (SCD2 snapshot)
   - `scd_type == "1"` and `streaming == true` → Pattern B (streaming append)

2. **Imports** at the top:
   ```python
   from pyspark import pipelines as dp
   from pyspark.sql import functions as F
   from pyspark.sql.window import Window
   ```

3. **Section header comment** per table:
   ```python
   # ============================================================
   # <TableName> Clean (<SCD Type 2 / Streaming Append>)
   # ============================================================
   ```

4. **Cast columns**: `.withColumn("<col>", F.col("<col>").cast("<type>"))` chains.

5. **Expectation decorators**: apply in order per the rules table in Phase 3.

6. **`clean_table_suffix`**: if set, append to clean table name
   (e.g. `products` + `_scd` → `<catalog>.clean.products_scd`).

### File: `src/03_gold/dimensions.py`

For each dimension in `sdp_config.gold.dimensions`:

1. Imports:
   ```python
   import pyspark.pipelines as dp
   import pyspark.sql.functions as F
   from pyspark.sql.window import Window
   ```

2. Follow pattern from `references/databricks_sdp_patterns.md § Gold Layer — Dimension Pattern`.

3. **`is_scd2_source == true`** → add `.filter(F.col("__END_AT").isNull())`.

4. **`dim_joins`** → generate the aliased join chain before final `.select()`.
   Joined columns use `F.coalesce(F.col("<alias>.<col>"), F.lit(-1/"Unknown"))`.

5. **Column ordering in `select()`**:
   - surrogate key first
   - natural key second
   - all other columns
   - `F.col("insert_date").alias("source_insert_date")`
   - `F.current_timestamp().alias("load_date")`

6. Add `# NOTE: Define <parent_dim> BEFORE <child_dim>` above dims consumed by other dims.

### File: `src/03_gold/facts.py`

For each fact in `sdp_config.gold.facts`:

1. Same imports as dimensions.

2. Follow pattern from `references/databricks_sdp_patterns.md § Gold Layer — Fact Pattern`.

3. **Bridge tables**: when `bridge_table` is defined on a dimension join:
   ```python
   bridge = dp.read("<bridge_table>").select(
       "<bridge_fact_key>", "<all unique bridge_dim_key columns>"
   ).alias("b")
   ```
   Then: fact → bridge, bridge → each dim.

4. All surrogate keys: `F.coalesce(F.col("<alias>.<sk>"), F.lit(-1)).alias("<sk>")`.

5. **Select column order**: natural key → surrogate keys → measures → `load_date`.

### File: `genie_instructions.md`

Generate a Genie Code custom instruction file that:

```markdown
# SDP Generator Instructions — <project_name>

## Project Configuration
Catalog: <catalog>
Schemas: raw=<raw> | clean=<clean> | gold=<gold>

## Source Tables
<list each ingestion table with schema>

## SDP File Structure
When generating code for this project, always use this exact structure:

  <project_name>/
  ├── sdp_config.json
  └── src/
      ├── 01_bronze/
      │   └── datalake_files/
      │       ├── config.json
      │       └── raw_ingestion_batch.py
      ├── 02_silver/
      │   └── datalake_clean.py
      └── 03_gold/
          ├── dimensions.py
          └── facts.py

## Bronze Layer Rules
- Use Auto Loader (cloudFiles) format
- Always add insert_date = current_timestamp()
- Always drop _rescued_data
- Read config from src/01_bronze/datalake_files/config.json

## Silver Layer Rules (datalake_clean.py)
- SCD Type 2 tables: use dp.read() + Window dedup + create_auto_cdc_from_snapshot_flow()
- Streaming tables: use dp.read_stream() + @dp.table(cluster_by_auto=True)
- Apply expectations as decorators on the view/function
- Cast numeric columns from STRING using .cast()

## Gold Layer Rules
- Dimensions: use @dp.materialized_view, filter __END_AT IS NULL for SCD2 sources
- Facts: use @dp.materialized_view, left joins only, F.coalesce(key, -1) for all SKs
- Always rename insert_date → source_insert_date, add load_date = current_timestamp()

## sdp_config.json
The sdp_config.json contains the full configuration for this pipeline.
When asked to generate or modify any layer, read sdp_config.json first.

<embed sdp_config.json contents here>
```

> After generating `genie_instructions.md`, tell the user:
> "Copy the contents of `genie_instructions.md` and paste it into your
> Databricks workspace Genie Code custom instructions.
> You can find this at: Workspace Settings → Genie Code → Custom Instructions."

---

## Post-Generation Message

After all files are created:

```
✅ SDP Files Generated!

📁 Project: <project_name>/

What to do next:
1. Review sdp_config.json — this is YOUR master config. Update it whenever
   your schema changes, then re-run this skill to regenerate the SDP files.

2. Set up Genie Code in Databricks:
   • Open Databricks workspace → Genie Code → Custom Instructions
   • Paste the contents of genie_instructions.md
   • Genie Code will now understand your project structure and generate
     correct SDP code when you chat with it inside the workspace

3. Upload your SDP files into the Databricks workspace:
   • Create a folder in your Databricks workspace for this project
   • Upload src/01_bronze/datalake_files/, src/02_silver/, src/03_gold/
   • Keep sdp_config.json alongside your source files for reference

4. Set up your data source:
   • Files → ensure your Volume or External Location exists and data is landing
   • Lakeflow Connect → configure your connector in Databricks UI first

5. Create and run the SDP pipeline in Databricks:
   • Go to Workflows → Pipelines → Create Pipeline
   • Add your source files as pipeline libraries
   • Set catalog, schema, and run the pipeline
```

---

## Regeneration Mode

If the user says **"regenerate"** and provides an existing `sdp_config.json`,
skip Phases 1–4. Read the config directly and proceed to Phase 5 (confirmation),
then regenerate all SDP source files and a fresh `genie_instructions.md`.

---

## Quality Checklist (run before finishing)

### Generated Files
- [ ] `sdp_config.json` passes mental schema validation
- [ ] `config.json` table count matches ingestion tables
- [ ] Every `clean` table has a corresponding source in `ingestion.tables`
- [ ] Every `gold.dimension` has a matching `source_clean_table` in `clean.tables`
- [ ] Every `gold.fact` joins reference dimensions defined in `gold.dimensions`
- [ ] No placeholder strings like `<...>` remain in any **generated project file**
  (placeholders are only for skill examples and reference docs — never generated code)

### File Locations
- [ ] Bronze files are at `src/01_bronze/datalake_files/` — NOT directly in `01_bronze/`
- [ ] Silver file is `src/02_silver/datalake_clean.py`
- [ ] Gold files are `src/03_gold/dimensions.py` and `src/03_gold/facts.py`

### Code Correctness
- [ ] Silver: SCD2 tables use `dp.read()` (batch), streaming tables use `dp.read_stream()`
- [ ] Gold: All facts use `F.coalesce(..., F.lit(-1))` for surrogate keys
- [ ] Gold: All dims include `source_insert_date` and `load_date`

### Genie Code
- [ ] `genie_instructions.md` is generated and includes embedded `sdp_config.json`

### Security / PII
- [ ] No real catalog names, emails, workspace URLs in skill files or examples
