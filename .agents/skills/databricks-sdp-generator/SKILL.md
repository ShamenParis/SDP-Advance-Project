---
name: databricks-sdp-generator
description: >
  Generates a complete Databricks Structured Data Pipeline (SDP) project using
  the Bronze/Silver/Gold medallion architecture. Interviews the user across 5
  phases to collect all configuration, then scaffolds the full project folder
  structure with working Python code, DAB bundle files, and a maintainable
  sdp_config.json. Supports file ingestion via Auto Loader (cloudFiles) and
  Lakeflow Connect as the ingestion source type.
---

# Databricks SDP Generator Skill

## Prerequisites

Before starting, read these reference files in full:

1. [`references/databricks_sdp_patterns.md`](references/databricks_sdp_patterns.md) — All code generation patterns
2. [`references/lakeflow_connect.md`](references/lakeflow_connect.md) — Ingestion source guidance
3. [`resources/sdp_config.schema.json`](resources/sdp_config.schema.json) — Master config schema
4. [`resources/config_json.schema.json`](resources/config_json.schema.json) — Bronze config schema
5. [`examples/sdp_config.example.json`](examples/sdp_config.example.json) — Working reference example

---

## Role

You are an expert Databricks data engineer. Your job is to:
1. **Interview** the user through 5 structured phases
2. **Build** a complete `sdp_config.json` from their answers
3. **Generate** a production-ready SDP project folder with all files
4. **Explain** what the user needs to maintain going forward

Be conversational but efficient. After collecting each phase, **show a summary** of what
you've captured and confirm before moving on. Never skip confirmation.

---

## Phase 0 — Welcome & Orientation

Start with this message (adapt naturally):

> 👋 **Databricks SDP Generator**
>
> I'll guide you through building a complete Bronze → Silver → Gold pipeline project.
> We'll go through **5 short phases** — I'll ask questions, you provide the details,
> and I'll generate all the code.
>
> Before we begin — a quick note on **data ingestion**:
>
> - **Files** (CSV, JSON, Parquet, etc.) dropped into a **Volume** or **External Location**
>   → I'll generate Auto Loader Bronze ingestion code.
> - **Connected systems** (Salesforce, databases, Kafka, etc.) via **Lakeflow Connect**
>   → Bronze is managed by the connector; your SDP starts at Silver.
>
> See: https://docs.databricks.com/aws/en/ingestion/overview
>
> Ready? Let's start with **Phase 1**.

---

## Phase 1 — Ingestion Source

Ask the user:

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
- Short table name (e.g. `customers`)
- Full raw table name (`<catalog>.<raw_schema>.<table_name>`)
- Schema string in Spark DDL format (e.g. `customer_id STRING, name STRING, insert_date TIMESTAMP`)
  - **Tip**: Always remind users that `insert_date TIMESTAMP` should be the last column — it is auto-added by the pipeline at ingest time
- Source path (Volume path or external location URI)
- File format (`csv`, `json`, `parquet`, `avro`, `orc`)
- For CSV: header (`true`/`false`), delimiter character

Ask if there are more tables. Loop until done.

### If source_type = "lakeflow_connect"
For **each source table**, collect:
- Short table name
- Full raw table name (where the connector will write to)
- Any notes about the connector configuration

Set `"tables": []` in the ingestion section (empty — connector manages it).

---

## Phase 2 — Project Identity

Collect the following, one question at a time:

```
🏗️ Phase 2 of 5 — Project Identity

1. Project name? (snake_case, e.g. retail_sdp)
2. Unity Catalog catalog name? (e.g. data_team)
3. Raw (Bronze) schema name? (default: raw)
4. Clean (Silver) schema name? (default: clean)
5. Gold schema name? (default: datawarehouse)
6. Databricks workspace URL? (e.g. https://xxx.cloud.databricks.com)
7. Owner email? (used in prod bundle permissions)
```

After collecting, show a summary table and confirm:

```
📋 Project Summary:
  Name:       <project_name>
  Catalog:    <catalog>
  Schemas:    raw=<raw> | clean=<clean> | gold=<gold>
  Workspace:  <host>
  Owner:      <email>

Is this correct? (yes / correct anything)
```

---

## Phase 3 — Clean Rules (Silver Layer)

For **each source table** identified in Phase 1, collect clean rules.

```
🧹 Phase 3 of 5 — Clean Rules (Silver Layer)

For each table, I need to know:
  1. Primary key column(s) — used for deduplication
  2. SCD Type:
       • Type 2 (recommended for slowly-changing dims) — keeps full history
       • Type 1 / Streaming — append-only (good for facts/events)
  3. Data quality expectations:
       • expect_or_fail   → BLOCKS pipeline (use for critical NOT NULL checks)
       • expect           → WARNS only (logged, pipeline continues)
       • expect_or_drop   → DROPS bad rows silently
       • expect_all       → batch WARN rules (dict form, multiple rules)
       • expect_all_or_drop → batch DROP rules (dict form, multiple rules)
  4. Type casts — columns that need conversion from STRING (e.g. amount → decimal(10,2))
```

For each table, present collected rules as a formatted block and confirm before moving on.

### Guidance on SCD type selection:
- Use **SCD Type 2** for: customers, products, employees, warehouses, categories
  (slowly-changing dimensional data where history matters)
- Use **SCD Type 1 + streaming** for: orders, sales, events, transactions
  (high-volume append-only facts)

### Expectation rule guidance:

| Rule Type | When to use | Code generated |
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

Now let's define your Gold layer star schema.

DIMENSIONS — one entry per dimension table:
  1. Which clean table does it read from?
  2. Is the source SCD Type 2? (yes → I'll add __END_AT IS NULL filter)
  3. What columns should the dimension expose?
  4. Natural key (business key) and surrogate key name?
  5. Does it join to any other dimension for enrichment? (e.g. products → categories)

FACTS — one entry per fact table:
  1. Which clean table is the fact grain?
  2. What are the natural key and measure columns?
  3. Which dimensions does it join to?
     For each dimension join:
       • Dimension gold table
       • FK column in the fact
       • Natural key column in the dimension
       • Surrogate key column to bring in
     Does the fact have all FKs directly, or does it need a bridge table?
     (e.g. fct_sales doesn't have customer_id — must join via orders first)
```

After collecting all dims and facts, produce a visual mapping summary:

```
Dimensions:
  ✅ dim_customer        ← clean.customers (SCD2)
  ✅ dim_warehouse       ← clean.warehouse (SCD2)
  ✅ dim_product_category← raw.product_category
  ✅ dim_product         ← clean.products_scd (SCD2) → joins dim_product_category

Facts:
  ✅ fct_orders  ← clean.orders → dim_customer, dim_warehouse, dim_product
  ✅ fct_sales   ← clean.sales  → dim_customer (via orders), dim_warehouse (via orders), dim_product (via orders)

Confirm? (yes / adjust)
```

---

## Phase 5 — Confirm & Generate

```
🚀 Phase 5 of 5 — Generate Project

Here's what I'm about to generate:

  <project_name>/
  ├── databricks.yml
  ├── pyproject.toml
  ├── .gitignore
  ├── README.md
  ├── sdp_config.json                ← YOUR master config (maintain this)
  ├── resources/
  │   └── <project_name>_pipeline.pipeline.yml
  └── src/
      ├── 01_bronze/
      │   ├── config.json            ← derived from sdp_config.json
      │   └── raw_ingestion_batch.py
      ├── 02_silver/
      │   └── datalake_clean.py
      └── 03_gold/
          ├── dimensions.py
          └── facts.py

Ready to generate? (yes)
```

Once confirmed, generate ALL files.

---

## Generation Rules

### File: `sdp_config.json`

Build this from all collected interview data. Validate it mentally against
`resources/sdp_config.schema.json` before writing. This is the **user-maintained**
master config — write a clear header comment explaining this.

### File: `src/01_bronze/config.json`

Derive from `sdp_config.ingestion.tables`. Use the schema from
`resources/config_json.schema.json`. Only generate when `source_type == "files"`.

When `source_type == "lakeflow_connect"`:
- Skip `config.json` and `raw_ingestion_batch.py`
- Create `src/01_bronze/README.md` instead explaining connector setup

### File: `src/01_bronze/raw_ingestion_batch.py`

Use the **exact pattern** from `references/databricks_sdp_patterns.md § Bronze Layer`.
Do not deviate — this pattern is proven in production.

### File: `src/02_silver/datalake_clean.py`

For each table in `sdp_config.clean.tables`:

1. **Check `scd_type` and `streaming` flags**:
   - `scd_type == "2"` → Pattern A (SCD2 snapshot)
   - `scd_type == "1"` and `streaming == true` → Pattern B (streaming append)

2. **Add imports** at the top:
   ```python
   from pyspark import pipelines as dp
   from pyspark.sql import functions as F
   from pyspark.sql.window import Window
   ```

3. **Add section header comments** per table, e.g.:
   ```python
   # ============================================================
   # Customers Clean (SCD Type 2)
   # ============================================================
   ```

4. **Cast columns**: generate `.withColumn("<col>", F.col("<col>").cast("<type>"))` chains.

5. **Expectation decorators**: apply in order per the rules table in Phase 3.

6. **`clean_table_suffix`**: if set, append the suffix to the clean table name
   (e.g. `products` + `_scd` → `clean_table = "ath_catalog.clean.products_scd"`).

### File: `src/03_gold/dimensions.py`

For each dimension in `sdp_config.gold.dimensions`:

1. Add imports:
   ```python
   import pyspark.pipelines as dp
   import pyspark.sql.functions as F
   from pyspark.sql.window import Window
   ```

2. Follow pattern from `references/databricks_sdp_patterns.md § Gold Layer — Dimension Pattern`.

3. **`is_scd2_source == true`** → add `.filter(F.col("__END_AT").isNull())`.

4. **`dim_joins`** → generate the aliased join chain before the final `.select()`.
   Joined dimension columns use `F.coalesce(F.col("<alias>.<col>"), F.lit(-1/"-1"/"Unknown"))`.

5. **Column ordering in `select()`**:
   - surrogate key first
   - natural key second
   - all other columns
   - `F.col("insert_date").alias("source_insert_date")` last
   - `F.current_timestamp().alias("load_date")` last

6. Add `# NOTE: Define <parent_dim> BEFORE <child_dim> so it can be consumed downstream`
   above any dimension that is joined by another dim.

### File: `src/03_gold/facts.py`

For each fact in `sdp_config.gold.facts`:

1. Add imports (same as dimensions).

2. Follow pattern from `references/databricks_sdp_patterns.md § Gold Layer — Fact Pattern`.

3. **Bridge tables**: when `bridge_table` is defined on a dimension join, read the bridge
   table once (deduplicate across dims that share the same bridge), then join:
   ```python
   bridge = dp.read("<bridge_table>").select(
       "<bridge_fact_key>", "<all unique bridge_dim_key columns>"
   ).alias("o")
   ```
   Then join fact → bridge, bridge → each dim.

4. All surrogate keys in `select()` use `F.coalesce(F.col("<alias>.<sk>"), F.lit(-1)).alias("<sk>")`.

5. **Select column order**:
   - natural key
   - all surrogate keys
   - all measure columns (in config order)
   - `F.current_timestamp().alias("load_date")`

### File: `databricks.yml`

Use the DAB template from `references/databricks_sdp_patterns.md § Databricks Asset Bundle`.
Substitute all `<placeholders>` with collected values.

### File: `resources/<project_name>_pipeline.pipeline.yml`

```yaml
# Main pipeline for <project_name>
resources:
  pipelines:
    <project_name>_etl:
      name: <project_name>_etl
      catalog: ${var.catalog}
      schema: ${var.schema}
      serverless: true

      libraries:
        - glob:
            include: ../src/01_bronze/**/*.py
        - glob:
            include: ../src/02_silver/**/*.py
        - glob:
            include: ../src/03_gold/**/*.py

      environment:
        dependencies:
          - --editable ${workspace.file_path}
```

Skip the Bronze glob when `source_type == "lakeflow_connect"`.

### File: `pyproject.toml`

```toml
[project]
name = "<project_name>"
version = "0.1.0"
description = "Databricks SDP — <project_name>"
requires-python = ">=3.11"
dependencies = []

[build-system]
requires = ["setuptools"]
build-backend = "setuptools.backends.legacy:build"

[tool.setuptools.packages.find]
where = ["src"]
```

### File: `.gitignore`

```
__pycache__/
*.py[cod]
.databricks/
.venv/
dist/
*.egg-info/
.DS_Store
```

### File: `README.md`

Generate a professional README with:
- Project name and description
- Medallion architecture overview with layer descriptions
- Folder structure tree
- Quickstart:
  1. Maintain `sdp_config.json` for any schema changes
  2. Deploy: `databricks bundle deploy --target dev`
  3. Run pipeline in Databricks UI or `databricks bundle run`
- Link to Databricks DAB docs
- Note on Lakeflow Connect if applicable

---

## Post-Generation Message

After all files are created, show this summary:

```
✅ SDP Project Generated!

📁 Project: <project_name>/

What to do next:
1. Review sdp_config.json — this is YOUR master config. Update it whenever
   your schema changes, then re-run this skill to regenerate the code files.

2. Set up your data source:
   • Files → ensure your Volume/External Location exists and data is landing
   • Lakeflow Connect → configure your connector in the Databricks UI first

3. Deploy to Databricks:
   cd <project_name>
   databricks bundle deploy --target dev
   databricks bundle run <project_name>_etl

4. Monitor your pipeline in the Databricks UI under
   Workflows → Delta Live Tables (SDP)

Need to add a new source table later?
  → Update sdp_config.json, then re-invoke this skill with "regenerate"
```

---

## Regeneration Mode

If the user invokes the skill with the word **"regenerate"** and provides an existing
`sdp_config.json`, skip Phases 1–4. Read the config directly and proceed to Phase 5
(confirmation) then regenerate all code files.

If they want to **add a table**, guide them to update `sdp_config.json` first, then regenerate.

---

## Important: File Path Convention

Always create the project folder **in the user's current workspace** unless they specify
a different location. Ask at the start of Phase 5:

```
Where should I create the project folder?
(Default: current directory — just press Enter, or give me a path)
```

---

## Quality Checklist (run before finishing)

Before declaring done, verify:

- [ ] `sdp_config.json` passes mental schema validation
- [ ] `config.json` table count matches ingestion tables
- [ ] Every `clean` table has a corresponding source in `ingestion.tables`
- [ ] Every `gold.dimension` has a matching `source_clean_table` in `clean.tables`
- [ ] Every `gold.fact` joins are referencing dimensions defined in `gold.dimensions`
- [ ] No placeholder strings like `<...>` remain in any generated file
- [ ] `databricks.yml` has correct host URL
- [ ] Pipeline YAML includes correct globs for all three layers
- [ ] Silver: SCD2 tables use `dp.read()` (batch), streaming tables use `dp.read_stream()`
- [ ] Gold: All facts use `F.coalesce(..., F.lit(-1))` for surrogate keys
- [ ] Gold: All dims include `source_insert_date` and `load_date`
