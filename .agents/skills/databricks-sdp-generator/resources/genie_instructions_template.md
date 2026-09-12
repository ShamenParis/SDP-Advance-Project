# Genie Code Instructions — SDP Pipeline Generator

> **How to use this file:**
> 1. Run the `databricks-sdp-generator` Antigravity skill to generate a populated version of this template for your project.
> 2. Copy the full contents of the generated `genie_instructions.md` from your project folder.
> 3. In Databricks: go to **Genie Code → Custom Instructions** and paste it there.
> 4. Genie Code will now understand your project layout and generate correct SDP code.

---

## Project Configuration

```
Project:   {{PROJECT_NAME}}
Catalog:   {{CATALOG}}
Schemas:   raw={{RAW_SCHEMA}} | clean={{CLEAN_SCHEMA}} | gold={{GOLD_SCHEMA}}
Workspace: {{WORKSPACE_HOST}}
Owner:     {{OWNER_EMAIL}}
```

---

## SDP File Structure

When generating, creating, or referencing files for this project, **always use this exact structure**:

```
{{PROJECT_NAME}}/
├── sdp_config.json                          ← master config (never auto-edit this)
└── src/
    ├── 01_bronze/
    │   └── datalake_files/
    │       ├── config.json                  ← Auto Loader table config (CSV/JSON/Parquet)
    │       └── raw_ingestion_batch.py       ← Auto Loader ingestion driver
    ├── 02_silver/
    │   └── datalake_clean.py                ← SCD2 + streaming clean rules
    └── 03_gold/
        ├── dimensions.py                    ← Materialized view dimensions
        └── facts.py                         ← Materialized view facts
```

> **Important**: Bronze files live in `src/01_bronze/datalake_files/` — not directly in `01_bronze/`.

---

## Source Tables (Bronze → Raw)

{{SOURCE_TABLES_SECTION}}

---

## Bronze Layer Rules (`src/01_bronze/datalake_files/`)

When generating Bronze layer code:

- Use **Auto Loader** (`cloudFiles`) streaming format for file ingestion
- Always set `cloudFiles.allowOverwrites = true` and `cloudFiles.schemaEvolutionMode = addNewColumns`
- Always append `insert_date = current_timestamp()` and drop `_rescued_data`
- The ingestion driver (`raw_ingestion_batch.py`) reads from `config.json` in the same folder
- `config.json` must have this structure per table:
  ```json
  {
    "name": "<catalog>.<schema>.<table>",
    "table_properties": {},
    "schema": "<col1> STRING, ... , insert_date TIMESTAMP",
    "file_format": "csv",
    "source_path": "/Volumes/<catalog>/<schema>/source/<table>/",
    "header": "true",
    "delimiter": ","
  }
  ```
- `insert_date TIMESTAMP` is always the **last column** in the schema string

---

## Silver Layer Rules (`src/02_silver/datalake_clean.py`)

All Silver code lives in a single file. Use these patterns based on table type:

### SCD Type 2 Tables (dimensions — slowly changing data)

```python
# Pattern: batch read → dedup → CDC flow
dp.create_streaming_table(name="<catalog>.<clean>.<table>")

@dp.temporary_view(name="<table>_latest_snapshot")
# Add @dp.expect_or_fail, @dp.expect decorators if quality rules exist
def <table>_latest_snapshot():
    window_spec = Window.partitionBy("<pk>").orderBy(F.col("insert_date").desc())
    return (
        dp.read("<catalog>.<raw>.<table>")          # batch read (not stream)
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
        # Add .withColumn() casts here if needed
    )

dp.create_auto_cdc_from_snapshot_flow(
    target="<catalog>.<clean>.<table>",
    source="<table>_latest_snapshot",
    keys=["<pk>"],
    stored_as_scd_type="2",
)
```

### Streaming Append Tables (facts — high-volume transactional)

```python
# Pattern: read_stream → @dp.table with cluster_by_auto
<table>_rules = { "<rule>": "<col> IS NOT NULL" }
<table>_drop_rules = { "<rule>": "<col> IS NOT NULL" }

@dp.table(name="<catalog>.<clean>.<table>", cluster_by_auto=True)
@dp.expect_all(<table>_rules)
@dp.expect_all_or_drop(<table>_drop_rules)
def clean_<table>():
    return (
        dp.read_stream("<catalog>.<raw>.<table>")   # streaming read
        .withColumn("<col>", F.col("<col>").cast("<type>"))
    )
```

### Quality Expectation Rules Summary

| Decorator | Behaviour | Use when |
|---|---|---|
| `@dp.expect_or_fail(name, expr)` | Blocks entire pipeline | Critical primary/foreign keys |
| `@dp.expect(name, expr)` | Logs warning, continues | Soft quality checks |
| `@dp.expect_or_drop(name, expr)` | Drops bad row silently | Optional enrichment columns |
| `@dp.expect_all(dict)` | Batch warn rules | Multiple warn checks at once |
| `@dp.expect_all_or_drop(dict)` | Batch drop rules | Multiple drop checks at once |

---

## Gold Layer Rules (`src/03_gold/`)

### dimensions.py — `@dp.materialized_view` pattern

```python
@dp.materialized_view(name="<catalog>.<gold>.<dim_name>", comment="...")
def <dim_name>():
    window_spec = Window.orderBy("<natural_key>")
    return (
        dp.read("<catalog>.<clean>.<source_table>")
        .filter(F.col("__END_AT").isNull())    # SCD2 sources only — current records
        .withColumn("<surrogate_key>", F.row_number().over(window_spec))
        .select(
            "<surrogate_key>",
            "<natural_key>",
            "<col_1>", "<col_2>",
            F.col("insert_date").alias("source_insert_date"),
            F.current_timestamp().alias("load_date"),
        )
    )
```

### facts.py — `@dp.materialized_view` with left joins

```python
@dp.materialized_view(name="<catalog>.<gold>.<fact_name>", comment="...")
def <fact_name>():
    fact = dp.read("<catalog>.<clean>.<source_table>").alias("f")
    dim_a = dp.read("<catalog>.<gold>.<dim_a_table>").alias("a")

    return (
        fact
        .join(dim_a, F.col("f.<fk>") == F.col("a.<nk>"), "left")
        .select(
            "f.<natural_key>",
            F.coalesce(F.col("a.<sk>"), F.lit(-1)).alias("<sk>"),  # -1 for unknown
            "f.<measure_1>",
            F.current_timestamp().alias("load_date"),
        )
    )
```

**Key rules:**
- Always use **left joins** in facts — never drop fact rows for missing dimension records
- Always use `F.coalesce(F.col("<alias>.<sk>"), F.lit(-1))` for all surrogate keys
- `insert_date` from source → always renamed to `source_insert_date` in Gold
- `F.current_timestamp()` → always added as `load_date`

---

## Master Config (`sdp_config.json`)

The `sdp_config.json` at the project root is the single source of truth for this pipeline.

{{SDP_CONFIG_CONTENTS}}

---

## When Generating Code — Key Reminders

1. **File locations matter**: Bronze code → `src/01_bronze/datalake_files/`, Silver → `src/02_silver/`, Gold → `src/03_gold/`
2. **SCD2 uses `dp.read()`** (batch), streaming facts use `dp.read_stream()`
3. **`cluster_by_auto=True`** only on streaming `@dp.table` calls — not on CDC tables
4. **Dimension order in Gold**: define parent dims before child dims that join to them
5. **`sdp_config.json` is user-maintained** — never overwrite it without explicit instruction
