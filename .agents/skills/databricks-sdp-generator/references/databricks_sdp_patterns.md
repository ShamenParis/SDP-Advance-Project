# Databricks SDP — Code Pattern Reference

This reference documents the **exact code patterns** to generate for each section
of an SDP project. All patterns use `<placeholder>` notation — never real values.

> **Convention**: `<catalog>` = Unity Catalog name, `<raw>` = raw schema,
> `<clean>` = Silver schema, `<gold>` = Gold schema (e.g. `datawarehouse`)

---

## Bronze Layer — `src/01_bronze/datalake_files/raw_ingestion_batch.py`

**Always generated verbatim.** Only `config.json` in the same folder changes per project.

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
import json

def ingestion_pipeline(
    table_name: str,
    table_properties: dict,
    schema: str,
    file_format: str,
    source_path: str,
    header: str,
    delimiter: str,
):
    @dp.table(
        name=table_name,
        table_properties=table_properties,
        schema=schema,
        cluster_by_auto=True,
    )
    def load_raw_data():
        df = (
            spark
            .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .option("cloudFiles.allowOverwrites", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("header", header)
            .option("delimiter", delimiter)
            .load(source_path)
            .withColumn("insert_date", F.current_timestamp())
            .drop("_rescued_data")
        )
        return df


with open("config.json", mode="r") as f:
    data = json.load(f)
    for tables in data.get("tables"):
        ingestion_pipeline(
            table_name=tables.get("name"),
            table_properties=tables.get("table_properties"),
            schema=tables.get("schema"),
            file_format=tables.get("file_format"),
            source_path=tables.get("source_path"),
            header=tables.get("header"),
            delimiter=tables.get("delimiter"),
        )
```

---

## Bronze Layer — `src/01_bronze/datalake_files/config.json` (template)

```json
{
  "tables": [
    {
      "name": "<catalog>.<raw>.<table_name>",
      "table_properties": {},
      "schema": "<col1> STRING, <col2> STRING, insert_date TIMESTAMP",
      "file_format": "csv",
      "source_path": "/Volumes/<catalog>/<raw>/source/<table_name>/",
      "header": "true",
      "delimiter": ","
    }
  ]
}
```

> **Note**: `insert_date TIMESTAMP` is always the last column in the schema string.
> It is added automatically by the pipeline at ingest time.

---

## Silver Layer — Pattern A: SCD Type 2 (snapshot deduplication)

Use when `scd_type == "2"`. Generates three blocks per table.

### Block 1 — Create Streaming Table target
```python
dp.create_streaming_table(name="<catalog>.<clean>.<table_name>")
```

### Block 2 — Temporary view with dedup + casts + expectations

```python
@dp.temporary_view(name="<table_name>_latest_snapshot")
@dp.expect_or_fail("<rule_name>", "<sql_expression>")   # if expect_or_fail rules exist
@dp.expect("<rule_name>", "<sql_expression>")            # if expect rules exist
def <table_name>_latest_snapshot():
    window_spec = Window.partitionBy("<pk_col>").orderBy(F.col("insert_date").desc())
    return (
        dp.read("<catalog>.<raw>.<table_name>")
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
        # --- cast_columns (one per entry in sdp_config) ---
        .withColumn("<col>", F.col("<col>").cast("<target_type>"))
    )
```

> **Rule decorator ordering**: Place `@dp.expect_or_fail` closest to the function
> (innermost), then `@dp.expect`. Python applies decorators bottom-up.

### Block 3 — CDC flow
```python
dp.create_auto_cdc_from_snapshot_flow(
    target="<catalog>.<clean>.<table_name>",
    source="<table_name>_latest_snapshot",
    keys=["<pk_col>"],
    stored_as_scd_type="2",
)
```

### Full SCD2 Example — no expectations, no casts
```python
# ============================================================
# <TableName> Clean (SCD Type 2)
# ============================================================
dp.create_streaming_table(name="<catalog>.<clean>.<table_name>")

@dp.temporary_view(name="<table_name>_latest_snapshot")
def <table_name>_latest_snapshot():
    window_spec = Window.partitionBy("<pk_col>").orderBy(F.col("insert_date").desc())
    return (
        dp.read("<catalog>.<raw>.<table_name>")
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

dp.create_auto_cdc_from_snapshot_flow(
    target="<catalog>.<clean>.<table_name>",
    source="<table_name>_latest_snapshot",
    keys=["<pk_col>"],
    stored_as_scd_type="2",
)
```

### SCD2 with expectations and casts
```python
# ============================================================
# <TableName> Clean (SCD Type 2 — with quality checks + casts)
# ============================================================
dp.create_streaming_table(name="<catalog>.<clean>.<table_name>_scd")

@dp.temporary_view(name="<table_name>_latest_snapshot")
@dp.expect_or_fail("validate_<pk_col>", "<pk_col> IS NOT NULL")
@dp.expect("validate_<name_col>", "<name_col> IS NOT NULL")
def <table_name>_latest_snapshot():
    window_spec = Window.partitionBy("<pk_col>").orderBy(F.col("insert_date").desc())
    return (
        dp.read("<catalog>.<raw>.<table_name>")
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("<amount_col>", F.col("<amount_col>").cast("decimal(10,2)"))
    )

dp.create_auto_cdc_from_snapshot_flow(
    target="<catalog>.<clean>.<table_name>_scd",
    source="<table_name>_latest_snapshot",
    keys=["<pk_col>"],
    stored_as_scd_type="2",
)
```

---

## Silver Layer — Pattern B: Streaming Append (`scd_type == "1"` + `streaming == true`)

Use for high-volume transactional tables (orders, sales, events).

```python
# Build rule dicts first (if expect_all or expect_all_or_drop are used)
<table_name>_rules = {
    "<rule_name>": "<sql_expression>",
    ...
}

<table_name>_drop_rules = {
    "<rule_name>": "<sql_expression>",
    ...
}

@dp.table(
    name="<catalog>.<clean>.<table_name>",
    cluster_by_auto=True,
)
@dp.expect_all(<table_name>_rules)             # if expect_all defined
@dp.expect_all_or_drop(<table_name>_drop_rules)  # if expect_all_or_drop defined
@dp.expect_or_drop("<rule_name>", "<sql>")     # single-rule drop expectations
def clean_<table_name>():
    return (
        dp.read_stream("<catalog>.<raw>.<table_name>")
        .withColumn("<col>", F.col("<col>").cast("<target_type>"))
    )
```

### Full Streaming Example — with expect_all + expect_all_or_drop + casts
```python
# ============================================================
# <TableName> Clean (Streaming Append — fact/event table)
# ============================================================
<table_name>_rules = {
    "validate_<pk_col>":   "<pk_col> IS NOT NULL",
    "validate_<fk_col>":   "<fk_col> IS NOT NULL",
}

<table_name>_drop_rules = {
    "valid_<amount_col>":   "<amount_col> IS NOT NULL",
    "valid_<qty_col>": "<qty_col> IS NOT NULL",
}

@dp.table(
    name="<catalog>.<clean>.<table_name>",
    cluster_by_auto=True,
)
@dp.expect_all(<table_name>_rules)
@dp.expect_all_or_drop(<table_name>_drop_rules)
def clean_<table_name>():
    return (
        dp.read_stream("<catalog>.<raw>.<table_name>")
        .withColumn("<amount_col>", F.col("<amount_col>").cast("decimal(10,2)"))
        .withColumn("<qty_col>",    F.col("<qty_col>").cast("int"))
    )
```

---

## Gold Layer — Dimension Pattern (`src/03_gold/dimensions.py`)

```python
@dp.materialized_view(
    name="<catalog>.<gold>.<dim_name>",
    comment="<comment>",
)
def <dim_name>():
    window_spec = Window.orderBy("<natural_key>")

    return (
        dp.read("<catalog>.<clean>.<source_table>")
        # Only include if is_scd2_source == true:
        .filter(F.col("__END_AT").isNull())
        .withColumn("<surrogate_key>", F.row_number().over(window_spec))
        .select(
            "<surrogate_key>",
            "<natural_key>",
            "<col_1>",
            "<col_2>",
            F.col("insert_date").alias("source_insert_date"),
            F.current_timestamp().alias("load_date"),
        )
    )
```

### Dimension with parent dim join (enrichment pattern)

```python
# NOTE: Define <parent_dim> BEFORE <child_dim> so it can be consumed downstream

@dp.materialized_view(
    name="<catalog>.<gold>.<child_dim_name>",
    comment="<child_dim> enriched with <parent_dim> surrogate keys",
)
def <child_dim_name>():
    window_spec = Window.orderBy("<natural_key>")

    rows = (
        dp.read("<catalog>.<clean>.<child_source_table>")
        .filter(F.col("__END_AT").isNull())
        .alias("p")
    )
    parent = dp.read("<catalog>.<gold>.<parent_dim_table>").alias("c")

    return (
        rows.join(parent, F.col("p.<join_key>") == F.col("c.<join_key>"), "left")
        .withColumn("<surrogate_key>", F.row_number().over(window_spec))
        .select(
            "<surrogate_key>",
            "p.<natural_key>",
            "p.<col_1>",
            F.coalesce(F.col("c.<parent_sk>"),    F.lit(-1)).alias("<parent_sk>"),
            F.coalesce(F.col("c.<parent_col>"),   F.lit("Unknown")).alias("<parent_col>"),
            "p.<measure_col>",
            F.col("p.insert_date").alias("source_insert_date"),
            F.current_timestamp().alias("load_date"),
        )
    )
```

---

## Gold Layer — Fact Pattern (`src/03_gold/facts.py`)

```python
@dp.materialized_view(
    name="<catalog>.<gold>.<fact_name>",
    comment="<comment>",
)
def <fact_name>():
    # 1. Read the clean fact grain
    <fact_alias> = dp.read("<catalog>.<clean>.<source_table>").alias("<fact_alias>")

    # 2. Read gold dimensions for surrogate keys
    dim_<alias1> = dp.read("<catalog>.<gold>.<dim_1_table>").alias("<alias1>")
    dim_<alias2> = dp.read("<catalog>.<gold>.<dim_2_table>").alias("<alias2>")

    return (
        <fact_alias>
        # Left joins — never drop fact rows when dim is missing
        .join(dim_<alias1>, F.col("<fact_alias>.<fk1>") == F.col("<alias1>.<dim_nk1>"), "left")
        .join(dim_<alias2>, F.col("<fact_alias>.<fk2>") == F.col("<alias2>.<dim_nk2>"), "left")
        .select(
            "<fact_alias>.<natural_key>",
            # Surrogate keys with null-guard (use -1 for unknown dimension)
            F.coalesce(F.col("<alias1>.<sk1>"), F.lit(-1)).alias("<sk1>"),
            F.coalesce(F.col("<alias2>.<sk2>"), F.lit(-1)).alias("<sk2>"),
            # Measure columns
            "<fact_alias>.<measure_1>",
            "<fact_alias>.<measure_2>",
            F.current_timestamp().alias("load_date"),
        )
    )
```

### Fact with bridge table (when fact lacks FK — must join via intermediate table)

```python
@dp.materialized_view(
    name="<catalog>.<gold>.<fact_name>",
    comment="<fact_name> enriched with dimension surrogate keys",
)
def <fact_name>():
    fact = dp.read("<catalog>.<clean>.<fact_source_table>").alias("f")

    # Bridge: <bridge_table> carries the FK columns that <fact_source_table> lacks
    bridge = dp.read("<catalog>.<clean>.<bridge_table>").select(
        "<bridge_join_key>", "<fk_1>", "<fk_2>", "<fk_3>"
    ).alias("b")

    dim_1 = dp.read("<catalog>.<gold>.<dim_1_table>").alias("d1")
    dim_2 = dp.read("<catalog>.<gold>.<dim_2_table>").alias("d2")

    return (
        fact
        .join(bridge, F.col("f.<bridge_join_key>") == F.col("b.<bridge_join_key>"), "left")
        .join(dim_1,  F.col("b.<fk_1>")  == F.col("d1.<dim_1_nk>"),  "left")
        .join(dim_2,  F.col("b.<fk_2>")  == F.col("d2.<dim_2_nk>"),  "left")
        .select(
            "f.<natural_key>",
            "f.<bridge_join_key>",
            F.coalesce(F.col("d1.<sk_1>"), F.lit(-1)).alias("<sk_1>"),
            F.coalesce(F.col("d2.<sk_2>"), F.lit(-1)).alias("<sk_2>"),
            "f.<measure_1>",
            "f.<measure_2>",
            F.current_timestamp().alias("load_date"),
        )
    )
```

---

## Key Code Generation Rules

1. **`F.coalesce(..., F.lit(-1))`** for integer surrogate keys in facts — maintains referential integrity when a dim record is missing.
2. **String surrogate keys use `F.lit("-1")`** (quoted) — e.g. when the natural key is VARCHAR.
3. **`insert_date` is always renamed** to `source_insert_date` in Gold dims/facts; `F.current_timestamp()` → `load_date` is appended.
4. **SCD2 dimensions always filter** `.filter(F.col("__END_AT").isNull())` to get current records only.
5. **Decorator order on `@dp.temporary_view`**: `expect_or_fail` closest to function, then `expect_or_drop`, then `expect`.
6. **`cluster_by_auto=True`** on streaming append Silver tables only — not on SCD2 CDC tables.
7. **`dp.read()` for SCD2 snapshot** views (batch). **`dp.read_stream()`** for append fact tables.
8. **`config.json` lives at `src/01_bronze/datalake_files/config.json`** — same folder as `raw_ingestion_batch.py`.
