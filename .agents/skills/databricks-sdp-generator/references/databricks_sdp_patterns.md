# Databricks SDP — Code Pattern Reference

This reference documents the **exact code patterns** to be generated for each
section of an SDP project. All patterns are derived from the proven
`Industry Level SDP` codebase.

---

## Bronze Layer — `raw_ingestion_batch.py`

**Always generated verbatim.** The config loop is fixed; only `config.json` changes.

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

## Silver Layer — Pattern A: SCD Type 2 (snapshot deduplication)

Use when `scd_type == "2"`. Generates three blocks per table.

### Block 1 — Create Streaming Table target
```python
dp.create_streaming_table(name="<catalog>.<clean_schema>.<table_name>")
```

### Block 2 — Temporary view with dedup + casts + expectations

```python
@dp.temporary_view(name="<table_name>_latest_snapshot")
@dp.expect_or_fail("<rule_name>", "<sql_expression>")   # if expect_or_fail rules exist
@dp.expect("<rule_name>", "<sql_expression>")            # if expect rules exist
def <table_name>_latest_snapshot():
    window_spec = Window.partitionBy("<pk_col>").orderBy(F.col("insert_date").desc())
    return (
        dp.read("<raw_table>")
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
        # --- cast_columns (generated per entry) ---
        .withColumn("<col>", F.col("<col>").cast("<target_type>"))
    )
```

> **Rule decorator ordering**: `@dp.expect_or_fail` → `@dp.expect` → `@dp.expect_or_drop`
> decorators are applied in reverse Python order (outermost = last applied),
> so place them closest to the function in priority order: fail > drop > warn.

### Block 3 — CDC flow
```python
dp.create_auto_cdc_from_snapshot_flow(
    target="<catalog>.<clean_schema>.<table_name>",
    source="<table_name>_latest_snapshot",
    keys=["<pk_col>"],
    stored_as_scd_type="2",
)
```

### Full SCD2 Example (customers)
```python
# Customer Clean
dp.create_streaming_table(name="ath_catalog.clean.customers")

@dp.temporary_view(name="customers_latest_snapshot")
def customers_latest_snapshot():
    window_spec = Window.partitionBy("customer_id").orderBy(F.col("insert_date").desc())
    return (
        dp.read("ath_catalog.raw.customers")
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

dp.create_auto_cdc_from_snapshot_flow(
    target="ath_catalog.clean.customers",
    source="customers_latest_snapshot",
    keys=["customer_id"],
    stored_as_scd_type="2",
)
```

### SCD2 with expectations and casts (products)
```python
dp.create_streaming_table(name="ath_catalog.clean.products_scd")

@dp.temporary_view(name="products_latest_snapshot")
@dp.expect_or_fail("validate_product_id", "product_id IS NOT NULL")
@dp.expect("validate_product_name", "product_name IS NOT NULL")
def products_latest_snapshot():
    window_spec = Window.partitionBy("product_id").orderBy(F.col("insert_date").desc())
    return (
        dp.read("ath_catalog.raw.products")
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
    )

dp.create_auto_cdc_from_snapshot_flow(
    target="ath_catalog.clean.products_scd",
    source="products_latest_snapshot",
    keys=["product_id"],
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
    name="<catalog>.<clean_schema>.<table_name>",
    cluster_by_auto=True,
)
@dp.expect_all(<table_name>_rules)           # if expect_all defined
@dp.expect_all_or_drop(<table_name>_drop_rules)  # if expect_all_or_drop defined
@dp.expect_or_drop("<rule_name>", "<sql>")  # single-rule drop expectations
def clean_<table_name>():
    return (
        dp.read_stream("<raw_table>")
        .withColumn("<col>", F.col("<col>").cast("<target_type>"))
        ...
    )
```

### Full Streaming Example (orders)
```python
order_rules = {
    "validate_order_id":    "order_id IS NOT NULL",
    "validate_customer_id": "customer_id IS NOT NULL",
    "validate_warehouse_id":"warehouse_id IS NOT NULL",
    "validate_product_id":  "product_id IS NOT NULL",
}

order_drop_rules = {
    "valid_amount":   "amount IS NOT NULL",
    "valid_quantity": "quantity IS NOT NULL",
}

@dp.table(
    name="ath_catalog.clean.orders",
    cluster_by_auto=True,
)
@dp.expect_all(order_rules)
@dp.expect_all_or_drop(order_drop_rules)
def clean_orders():
    return (
        dp.read_stream("ath_catalog.raw.orders")
        .withColumn("amount",   F.col("amount").cast("decimal(10,2)"))
        .withColumn("quantity", F.col("quantity").cast("int"))
    )
```

---

## Gold Layer — Dimension Pattern

```python
@dp.materialized_view(
    name="<gold_table>",
    comment="<comment>",
)
def <dim_name>():
    window_spec = Window.orderBy("<natural_key>")

    return (
        dp.read("<source_clean_table>")
        # Only include if is_scd2_source == true:
        .filter(F.col("__END_AT").isNull())
        .withColumn("<surrogate_key>", F.row_number().over(window_spec))
        .select(
            "<surrogate_key>",
            "<col_1>",
            "<col_2>",
            ...
            F.col("insert_date").alias("source_insert_date"),
            F.current_timestamp().alias("load_date"),
        )
    )
```

### Dimension with parent dim join (dim_product → dim_product_category)

```python
@dp.materialized_view(
    name="ath_catalog.datawarehouse.dim_product",
    comment="Product dimension enriched with category surrogate keys",
)
def dim_product():
    window_spec = Window.orderBy("product_id")

    products = (
        dp.read("ath_catalog.clean.products_scd")
        .filter(F.col("__END_AT").isNull())
        .alias("p")
    )
    categories = dp.read("ath_catalog.datawarehouse.dim_product_category").alias("c")

    return (
        products.join(categories, F.col("p.category_id") == F.col("c.category_id"), "left")
        .withColumn("product_key", F.row_number().over(window_spec))
        .select(
            "product_key",
            "p.product_id",
            "p.product_name",
            F.coalesce(F.col("c.category_key"),    F.lit(-1)).alias("category_key"),
            F.coalesce(F.col("p.category_id"),     F.lit("-1")).alias("category_id"),
            F.coalesce(F.col("c.category_name"),   F.lit("Unknown")).alias("category_name"),
            F.coalesce(F.col("c.description"),     F.lit("Unknown Category")).alias("category_description"),
            "p.unit_price",
            "p.sku",
            F.col("p.insert_date").alias("source_insert_date"),
            F.current_timestamp().alias("load_date"),
        )
    )
```

---

## Gold Layer — Fact Pattern

```python
@dp.materialized_view(
    name="<gold_table>",
    comment="<comment>",
)
def <fact_name>():
    # 1. Read the clean fact grain
    <fact_alias> = dp.read("<source_clean_table>").alias("<fact_alias>")

    # 2. Read gold dimensions for surrogate keys
    dim_<alias> = dp.read("<dim_gold_table>").alias("<alias>")
    ...

    return (
        <fact_alias>
        .join(dim_<alias>, F.col("<fact_alias>.<fact_fk>") == F.col("<alias>.<dim_natural_key>"), "left")
        ...
        .select(
            "<fact_alias>.<natural_key>",
            # Surrogate key with null-guard
            F.coalesce(F.col("<alias>.<dim_surrogate_key>"), F.lit(-1)).alias("<dim_surrogate_key>"),
            ...
            # Measure columns
            "<fact_alias>.<measure_1>",
            ...
            F.current_timestamp().alias("load_date"),
        )
    )
```

### Fact with bridge table (fct_sales — gets dim FKs via orders)

```python
@dp.materialized_view(
    name="ath_catalog.datawarehouse.fct_sales",
    comment="Sales fact table enriched with dimension surrogate keys",
)
def fct_sales():
    sales  = dp.read("ath_catalog.clean.sales").alias("s")

    # Bridge: orders carries the dimension foreign keys for sales
    orders = dp.read("ath_catalog.clean.orders").select(
        "order_id", "customer_id", "warehouse_id", "product_id"
    ).alias("o")

    dim_cust = dp.read("ath_catalog.datawarehouse.dim_customer").alias("c")
    dim_wh   = dp.read("ath_catalog.datawarehouse.dim_warehouse").alias("w")
    dim_prod = dp.read("ath_catalog.datawarehouse.dim_product").alias("p")

    return (
        sales
        .join(orders,    F.col("s.order_id")     == F.col("o.order_id"),     "left")
        .join(dim_cust,  F.col("o.customer_id")  == F.col("c.customer_id"),  "left")
        .join(dim_wh,    F.col("o.warehouse_id") == F.col("w.warehouse_id"), "left")
        .join(dim_prod,  F.col("o.product_id")   == F.col("p.product_id"),   "left")
        .select(
            "s.sale_id",
            "s.order_id",
            F.coalesce(F.col("c.customer_key"),  F.lit(-1)).alias("customer_key"),
            F.coalesce(F.col("w.warehouse_key"), F.lit(-1)).alias("warehouse_key"),
            F.coalesce(F.col("p.product_key"),   F.lit(-1)).alias("product_key"),
            "s.sale_datetime",
            "s.payment_method",
            "s.gross_amount",
            "s.discount_amount",
            "s.net_amount",
            F.current_timestamp().alias("load_date"),
        )
    )
```

---

## Databricks Asset Bundle (DAB) Files

### `databricks.yml`

```yaml
bundle:
  name: <project_name>

include:
  - resources/*.yml

variables:
  catalog:
    description: The catalog to use
  schema:
    description: The schema to use

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: <workspace_host>
    variables:
      catalog: <catalog>
      schema: ${workspace.current_user.short_name}
  prod:
    mode: production
    workspace:
      host: <workspace_host>
      root_path: /Workspace/Users/<owner_email>/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: <catalog>
      schema: prod
    permissions:
      - user_name: <owner_email>
        level: CAN_MANAGE
```

### `resources/<project_name>_pipeline.pipeline.yml`

```yaml
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

---

## Key Rules for Code Generation

1. **Always use `F.coalesce(..., F.lit(-1))`** for integer surrogate keys in facts — maintains referential integrity when a dim record is missing.
2. **String surrogate keys use `F.lit("-1")`** (quoted) — e.g. when a natural key is a VARCHAR.
3. **`insert_date` is always renamed** to `source_insert_date` in Gold dims/facts, and `F.current_timestamp()` → `load_date` is always appended.
4. **SCD2 dimensions always filter** `.filter(F.col("__END_AT").isNull())` to get current records only.
5. **Rule decorator order** on `@dp.temporary_view`: place `expect_or_fail` outermost (closest to function wins in Python decoration), then `expect_or_drop`, then `expect`.
6. **`cluster_by_auto=True`** on all streaming append Silver tables. Not used on CDC streaming tables.
7. **`dp.read()` for SCD2 snapshot** views (batch). **`dp.read_stream()`** for append fact tables.
