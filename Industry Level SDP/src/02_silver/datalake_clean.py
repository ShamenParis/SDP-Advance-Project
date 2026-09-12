from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Customer Clean
dp.create_streaming_table(name="ath_catalog.clean.customers")

@dp.temporary_view(name="customers_latest_snapshot")
def customers_latest_snapshot():
    # Use dp.read() to read RAW as a batch, not a stream
    # Partition by the primary key and sort by insert_date to keep only the newest record
    window_spec = Window.partitionBy("customer_id").orderBy(F.col("insert_date").desc())
    
    return (
        dp.read("ath_catalog.raw.customers")
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

dp.create_auto_cdc_from_snapshot_flow(
  target = "ath_catalog.clean.customers",
  source = "customers_latest_snapshot", # Feed it the deduplicated view
  keys = ["customer_id"],
  stored_as_scd_type = "2"
)

# Warehouse Clean
dp.create_streaming_table(
    name="ath_catalog.clean.warehouse"
)

@dp.temporary_view(name="warehouse_latest_snapshot")
def warehouse_latest_snapshot():
    window_spec = Window.partitionBy("warehouse_id").orderBy(F.col("insert_date").desc())
    return (
        dp.read("ath_catalog.raw.warehouse")
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

dp.create_auto_cdc_from_snapshot_flow(
    target="ath_catalog.clean.warehouse",
    source="warehouse_latest_snapshot",
    keys=["warehouse_id"], 
    stored_as_scd_type="2"
)

# Product Clean
dp.create_streaming_table(name="ath_catalog.clean.products_scd")

@dp.temporary_view(name='products_latest_snapshot')
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
    stored_as_scd_type="2"
)

# Sales Clean
sales_rules = {
    "validate_order_id": "order_id IS NOT NULL",
    "validate_payment_method": "payment_method IS NOT NULL"
}

@dp.table(
    name='ath_catalog.clean.sales',
    cluster_by_auto=True
)
@dp.expect_all(sales_rules)
@dp.expect_or_drop("valid_net_amount", "net_amount IS NOT NULL")
def clean_sales():
    return (
        dp.read_stream('ath_catalog.raw.sales')
        .withColumn('gross_amount', F.col('gross_amount').cast("decimal(10,2)"))
        .withColumn('discount_amount', F.col('discount_amount').cast("decimal(10,2)"))
        .withColumn('net_amount', F.col('net_amount').cast("decimal(10,2)"))
    )

# Orders Clean
order_rules = {
    "validate_order_id": "order_id IS NOT NULL",
    "validate_customer_id": "customer_id IS NOT NULL",
    "validate_warehouse_id": "warehouse_id IS NOT NULL",
    "validate_product_id": "product_id IS NOT NULL"
}

order_drop_rules = {
    "valid_amount": "amount IS NOT NULL",
    "valid_quantity": "quantity IS NOT NULL"
}

@dp.table(
    name='ath_catalog.clean.orders',
    cluster_by_auto=True
)
@dp.expect_all(order_rules)
@dp.expect_all_or_drop(order_drop_rules)
def clean_orders():
    return (
        dp.read_stream('ath_catalog.raw.orders')
        .withColumn('amount', F.col('amount').cast("decimal(10,2)"))
        .withColumn('quantity', F.col('quantity').cast("int"))
    )