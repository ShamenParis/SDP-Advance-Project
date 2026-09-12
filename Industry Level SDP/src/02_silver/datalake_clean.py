from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Customer Clean
dp.create_streaming_table(name="ath_catalog.clean.customers")

dp.create_auto_cdc_from_snapshot_flow(
  target = "ath_catalog.clean.customers",
  source = "ath_catalog.raw.customers",
  keys=["customer_id"],
  stored_as_scd_type = "2",
  track_history_column_list = None,
  track_history_except_column_list = None
)

# Warehouse Clean
dp.create_streaming_table(
    name="ath_catalog.clean.warehouse"
)

dp.create_auto_cdc_from_snapshot_flow(
    target="ath_catalog.clean.warehouse",
    source="ath_catalog.raw.warehouse",
    keys=["warehouse_id"], 
    stored_as_scd_type="2"
)

# Product Clean
@dp.temporary_view(name='clean_products_view')
@dp.expect_or_fail("validate_product_id", "product_id IS NOT NULL")
@dp.expect("validate_product_name", "product_name IS NOT NULL")
def clean_products():
    return (
        dp.read('ath_catalog.raw.products')
        .withColumn('unit_price', F.col('unit_price').cast("decimal(10,2)"))
    )

dp.create_streaming_table(name="ath_catalog.clean.products_scd")

dp.create_auto_cdc_from_snapshot_flow(
    target="ath_catalog.clean.products_scd",
    source="clean_products_view",
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