import pyspark.pipelines as dp
import pyspark.sql.functions as F

# ========================================
# Fact Tables for Gold Layer
# ========================================

@dp.materialized_view(
    name="ath_catalog.datawarehouse.fct_orders",
    comment="Orders fact table enriched with dimension surrogate keys"
)
def fct_orders():
    # 1. Read the clean incremental fact
    orders = dp.read("ath_catalog.clean.orders").alias("o")
    
    # 2. Read the gold dimensions to get the surrogate keys
    dim_cust = dp.read("ath_catalog.datawarehouse.dim_customer").alias("c")
    dim_wh = dp.read("ath_catalog.datawarehouse.dim_warehouse").alias("w")
    dim_prod = dp.read("ath_catalog.datawarehouse.dim_product").alias("p")

    return (
        orders
        # Left joins ensure we don't drop fact records if a dimension is missing
        .join(dim_cust, F.col("o.customer_id") == F.col("c.customer_id"), "left")
        .join(dim_wh, F.col("o.warehouse_id") == F.col("w.warehouse_id"), "left")
        .join(dim_prod, F.col("o.product_id") == F.col("p.product_id"), "left")
        .select(
            "o.order_id",
            # Replace nulls with -1 to maintain referential integrity for unknown dimensions
            F.coalesce(F.col("c.customer_key"), F.lit(-1)).alias("customer_key"),
            F.coalesce(F.col("w.warehouse_key"), F.lit(-1)).alias("warehouse_key"),
            F.coalesce(F.col("p.product_key"), F.lit(-1)).alias("product_key"),
            "o.order_datetime",
            "o.amount",
            "o.quantity",
            F.current_timestamp().alias("load_date")
        )
    )

@dp.materialized_view(
    name="ath_catalog.datawarehouse.fct_sales",
    comment="Sales fact table enriched with dimension surrogate keys"
)
def fct_sales():
    # 1. Read the sales fact
    sales = dp.read("ath_catalog.clean.sales").alias("s")
    
    # 2. Read clean orders to fetch the natural dimension IDs associated with the sale
    orders = dp.read("ath_catalog.clean.orders").select(
        "order_id", "customer_id", "warehouse_id", "product_id"
    ).alias("o")
    
    # 3. Read the gold dimensions
    dim_cust = dp.read("ath_catalog.datawarehouse.dim_customer").alias("c")
    dim_wh = dp.read("ath_catalog.datawarehouse.dim_warehouse").alias("w")
    dim_prod = dp.read("ath_catalog.datawarehouse.dim_product").alias("p")

    return (
        sales
        # Join to orders to grab the natural keys for the transaction
        .join(orders, F.col("s.order_id") == F.col("o.order_id"), "left")
        # Join to dimensions to get the surrogate keys
        .join(dim_cust, F.col("o.customer_id") == F.col("c.customer_id"), "left")
        .join(dim_wh, F.col("o.warehouse_id") == F.col("w.warehouse_id"), "left")
        .join(dim_prod, F.col("o.product_id") == F.col("p.product_id"), "left")
        .select(
            "s.sale_id",
            "s.order_id",
            F.coalesce(F.col("c.customer_key"), F.lit(-1)).alias("customer_key"),
            F.coalesce(F.col("w.warehouse_key"), F.lit(-1)).alias("warehouse_key"),
            F.coalesce(F.col("p.product_key"), F.lit(-1)).alias("product_key"),
            "s.sale_datetime",
            "s.payment_method",
            "s.gross_amount",
            "s.discount_amount",
            "s.net_amount",
            F.current_timestamp().alias("load_date")
        )
    )