import pyspark.pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# ========================================
# Dimension Tables for Gold Layer
# ========================================

@dp.materialized_view(
    name="ath_catalog.datawarehouse.dim_warehouse",
    comment="Warehouse dimension table with surrogate keys"
)
def dim_warehouse():
    window_spec = Window.orderBy("warehouse_id")
    
    return (
        dp.read("ath_catalog.clean.warehouse")
        .filter(F.col("__END_AT").isNull())     # Current records only
        .withColumn("warehouse_key", F.row_number().over(window_spec))
        .select(
            "warehouse_key",
            "warehouse_id",
            "warehouse_name",
            "location",
            "capacity_sqft",
            "manager_name",
            F.col("insert_date").alias("source_insert_date"),
            F.current_timestamp().alias("load_date")
        )
    )


@dp.materialized_view(
    name="ath_catalog.datawarehouse.dim_customer",
    comment="Customer dimension table with surrogate keys"
)
def dim_customer():
    window_spec = Window.orderBy("customer_id")
    
    return (
        dp.read("ath_catalog.clean.customers")
        .filter(F.col("__END_AT").isNull())
        .withColumn("customer_key", F.row_number().over(window_spec))
        .select(
            "customer_key",
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "city",
            "country",
            F.col("insert_date").alias("source_insert_date"),
            F.current_timestamp().alias("load_date")
        )
    )


@dp.materialized_view(
    name="ath_catalog.datawarehouse.dim_product_category",
    comment="Product category dimension table with surrogate keys"
)
def dim_product_category():
    # Define this BEFORE dim_product so it can be consumed downstream
    window_spec = Window.orderBy("category_id")
    
    return (
        dp.read("ath_catalog.raw.product_category")
        .withColumn("category_key", F.row_number().over(window_spec))
        .select(
            "category_key",
            "category_id",
            "category_name",
            "description",
            F.col("insert_date").alias("source_insert_date"),
            F.current_timestamp().alias("load_date")
        )
    )


@dp.materialized_view(
    name="ath_catalog.datawarehouse.dim_product",
    comment="Product dimension enriched with category surrogate keys"
)
def dim_product():
    window_spec = Window.orderBy("product_id")
    
    products = (
        dp.read("ath_catalog.clean.products_scd")
        .filter(F.col("__END_AT").isNull())
        .alias("p")
    )
    
    # Read from the GOLD category dimension, not RAW, to inherit the category_key
    categories = dp.read("ath_catalog.datawarehouse.dim_product_category").alias("c")
    
    return (
        products.join(categories, F.col("p.category_id") == F.col("c.category_id"), "left")
        .withColumn("product_key", F.row_number().over(window_spec))
        .select(
            "product_key",
            "p.product_id",
            "p.product_name",
            # Include the surrogate key from the category dimension
            F.coalesce(F.col("c.category_key"), F.lit(-1)).alias("category_key"),
            F.coalesce(F.col("p.category_id"), F.lit("-1")).alias("category_id"),
            F.coalesce(F.col("c.category_name"), F.lit("Unknown")).alias("category_name"),
            F.coalesce(F.col("c.description"), F.lit("Unknown Category")).alias("category_description"),
            "p.unit_price",
            "p.sku",
            F.col("p.insert_date").alias("source_insert_date"),
            F.current_timestamp().alias("load_date")
        )
    )