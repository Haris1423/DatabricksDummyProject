import dlt 
from pyspark.sql.functions import * 


#GOLD STREAMING VIEW on TOP OF SILVER VIEW NOT ON TABLE

@dlt.view (
    name = "products_gold_view"
)


def sales_gold_view():
    df = spark.readStream.table("products_silver_view")
    return df


#CREATE FACT TABLE USING DLT.AUTO_CDC_FLOW

dlt.create_streaming_table(
    name = "dim_products"
)

dlt.create_auto_cdc_flow(
    target = "dim_products",
    source = "products_gold_view",
    keys = ["product_id"],
    sequence_by = col("processed_date"),
    stored_as_scd_type = 2
)