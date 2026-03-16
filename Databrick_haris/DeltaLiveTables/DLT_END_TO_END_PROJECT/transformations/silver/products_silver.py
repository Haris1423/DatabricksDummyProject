import dlt
from pyspark.sql.functions import * 


#STREAMING VIEW will be source for SCD Streaming Table
@dlt.view(
    name = "products_silver_view"
)

def products_silver_view():
    df_products = spark.readStream.table("products_bronze")
    df_products = df_products.withColumn("processed_date", current_timestamp())
    return df_products

#SILVER TABLE WITH UPSERT 

#CREATE A EMPTY STREAMING TABLE FOR UPSERT

dlt.create_streaming_table(
    name = "products_silver"
)

dlt.create_auto_cdc_flow (
    target = "products_silver",
    source = "products_silver_view",
    keys = ["product_id"],
    sequence_by= col("processed_date"),
    stored_as_scd_type = 1
)