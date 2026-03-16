import dlt 
from pyspark.sql.functions import * 


#GOLD STREAMING VIEW on TOP OF SILVER VIEW NOT ON TABLE

@dlt.view (
    name = "customers_gold_view"
)


def sales_gold_view():
    df = spark.readStream.table("customers_silver_view")
    return df


#CREATE FACT TABLE USING DLT.AUTO_CDC_FLOW

dlt.create_streaming_table(
    name = "dim_customers"
)

dlt.create_auto_cdc_flow(
    target = "dim_customers",
    source = "customers_gold_view",
    keys = ["customer_id"],
    sequence_by = col("processed_date"),
    stored_as_scd_type = 2
)