import dlt 
from pyspark.sql.functions import * 


#GOLD STREAMING VIEW on TOP OF SILVER VIEW NOT ON TABLE

@dlt.view (
    name = "sales_gold_view"
)


def sales_gold_view():
    df = spark.readStream.table("sales_silver_view")
    return df


#CREATE FACT TABLE USING DLT.AUTO_CDC_FLOW

dlt.create_streaming_table(
    name = "Fact_sales"
)

dlt.create_auto_cdc_flow(
    target = "Fact_sales",
    source = "sales_gold_view",
    keys = ["sales_id"],
    sequence_by = col("processed_date"),
    stored_as_scd_type = 1
)