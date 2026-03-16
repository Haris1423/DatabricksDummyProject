import dlt 
from pyspark.sql.functions import * 


#GOLD STREAMING VIEW on TOP OF SILVER VIEW NOT ON TABLE

@dlt.view (
    name = "stores_gold_view"
)


def stores_gold_view():
    df = spark.readStream.table("stores_silver_view")
    return df


#CREATE FACT TABLE USING DLT.AUTO_CDC_FLOW

dlt.create_streaming_table(
    name = "dim_stores"
)

dlt.create_auto_cdc_flow(
    target = "dim_stores",
    source = "stores_gold_view",
    keys = ["store_id"],
    sequence_by = col("processed_date"),
    stored_as_scd_type = 2,
    except_column_list= ["processed_date"]
)