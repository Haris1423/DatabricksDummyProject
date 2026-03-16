import dlt
from pyspark.sql.functions import * 


#STREAMING VIEW will be source for SCD Streaming Table
@dlt.view(
    name = "customers_silver_view"
)

def customers_silver_view():
    df_customers = spark.readStream.table("customers_bronze")
    df_customers = df_customers.withColumn("name", upper(col("name")))
    df_customers = df_customers.withColumn("domain", split(col("email"),'@')[1])
    df_customers = df_customers.withColumn("processed_date", current_timestamp())
    return df_customers

#SILVER TABLE WITH UPSERT 

#CREATE A EMPTY STREAMING TABLE FOR UPSERT

dlt.create_streaming_table(
    name = "customers_silver"
)

dlt.create_auto_cdc_flow (
    target = "customers_silver",
    source = "customers_silver_view",
    keys = ["customer_id"],
    sequence_by= col("processed_date"),
    stored_as_scd_type = 1
)