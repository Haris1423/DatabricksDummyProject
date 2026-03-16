import dlt 
from pyspark.sql.functions import *

# STAGING TABLE 
@dlt.table(
    name="scd3_stg"
)
def scd3_stg():
    df = spark.readStream.table("databricksharis.bronze.source")
    return df

# CREATE AN EMPTY STREAMING TABLE 
dlt.create_streaming_table(
    name="scd3_table"
)

# scd3 running after that except_column_list otherwise it creates multiple entries for existing du to different process date

dlt.create_auto_cdc_flow(
    target="scd3_table",
    source="scd3_stg",
    keys=["product_id"],
    sequence_by=col("processdate"),
    stored_as_scd_type=2,
    except_column_list= ['processdate']
)