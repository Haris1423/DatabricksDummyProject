#delete batch3 file from autovol/raw folder
#we have a source raw folder> create a streaming table > create view for transformations
import dlt
from pyspark.sql.functions import * 


#CREATE STREAMING TABLE (USING AUTOLOADER)
@dlt.table (
    name = "auto_vol"
)

 # .option("cloudFiles.schemaLocation", "dbfs:/FileStore/tables/schema")\ np schema location
   
def auto_val():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .load("/Volumes/databricksharis/bronze/auto_vol/raw/")
    return df



#create a materialized view 

@dlt.table (
    name = "auto_vol_enr"
)

def auto_val_enr():
    df = spark.read.table("auto_vol")
    df = df.withColumn("Flag", lit("Yes"))
    return df
