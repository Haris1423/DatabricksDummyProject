#CREATE A STREAMING TABLE ---- STREAMING TABLE : reads data incrementally, we have to use append only source

import dlt 
from pyspark.sql.functions import *

#raw
@dlt.table (
    name = 'sales_stg'  # this will be created in the catalog
)

def sales_stg():
    df = spark.readStream.option("skipChangeCommits", True)\
        .table("databricksharis.silver.sales_enr")
    return df

#Streaming View should have a source of Streaming Table or View 

#create a Mat view 

#enriched
@dlt.table(
    name = 'sales_trn'
)

def sales_trn():
    df = spark.read.table('sales_stg')
    df = df.withColumn("price_after_discount",col("total_amount")-col("discount"))
    return df

#create another curated  mat view

#curated 

@dlt.table(
    name = 'sales_cur'
)
def sales_cur():
    df = spark.read.table("sales_trn")
    return df




