# from pyspark import pipelines as dp
import dlt

# create streaming table
dlt.create_streaming_table(
    name="append_table"
)

@dlt.append_flow(
    target="append_table"
)
def flow1():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/databricksharis/bronze/auto_vol/flow1/")
    )


@dlt.append_flow(
    target="append_table"
)
def flow2():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/databricksharis/bronze/auto_vol/flow2/")
    )