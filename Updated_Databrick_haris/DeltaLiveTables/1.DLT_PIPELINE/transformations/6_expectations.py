import dlt

#parametrize , define parameters in setting > add configurations
table_name = spark.conf.get("tablename")

expectations = {
    "rule1" : "product_id is not null",
    "rule2" : "category is not null"
}

@dlt.table (
    name = "expect_table"
)

#by default it's warn
@dlt.expect_all_or_drop(expectations)
def expect_table ():
    df = spark.read.table(f"databricksharis.silver.{table_name}")
    return df