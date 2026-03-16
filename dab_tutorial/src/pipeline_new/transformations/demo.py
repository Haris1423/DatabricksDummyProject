import dlt 

@dlt.table (
    name = "demo_table_dab_new"
)

def demo_table_dab_new():
  return spark.range(100)