import dlt

@dlt.table (
    name = 'demo_table_dab'
)


def demo_table_dab():
    return spark.range(100)