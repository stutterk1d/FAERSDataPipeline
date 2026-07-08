import os
from pyspark.sql import SparkSession


def get_spark(app_name="FAERS-Pipeline"):
    on_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
    if on_databricks:
        return SparkSession.builder.appName(app_name).getOrCreate()

    from delta import configure_spark_with_delta_pip
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def show_df(df, n=20):
    try:
        display(df)
    except Exception:
        return df.limit(n).toPandas()
