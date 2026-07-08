from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import Window
import os


def merge_demo_silver(spark, new_demo_df, silver_path):
    delta_path = os.path.join(silver_path, "demo")

    window = Window.partitionBy("caseid").orderBy(
        F.col("caseversion").desc(),
        F.col("fda_dt").desc(),
        F.col("primaryid").desc()
    )
    source = (new_demo_df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    source_count = source.count()

    if not DeltaTable.isDeltaTable(spark, delta_path):
        source.write.format("delta").mode("overwrite").save(delta_path)
        return {"inserted": source_count, "updated": 0, "source_rows": source_count}

    delta_table = DeltaTable.forPath(spark, delta_path)
    before_count = spark.read.format("delta").load(delta_path).count()

    delta_table.alias("target").merge(
        source.alias("source"),
        "target.caseid = source.caseid"
    ).whenMatchedUpdateAll(
        condition="source.caseversion > target.caseversion"
    ).whenNotMatchedInsertAll(
    ).execute()

    after_count = spark.read.format("delta").load(delta_path).count()
    inserted = after_count - before_count
    updated = source_count - inserted

    return {"inserted": inserted, "updated": updated, "source_rows": source_count}


def merge_child_table(spark, new_df, silver_path, table_name, demo_silver_path):
    delta_path = os.path.join(silver_path, table_name)

    valid_ids = spark.read.format("delta").load(demo_silver_path) \
        .select("primaryid")
    new_valid = new_df.join(valid_ids, on="primaryid", how="inner")

    if not DeltaTable.isDeltaTable(spark, delta_path):
        new_valid.write.format("delta").mode("overwrite").save(delta_path)
        return {"written": new_valid.count()}

    new_primaryids = new_valid.select("primaryid").distinct()
    delta_table = DeltaTable.forPath(spark, delta_path)
    new_id_list = [row.primaryid for row in new_primaryids.collect()]

    if new_id_list:
        batch_size = 10000
        for i in range(0, len(new_id_list), batch_size):
            batch = new_id_list[i:i + batch_size]
            delta_table.delete(F.col("primaryid").isin(batch))

    new_valid.write.format("delta").mode("append").save(delta_path)
    return {"written": new_valid.count()}
