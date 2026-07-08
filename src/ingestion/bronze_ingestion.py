import os
import glob
from pyspark.sql.functions import lit, current_timestamp


FILE_TYPES = {
    "DEMO": "DEMO*.txt",
    "DRUG": "DRUG*.txt",
    "REAC": "REAC*.txt",
    "OUTC": "OUTC*.txt",
    "RPSR": "RPSR*.txt",
    "THER": "THER*.txt",
    "INDI": "INDI*.txt",
}


def read_faers_raw(spark, filepath):
    return spark.read.csv(
        filepath,
        sep="$",
        header=True,
        inferSchema=False,
        encoding="ISO-8859-1",
        multiLine=True,
        mode="PERMISSIVE",
    )


def ingest_to_bronze(spark, raw_dir, bronze_dir, file_type, year, quarter):
    pattern = FILE_TYPES[file_type]
    matches = glob.glob(os.path.join(raw_dir, pattern))
    if not matches:
        print(f"  warning: {file_type}: no file matching {pattern}")
        return 0

    filepath = matches[0]
    filename = os.path.basename(filepath)
    df = read_faers_raw(spark, filepath)

    df_bronze = (df
        .withColumn("_source_file", lit(filename))
        .withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_year", lit(year))
        .withColumn("_quarter", lit(quarter))
    )

    output_path = os.path.join(bronze_dir, file_type.lower())
    (df_bronze.write
        .format("delta")
        .mode("append")
        .partitionBy("_year", "_quarter")
        .save(output_path))

    row_count = df_bronze.count()
    print(f"  done: {file_type}: {row_count:,} rows from {filename}")
    return row_count


def ingest_quarter_to_bronze(spark, project_root, year, quarter_num):
    quarter_label = f"Q{quarter_num}"
    raw_dir = os.path.join(project_root, f"data/raw/{year}q{quarter_num}")
    bronze_dir = os.path.join(project_root, "data/bronze")
    if not os.path.exists(raw_dir):
        print(f"warning: Raw directory not found: {raw_dir}")
        return {}

    print(f"Ingesting {year} {quarter_label}")
    results = {}
    for file_type in FILE_TYPES:
        count = ingest_to_bronze(spark, raw_dir, bronze_dir,
                                  file_type, year, quarter_label)
        results[file_type] = count

    total = sum(results.values())
    print(f"  Total: {total:,} rows for {year} {quarter_label}")
    return results
