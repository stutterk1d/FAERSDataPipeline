from pyspark.sql import functions as F
from pyspark.sql import Window


def transform_demo_silver(demo_bronze):
    df = demo_bronze.drop("_source_file", "_ingestion_ts")
    df = df.filter(F.col("primaryid").isNotNull() & (F.col("primaryid") != ""))

    df = (df
        .withColumn("caseversion", F.col("caseversion").cast("int"))
        .withColumn("age", F.col("age").cast("double"))
        .withColumn("wt", F.col("wt").cast("double"))
    )

    df = df.withColumn("event_dt_parsed",
        F.when(F.length("event_dt") == 8, F.to_date("event_dt", "yyyyMMdd"))
         .when(F.length("event_dt") == 6,
               F.to_date(F.concat("event_dt", F.lit("01")), "yyyyMMdd"))
         .when(F.length("event_dt") == 4,
               F.to_date(F.concat("event_dt", F.lit("0101")), "yyyyMMdd"))
         .otherwise(F.lit(None).cast("date"))
    )

    df = df.withColumn("event_dt_parsed",
        F.when(
            (F.col("event_dt_parsed") >= F.lit("1900-01-01").cast("date")) &
            (F.col("event_dt_parsed") <= F.current_date()),
            F.col("event_dt_parsed")
        ).otherwise(F.lit(None).cast("date"))
    )

    df = df.withColumn("age_in_years",
        F.when(F.col("age_cod") == "YR", F.col("age"))
         .when(F.col("age_cod") == "DEC", F.col("age") * 10)
         .when(F.col("age_cod") == "MON", F.col("age") / 12)
         .when(F.col("age_cod") == "WK", F.col("age") / 52)
         .when(F.col("age_cod") == "DY", F.col("age") / 365.25)
         .when(F.col("age_cod") == "HR", F.col("age") / 8766)
         .otherwise(F.lit(None).cast("double"))
    )
    df = df.withColumn("age_in_years",
        F.when((F.col("age_in_years") >= 0) & (F.col("age_in_years") <= 120),
               F.round("age_in_years", 1))
         .otherwise(F.lit(None).cast("double"))
    )

    df = df.withColumn("sex",
        F.when(F.col("sex").isin("M", "F"), F.col("sex"))
         .when(F.col("sex") == "UNK", "UNK")
         .when(F.col("sex") == "NS", "NS")
         .otherwise("UNK")
    )

    for col_name in ["reporter_country", "occr_country", "mfr_sndr", "occp_cod"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.trim(F.col(col_name)))

    window = Window.partitionBy("caseid").orderBy(
        F.col("caseversion").desc(),
        F.col("fda_dt").desc(),
        F.col("primaryid").desc()
    )
    df = (df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return df


def transform_drug_silver(drug_bronze):
    df = drug_bronze.drop("_source_file", "_ingestion_ts")
    df = df.filter(F.col("primaryid").isNotNull() & (F.col("primaryid") != ""))
    df = (df
        .withColumn("drugname", F.upper(F.trim(F.col("drugname"))))
        .withColumn("prod_ai", F.upper(F.trim(F.col("prod_ai"))))
    )
    df = df.withColumn("drug_seq", F.col("drug_seq").cast("int"))
    df = df.withColumn("role_cod", F.upper(F.trim(F.col("role_cod"))))
    for col_name in ["route", "dose_form", "dose_freq"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.trim(F.col(col_name)))
    return df


def transform_reac_silver(reac_bronze):
    df = reac_bronze.drop("_source_file", "_ingestion_ts")
    df = df.filter(
        F.col("primaryid").isNotNull() & (F.col("primaryid") != "") &
        F.col("pt").isNotNull() & (F.col("pt") != "")
    )
    df = df.withColumn("pt", F.upper(F.trim(F.col("pt"))))
    return df


def transform_outc_silver(outc_bronze):
    df = outc_bronze.drop("_source_file", "_ingestion_ts")
    df = df.filter(F.col("primaryid").isNotNull() & (F.col("primaryid") != ""))
    df = df.withColumn("outc_cod", F.upper(F.trim(F.col("outc_cod"))))
    return df


def transform_rpsr_silver(rpsr_bronze):
    df = rpsr_bronze.drop("_source_file", "_ingestion_ts")
    df = df.filter(F.col("primaryid").isNotNull() & (F.col("primaryid") != ""))
    df = df.withColumn("rpsr_cod", F.upper(F.trim(F.col("rpsr_cod"))))
    return df


def transform_ther_silver(ther_bronze):
    df = ther_bronze.drop("_source_file", "_ingestion_ts")
    df = df.filter(F.col("primaryid").isNotNull() & (F.col("primaryid") != ""))
    df = df.withColumn("dsg_drug_seq", F.col("dsg_drug_seq").cast("int"))
    for date_col in ["start_dt", "end_dt"]:
        if date_col in df.columns:
            df = df.withColumn(f"{date_col}_parsed",
                F.when(F.length(date_col) == 8, F.to_date(date_col, "yyyyMMdd"))
                 .when(F.length(date_col) == 6,
                       F.to_date(F.concat(F.col(date_col), F.lit("01")), "yyyyMMdd"))
                 .when(F.length(date_col) == 4,
                       F.to_date(F.concat(F.col(date_col), F.lit("0101")), "yyyyMMdd"))
                 .otherwise(F.lit(None).cast("date"))
            )
            df = df.withColumn(f"{date_col}_parsed",
                F.when(
                    (F.col(f"{date_col}_parsed") >= F.lit("1900-01-01").cast("date")) &
                    (F.col(f"{date_col}_parsed") <= F.current_date()),
                    F.col(f"{date_col}_parsed")
                ).otherwise(F.lit(None).cast("date"))
            )
    return df


def transform_indi_silver(indi_bronze):
    df = indi_bronze.drop("_source_file", "_ingestion_ts")
    df = df.filter(F.col("primaryid").isNotNull() & (F.col("primaryid") != ""))
    df = df.withColumn("indi_drug_seq", F.col("indi_drug_seq").cast("int"))
    df = df.withColumn("indi_pt", F.upper(F.trim(F.col("indi_pt"))))
    return df


def filter_to_valid_ids(demo_silver, child_df):
    valid_ids = demo_silver.select("primaryid")
    return child_df.join(valid_ids, on="primaryid", how="inner")
