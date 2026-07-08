from pyspark.sql import functions as F


def build_drug_event_pairs(drug_df, reac_df):
    suspect_drugs = drug_df.filter(F.col("role_cod") == "PS") \
        .select("primaryid", "drugname")
    return suspect_drugs.join(
        reac_df.select("primaryid", "pt"),
        on="primaryid"
    ).select("primaryid", "drugname", "pt").distinct()


def compute_disproportionality(spark, drug_event_pairs):
    N = drug_event_pairs.select("primaryid").distinct().count()

    drug_event_counts = drug_event_pairs.groupBy("drugname", "pt") \
        .agg(F.countDistinct("primaryid").alias("a"))
    drug_totals = drug_event_pairs.groupBy("drugname") \
        .agg(F.countDistinct("primaryid").alias("drug_total"))
    event_totals = drug_event_pairs.groupBy("pt") \
        .agg(F.countDistinct("primaryid").alias("event_total"))

    contingency = (drug_event_counts
        .join(drug_totals, on="drugname")
        .join(event_totals, on="pt")
        .withColumn("b", F.col("drug_total") - F.col("a"))
        .withColumn("c", F.col("event_total") - F.col("a"))
        .withColumn("d", F.lit(N) - F.col("a") - F.col("b") - F.col("c"))
        .withColumn("N", F.lit(N))
    )

    for c in ["a", "b", "c", "d", "N"]:
        contingency = contingency.withColumn(c, F.col(c).cast("double"))

    contingency = (contingency
        .withColumn("_needs_correction",
            (F.col("a") == 0) | (F.col("b") == 0) |
            (F.col("c") == 0) | (F.col("d") == 0))
        .withColumn("a_adj",
            F.when(F.col("_needs_correction"), F.col("a") + 0.5).otherwise(F.col("a")))
        .withColumn("b_adj",
            F.when(F.col("_needs_correction"), F.col("b") + 0.5).otherwise(F.col("b")))
        .withColumn("c_adj",
            F.when(F.col("_needs_correction"), F.col("c") + 0.5).otherwise(F.col("c")))
        .withColumn("d_adj",
            F.when(F.col("_needs_correction"), F.col("d") + 0.5).otherwise(F.col("d")))
    )

    contingency = contingency.withColumn("PRR",
        (F.col("a_adj") / (F.col("a_adj") + F.col("b_adj"))) /
        (F.col("c_adj") / (F.col("c_adj") + F.col("d_adj")))
    )
    contingency = contingency.withColumn("ROR",
        (F.col("a_adj") * F.col("d_adj")) / (F.col("b_adj") * F.col("c_adj"))
    )
    contingency = contingency.withColumn("chi2",
        (F.col("N") *
         F.pow(
             F.abs(F.col("a") * F.col("d") - F.col("b") * F.col("c")) - F.col("N") / 2,
             2
         )) /
        ((F.col("a") + F.col("b")) *
         (F.col("c") + F.col("d")) *
         (F.col("a") + F.col("c")) *
         (F.col("b") + F.col("d")))
    )
    contingency = (contingency
        .withColumn("SE_ln_ROR",
            F.sqrt(1 / F.col("a_adj") + 1 / F.col("b_adj") +
                   1 / F.col("c_adj") + 1 / F.col("d_adj")))
        .withColumn("ROR_lower",
            F.exp(F.log(F.col("ROR")) - 1.96 * F.col("SE_ln_ROR")))
        .withColumn("ROR_upper",
            F.exp(F.log(F.col("ROR")) + 1.96 * F.col("SE_ln_ROR")))
    )
    contingency = contingency.withColumn("is_signal",
        (F.col("PRR") >= 2) & (F.col("chi2") >= 4) & (F.col("a") >= 3)
    )
    return contingency
