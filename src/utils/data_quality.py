from pyspark.sql import functions as F
from datetime import datetime


def run_faers_dq_checks(spark, demo_df, drug_df, reac_df, outc_df,
                         ther_df, indi_df, rpsr_df, run_label="unknown"):
    ts = datetime.now().isoformat()
    checks = []

    null_checks = [
        ("DEMO", demo_df, "primaryid", "critical"),
        ("DEMO", demo_df, "caseid", "critical"),
        ("DEMO", demo_df, "caseversion", "critical"),
        ("DEMO", demo_df, "sex", "expected"),
        ("DEMO", demo_df, "age", "expected"),
        ("DEMO", demo_df, "event_dt", "expected"),
        ("DEMO", demo_df, "reporter_country", "expected"),
        ("DRUG", drug_df, "primaryid", "critical"),
        ("DRUG", drug_df, "drugname", "critical"),
        ("DRUG", drug_df, "role_cod", "critical"),
        ("REAC", reac_df, "primaryid", "critical"),
        ("REAC", reac_df, "pt", "critical"),
    ]

    for tbl_name, df, col_name, criticality in null_checks:
        total = df.count()
        nulls = df.filter(F.col(col_name).isNull() | (F.col(col_name) == "")).count()
        pct = round(nulls / total * 100, 2) if total > 0 else 0
        if criticality == "critical":
            result = "PASS" if nulls == 0 else "FAIL"
        else:
            result = "PASS" if pct == 0 else ("WARN" if pct < 50 else "FAIL")
        checks.append((f"null_{col_name}", tbl_name,
                        f"{nulls:,} nulls ({pct}%)", result, ts, run_label))

    invalid_sex = demo_df.filter(
        F.col("sex").isNotNull() & ~F.col("sex").isin(["M", "F", "UNK", "NS"])
    ).count()
    checks.append(("domain_sex", "DEMO", f"{invalid_sex:,} invalid values",
                    "PASS" if invalid_sex == 0 else "FAIL", ts, run_label))

    invalid_role = drug_df.filter(
        F.col("role_cod").isNotNull() & ~F.col("role_cod").isin(["PS", "SS", "C", "I"])
    ).count()
    checks.append(("domain_role_cod", "DRUG", f"{invalid_role:,} invalid values",
                    "PASS" if invalid_role == 0 else "FAIL", ts, run_label))

    valid_outcomes = ["DE", "LT", "HO", "DS", "CA", "RI", "OT"]
    invalid_outc = outc_df.filter(
        F.col("outc_cod").isNotNull() & ~F.col("outc_cod").isin(valid_outcomes)
    ).count()
    checks.append(("domain_outc_cod", "OUTC", f"{invalid_outc:,} invalid values",
                    "PASS" if invalid_outc == 0 else "FAIL", ts, run_label))

    invalid_age = demo_df.filter(
        F.col("age_in_years").isNotNull() &
        ((F.col("age_in_years") < 0) | (F.col("age_in_years") > 120))
    ).count()
    checks.append(("range_age_in_years", "DEMO",
                    f"{invalid_age:,} out of range (0-120)",
                    "PASS" if invalid_age == 0 else "WARN", ts, run_label))

    demo_ids = demo_df.select("primaryid").distinct()
    for child_name, child_df in [("DRUG", drug_df), ("REAC", reac_df),
                                   ("OUTC", outc_df), ("RPSR", rpsr_df),
                                   ("THER", ther_df), ("INDI", indi_df)]:
        orphans = child_df.select("primaryid").distinct().subtract(demo_ids).count()
        checks.append((f"ref_integrity_{child_name}", child_name,
                        f"{orphans:,} orphan primaryids",
                        "PASS" if orphans == 0 else "FAIL", ts, run_label))

    demo_total = demo_df.count()
    demo_unique_pid = demo_df.select("primaryid").distinct().count()
    checks.append(("duplicate_primaryid", "DEMO",
                    f"{demo_total - demo_unique_pid:,} duplicate primaryids",
                    "PASS" if demo_total == demo_unique_pid else "FAIL", ts, run_label))

    demo_unique_case = demo_df.select("caseid").distinct().count()
    checks.append(("duplicate_caseid", "DEMO",
                    f"{demo_total - demo_unique_case:,} duplicate caseids",
                    "PASS" if demo_total == demo_unique_case else "WARN", ts, run_label))

    for tbl_name, df in [("DEMO", demo_df), ("DRUG", drug_df),
                          ("REAC", reac_df), ("OUTC", outc_df),
                          ("THER", ther_df), ("INDI", indi_df)]:
        checks.append(("row_count", tbl_name, f"{df.count():,} rows",
                        "PASS", ts, run_label))

    schema = ["check_name", "table_name", "metric", "result", "timestamp", "run_label"]
    return spark.createDataFrame(checks, schema=schema)
