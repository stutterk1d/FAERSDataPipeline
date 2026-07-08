# FAERS Data Dictionary

## Source Files (per quarterly release)

| File | Description | Key Columns |
|------|-------------|-------------|
| DEMO | One row per adverse event report | `primaryid` (PK), `caseid`, `caseversion`, `age`, `sex`, `reporter_country` |
| DRUG | One+ rows per report (each drug involved) | `primaryid` (FK), `drug_seq`, `role_cod` (PS/SS/C/I), `drugname`, `prod_ai` |
| REAC | One+ rows per report (each reaction) | `primaryid` (FK), `pt` (MedDRA Preferred Term) |
| OUTC | Zero+ rows per report (outcomes) | `primaryid` (FK), `outc_cod` (DE/LT/HO/DS/CA/RI/OT) |
| RPSR | Zero+ rows per report (report sources) | `primaryid` (FK), `rpsr_cod` |
| THER | Zero+ rows per report (therapy dates) | `primaryid` (FK), `dsg_drug_seq` (FK to DRUG), `start_dt`, `end_dt` |
| INDI | Zero+ rows per report (indications) | `primaryid` (FK), `indi_drug_seq` (FK to DRUG), `indi_pt` |

## Key Relationships

```
DEMO (1) ──< (1+) DRUG    [via primaryid]
DEMO (1) ──< (1+) REAC    [via primaryid]
DEMO (1) ──< (0+) OUTC    [via primaryid]
DEMO (1) ──< (0+) RPSR    [via primaryid]
DRUG (1) ──< (0+) THER    [via primaryid + drug_seq]
DRUG (1) ──< (0+) INDI    [via primaryid + drug_seq]
```

## Deduplication Logic

- Each `caseid` can have multiple versions (`caseversion`)
- `primaryid` = `caseid` concatenated with `caseversion`
- **Keep only the highest `caseversion` per `caseid`** (most recent follow-up)

## Code Lookups

**role_cod (Drug Role):** PS = Primary Suspect, SS = Secondary Suspect, C = Concomitant, I = Interacting

**outc_cod (Outcome):** DE = Death, LT = Life-Threatening, HO = Hospitalization, DS = Disability, CA = Congenital Anomaly, RI = Required Intervention, OT = Other Serious

**sex:** M = Male, F = Female, UNK = Unknown, NS = Not Specified

**age_cod:** YR = Years, MON = Months, WK = Weeks, DY = Days, HR = Hours, DEC = Decades

## Signal Detection Thresholds (Evans' Criteria)

| Metric | Threshold | Meaning |
|--------|-----------|---------|
| PRR | >= 2.0 | Drug-event pair reported >=2x more than expected |
| chi-squared | >= 4.0 | Statistically significant (~p <= 0.05) |
| N (cell a) | >= 3 | At least 3 reports of the drug-event pair |
