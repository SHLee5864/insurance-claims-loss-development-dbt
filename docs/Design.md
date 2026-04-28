
# Motor Insurance Claims Development & Loss Forecasting Pipeline
## Complete Project Documentation

**Version:** 1.0  
**Author:** SukHee Lee  
**Date:** April 2026  
**Stack:** dbt + DuckDB (local) / Databricks (production)

---

# Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Business Context & Requirements](#2-business-context--requirements)
3. [Actuarial Foundations](#3-actuarial-foundations)
4. [Data Generation (generate.py)](#4-data-generation-generatepy)
5. [Pipeline Architecture](#5-pipeline-architecture)
6. [RAW Layer — Seed Tables](#6-raw-layer--seed-tables)
7. [STG Layer — Staging Models](#7-stg-layer--staging-models)
8. [INT Layer — Intermediate Models](#8-int-layer--intermediate-models)
9. [MART Layer — Analytical Models](#9-mart-layer--analytical-models)
10. [Testing Strategy](#10-testing-strategy)
11. [Databricks Deployment](#11-databricks-deployment)
12. [Key Design Decisions](#12-key-design-decisions)
13. [Simplifications vs. Production](#13-simplifications-vs-production)
14. [Appendix](#14-appendix)

---

# 1. Executive Summary

This project implements a production-grade data pipeline for actuarial loss reserving in motor (auto) insurance. It transforms raw insurance data — policies, claims, payments, and reserves — into the core analytical outputs used by actuaries, pricing teams, and finance departments:

- **Loss Development Triangles** — matrices showing how claims costs evolve over time
- **Loss Development Factors (LDFs)** — growth rates between development periods
- **Ultimate Loss Estimates** — projected total claim costs including future payments
- **Loss Ratios** — profitability metrics comparing losses to premiums

The pipeline is built with **dbt** (data build tool) for transformation logic and runs on both **DuckDB** (local development) and **Databricks** (production). All data is synthetically generated using actuarially realistic distributions.

### Who Uses These Outputs?

| Team | What They Need | Which MART Model |
|------|---------------|-----------------|
| Reserving / Actuarial | Ultimate loss estimates, triangle development patterns | mart_incurred_triangle, mart_ldf, mart_ultimate_loss |
| Pricing | Loss ratios by accident year to adjust premium rates | mart_loss_ratio_by_ay |
| Risk Management | Portfolio loss trends, exposure concentration | mart_exposure_alignment |
| Finance | Period-end loss ratio reporting | mart_loss_ratio_by_ay, mart_ultimate_loss |
| Regulatory (IFRS 17) | Best Estimate Liability inputs, reserve adequacy checks | mart_incurred_triangle, mart_ultimate_loss |

---

# 2. Business Context & Requirements

## 2.1 The Business Problem

When someone has a car accident, the insurance company pays for the damage. But claims don't close instantly — some take months or years to fully settle, especially bodily injury claims involving medical treatment or litigation.

At any point in time, the insurer needs to answer: **"How much will we ultimately pay for all the accidents that have already happened?"**

This is called **loss reserving**, and it's one of the most critical functions in an insurance company because:

- **Too little reserve** → the company appears profitable but is actually insolvent
- **Too much reserve** → capital is locked up unnecessarily, reducing returns to shareholders
- **Regulators require it** → IFRS 17, Solvency II, and local regulations mandate rigorous reserving

## 2.2 Project Scope

### In Scope

- Paid loss development tracking (actual cash payments over time)
- Incurred loss development tracking (payments + outstanding reserves)
- Loss development triangle construction at Accident Year × Development Year grain
- Loss Development Factor (LDF) calculation using the chain-ladder method
- Ultimate loss estimation per accident year
- Loss ratio calculation (ultimate loss / earned premium)
- Exposure alignment by accident year and region
- Macro-economic effects modeling (COVID, weather, inflation, travel boom)
- Synthetic data generation with actuarially realistic distributions

### Out of Scope

- IFRS 17 cashflow projection models (covered in Medium-2 project)
- GLM / Machine Learning pricing models
- Real-time streaming data processing
- Reinsurance recovery logic (covered in Medium-3 project)
- Tail factor estimation beyond 10 development years
- Confidence intervals (Mack's Method — covered in Large project)

## 2.3 Key Business Rules

**Accident Year (AY):** `accident_year = EXTRACT(YEAR FROM accident_date)`. All analysis is organized by when the accident happened, not when the claim was reported or paid.

**Development Year (dev_year):** `dev_year = valuation_year - accident_year + 1`. Uses 1-based indexing (actuarial standard). Dev_year 1 means the accident year itself. Dev_year 2 means one year after the accident.

**Incurred Loss:** `incurred = paid_to_date + case_reserve + expense_reserve`. The total estimated cost of a claim at any point in time.

**Loss Development Factor:** `LDF(d) = AVG(incurred[d+1] / incurred[d])` averaged across all accident years. Measures how much incurred grows from one development year to the next.

**Ultimate Loss:** `ultimate_loss = latest_incurred × cumulative_LDF`. Projects the final total cost by applying remaining development factors.

**Loss Ratio:** `loss_ratio = ultimate_loss / earned_premium`. The core profitability metric. Below 70% is generally profitable for motor insurance; above 100% means claims exceed premiums.

## 2.4 Target Segments

The synthetic data models a **corporate motor insurance portfolio** with:

- **Loss Types:** Collision (60%), Bodily Injury (20%), Glass (15%), Theft (5%)
- **Regions:** North, South, East, West (uniform distribution)
- **Policy Period:** 2015–2024 (10 accident years)
- **Portfolio Size:** ~2,200 policies, ~1,400 claims

---

# 3. Actuarial Foundations

This section explains the actuarial concepts implemented in the pipeline. Understanding these is essential for reading the code, interpreting results, and explaining the project in interviews.

## 3.1 What is a Loss Development Triangle?

A loss development triangle is the fundamental tool in actuarial reserving. It's a matrix that shows how the estimated cost of claims changes over time.

**Structure:**
- **Rows** = Accident Years (when the loss occurred)
- **Columns** = Development Years (how much time has passed since the accident)
- **Cells** = Cumulative incurred amount at that point

**Example (simplified, amounts in thousands):**

| AY \ Dev Year | 1 | 2 | 3 | 4 | 5 |
|---|---|---|---|---|---|
| 2020 | 1,000 | 1,500 | 1,700 | 1,750 | 1,760 |
| 2021 | 1,100 | 1,600 | 1,800 | 1,850 | ? |
| 2022 | 900 | 1,400 | 1,600 | ? | ? |
| 2023 | 1,200 | 1,700 | ? | ? | ? |
| 2024 | 1,050 | ? | ? | ? | ? |

The empty cells in the lower-right corner represent the unknown future — the purpose of reserving is to fill them.

**Why does incurred increase over time?**

Early in a claim's life, the insurer has limited information. Initial reserve estimates tend to be conservative (underestimated). As more information becomes available — medical reports, legal assessments, repair costs — the estimate converges toward the true cost. This generally upward pattern is what makes the chain-ladder method work.

However, real-world triangles can also show reserve releases (incurred decreasing) when initial estimates turn out to be too high.

## 3.2 Incurred Loss

Incurred loss is the total economic cost of a claim as estimated at a given point in time:

```
Incurred = Paid-to-date + Outstanding Reserve
```

- **Paid-to-date:** actual cash payments already made to the claimant
- **Outstanding Reserve (case reserve):** the actuary's estimate of how much more will be paid
- **Expense reserve:** allocated loss adjustment expenses (typically 10% of case reserve in this model)

As claims develop:
- Paid increases (more payments are made)
- Reserve decreases (less remains to be paid)
- Incurred should converge to the true ultimate cost

## 3.3 Paid Triangle vs. Incurred Triangle

Both triangles have the same structure but serve different purposes:

**Paid Triangle:**
- Contains only actual cash payments — no estimation involved
- Useful for cash flow forecasting and payment pattern analysis
- More "factual" but slower to reflect ultimate cost (payments lag behind actual liability)

**Incurred Triangle:**
- Contains payments + reserve estimates — incorporates the actuary's best judgment
- Better predictor of ultimate cost because it includes known future obligations
- Used as the primary input for chain-ladder analysis in this pipeline
- Quality depends on reserve accuracy — bad reserves produce bad LDFs

## 3.4 Loss Development Factor (LDF)

The LDF measures how much incurred grows from one development period to the next:

```
LDF(d) = Incurred at dev_year (d+1) / Incurred at dev_year (d)
```

**Key properties:**
- **LDF > 1.0:** incurred is still growing (normal for immature claims)
- **LDF ≈ 1.0:** claims are mature, very little change expected
- **LDF < 1.0:** incurred is decreasing — may indicate reserve releases or data issues

In this pipeline, LDF is calculated as the **simple average** across all accident years:

```
LDF(d) = AVG( Incurred[d+1] / Incurred[d] )  for all AYs that have data at both dev_year d and d+1
```

**Alternative methods (not implemented here):**
- **Volume-weighted average:** `SUM(Incurred[d+1]) / SUM(Incurred[d])` — gives more weight to larger accident years
- **Medial average:** excludes highest and lowest — reduces outlier impact
- **Bornhuetter-Ferguson:** blends chain-ladder with an a priori expected loss ratio
- **Geometric average:** appropriate when LDFs have multiplicative structure

## 3.5 Cumulative LDF (CDF)

The cumulative LDF represents the total remaining development from a given point to ultimate:

```
CDF(d) = LDF(d) × LDF(d+1) × ... × LDF(n)
```

**Example:**
- LDF(1→2) = 1.24, LDF(2→3) = 1.07, LDF(3→4) = 1.03
- CDF at dev_year 1 = 1.24 × 1.07 × 1.03 = 1.367 (36.7% more development expected)
- CDF at dev_year 3 = 1.03 (only 3% more expected)
- CDF at the most mature dev_year = 1.0 (fully developed, handled via COALESCE)

**SQL Implementation:**

SQL has no PRODUCT aggregate function, so cumulative multiplication is achieved via the log-sum-exp pattern:

```sql
CDF = EXP(SUM(LN(ldf)) OVER (
    ORDER BY dev_year_from DESC
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
))
```

This works because: `ln(a × b × c) = ln(a) + ln(b) + ln(c)`, so `a × b × c = exp(ln(a) + ln(b) + ln(c))`.

## 3.6 Ultimate Loss

Ultimate loss is the total expected cost of all claims for a given accident year:

```
Ultimate Loss = Latest Incurred × Cumulative LDF
```

**Interpretation:**
- **Immature AY (e.g., 2024, dev_year 1):** CDF is large (e.g., 1.24) → significant development expected. The pipeline projects 24% more cost on top of current incurred.
- **Mature AY (e.g., 2015, dev_year 10):** CDF = 1.0 → no further development expected. Ultimate loss equals the latest incurred amount.

This is the **Chain-Ladder method** — the simplest and most widely used actuarial reserving technique.

## 3.7 Loss Ratio

The loss ratio is the core profitability metric in insurance:

```
Loss Ratio = Ultimate Loss / Earned Premium
```

**Benchmarks (motor insurance):**
- **55–70%:** profitable (typical target)
- **70–80%:** break-even range (after expenses and commissions)
- **80%+:** loss-making before expenses
- **>100%:** technical loss — claims exceed premiums collected

In this pipeline, loss ratio is calculated at two levels:
- `mart_ultimate_loss`: one row per AY, latest valuation only (dashboard/summary)
- `mart_loss_ratio_by_ay`: all AY × valuation_year combinations (development tracking)

## 3.8 Earned Premium and Exposure

**Earned Premium** is the portion of the total premium that corresponds to the coverage period that has already elapsed. If a 12-month policy with premium €12,000 starts in July, by December only €6,000 is "earned."

**Exposure** measures the amount of risk the insurer is carrying. In motor insurance, 1 exposure unit = 1 vehicle-year. A 12-month policy = 1.0 exposure; a 6-month policy = 0.5 exposure.

In this pipeline:
- `earned_exposure = 1/12` per month (monthly fraction of annual policy)
- `earned_premium = annual_premium / 12` per month

## 3.9 Chain-Ladder Assumptions and Limitations

The chain-ladder method relies on these assumptions:

1. **Past development patterns will continue:** LDFs from historical accident years are predictive of future development
2. **Accident years are independent:** no correlation between different accident years
3. **Development is complete by the latest observed period:** no tail factor needed

**Limitations of this implementation:**
- **No tail factor:** assumes development stops at the most mature observed dev_year (10). In reality, bodily injury claims can develop for 20+ years.
- **No confidence intervals:** Mack's method provides standard errors around ultimate loss estimates. This pipeline produces point estimates only.
- **No segmentation:** single triangle for all loss types combined. Production would segment by loss_type or region.
- **Simple average LDF:** volume-weighted or Bornhuetter-Ferguson may be more appropriate for volatile portfolios.

## 3.10 Macro-Economic Effects in Insurance

Insurance claims are not generated in a vacuum — external events affect both frequency (how many claims) and severity (how much each claim costs).

This pipeline models four macro effects:

| Factor | What it Affects | How |
|--------|----------------|-----|
| COVID lockdowns | Claim frequency | Fewer people driving → 20–40% frequency reduction (2020–2021) |
| Weather events | Payment severity | Storm damage, flooding → 30–40% severity increase |
| Travel boom | Claim frequency | Post-COVID travel surge → 15% frequency increase (2022+) |
| Inflation | Payment amounts | Repair costs, medical costs rising → 2–10% annual increase |

These are modeled in `generate.py` and reflected in the `calendar` seed table with boolean flags and an inflation factor.

---

# 4. Data Generation (generate.py)

All data in this project is synthetically generated by a Python script (`generate.py`). This section documents the generation logic, distributions used, and the reasoning behind parameter choices.

## 4.1 Overview

The generator creates 7 CSV files that serve as dbt seeds:

| File | Rows | Description |
|------|------|-------------|
| raw_policy.csv | 2,200 | Insurance contracts |
| raw_exposure.csv | ~27,000 | Monthly earned exposure per policy |
| raw_claims.csv | ~1,400 | Reported claims |
| raw_payments.csv | ~3,700 | Individual payment transactions |
| raw_reserve_snapshot.csv | ~75,000 | Monthly reserve estimates per claim |
| raw_transactions.csv | ~7,000 | Claim lifecycle events |
| calendar.csv | 120 | Monthly calendar with macro flags |

## 4.2 Policy Generation

- **Count:** 2,200 policies
- **Inception dates:** uniformly distributed across 2015-01-01 to 2024-12-31
- **Premium:** log-normal distribution with mean=9.9, sigma=0.3, clipped to [5,000, 50,000]€
  - Median ≈ €19,886 — represents a fleet/corporate motor portfolio
  - Log-normal is used because premium distributions are right-skewed (few very expensive policies)
- **Region:** uniform across North, South, East, West
- **Expiration:** inception + 1 year

## 4.3 Exposure Generation

Exposure is created by cross-joining policies with the calendar:
- Each policy gets one row per active month
- `earned_exposure = 1/12` (monthly fraction)
- `earned_premium = annual_premium / 12`
- Filtered to months where `date_month BETWEEN inception_month AND expiration_month`

## 4.4 Claim Generation

Claims are generated using a **Poisson frequency model:**

```
P(claim) = BASE_RATE × covid_factor × travel_factor
```

- **BASE_RATE = 5%** — per policy-month, calibrated to motor insurance industry benchmarks
- **COVID factor:** reduces frequency during lockdown periods (0.6–0.8)
- **Travel factor:** increases frequency post-2022 (1.15)

For each generated claim:
- `accident_date = exposure_month + random(0–27) days`
- `reported_date = accident_date + random(1–13) days`
- `loss_type` drawn from: collision (60%), bodily (20%), glass (15%), theft (5%)
- `claimant_age` = random(18–79)

## 4.5 Payment Generation

Each claim gets a **total expected loss** drawn from a log-normal distribution, parameterized by loss type:

| Loss Type | μ | σ | Median (€) | Payment Count | Timing (months) |
|-----------|---|---|-----------|--------------|----------------|
| Collision | 9.2 | 0.7 | ~9,897 | 2–4 | 3–18 |
| Bodily | 10.2 | 0.8 | ~26,881 | 3–7 | 6–36 |
| Glass | 7.5 | 0.4 | ~1,808 | 1–2 | 1–6 |
| Theft | 9.0 | 0.6 | ~8,103 | 1–3 | 1–12 |

The total expected loss is split into installments using a **Dirichlet distribution** (ensures splits sum to 1.0). Each payment amount is further adjusted by:
- Weather severity factor at the payment date
- Payments beyond 2024-12-31 are excluded

**Note:** Inflation is NOT applied in generate.py. Raw payment amounts are nominal. Inflation adjustment happens in the INT layer (`int_payments_enriched`), maintaining separation of concerns between data generation and business transformation.

## 4.6 Reserve Generation — Learning Curve Model

The reserve model simulates realistic actuarial behavior where initial estimates are conservative and converge to the true ultimate over time:

```
estimate(m) = total_expected × [0.60 + 0.40 × (1 - exp(-m / 18))]
case_reserve = max(0, estimate × noise - cumulative_paid)
expense_reserve = case_reserve × 10%
```

**Parameters:**
- `INITIAL_ESTIMATE_RATIO = 0.60` — initially recognize only 60% of ultimate
- `LEARNING_TAU = 18 months` — convergence speed
- `noise = Normal(1.0, 0.03)` — ±3% random variation per snapshot

**Why this model?**

The previous version used an IBNR decay model (`reserve = remaining × (1 + IBNR × exp(-m/τ))`), which caused incurred to decrease over time (LDF < 1). This is actuarially unrealistic — incurred should generally increase as more information becomes available.

The learning curve model produces the correct pattern:
- Early months: estimate is low → incurred is low
- Over time: estimate converges to true ultimate → incurred increases
- Result: LDF > 1.0, matching the standard actuarial expectation

## 4.7 Transaction Generation

Each claim gets 5 sequential lifecycle events:
1. Received (accident + 0–2 days)
2. Opened (received + 0–4 days)
3. Investigation (opened + 1–19 days)
4. Approved (investigation + 1–9 days)
5. Closed (approved + 5–59 days)

10% of claims are randomly reopened after closure.

## 4.8 Calendar Table

The calendar table contains 120 rows (2015-01 to 2024-12) with:
- `is_covid_wave` / `is_covid_lockdown` — boolean flags
- `is_weather_event` — boolean flag for specific months with severe weather
- `travel_boom` — boolean, true from 2022 onwards
- `inflation_factor` — deterministic multiplier (1.00 to 1.12)

---

# 5. Pipeline Architecture

## 5.1 Four-Layer Architecture

```
RAW (seed/CSV) → STG (view) → INT (view) → MART (table)
```

| Layer | Materialization | Role |
|-------|----------------|------|
| RAW | seed | Synthetic CSV data generated by Python. No transformations. |
| STG | view | Type casting, column renaming, surrogate key generation, basic data quality tests. |
| INT | view | Business logic: dimensional enrichment, date spine construction, development axis calculation, inflation adjustment. |
| MART | table | Actuarial analytical outputs: triangles, LDFs, ultimate loss, loss ratios. |

### Why views for STG and INT?

At the current scale (~1,400 claims, ~75,000 reserve snapshots), the view chain performs well on Databricks. Views avoid storage duplication and always reflect the latest data.

In production with larger datasets, `int_claim_monthly_paid` would be the first candidate for table materialization due to its Claim × Month date spine explosion (~1,400 claims × ~60 months = ~84,000 rows).

### Why tables for MART?

MART models are the final analytical outputs consumed by end users, dashboards, and downstream systems. Materializing as tables ensures:
- Fast query performance for repeated analysis
- Stable results for reporting (not recomputed on every query)
- Clear contract between the pipeline and consumers

## 5.2 Model Lineage (DAG)

```
stg_claims ─────────┐
stg_policy ──────────┤
stg_calendar ────────┼──► int_claim_base ──► int_payments_enriched ──► int_claim_monthly_paid ──► int_dev_month ──┐
stg_payments ────────┘                                                                                          │
                                                                                                                ├──► mart_paid_triangle
stg_reserve_snapshot ──► int_reserve_snapshot_enriched ──────────────────────────────────────────────────────────┤
                                                                                                                ├──► mart_incurred_triangle ──► mart_ldf ──► mart_ultimate_loss
                                                                                                                │                                       └──► mart_loss_ratio_by_ay
stg_exposure ──► int_exposure_summary ──────────────────────────────────────────────────────────────────────────────► mart_exposure_alignment
```

**Three main branches:**
1. **Paid branch:** claims → payments → monthly paid spine → development month → paid triangle + incurred triangle
2. **Reserve branch:** reserve snapshots → enriched snapshots → incurred triangle
3. **Exposure branch:** exposure → summary → exposure alignment + loss ratio + ultimate loss

## 5.3 Data Flow Summary

| Stage | What Happens | Key Transformation |
|-------|-------------|-------------------|
| RAW → STG | Type casting, renaming | VARCHAR dates → DATE, amounts → DOUBLE |
| STG → INT | Dimensional enrichment | Claims + policy (region) + calendar (macro flags) |
| STG → INT | Date spine creation | Claim × Valuation Month cross-join |
| STG → INT | Inflation adjustment | payment_amount × macro_inflation_factor |
| STG → INT | Development axis | dev_month, dev_year, dev_quarter calculation |
| INT → MART | Triangle aggregation | Claim-level → (AY, dev_year) level |
| MART → MART | LDF calculation | Self-join on consecutive dev_years, average ratio |
| MART → MART | Ultimate projection | Latest incurred × cumulative LDF |
| MART → MART | Loss ratio | Ultimate loss / earned premium |

---

# 6. RAW Layer — Seed Tables

All tables are generated by `generate.py` and loaded as dbt seeds. No transformations are applied at this layer. The RAW layer represents what would come from source systems (policy admin, claims management) in a real insurance company.

## 6.1 raw_policy

**Description:** Policy-level master data. One row per insurance contract.  
**Rows:** 2,200  
**Grain:** `policy_number` (unique)

| Column | Type | Key | Description |
|--------|------|-----|-------------|
| policy_number | VARCHAR | PK | Unique policy identifier (e.g., C0000001) |
| inception_date | DATE | | Policy coverage start date |
| policy_holder_id | INT | | Policyholder identifier |
| premium_amount | DOUBLE | | Annual premium (log-normal, €5k–50k) |
| expiration_date | DATE | | Policy coverage end date (inception + 1 year) |
| region | VARCHAR | | Geographic region (North/South/East/West) |

**Source tests:** not_null + unique on policy_number

## 6.2 raw_exposure

**Description:** Monthly earned exposure per policy. Generated via cross-join of policy × calendar, filtered to active months.  
**Rows:** ~27,000  
**Grain:** `(policy_number, date_month)`

| Column | Type | Key | Description |
|--------|------|-----|-------------|
| policy_number | VARCHAR | FK | References raw_policy |
| date_month | DATE | | First day of the exposure month |
| inception_date | DATE | | From policy (denormalized) |
| expiration_date | DATE | | From policy (denormalized) |
| premium_amount | DOUBLE | | Annual premium (denormalized) |
| earned_exposure | DOUBLE | | Always 1/12 (monthly fraction) |
| earned_premium | DOUBLE | | premium_amount / 12 |

**Source tests:** not_null on policy_number and date_month. unique_combination_of_columns [policy_number, date_month].

## 6.3 raw_claims

**Description:** Claim-level data. One row per reported insurance claim.  
**Rows:** ~1,400  
**Grain:** `claim_id` (unique)

| Column | Type | Key | Description |
|--------|------|-----|-------------|
| claim_id | VARCHAR | PK | Unique claim identifier (e.g., CL0000001) |
| policy_number | VARCHAR | FK | References raw_policy |
| accident_date | DATE | | Date the accident occurred |
| reported_date | DATE | | Date the claim was reported (1–13 days after accident) |
| loss_type | VARCHAR | | collision (60%) / bodily (20%) / glass (15%) / theft (5%) |
| claimant_age | INT | | Age of claimant (18–79) |

**Source tests:** not_null + unique on claim_id. relationship: policy_number → stg_policy.

## 6.4 raw_payments

**Description:** Individual payment transactions per claim.  
**Rows:** ~3,700  
**Grain:** `payment_id` (unique)

| Column | Type | Key | Description |
|--------|------|-----|-------------|
| payment_id | VARCHAR | PK | Unique payment identifier |
| claim_id | VARCHAR | FK | References raw_claims |
| payment_date | DATE | | Date of payment |
| payment_amount | DOUBLE | | Nominal amount paid (before inflation adjustment) |

**Source tests:** not_null + unique on payment_id. relationship: claim_id → stg_claims.

## 6.5 raw_reserve_snapshot

**Description:** Monthly reserve snapshot per claim.  
**Rows:** ~75,000  
**Grain:** `(claim_id, valuation_date)`

| Column | Type | Key | Description |
|--------|------|-----|-------------|
| claim_id | VARCHAR | FK | References raw_claims |
| valuation_date | DATE | | First day of valuation month |
| case_reserve_amount | DOUBLE | | Estimated outstanding reserve |
| expense_reserve_amount | DOUBLE | | case_reserve × 10% |

**Source tests:** not_null on claim_id and valuation_date. unique_combination_of_columns [claim_id, valuation_date]. relationship: claim_id → stg_claims.

## 6.6 raw_transactions

**Description:** Claim lifecycle event log.  
**Rows:** ~7,000  
**Grain:** `transaction_id` (unique)

| Column | Type | Key | Description |
|--------|------|-----|-------------|
| transaction_id | VARCHAR | PK | Unique transaction identifier |
| claim_id | VARCHAR | FK | References raw_claims |
| event_type | VARCHAR | | received / opened / investigation / approved / closed / reopened |
| event_timestamp | TIMESTAMP | | When the event occurred |
| handler_id | VARCHAR | | H001 / H002 / H003 / AUTO |
| auto_or_manual | VARCHAR | | "auto rule" (30%) / "manual adjuster" (70%) |
| comment | VARCHAR | | Free text (empty in synthetic data) |

**Source tests:** not_null + unique on transaction_id. relationship: claim_id → stg_claims.

## 6.7 calendar

**Description:** Monthly calendar with macro-economic indicators.  
**Rows:** 120 (2015-01 to 2024-12)  
**Grain:** `date_month` (unique)

| Column | Type | Key | Description |
|--------|------|-----|-------------|
| date_month | DATE | PK | First day of month |
| is_covid_wave | BOOLEAN | | COVID wave active |
| is_covid_lockdown | BOOLEAN | | COVID lockdown active |
| is_weather_event | BOOLEAN | | Major weather event |
| travel_boom | BOOLEAN | | Post-COVID travel surge (2022+) |
| inflation_factor | DOUBLE | | Macro inflation multiplier (1.00–1.12) |

**Source tests:** not_null + unique on date_month.

---

# 7. STG Layer — Staging Models

All staging models are materialized as **views**. They apply type casting and surrogate key generation only — no business logic. The STG layer is the contract between raw data and the rest of the pipeline.

## 7.1 Design Principles

- **No business logic** — only type casting, column renaming, and surrogate keys
- **Views** — no storage cost, always reflects current raw data
- **Column-level quality tests** — not_null, unique, accepted_values, composite unique
- **Custom test macros** — `accident_before_report_date`, `greater_than_or_equal_to_zero`

## 7.2 stg_calendar

**Source:** raw.calendar  
**Transformations:** CAST date_month AS DATE, booleans AS BOOLEAN, inflation_factor AS DOUBLE.  
**Tests:** not_null on all columns. accepted_values [true, false] on boolean flags.

## 7.3 stg_claims

**Source:** raw.raw_claims  
**Transformations:** CAST dates AS DATE, claimant_age AS INT. Generate surrogate `claim_key` via dbt_utils.  
**Design note:** Region intentionally excluded — it's a policy attribute, available via JOIN to stg_policy when needed.  
**Tests:** not_null + unique on claim_id, claim_key. accepted_values on loss_type ['collision', 'bodily', 'glass', 'theft']. Custom macro: accident_before_report_date.

## 7.4 stg_exposure

**Source:** raw.raw_exposure  
**Transformations:** CAST dates AS DATE, amounts AS DOUBLE. policy_holder_id excluded (belongs to stg_policy).  
**Tests:** unique_combination_of_columns [policy_number, date_month]. greater_than_or_equal_to_zero on earned_exposure, earned_premium.

## 7.5 stg_payments

**Source:** raw.raw_payments  
**Transformations:** CAST payment_date AS DATE, payment_amount AS DOUBLE.  
**Tests:** not_null + unique on payment_id. not_null on claim_id, payment_date, payment_amount.

## 7.6 stg_policy

**Source:** raw.raw_policy  
**Transformations:** CAST dates AS DATE, premium_amount AS DOUBLE. Generate surrogate `policy_key` via dbt_utils.  
**Tests:** not_null + unique on policy_number, policy_key. accepted_values on region. greater_than_or_equal_to_zero on premium_amount. Model-level: expiration_date > inception_date.

## 7.7 stg_reserve_snapshot

**Source:** raw.raw_reserve_snapshot  
**Transformations:** CAST valuation_date AS DATE, amounts AS DOUBLE.  
**Tests:** unique_combination_of_columns [claim_id, valuation_date]. greater_than_or_equal_to_zero on case_reserve_amount, expense_reserve_amount.

## 7.8 stg_transactions

**Source:** raw.raw_transactions  
**Transformations:** CAST event_timestamp AS TIMESTAMP.  
**Tests:** not_null + unique on transaction_id. accepted_values on event_type ['received', 'opened', 'investigation', 'approved', 'closed', 'reopened']. accepted_values on auto_or_manual ['auto rule', 'manual adjuster'].

---

# 8. INT Layer — Intermediate Models

The INT layer is where business logic lives. It enriches claims with dimensional attributes, builds date spines, computes development axes, and applies inflation adjustments. All models are materialized as **views**.

## 8.1 int_claim_base

**Role:** Central claim dimension table.  
**Grain:** claim_id (unique)  
**Upstream:** stg_claims, stg_policy, stg_calendar

**What it does:**
- Derives `accident_year` (EXTRACT YEAR) and `accident_month` (DATE_TRUNC)
- LEFT JOIN stg_policy → adds `region` (policy-level attribute)
- LEFT JOIN stg_calendar on accident_month → adds macro flags (covid_wave_flag, covid_lockdown_flag, weather_event_flag, travel_boom_flag)

**Why region comes from policy:** Region is a property of the insurance contract, not the claim event. In theory, the accident region could differ from the policy region (e.g., someone drives to another region and has an accident). However, in this synthetic dataset, both are identical, so region is normalized to stg_policy as the single source of truth to avoid redundancy.

| Column | Description |
|--------|-------------|
| claim_id | PK. Unique claim identifier |
| claim_key | Surrogate key from dbt_utils |
| policy_number | FK to stg_policy |
| accident_date / reported_date | Date of loss / reporting |
| accident_year / accident_month | Derived time dimensions |
| loss_type | collision / bodily / glass / theft |
| claimant_age | Age at time of loss |
| region | From policy (North/South/East/West) |
| covid_wave_flag | COVID wave active at accident month |
| covid_lockdown_flag | COVID lockdown active |
| weather_event_flag | Weather event at accident month |
| travel_boom_flag | Travel surge at accident month |

## 8.2 int_payments_enriched

**Role:** Enriches payments with inflation adjustment and accident year.  
**Grain:** payment_id (unique)  
**Upstream:** stg_payments, int_claim_base, stg_calendar

**Key design decision — inflation applied here, not in generate.py:**

Raw payment amounts are nominal (unadjusted). The inflation adjustment is a business transformation:

```sql
payment_amount_inflated = payment_amount × macro_inflation_factor
```

This maintains separation of concerns: the raw layer contains what actually happened; the INT layer applies analytical adjustments. If the inflation methodology changes, only the INT model needs updating — not the data generator.

## 8.3 int_claim_monthly_paid

**Role:** Builds the Claim × Valuation Month date spine — the foundation for loss development triangles.  
**Grain:** (claim_id, valuation_month) — composite unique  
**Upstream:** int_claim_base, stg_calendar, int_payments_enriched

**How it works:**
1. Cross-join all claims with all calendar months from accident_month onward
2. LEFT JOIN payments to get `paid_in_month` for each (claim, month) combination
3. COALESCE to 0 where no payment exists
4. Window function `SUM() OVER (PARTITION BY claim_id ORDER BY valuation_month)` → `paid_to_date`

This creates a complete time series for every claim, with cumulative paid amounts at every month — even months where no payment was made. This "spine" is essential for triangle construction because the triangle needs values at every development point, not just when payments happen.

**Performance note:** This model causes row explosion: ~1,400 claims × ~60 avg months = ~84,000 rows. In production with millions of claims, this would be the first candidate for table materialization.

## 8.4 int_dev_month

**Role:** Adds development time axes to the monthly paid spine.  
**Grain:** (claim_id, valuation_month) — composite unique  
**Upstream:** int_claim_monthly_paid

**Derived columns:**
- `development_month = DATEDIFF(month, accident_month, valuation_month)` — 0-based
- `development_year = EXTRACT(YEAR, valuation) - EXTRACT(YEAR, accident) + 1` — 1-based (actuarial standard)
- `development_quarter = DATEDIFF(month) / 3`
- `valuation_year = EXTRACT(YEAR FROM valuation_month)`

**Why 1-based development_year?** Actuarial convention counts development from year 1 (the accident year itself). This aligns with `mart_incurred_triangle` and `mart_ldf` where `dev_year = valuation_year - accident_year + 1`. Consistency across layers prevents subtle join mismatches.

## 8.5 int_reserve_snapshot_enriched

**Role:** Enriches reserve snapshots with claim dimensions.  
**Grain:** (claim_id, valuation_month) — composite unique  
**Upstream:** stg_reserve_snapshot, int_claim_base

**What it does:**
- Derives `valuation_month = DATE_TRUNC('month', valuation_date)`
- LEFT JOIN int_claim_base → inherits `accident_year`, `accident_month`
- Reserve amounts preserved as-is from STG (no transformation)

## 8.6 int_exposure_summary

**Role:** Aggregates monthly exposure into Accident Year × Region summary.  
**Grain:** (accident_year, region) — composite unique  
**Upstream:** stg_exposure, stg_policy

**What it does:**
- LEFT JOIN stg_policy → adds region
- `accident_year = EXTRACT(YEAR FROM date_month)` — named for downstream join compatibility; actually represents the calendar year of premium earning
- GROUP BY (accident_year, region) → SUM(earned_exposure), SUM(earned_premium)

---

# 9. MART Layer — Analytical Models

All MART models are materialized as **tables**. They represent the final analytical outputs consumed by actuaries, dashboards, and regulatory reports.

## 9.1 mart_paid_triangle

**Role:** Cumulative paid loss triangle.  
**Grain:** (accident_year, dev_month) — composite unique  
**Upstream:** int_dev_month, int_claim_base

**What it does:**
- GROUP BY (accident_year, dev_month)
- SUM(paid_to_date) across all claims
- COUNT(DISTINCT claim_id) as n_claims

**Actuarial use:** Cash flow forecasting, payment pattern analysis, validating reserve assumptions. In this pipeline, the incurred triangle (not paid) is used for chain-ladder because incurred is a better predictor of ultimate cost.

## 9.2 mart_incurred_triangle

**Role:** Incurred loss triangle — the core input for chain-ladder LDF calculation.  
**Grain:** (accident_year, dev_year) — composite unique  
**Upstream:** int_dev_month, int_reserve_snapshot_enriched

**What it does:**
1. Filters to **year-end snapshots only** (`WHERE MONTH(valuation_month) = 12`) — creates clean annual triangle
2. Joins paid (int_dev_month) with reserve (int_reserve_snapshot_enriched) on (claim_id, valuation_month)
3. `incurred_amount = paid_to_date + COALESCE(reserve_amount, 0)`
4. Aggregates from claim-level to (accident_year, valuation_year) level
5. Derives `dev_year = valuation_year - accident_year + 1`

**Why year-end only?** Annual grain is the actuarial standard for loss development triangles. Monthly grain would create 120 development periods instead of 10, making the triangle sparse and LDFs unstable. In production, quarterly triangles are common for more frequent monitoring.

| Column | Description |
|--------|-------------|
| accident_year | Year of loss occurrence |
| valuation_year | Calendar year of evaluation |
| dev_year | valuation_year - accident_year + 1 (1-based) |
| paid_to_date | Total paid amount at this valuation point |
| reserve_amount | Total outstanding reserve (case + expense) |
| incurred_amount | paid_to_date + reserve_amount |

## 9.3 mart_ldf

**Role:** Loss Development Factors and Cumulative LDFs — the engine of the chain-ladder method.  
**Grain:** (dev_year_from, dev_year_to) — composite unique  
**Upstream:** mart_incurred_triangle

**SQL logic step by step:**

**Step 1 — Pairs CTE:** Self-join `mart_incurred_triangle` on (same accident_year, consecutive dev_years):
```sql
FROM tri a
JOIN tri b
    ON a.accident_year = b.accident_year
    AND b.dev_year = a.dev_year + 1
WHERE a.incurred_amount > 0
  AND b.incurred_amount > 0
```

**Step 2 — Average LDF:** For each dev_year transition, average the link ratios across all accident years:
```sql
AVG(incurred_to / incurred_from) GROUP BY dev_year_from, dev_year_to
```

**Step 3 — Cumulative LDF:** Reverse cumulative product using log-sum-exp:
```sql
EXP(SUM(LN(ldf)) OVER (
    ORDER BY dev_year_from DESC
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
)) AS cumulative_ldf
```

**Expected output pattern:**

| dev_year_from | dev_year_to | ldf | cumulative_ldf |
|---|---|---|---|
| 1 | 2 | 1.2316 | 1.3701 |
| 2 | 3 | 1.0655 | 1.3123 |
| 3 | 4 | 1.0255 | 1.3458 |
| ... | ... | ≈1.00 | ≈1.37 |
| 9 | 10 | 1.0066 | 1.3701 |

LDF is highest in early development (1.23 from year 1→2), then converges toward 1.0 as claims mature.

## 9.4 mart_ultimate_loss

**Role:** Ultimate loss estimate per accident year — the summary output.  
**Grain:** accident_year (unique)  
**Upstream:** mart_incurred_triangle, mart_ldf, int_exposure_summary

**Key logic:**
1. Find the latest valuation year: `MAX(valuation_year)` from mart_incurred_triangle
2. Filter to latest valuation only → one row per accident year
3. LEFT JOIN mart_ldf on `dev_year = dev_year_from`
4. `ultimate_loss = incurred_amount × COALESCE(cumulative_ldf, 1.0)`

**Why COALESCE?** The most mature accident year (e.g., AY 2015 at dev_year 10) has no dev_year_from = 10 in mart_ldf because there's no dev_year 11 to form a pair. The LEFT JOIN returns NULL. COALESCE(cumulative_ldf, 1.0) handles this: fully developed years need no projection.

| Column | Description |
|--------|-------------|
| accident_year | PK |
| valuation_year | Latest valuation year (same for all rows) |
| dev_year | Current development age |
| latest_incurred | Incurred at latest valuation |
| cumulative_ldf | CDF applied (1.0 for mature years) |
| ultimate_loss | latest_incurred × cumulative_ldf |
| earned_premium | Total earned premium for this AY |
| loss_ratio | ultimate_loss / earned_premium |

## 9.5 mart_loss_ratio_by_ay

**Role:** Loss ratio evolution across all Accident Year × Valuation Year combinations.  
**Grain:** (accident_year, valuation_year) — composite unique  
**Upstream:** mart_incurred_triangle, mart_ldf, int_exposure_summary

**Why this exists in addition to mart_ultimate_loss:**
- `mart_ultimate_loss` = 1 row per AY (latest snapshot only) → summary dashboard
- `mart_loss_ratio_by_ay` = N rows per AY (every valuation point) → actuarial review

Actuaries don't just want the final number — they want to see how estimates evolved. A stable loss ratio across valuation years means confidence in reserving adequacy. A loss ratio that keeps increasing may signal under-reserving.

## 9.6 mart_exposure_alignment

**Role:** Aligns earned exposure, premium, and ultimate loss at Accident Year × Region grain.  
**Grain:** (accident_year, region) — composite unique  
**Upstream:** int_exposure_summary, mart_ultimate_loss

**Known limitation:** ultimate_loss is at AY level (not region-split). The same ultimate_loss value appears for all regions within an AY. loss_ratio here reflects regional premium share against total AY ultimate loss. True region-level loss development would require a separate triangle per region, which thins the data and increases LDF volatility.

---

# 10. Testing Strategy

## 10.1 Test Philosophy by Layer

Each layer has different responsibilities, so each layer needs different tests:

| Layer | Test Focus | Examples |
|-------|-----------|---------|
| Source (source.yml) | Structural integrity | PKs, FKs (relationships), grain uniqueness |
| STG | Column-level quality | Type validation, accepted_values, composite unique, custom macros |
| INT | Business logic validation | Derived columns, aggregation correctness, relationship to claim dimension |
| MART | Actuarial domain validation | LDF > 0, loss_ratio >= 0, grain uniqueness |

**Why test at both Source AND STG?** Source tests validate raw data integrity. STG tests validate the transformation contract. Testing at both layers isolates failures — a source test failure means bad raw data; a STG test failure means a transformation bug.

## 10.2 Severity Classification

- **ERROR:** Structural failures (null PKs, broken relationships, duplicate grain) → pipeline stops
- **WARN:** Domain anomalies valid in practice (e.g., negative payments from refunds/reversals)

Negative payment amounts are intentionally retained in the seed data and flagged as WARN — mirroring real-world scenarios where claim reversals produce negative values.

## 10.3 Custom Test Macros

**accident_before_report_date:** Validates that `accident_date <= reported_date` for every claim.

**greater_than_or_equal_to_zero:** Generic test for non-negative amounts (reserves, exposure, premium).

## 10.4 Test Count Summary

Total: ~150 tests across the pipeline (exact count from `dbt test`).

---

# 11. Databricks Deployment

## 11.1 Connection Setup

The pipeline runs on Databricks SQL Warehouse (Serverless) via `dbt-databricks`:

```yaml
insurance_claims_loss_development_dbt:
  target: prod
  outputs:
    prod:
      type: databricks
      host: <workspace>.cloud.databricks.com
      http_path: /sql/1.0/warehouses/<warehouse-id>
      token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"
      catalog: <catalog>
      schema: insurance_dbt
      threads: 4
```

`profiles.yml` lives in `~/.dbt/` and is never committed to version control.

## 11.2 Execution

```bash
dbt seed      # Load CSV data as Delta tables
dbt run       # Execute all models
dbt test      # Run all tests
dbt docs generate  # Generate documentation
dbt docs serve     # View lineage graph locally
```

## 11.3 Schema

All models are created in the `insurance_dbt` schema:
- STG models: views (e.g., `insurance_dbt.stg_claims`)
- INT models: views (e.g., `insurance_dbt.int_claim_base`)
- MART models: tables (e.g., `insurance_dbt.mart_incurred_triangle`)

## 11.4 DuckDB vs Databricks

The pipeline is dual-target: DuckDB for local development, Databricks for production. Minor SQL dialect differences (DATE_TRUNC, EXTRACT, DATEDIFF) were resolved during deployment. Both targets produce identical analytical results.

---

# 12. Key Design Decisions

## 12.1 Why dev_year instead of calendar year for LDFs?

Calendar-year based LDFs (e.g., 2015→2016, 2016→2017) cannot align different accident years on the same development axis. dev_year (= valuation_year - accident_year + 1) normalizes all accident years to the same development timeline, enabling like-for-like comparison. This is the actuarial standard.

## 12.2 Why apply inflation in INT, not in generate.py?

Raw data should contain nominal (unadjusted) values. Inflation adjustment is a business transformation that belongs in the pipeline. This ensures the raw layer is a faithful record of what happened, while INT applies the analytical adjustments. If the methodology changes, only the INT model is updated.

## 12.3 Why is region excluded from stg_claims?

Region is a policy-level attribute. In this synthetic dataset, claim region is always identical to policy region (copied from the same source). Storing it in both tables creates update anomalies and redundancy. Region is available via JOIN to stg_policy through int_claim_base.

In a real system where accident location differs from policy location, a separate `accident_region` column would be added to claims — independent of the policy region.

## 12.4 Why year-end snapshots for the incurred triangle?

Annual grain is the actuarial standard. Monthly grain would create 120 development periods, making the triangle sparse and LDFs unstable. Quarterly triangles are used in production for more frequent monitoring, but annual is sufficient for this portfolio size.

## 12.5 Why simple average LDF instead of volume-weighted?

With only 10 accident years, volume-weighted LDF can be dominated by a single large accident year. Simple average gives equal weight to each AY's development pattern. Production systems typically offer both methods for actuarial judgment.

## 12.6 Why COALESCE(cumulative_ldf, 1.0)?

The most mature AY has no LDF pair. Without COALESCE, ultimate_loss would be NULL. Setting CDF = 1.0 means "no further development expected" — the actuarially correct default for fully mature claims.

## 12.7 Why two loss ratio models?

`mart_ultimate_loss` provides a one-row-per-AY summary for dashboards. `mart_loss_ratio_by_ay` provides the full development history for actuarial review. Different consumers have different needs.

## 12.8 Why was int_triangle_input removed?

This model was 100% identical to `int_reserve_snapshot_enriched` and was not referenced by any downstream model. It was originally planned as a unified paid + reserve fact table but was never implemented. Removing it cleaned up the DAG.

---

# 13. Simplifications vs. Production

| This Project | Production Reality |
|---|---|
| Synthetic Python-generated data | Raw data from claims management systems (Guidewire, Duck Creek) |
| Single-node SQL chain ladder | Mack's Method with standard error and confidence intervals |
| Simple average LDF | Volume-weighted LDF or Bornhuetter-Ferguson method |
| Static macro calendar flags | Live macro feeds (CPI, weather APIs) |
| No CI/CD | GitHub Actions → dbt test on PR → Databricks job on merge |
| DuckDB for local dev | Delta Lake on ADLS Gen2 / S3 |
| Reserve: learning curve model | Actuarial judgment + stochastic reserve models |
| Annual premium ~20k€ (fleet/corp) | Mix of individual and commercial portfolios |
| Log-normal severity | Log-normal — actuarially standard (always positive, right-skewed) |
| 5% base rate frequency | Closer to motor insurance industry benchmark |
| No tail factor | Tail factor for long-tail lines (bodily injury 20+ years) |
| No regional triangle segmentation | Segment by loss type, region, claim size |
| Annual triangle only | Quarterly or monthly triangles for frequent monitoring |

---

# 14. Appendix

## 14.1 Technology Stack

- **dbt-core** ≥ 1.7
- **dbt-duckdb** (local development)
- **dbt-databricks** 1.11.6 (production)
- **dbt-utils** — surrogate keys, generic tests, unique_combination_of_columns
- **Python 3.10+** — synthetic data generation (numpy, pandas)
- **Databricks SQL Warehouse** (Serverless)

## 14.2 Repository Structure

```
insurance-claims-loss-development-dbt/
├── models/
│   ├── staging/          # 7 STG models + 7 YML files
│   ├── intermediate/     # 5 INT models + 5 YML files
│   ├── mart/             # 6 MART models + 1 YML file
│   └── sources/          # source.yml
├── seeds/                # 7 CSV files (generated by generate.py)
├── macros/               # Custom test macros
├── tests/                # Generic tests
├── docs/                 # DAG screenshot
├── README.md             # English
├── README FR.md          # French
├── dbt_project.yml
├── packages.yml
└── generate.py           # Data generator (not in seeds/)
```

## 14.3 Project Series

This is Part 2 of a 5-project series building toward a comprehensive insurance data platform:

1. **Small:** Insurance Policy Admin Mart — Portfolio Structure & KPIs
2. **Medium-1:** This project — Non-Life Loss Development Pipeline
3. **Medium-2:** Life Insurance BEL Pipeline (planned)
4. **Medium-3:** Reinsurance IFRS 17 — Retro Linkage & Loss Recovery (planned)
5. **Large:** IFRS 17 Analytics Platform on Azure — E2E with CI/CD + Mack's Method (planned)

## 14.4 Author

**SukHee Lee** — Actuarial Data Analyst | IFRS 17 · dbt · Databricks  
Building insurance data pipelines across reserving, IFRS 17, and analytics engineering workflows.

GitHub: github.com/SHLee5864  
Medium: medium.com/@lsh5864

