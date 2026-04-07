"""
generate.py — Synthetic Insurance Claims Dataset
================================================
Actuarial design principles:
  - Claim severity: Log-normal (standard for Non-Life)
  - Claim frequency: Negative Binomial approximated via Poisson-Gamma
  - Payment: Total expected loss split into installments (Dirichlet)
  - Reserve: Ultimate - paid_to_date (case reserve = remaining expected)
  - Reserve: Learning curve model — initial underestimation converging to ultimate
  - Inflation: Deterministic factor by year
  - Macro shocks: COVID frequency reduction, weather severity loading

Known simplifications vs. production:
  - No claim reopening effect on reserve (reserve monotonically decreases)
  - No tail development beyond 10 years
  - No correlation between claims within same policy
  - No large loss / catastrophe loading
  - Frequency model uses fixed exposure per policy-month (no experience rating)
  - Payment timing is random (no legal/medical pipeline modeling)
"""

import pandas as pd
import numpy as np
import uuid
from datetime import timedelta

np.random.seed(42)

# ── Calendar ──────────────────────────────────────────────────────────────
calendar = pd.read_csv("calendar.csv")
calendar["date_month"] = pd.to_datetime(calendar["date_month"])

# ── Inflation factors (deterministic) ─────────────────────────────────────
inflation_map = {
    2015: 1.00, 2016: 1.00, 2017: 1.00, 2018: 1.00, 2019: 1.00,
    2020: 1.00, 2021: 1.02, 2022: 1.05, 2023: 1.08, 2024: 1.10,
    2025: 1.12
}

# ── Macro factor functions ────────────────────────────────────────────────
def covid_frequency_factor(dt):
    """COVID lockdown reduces claim frequency."""
    ym = dt.strftime("%Y-%m")
    if "2020-03" <= ym <= "2020-05": return 0.6
    if "2020-10" <= ym <= "2020-12": return 0.7
    if "2021-01" <= ym <= "2021-06": return 0.8
    return 1.0

def weather_severity_factor(dt):
    """Weather events increase severity."""
    ym = dt.strftime("%Y-%m")
    if ym == "2020-10":             return 1.4
    if ym in ("2022-06", "2022-07"): return 1.3
    if ym == "2023-11":             return 1.4
    return 1.0

def travel_factor(dt):
    """Post-COVID travel boom increases exposure frequency."""
    return 1.15 if dt.year >= 2022 else 1.0

# ═══════════════════════════════════════════════════════════════════════════
# 1. POLICY GENERATION
# ═══════════════════════════════════════════════════════════════════════════
N_POLICIES = 2200

policies = pd.DataFrame({
    "policy_number":   ["C" + str(i).zfill(7) for i in range(N_POLICIES)],
    "inception_date":  np.random.choice(
        pd.date_range("2015-01-01", "2024-12-31"), N_POLICIES),
    "policy_holder_id": np.random.randint(10000, 99999, N_POLICIES),
    "premium_amount":  np.random.lognormal(mean=9.9, sigma=0.3, size=N_POLICIES).clip(5000, 50000),
    # Log-normal premium: median ≈ 493, more realistic than normal
    "region": np.random.choice(["North", "South", "East", "West"], N_POLICIES)
})
policies["expiration_date"] = policies["inception_date"] + pd.DateOffset(years=1)

# ═══════════════════════════════════════════════════════════════════════════
# 2. EXPOSURE GENERATION
# ═══════════════════════════════════════════════════════════════════════════
calendar_trimmed = calendar[["date_month"]]

exposure = policies.merge(calendar_trimmed, how="cross")
exposure = exposure[
    exposure["date_month"].between(
        exposure["inception_date"].dt.to_period("M").dt.to_timestamp(),
        exposure["expiration_date"].dt.to_period("M").dt.to_timestamp()
    )
].copy()

exposure = exposure.drop(columns=["policy_holder_id", "region"])

exposure["earned_exposure"] = 1 / 12
exposure["earned_premium"]  = exposure["premium_amount"] / 12

# ═══════════════════════════════════════════════════════════════════════════
# 3. CLAIM GENERATION
# Frequency: Poisson with rate = base_rate × macro_factors
# ═══════════════════════════════════════════════════════════════════════════
# NOTE: 5% base rate — closer to motor insurance industry benchmark
# In production: experience-rated frequency using GLM (Poisson regression)
# with features: age, region, vehicle type, years claim-free

BASE_RATE = 0.05

claims = []
for _, row in exposure.iterrows():
    rate = BASE_RATE * covid_frequency_factor(row["date_month"]) \
                     * travel_factor(row["date_month"])
    if np.random.rand() < rate:
        accident_day = row["date_month"] + timedelta(days=np.random.randint(0, 28))
        claims.append({
            "claim_id":      "CL" + str(len(claims) + 1).zfill(7),
            "policy_number": row["policy_number"],
            "accident_date": accident_day,
            "reported_date": accident_day + timedelta(days=np.random.randint(1, 14)),
            "loss_type":     np.random.choice(
                ["collision", "bodily", "glass", "theft"],
                p=[0.60, 0.20, 0.15, 0.05]
            ),
            "claimant_age":  np.random.randint(18, 80)
        })

claims = pd.DataFrame(claims)
print(f"Claims generated: {len(claims)}")

# ═══════════════════════════════════════════════════════════════════════════
# 4. PAYMENT + RESERVE — Correlated via total_expected_loss
#
# Design:
#   total_expected_loss ~ LogNormal(μ, σ) × severity_loading
#   → shared anchor for both payments and reserve
#
#   Payments: total_expected split into n installments (Dirichlet)
#             each installment adjusted for inflation + weather
#
#   Reserve(m) = max(0, estimate(m) - paid_to_date(m))
#   estimate(m) = total_expected × [0.60 + 0.40 × (1 - exp(-m/18))]
#   → initial underestimation gradually corrected as claim develops
#
#   IBNR_loading: uncertainty margin in early months (fades with time)
#   This ensures reserve ≥ remaining expected payments in early dev months,
#   converging to 0 as claim matures.
#
# Known simplification:
#   - Reserve strengthening (mid-development reserve increase) not modeled
#   - No correlation across claims (each independently sampled)
#   - IBNR loading is deterministic, not stochastic
# ═══════════════════════════════════════════════════════════════════════════

# Severity parameters by loss_type (log-normal μ, σ)
# Source: calibrated to approximate French motor insurance benchmarks
SEVERITY_PARAMS = {
    "collision": (9.2, 0.7),   # median ≈ 9,897
    "bodily":    (10.2, 0.8),  # median ≈ 26,881 (higher, long-tail)
    "glass":     (7.5, 0.4),   # median ≈ 1,808  (low severity)
    "theft":     (9.0, 0.6),   # median ≈ 8,103
}

IBNR_LOADING  = 0.25   # 25% margin on top of expected in early months
IBNR_DECAY_TAU = 24    # IBNR fades over 24 months

payments  = []
snapshots = []

for _, cl in claims.iterrows():
    loss_type = cl["loss_type"]
    mu, sigma = SEVERITY_PARAMS[loss_type]

    # ── Total expected loss (anchor) ──────────────────────────────────────
    # Weather loading at accident date
    weather_at_accident = weather_severity_factor(cl["accident_date"])
    total_expected = np.random.lognormal(mean=mu, sigma=sigma) * weather_at_accident

    # ── Payments ──────────────────────────────────────────────────────────
    # payment count by loss_type
    payment_schedule = {
        "glass":     (1, 2),   # Low severity, quick payment
        "collision": (2, 4),
        "theft":     (1, 3),
        "bodily":    (3, 7),   # long-tail split (injury treatment period)
    }
    lo, hi = payment_schedule[loss_type]
    n_payments = np.random.randint(lo, hi + 1)

    # payment timing: distributed by dev_month (minimizing timing noise)
    # glass/theft: quick settlement (dev_month 1~6)
    # collision:  mid-term settlement (dev_month 3~18)
    # bodily:      long-term settlement (dev_month 6~36)
    timing_range = {
        "glass":     (1, 6),
        "collision": (3, 18),
        "theft":     (1, 12),
        "bodily":    (6, 36),
    }
    t_lo, t_hi = timing_range[loss_type]
    pay_month_offsets = sorted(
        np.random.choice(range(t_lo, t_hi + 1), size=n_payments, replace=False)
    )

    # Split total_expected proportionally (Dirichlet ensures sum=1)
    splits = np.random.dirichlet(np.ones(n_payments))

    paid_amounts = []
    for split, month_offset in zip(splits, pay_month_offsets):
        pay_date = (cl["accident_date"] + pd.DateOffset(months=int(month_offset))).normalize()
        if pay_date > pd.Timestamp("2024-12-31"):
            continue
        amt = total_expected * split \
              * weather_severity_factor(pay_date)
        paid_amounts.append((pay_date, round(float(amt), 2)))
        payments.append({
            "payment_id":     "P" + uuid.uuid4().hex[:12],
            "claim_id":       cl["claim_id"],
            "payment_date":   pay_date,
            "payment_amount": round(float(amt), 2)
        })

    # ── Reserve snapshots ─────────────────────────────────────────────────
    INITIAL_ESTIMATE_RATIO = 0.60  # Initially recognize only 60% of the ultimate
    LEARNING_TAU = 18              # Converge to true ultimate over 18 months

    cumulative_paid = 0.0
    for m in range(120):
        val_date = (cl["accident_date"] + pd.DateOffset(months=m)).normalize()
        if val_date > pd.Timestamp("2024-12-31"):
            break

        cumulative_paid = sum(
            amt for pd_date, amt in paid_amounts
            if pd.Timestamp(pd_date) <= val_date
        )

        # Estimate gradually converges to true ultimate
        learning_progress = 1 - np.exp(-m / LEARNING_TAU)
        current_estimate = (
            total_expected * INITIAL_ESTIMATE_RATIO
            + total_expected * (1 - INITIAL_ESTIMATE_RATIO) * learning_progress
        )

        # Add noise (reserves are imprecise in practice)
        noise = np.random.normal(1.0, 0.03)  # ±3% 
        current_estimate *= noise

        case_reserve = max(0.0, current_estimate - cumulative_paid)

        snapshots.append({
            "claim_id":               cl["claim_id"],
            "valuation_date":         val_date,
            "case_reserve_amount":    round(float(case_reserve), 2),
            "expense_reserve_amount": round(float(case_reserve * 0.10), 2)
        })

payments  = pd.DataFrame(payments)
snapshots = pd.DataFrame(snapshots)
print(f"Payments:  {len(payments)}")
print(f"Snapshots: {len(snapshots)}")

# ═══════════════════════════════════════════════════════════════════════════
# 5. TRANSACTIONS (Claim Lifecycle Events)
# ═══════════════════════════════════════════════════════════════════════════
transactions = []
for _, cl in claims.iterrows():
    accident     = cl["accident_date"]
    received_ts  = accident      + timedelta(days=np.random.randint(0, 3))
    opened_ts    = received_ts   + timedelta(days=np.random.randint(0, 5))
    invest_ts    = opened_ts     + timedelta(days=np.random.randint(1, 20))
    approved_ts  = invest_ts     + timedelta(days=np.random.randint(1, 10))
    closed_ts    = approved_ts   + timedelta(days=np.random.randint(5, 60))

    lifecycle = [
        ("received",     received_ts),
        ("opened",       opened_ts),
        ("investigation", invest_ts),
        ("approved",     approved_ts),
        ("closed",       closed_ts),
    ]
    if np.random.rand() < 0.10:
        reopen_ts = closed_ts + timedelta(days=np.random.randint(10, 120))
        lifecycle.append(("reopened", reopen_ts))

    for ev_type, ts in lifecycle:
        transactions.append({
            "transaction_id":   "T" + uuid.uuid4().hex[:12],
            "claim_id":         cl["claim_id"],
            "event_type":       ev_type,
            "event_timestamp":  ts,
            "handler_id":       np.random.choice(["H001", "H002", "H003", "AUTO"]),
            "auto_or_manual":   "auto rule" if np.random.rand() < 0.3 else "manual adjuster",
            "comment":          ""
        })

transactions = pd.DataFrame(transactions)

# ═══════════════════════════════════════════════════════════════════════════
# 6. SAVE
# ═══════════════════════════════════════════════════════════════════════════
policies.to_csv("raw_policy.csv",            index=False)
exposure.to_csv("raw_exposure.csv",          index=False)
claims.to_csv("raw_claims.csv",              index=False)
payments.to_csv("raw_payments.csv",          index=False)
snapshots.to_csv("raw_reserve_snapshot.csv", index=False)
transactions.to_csv("raw_transactions.csv",  index=False)

print("✅ Synthetic dataset generated.")
print(f"   Policies:     {len(policies):,}")
print(f"   Exposure rows:{len(exposure):,}")
print(f"   Claims:       {len(claims):,}")
print(f"   Payments:     {len(payments):,}")
print(f"   Snapshots:    {len(snapshots):,}")
print(f"   Transactions: {len(transactions):,}")