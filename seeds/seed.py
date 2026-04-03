import pandas as pd
import numpy as np
import uuid
from datetime import date, datetime, timedelta
calendar = pd.read_csv("calendar.csv")
calendar["date_month"] = pd.to_datetime(calendar["date_month"])
inflation_map = {
    2015:1.00, 2016:1.00, 2017:1.00, 2018:1.00, 2019:1.00,
    2020:1.00, 2021:1.02, 2022:1.05, 2023:1.08, 2024:1.10,
    2025:1.12
}

def covid_frequency_factor(date):
    ym = date.strftime("%Y-%m")
    if "2020-03" <= ym <= "2020-05": return 0.6
    if "2020-10" <= ym <= "2020-12": return 0.7
    if "2021-01" <= ym <= "2021-06": return 0.8
    return 1.0

def weather_severity_factor(date):
    ym = date.strftime("%Y-%m")
    if ym == "2020-10": return 1.4
    if ym in ("2022-06","2022-07"): return 1.3
    if ym == "2023-11": return 1.4
    return 1.0

def travel_factor(date):
    return 1.15 if date.year >= 2022 else 1.0
def generate_payment_amount(base, date):
    infl = inflation_map.get(date.year, inflation_map[max(inflation_map.keys())])
    weather = weather_severity_factor(date)
    return base * infl * weather

###
# POLICY GENERATION
###

N_POLICIES = 2200
policy_ids = ["P" + str(i).zfill(6) for i in range(N_POLICIES)]
policies = pd.DataFrame({
    "policy_number": policy_ids,
    "inception_date": np.random.choice(
        pd.date_range("2015-01-01","2024-12-31"), N_POLICIES),
    "policy_holder_id": np.random.randint(10000,99999, N_POLICIES),
    "premium_amount": np.random.normal(500,100, N_POLICIES).clip(200,2000),
    "region": np.random.choice(["North","South","East","West"], N_POLICIES)
})
policies["expiration_date"] = policies["inception_date"] + pd.DateOffset(years=1)

###
# EXPOSURE GENERATION
###

calendar_trimmed = calendar[["date_month"]]  # macro flag 제거

exposure = policies.merge(calendar_trimmed, how="cross")
exposure = exposure[
    exposure["date_month"].between(
        exposure["inception_date"].dt.to_period('M').dt.to_timestamp(),
        exposure["expiration_date"].dt.to_period('M').dt.to_timestamp()
    )
]

exposure["earned_exposure"] = 1/12
exposure["earned_premium"] = exposure["premium_amount"] / 12

###
# CLAIM GENERATION

###
claims = []
for idx, row in exposure.iterrows():
    base_rate = 0.02
    freq_factor = (
        covid_frequency_factor(row["date_month"]) *
        travel_factor(row["date_month"])
    )
    if np.random.rand() < base_rate * freq_factor:
        accident_day = row["date_month"] + timedelta(days=np.random.randint(0,28))
        claims.append({
            "claim_id":"C" + str(len(claims)+1).zfill(7),
            "policy_number":row["policy_number"],
            "accident_date":accident_day,
            "reported_date":accident_day + timedelta(days=np.random.randint(1,14)),
            "loss_type":np.random.choice(["collision","bodily","glass","theft"], p=[0.6,0.2,0.15,0.05]),
            "region":row["region"],
            "claimant_age":np.random.randint(18,80)
        })
claims = pd.DataFrame(claims)

###
# TRANSACTIONS (Claim Lifecycle Events)
###

transactions = []

event_types = [
    "received",
    "opened",
    "investigation",
    "approved",
    "closed",
    "reopened"
]

for idx, cl in claims.iterrows():
    # 기본 lifecycle: received → opened → investigation → approved → closed
    accident = cl.accident_date
    received_ts = accident + timedelta(days=np.random.randint(0, 3))
    opened_ts = received_ts + timedelta(days=np.random.randint(0, 5))
    investigation_ts = opened_ts + timedelta(days=np.random.randint(1, 20))
    approved_ts = investigation_ts + timedelta(days=np.random.randint(1, 10))
    closed_ts = approved_ts + timedelta(days=np.random.randint(5, 60))

    lifecycle = [
        ("received", received_ts),
        ("opened", opened_ts),
        ("investigation", investigation_ts),
        ("approved", approved_ts),
        ("closed", closed_ts)
    ]

    # 10% 확률로 reopen 이벤트 추가
    if np.random.rand() < 0.10:
        reopen_ts = closed_ts + timedelta(days=np.random.randint(10, 120))
        lifecycle.append(("reopened", reopen_ts))

    # 기록 저장
    for ev_type, ts in lifecycle:
        transactions.append({
            "transaction_id": "T" + uuid.uuid4().hex[:12],
            "claim_id": cl.claim_id,
            "event_type": ev_type,
            "event_timestamp": ts,
            "handler_id": np.random.choice(["H001","H002","H003","AUTO"]),
            "auto_or_manual": "auto rule" if np.random.rand() < 0.3 else "manual adjuster",
            "comment": ""
        })

transactions = pd.DataFrame(transactions)

###
# PAYMENTS
###

payments = []
for idx, cl in claims.iterrows():
    n = np.random.randint(1,4)
    for i in range(n):
        pay_date = cl.accident_date + timedelta(days=np.random.randint(30,300))
        base = np.random.normal(1200,400)
        amt = generate_payment_amount(base, pay_date)
        payments.append({
            "payment_id":"P" + uuid.uuid4().hex[:12],
            "claim_id":cl.claim_id,
            "payment_date":pay_date,
            "payment_amount":round(float(amt),2)
        })
payments = pd.DataFrame(payments)

###
# RESERVE SNAPSHOT
###

snapshots = []
for idx, cl in claims.iterrows():
    for m in range(36):
        val_date = (cl.accident_date + pd.DateOffset(months=m)).normalize()
        if val_date > pd.Timestamp("2024-12-31"): break
        base_res = np.random.normal(3000,800) * np.exp(-m/18)
        base_res *= weather_severity_factor(val_date)
        snapshots.append({
            "claim_id":cl.claim_id,
            "valuation_date":val_date,
            "case_reserve_amount":max(0,base_res),
            "expense_reserve_amount":max(0,base_res*0.1)
        })
snapshots = pd.DataFrame(snapshots)

###
# SAVE
###

policies.to_csv("raw_policy.csv", index=False)
exposure.to_csv("raw_exposure.csv", index=False)
claims.to_csv("raw_claims.csv", index=False)
payments.to_csv("raw_payments.csv", index=False)
snapshots.to_csv("raw_reserve_snapshot.csv", index=False)
transactions.to_csv("raw_transactions.csv", index=False)

print("✅ Synthetic dataset generated.")
