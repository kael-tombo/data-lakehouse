"""Synthetic operational data: customers + orders in daily CSV batches, with realistic dirt
(duplicates across batches, whitespace/casing, bad values) so the silver layer has work to do."""
import argparse
import os
import random
from datetime import date, timedelta

REGIONS = ["Atsimo", " atsinanana ", "ANALAMANGA", "Boeny"]


def generate(out, days=3, seed=1, n_customers=100, per_day=200, start=date(2026, 10, 1), dirty=True):
    os.makedirs(out, exist_ok=True)
    r = random.Random(seed)
    with open(f"{out}/customers.csv", "w") as f:
        f.write("customer_id,name,region\n")
        for i in range(1, n_customers + 1):
            f.write(f"{i},Customer {i}, {r.choice(REGIONS)}\n")
    oid = 0
    for d in range(days):
        day = start + timedelta(days=d)
        with open(f"{out}/orders_day{d + 1}.csv", "w") as f:
            f.write("order_id,customer_id,order_date,quantity,unit_price,region,status\n")
            for _ in range(per_day):
                oid += 1
                q, p = r.randint(1, 10), round(r.uniform(5, 120), 2)
                row = [oid, r.randint(1, n_customers), day.isoformat(), q, p, r.choice(REGIONS), r.choice(["paid", "PAID", "cancelled", "shipped"])]
                if dirty and r.random() < 0.02:
                    row[3] = -1                       # invalid quantity -> quarantine
                if dirty and r.random() < 0.01:
                    row[2] = "not-a-date"             # invalid date -> quarantine
                f.write(",".join(map(str, row)) + "\n")
            if dirty and d > 0:                       # late corrections re-sent from previous day (duplicates by key)
                for k in range(1, 6):
                    f.write(f"{oid - per_day - k},1,{(day - timedelta(days=1)).isoformat()},9,10.0,Boeny,PAID\n")
    print("generated", days, "daily batches in", out)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--out", default="data/raw"); ap.add_argument("--days", type=int, default=3)
    a = ap.parse_args(); generate(a.out, a.days)
