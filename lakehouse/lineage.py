"""Lightweight run-level lineage: which inputs produced which outputs, row counts, batch ids."""
import json
import os
import time
from datetime import datetime, timezone

from .spark_session import LAKE_ROOT


def record(step: str, inputs: list, outputs: list, metrics: dict, batch_id: str):
    entry = {"ts": datetime.now(timezone.utc).isoformat(), "step": step, "batch_id": batch_id,
             "inputs": inputs, "outputs": outputs, "metrics": metrics}
    if not LAKE_ROOT.startswith("s3a://"):
        os.makedirs(f"{LAKE_ROOT}/_lineage", exist_ok=True)
        with open(f"{LAKE_ROOT}/_lineage/lineage.jsonl", "a") as f:
            f.write(json.dumps(entry) + "\n")
    return entry
