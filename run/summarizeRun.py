#!/usr/bin/env python3
"""Summarize one JVM's fixed completion window from BenchmarkSQL raw CSVs."""

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from pathlib import Path


TRANSACTIONS = ("NEW_ORDER", "PAYMENT", "ORDER_STATUS", "DELIVERY", "STOCK_LEVEL", "DELIVERY_BG")


def connection_window(path, start_ms, end_ms):
    """Count authenticated new physical sockets; failed authentication is excluded."""
    if not path.exists():
        return None, "connections.csv is missing"
    count = 0
    started = None
    ended = None
    reconnects = 0
    last_timestamp = 0
    with path.open(newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            timestamp = int(row["epoch_ms"])
            if timestamp < last_timestamp or ended is not None:
                raise ValueError("connection events are out of order")
            last_timestamp = timestamp
            if row["event"] == "authenticated":
                count += 1
                if started is not None and row["terminal"] and start_ms <= timestamp - started < end_ms:
                    reconnects += 1
            elif row["event"] == "start":
                if started is not None or count == 0:
                    raise ValueError("invalid connection measurement start")
                started = timestamp
            elif row["event"] == "end":
                ended = timestamp
            else:
                raise ValueError("unknown connection event")
            if int(row["handshakes"]) != count:
                raise ValueError("connection event sequence is incomplete")
    if started is None or ended is None or ended - started < end_ms:
        return None, "connection measurement does not cover the full window"
    return reconnects, None


def percentile95(samples):
    if not samples:
        return None
    samples.sort()
    return samples[math.ceil(len(samples) * 0.95) - 1]


def summarize(directory, warmup_seconds, measurement_seconds):
    if warmup_seconds < 0 or measurement_seconds <= 0:
        raise ValueError("window must have nonnegative warmup and positive measurement duration")
    start_ms = warmup_seconds * 1000
    end_ms = start_ms + measurement_seconds * 1000
    buckets = defaultdict(lambda: {"latency": [], "dblatency": [], "outcomes": Counter()})
    last_completion = 0
    run_ids = set()
    with (directory / "result.csv").open(newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            elapsed = int(row["elapsed"])
            last_completion = max(last_completion, elapsed)
            run_ids.add(row["run"])
            if row["ttype"] not in TRANSACTIONS:
                raise ValueError("unknown transaction type: " + row["ttype"])
            if not start_ms <= elapsed < end_ms:
                continue
            error, rollback = int(row["error"]), int(row["rbk"])
            latency, dblatency = int(row["latency"]), int(row["dblatency"])
            if error not in (0, 1) or rollback not in (0, 1) or min(latency, dblatency) < 0:
                raise ValueError("invalid transaction result")
            bucket = buckets[row["ttype"]]
            bucket["latency"].append(latency)
            bucket["dblatency"].append(dblatency)
            # A rollback failure is an error, even for an intentional rollback.
            bucket["outcomes"]["errors" if error else "intentional_rollbacks" if rollback else "successful"] += 1
    if len(run_ids) != 1:
        raise ValueError("result.csv must contain exactly one run")
    problems = []
    if last_completion < end_ms:
        problems.append("transaction results do not cover the full window")
    reconnects, problem = connection_window(directory / "connections.csv", start_ms, end_ms)
    if problem:
        problems.append(problem)
    per_type = {}
    for name in TRANSACTIONS:
        bucket = buckets[name]
        per_type[name] = {
            "attempts": len(bucket["latency"]),
            "successful": bucket["outcomes"]["successful"],
            "errors": bucket["outcomes"]["errors"],
            "intentional_rollbacks": bucket["outcomes"]["intentional_rollbacks"],
            "p95_ms": percentile95(bucket["latency"]),
            "db_p95_ms": percentile95(bucket["dblatency"]),
        }
    successful = sum(per_type[name]["successful"] for name in TRANSACTIONS if name != "DELIVERY_BG")
    return {
        "run": next(iter(run_ids)),
        "window": {"warmup_seconds": warmup_seconds, "measurement_seconds": measurement_seconds,
                   "selection": "completion time in [warmup, warmup + measurement)"},
        "measurement_valid": not problems,
        "measurement_problems": problems,
        "tpmC": per_type["NEW_ORDER"]["successful"] * 60 / measurement_seconds,
        "successful_transactions_per_second": successful / measurement_seconds,
        "errors": sum(row["errors"] for row in per_type.values()),
        "intentional_rollbacks": sum(row["intentional_rollbacks"] for row in per_type.values()),
        "reconnects": reconnects,
        "reconnect_definition": "terminal sockets completing MySQL authentication during the window; excludes failed authentication and background discovery sockets",
        "transactions": per_type,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_directory", type=Path)
    parser.add_argument("--warmup-seconds", type=int, required=True)
    parser.add_argument("--measurement-seconds", type=int, required=True)
    args = parser.parse_args()
    result = summarize(args.data_directory, args.warmup_seconds, args.measurement_seconds)
    print(json.dumps(result, indent=2, allow_nan=False))
    return 0 if result["measurement_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
