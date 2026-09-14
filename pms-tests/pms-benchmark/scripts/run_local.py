#!/usr/bin/env python3
"""Run a fixed local-only matrix; preserve raw CSV, JVM metadata and a summary."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
from datetime import datetime, timezone


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java-home", default=os.environ.get("JAVA_HOME"), required="JAVA_HOME" not in os.environ)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--num", type=int, default=100000)
    parser.add_argument("--reads", type=int, default=1000000)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    module = Path(__file__).resolve().parents[1]
    repo = module.parents[1]
    jar = module / "target/pms-benchmark-0.1-SNAPSHOT-runner.jar"
    if not jar.is_file():
        parser.error("Build pms-tests/pms-benchmark with Maven package first")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "startedAt": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "cpuCount": os.cpu_count(),
        "gitCommit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
        "gitDirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=repo, text=True).strip()),
        "jarSha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
        "commands": [],
    }
    summary = []
    for value_size in (100, 1024):
        for threads in (1, 4):
            for profile in ("write", "memtable", "sst"):
                name = f"v{value_size}-t{threads}-{profile}"
                print(name, flush=True)
                cases = "fillseq,fillrandom" if profile == "write" else "readrandom,readmissing"
                command = [str(Path(args.java_home) / "bin/java"), "-Xms1g", "-Xmx1g", "-jar", str(jar),
                    f"--benchmarks={cases}", f"--num={args.num}", f"--reads={args.reads}",
                    f"--threads={threads}", f"--value_size={value_size}", "--key_size=16",
                    f"--prepare={'sst' if profile == 'write' else profile}",
                    "--warmup_runs=1", f"--runs={args.runs}", "--latency_samples=10000",
                    "--use_mmap=false", "--delete_temp_db=true"]
                metadata["commands"].append({"name": name, "argv": command})
                (output / "manifest.json").write_text(json.dumps(metadata, indent=2) + "\n")
                with (output / f"{name}.csv").open("w") as stdout, (output / f"{name}.log").open("w") as stderr:
                    subprocess.run(command, stdout=stdout, stderr=stderr, check=True)
                with (output / f"{name}.csv").open() as source:
                    rows = [row for row in csv.DictReader(source) if row["phase"] == "measure"]
                for case in cases.split(","):
                    records = [row for row in rows if row["benchmark"] == case]
                    if len(records) != args.runs:
                        raise RuntimeError(f"Missing measurements: {name}/{case}")
                    throughput = [float(row["ops_per_sec"]) for row in records]
                    summary.append({"profile": profile, "value_bytes": value_size, "threads": threads,
                        "benchmark": case, "median_ops_per_sec": statistics.median(throughput),
                        "min_ops_per_sec": min(throughput), "max_ops_per_sec": max(throughput),
                        "median_p99_ms": statistics.median(float(row["p99_ms"]) for row in records)})
    with (output / "summary.csv").open("w") as target:
        writer = csv.DictWriter(target, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)
    metadata["finishedAt"] = datetime.now(timezone.utc).isoformat()
    (output / "manifest.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(output / "summary.csv", flush=True)


if __name__ == "__main__":
    main()
