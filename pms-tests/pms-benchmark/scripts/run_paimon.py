#!/usr/bin/env python3
"""Run direct/cached local lookup baselines in separate JVMs; results are local artifacts."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys


def main():
    root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--java-home', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True, help='New output directory (prefer target/)')
    parser.add_argument('--num', type=int, default=100000)
    parser.add_argument('--reads', type=int, default=20000)
    parser.add_argument('--cached-reads', type=int, default=1000000,
                        help='More queries for the faster cached path to avoid very short measurements')
    parser.add_argument('--value-size', type=int, default=100)
    parser.add_argument('--runs', type=int, default=3)
    args = parser.parse_args()
    if args.num < 2 or min(args.reads, args.cached_reads) < 4 or args.value_size < 1 or args.runs < 1:
        parser.error('Require num >= 2, reads >= 4, value-size >= 1 and runs >= 1')
    jar = root / 'pms-tests/pms-benchmark/target/pms-benchmark-0.1-SNAPSHOT-runner.jar'
    java = args.java_home.resolve() / 'bin/java'
    if not jar.is_file() or not java.is_file():
        parser.error('Build the runner first and provide a valid Java home')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest = {
        'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
        'git_status': subprocess.check_output(['git', 'status', '--short'], cwd=root, text=True),
        'jar_sha256': hashlib.sha256(jar.read_bytes()).hexdigest(),
        'java_version': subprocess.run([str(java), '-version'], capture_output=True, text=True, check=True).stderr,
        'commands': [],
    }
    all_rows = []
    for threads in (1, 4):
        for mode in ('direct', 'cached'):
            name = f'{mode}-t{threads}'
            command = [str(java), '-Xms1g', '-Xmx1g',
                       '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn', '-cp', str(jar),
                       'org.qwh.pms.benchmark.paimon.PaimonLookupBenchMain',
                       f'--mode={mode}', f'--threads={threads}', f'--num={args.num}',
                       f'--reads={args.reads if mode == "direct" else args.cached_reads}', f'--value_size={args.value_size}',
                       '--warmup_runs=1', f'--runs={args.runs}', '--seed=24301',
                       f'--db={output / "data"}', '--delete_temp_db=true']
            entry = {'name': name, 'argv': command, 'status': 'running'}
            manifest['commands'].append(entry)
            (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
            print(f'Running {name}', flush=True)
            with (output / f'{name}.csv').open('w') as stdout, (output / f'{name}.log').open('w') as stderr:
                process = subprocess.run(command, cwd=root, stdout=stdout, stderr=stderr)
            entry.update(status='passed' if process.returncode == 0 else 'failed', exit_code=process.returncode)
            (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
            if process.returncode:
                raise RuntimeError(f'{name} failed; see {output / (name + ".log")}')
            with (output / f'{name}.csv').open() as stream:
                rows = list(csv.DictReader(stream))
            measured = [row for row in rows if row['phase'] == 'measurement']
            if len(measured) != 2 * args.runs or any(int(row['errors']) for row in rows):
                raise RuntimeError(f'{name}: incomplete or invalid results')
            all_rows.extend(measured)
    with (output / 'summary.csv').open('w') as stream:
        writer = csv.writer(stream)
        writer.writerow(['mode', 'case', 'threads', 'median_ops_per_sec', 'min_ops_per_sec',
                         'max_ops_per_sec', 'median_p99_us', 'errors'])
        for mode in ('direct', 'cached'):
            for case in ('hit', 'miss'):
                for threads in (1, 4):
                    group = [row for row in all_rows if row['mode'] == mode
                             and row['case'] == case and int(row['threads']) == threads]
                    rates = [float(row['ops_per_sec']) for row in group]
                    writer.writerow([mode, case, threads, round(statistics.median(rates), 2),
                                     min(rates), max(rates),
                                     round(statistics.median(float(row['p99_us']) for row in group), 2), 0])
    print((output / 'summary.csv').read_text(), end='')


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)
