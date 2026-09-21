import concurrent.futures
import csv
from dataclasses import dataclass, field
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Callable

# experiment driver, this file contains the main run loop, result collection and csv output


# ---------- config ----------
@dataclass
class EvalConfig:
    """
    Experiments should extend this class with their own experiment specific fields.
    """

    n_runs: int = 3
    n_supplementary_runs: int = 2
    local_out_dir: str = "./npf-out"
    monitor_cpu: bool = True
    cpu_max: int = 1024


# ---------- Logs collection ----------


@dataclass
class MetricSpec:
    """
    Describes a kind of result collected from the client logs.

    - `key` is the result key. It matches `RESULT-<key> <value>` log lines by default
    - `column` is the CSV column the values are written to in the output csv (e.g. "y_LATENCY")
    - `cast` defines whether to convert the captured value to a proper type (default: float)
    - `pattern`: optional regex (with one capture group) overriding the default
    """

    key: str
    column: str
    cast: Callable[[str], Any] = float
    pattern: str | None = None
    regex: re.Pattern = field(init=False)

    def __post_init__(self):
        self.regex = re.compile(self.pattern or rf"^RESULT-{self.key}\s+(\S+)\s*$")


# default metric, matching the RESULT-LATENCY lines
LAT_RESULT_RE = re.compile(r"^RESULT-LATENCY\s+([0-9.]+)\s*$")
LATENCY_METRIC = MetricSpec(
    key="LATENCY", column="y_LATENCY", pattern=LAT_RESULT_RE.pattern
)


def download_run_logs_host(host, run_dir, local_root):
    host_dir = local_root / host.alias
    host_dir.mkdir(exist_ok=True)

    subprocess.run(
        [
            "rsync",
            "-az",
            "-e",
            "ssh -o StrictHostKeyChecking=no -o BatchMode=yes -o LogLevel=ERROR",
            "--include=*/",
            "--include=client_*.stdout",
            "--exclude=*",
            f"root@{host.address}:{run_dir}/client/",
            f"{host_dir}/",
        ],
        check=False,
    )
    return host_dir


def collect_results(cfg, client_hosts, run_dir, test_name, metrics):
    """
    Downloads the client logs and extracts metrics

    Returns a dictionnary mapping each `MetricSpec.key` to the list of values found in
    the logs.
    """
    local_root = Path(cfg.local_out_dir) / "raw" / test_name / Path(run_dir).name
    local_root.mkdir(parents=True, exist_ok=True)

    # pull all hosts in parallel
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(4, max(1, len(client_hosts)))
    ) as ex:
        host_dirs = list(
            ex.map(
                lambda h: download_run_logs_host(h, run_dir, local_root), client_hosts
            )
        )

    results: dict[str, list] = {metric.key: [] for metric in metrics}
    for host_dir in host_dirs:
        for f in host_dir.rglob("client_*.stdout"):
            for line in f.read_text(errors="replace").splitlines():
                line = line.strip()
                for metric in metrics:
                    m = metric.regex.match(line)
                    if m:
                        try:
                            results[metric.key].append(metric.cast(m.group(1)))
                        except ValueError:
                            pass

    return results


def collect_latencies(cfg, client_hosts, run_dir, test_name):
    # convenience wrapper for latency experiments
    return collect_results(cfg, client_hosts, run_dir, test_name, [LATENCY_METRIC])[
        LATENCY_METRIC.key
    ]


def write_csv(path, rows, fieldnames):
    print("Writing CSV file...")
    with path.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            quoting=csv.QUOTE_NONNUMERIC,
        )
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {path} ({len(rows)} rows)")


# ---------- driver ----------
def run_eval(matrix, cfg, run_once, test_name, row_fields, metrics):
    """
    Performs a matrix of test runs, collects the results/CPU samples and writes the CSVs.

    - `matrix` contains the runs to execute
    - `cfg` = run loop config; must provide `n_runs`, `n_supplementary_runs` and
      `local_out_dir`
    - `run_once(cfg, rc, run_index, test_name) -> (results, cpu_samples)`:
      experiment specific way of organizing a single run. `results` maps each
      metric key to its list of values
    - `row_fields(rc) -> dict` function that returns the columns that are used to identify a run `rc` in the results csv
    - `metrics`: the `MetricSpec`s collected by `run_once`; the first one is the
      primary metric (a run is only retried if it produced no value for it)

    One output CSV is writtent per metric to observe. The main (primary) metric will have its results
    go in a "test_name.csv" with the others going to "test_name_metric_key.csv" files
    Each "rc" (run config) will be repeated until the at least `cfg.n_runs` are successful
    but no more than `cfg.n_runs + cfg.n_supplementary_runs` are made.

    Returns a dict mapping each metric key to the path of its CSV.
    """
    start = time.time()
    primary = metrics[0]
    cpu_path = Path(cfg.local_out_dir) / f"{test_name}_cpu.csv"
    Path(cfg.local_out_dir).mkdir(parents=True, exist_ok=True)

    rows_per_metric: dict[str, list] = {metric.key: [] for metric in metrics}
    idx_per_metric: dict[str, int] = {metric.key: 0 for metric in metrics}
    cpu_rows, cpu_idx = [], 0
    extra_keys: list[str] = []

    for rc in matrix:
        extra = row_fields(rc)
        if not extra_keys:
            extra_keys = list(extra.keys())

        successful = 0
        for attempt in range(cfg.n_runs + cfg.n_supplementary_runs):
            if successful >= cfg.n_runs:
                break

            print(f"=> {extra} run={successful} (attempt {attempt + 1})")

            # run the test
            try:
                results, cpu = run_once(cfg, rc, successful, test_name)
            except Exception as e:
                print(f"failed: {e}")
                continue

            if not results.get(primary.key):
                print(f"no {primary.key} results, retrying with double the test length")
                rc.test_length *= 2
                continue

            n_samples = {m.key: len(results.get(m.key, [])) for m in metrics}
            print(f"-> collected {n_samples} samples, {len(cpu)} cpu samples\n")

            for metric in metrics:
                for y in results.get(metric.key, []):
                    rows_per_metric[metric.key].append(
                        {
                            "index": idx_per_metric[metric.key],
                            **extra,
                            metric.column: y,
                            "run_index": successful,
                        }
                    )
                    idx_per_metric[metric.key] += 1
            for time_relative, cpu_id, utilization in cpu:
                cpu_rows.append(
                    {
                        "index": cpu_idx,
                        **extra,
                        "run_index": successful,
                        "time_rel": time_relative,
                        "cpu_id": cpu_id,
                        "utilization_percentage": utilization,
                    }
                )
                cpu_idx += 1
            successful += 1

    elapsed = time.time() - start
    print(f"\nTest finished in {elapsed} seconds")

    paths: dict[str, Path] = {}
    for i, metric in enumerate(metrics):
        name = f"{test_name}.csv" if i == 0 else f"{test_name}_{metric.key.lower()}.csv"
        path = Path(cfg.local_out_dir) / name
        write_csv(
            path,
            fieldnames=["index", *extra_keys, metric.column, "run_index"],
            rows=rows_per_metric[metric.key],
        )
        paths[metric.key] = path

    if cpu_rows:
        write_csv(
            cpu_path,
            fieldnames=[
                "index",
                *extra_keys,
                "run_index",
                "time_rel",
                "cpu_id",
                "utilization_percentage",
            ],
            rows=cpu_rows,
        )

    return paths
