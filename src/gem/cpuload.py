import base64
import csv
from pathlib import Path

from .remote import create_fabric_conn, run_cmd_ssh_parallel


# NOTE: code directly from NPF's cpuload.npf module, it has been modified to work in this context

# The script reads /proc/stat each second and output the following CSV columns: t_rel, cpu_id, util_pct (time relative to start, cpu id, utilization percentage)
# The rows where cpu_id is equal to -1 correspond to the mean across observed cores for that time sample
CPULOAD_SCRIPT = r"""
import sys, time
from collections import defaultdict
out_path, test_length = sys.argv[1], float(sys.argv[2])
cpu_min, cpu_max = int(sys.argv[3]), int(sys.argv[4])
last_idle, last_total = defaultdict(float), defaultdict(float)
start = time.time()
with open(out_path, "w", buffering=1) as out:
    out.write("t_rel,cpu_id,util_pct\n")
    first = True
    while time.time() - start < test_length:
        t_rel = time.time() - start
        rows, csum, ccnt = [], 0.0, 0
        with open("/proc/stat") as f:
            f.readline()  # skip aggregate cpu line
            for line in f:
                parts = line.strip().split()
                if not parts or not parts[0].startswith("cpu"):
                    break
                try:
                    cpuid = int(parts[0][3:])
                except ValueError:
                    continue
                vals = [float(x) for x in parts[1:]]
                idle, total = vals[3], sum(vals)
                di = idle - last_idle[cpuid]
                dt = total - last_total[cpuid]
                last_idle[cpuid], last_total[cpuid] = idle, total
                if first or dt <= 0:
                    continue
                util = 100.0 * (1.0 - di / dt)
                if cpu_min <= cpuid < cpu_max:
                    rows.append((cpuid, util))
                    csum += util
                    ccnt += 1
        if not first:
            for cid, u in rows:
                out.write("%.3f,%d,%.3f\n" % (t_rel, cid, u))
            if ccnt:
                out.write("%.3f,-1,%.3f\n" % (t_rel, csum / ccnt))
        first = False
        time.sleep(1)
"""
_CPULOAD_B64 = base64.b64encode(CPULOAD_SCRIPT.encode()).decode()


def install_cpuload(hosts, remote_path):
    cmd = f"echo {_CPULOAD_B64} | base64 -d > {remote_path}"
    return run_cmd_ssh_parallel(cmd, hosts)


def collect_cpuload(cfg, server_host, run_dir, test_name):
    local_dir = (
        Path(cfg.local_out_dir) / "raw" / test_name / Path(run_dir).name / "server"
    )
    local_dir.mkdir(parents=True, exist_ok=True)

    # download cpuload.csv
    conn = create_fabric_conn(server_host, user="root")
    remote_file = f"{run_dir}/server/cpuload.csv"
    local_file = str(local_dir / "cpuload.csv")
    try:
        conn.get(remote_file, local_file)
    except (FileNotFoundError, IOError):
        print(f"Failed to download cpuload.csv from {server_host}")

    samples = []
    csv_file = local_dir / "cpuload.csv"
    if csv_file.exists():
        with csv_file.open() as f:
            reader = csv.reader(f)
            next(reader, None)  # header
            for parts in reader:
                if len(parts) != 3:
                    continue
                try:
                    samples.append((float(parts[0]), int(parts[1]), float(parts[2])))
                except ValueError:
                    continue
    return samples
