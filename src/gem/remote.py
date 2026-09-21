import shlex
import time

import enoslib as en
from enoslib.api import Results
from fabric import Connection

# remote command helpers


def _report(label, results, elapsed, *, quiet=False, verb="done"):
    total = len(results)
    failed = len(results.filter(status=en.STATUS_FAILED)) + len(
        results.filter(status=en.STATUS_UNREACHABLE)
    )
    if total == 0 or (quiet and failed == 0):
        return

    line = f"  [{label}] {verb} on {total - failed}/{total} host(s) in {elapsed:.1f}s"
    if failed:
        line += f"  ** {failed} host(s) FAILED **"
    print(line)


def _as_host(host, user):
    if isinstance(host, str):
        return en.Host(address=host, user=user)
    if user is None or host.user == user:
        return host
    return en.Host(
        address=host.address,
        alias=host.alias,
        user=user,
        keyfile=host.keyfile,
        port=host.port,
        extra=host.extra,
    )


def _host_with_vars(host, user, **variables):
    host = _as_host(host, user)
    if not variables:
        return host
    return en.Host(
        address=host.address,
        alias=host.alias,
        user=host.user,
        keyfile=host.keyfile,
        port=host.port,
        extra={**host.extra, **variables},
    )


def bg_inner_cmd(stdout, stderr, cmd):
    # Start cmd on each host with setsid so it survives SSH channel close
    return (
        f'mkdir -p "$(dirname {stdout})" "$(dirname {stderr})" && '
        f"setsid bash -c {shlex.quote(cmd)} > {stdout} 2> {stderr} < /dev/null &"
    )


def run_cmd_bg_enos(cmd, hosts, *, stdout, stderr, task_name="bg"):
    start = time.monotonic()
    with en.config_context(ansible_stdout="noop"):
        results = en.run_command(
            bg_inner_cmd(stdout, stderr, cmd), roles=hosts, task_name=task_name
        )
    _report(task_name, results, time.monotonic() - start, verb="started")
    return results


def ssh_bg(cmd, host, *, stdout, stderr, user="root"):
    # Start command in the bg of the host
    start = time.monotonic()
    with en.config_context(ansible_stdout="noop"):
        results = en.run_command(
            bg_inner_cmd(stdout, stderr, cmd),
            roles=[_as_host(host, user)],
            on_error_continue=True,
            task_name="bg",
        )
    _report("bg", results, time.monotonic() - start, verb="started")
    return results


def ssh_bg_hosts(hosts_cmds, *, task_name="bg", user="root"):

    roles = [
        _host_with_vars(host, user, bg_cmd=bg_inner_cmd(stdout, stderr, cmd))
        for host, cmd, stdout, stderr in hosts_cmds
    ]
    if not roles:
        return Results()

    start = time.monotonic()
    with en.config_context(ansible_stdout="noop"):
        results = en.run_command(
            "{{ bg_cmd }}", roles=roles, on_error_continue=True, task_name=task_name
        )
    _report(task_name, results, time.monotonic() - start, verb="started")
    return results


# run a command synchronously on all given hosts in parallel
def run_cmd_ssh_parallel(
    cmd, hosts, *, check=True, user="root", label="cmd", quiet=False
):
    roles = [_as_host(host, user) for host in hosts]
    if not roles:
        return Results()

    start = time.monotonic()
    with en.config_context(ansible_stdout="noop"):
        results = en.run_command(
            cmd, roles=roles, on_error_continue=True, task_name=label
        )
    _report(label, results, time.monotonic() - start, quiet=quiet)

    if check:
        failed = list(results.filter(status=en.STATUS_FAILED)) + list(
            results.filter(status=en.STATUS_UNREACHABLE)
        )
        if failed:
            details = "; ".join(
                f"{res.host} [{res.status}]"
                + (f": {res.stderr.strip()}" if res.stderr else "")
                for res in failed
            )
            raise RuntimeError(f"command failed on {len(failed)} host(s): {details}")

    return results


# send a kill signal to the names of program on the given nodes
def send_pkill_hosts(hosts, names):
    joined = " ; ".join(f"pkill -9 {n} || true" for n in names)
    return run_cmd_ssh_parallel(joined, hosts, check=False, label="pkill", quiet=True)


# fabric connection, only kept for file transfers (Connection.get/.put)
def create_fabric_conn(host, user="root") -> Connection:
    host_addr = host.address if hasattr(host, "address") else host
    return Connection(host=host_addr, user=user)
