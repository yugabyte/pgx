#!/usr/bin/env python3
"""
PGX Two-Container Docker Test Setup
-----------------------------------
Runs YugabyteDB in one container and pgx Go tests in another.
Test output streams to your terminal and is also written to a log file.

- YSQL Connection Manager is OFF by default for tests (YB_ENABLE_YSQL_CONN_MGR=0). With it on,
  pooled connections hit different backends, causing "relation does not exist", "prepared statement
  does not exist", and type/table visibility failures. Set YB_ENABLE_YSQL_CONN_MGR=1 to test with it.
- Full test output is saved to PGX_TEST_OUTPUT_LOG (default: pgx_test_output.log in PGX_DIR).
- A failure summary is printed at the end; open the log file to see full details.
- CONTRIBUTING.md setup: postgresql_setup.sql users (pgx_md5, pgx_scram, etc.) are created so
  SCRAM/MD5/plain password tests do not skip.
- TLS is OFF by default (YB_ENABLE_TLS=0) because enabling client-to-server encryption can prevent
  YSQL from binding to 5433 in some setups. Set YB_ENABLE_TLS=1 to enable TLS (certs + TLS env vars);
  if YSQL then fails to start, check docker logs and use YB_ENABLE_TLS=0 to run tests without TLS.
- Some tests are skipped or flaky on YugabyteDB by design (pgxtest.SkipYugabyteDB, LISTEN/NOTIFY, etc.).

Usage: python run_pgx_docker_tests.py
  (run from the pgx repo root, or set PGX_DIR env var)

See COMMAND_SOURCES.md for where each command originated.
"""

import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

FAIL_CONTEXT_LINES = 25


def extract_failures_from_output(output: str) -> list[tuple[str, str]]:
    """Parse go test output and return list of (test_name, error_message) for each failure."""
    failures: list[tuple[str, str]] = []
    lines = output.splitlines()
    buffer: list[str] = []

    for line in lines:
        buffer.append(line)
        if len(buffer) > FAIL_CONTEXT_LINES:
            buffer.pop(0)

        fail_match = re.match(r"^--- FAIL: (\S+)\s+\([\d.]+s\)", line)
        if fail_match:
            test_name = fail_match.group(1)
            context_lines: list[str] = []
            for b in buffer[:-1]:
                if any(
                    marker in b
                    for marker in (
                        "Error:",
                        "Error Trace:",
                        "got:",
                        "want:",
                        ".go:",
                        "Unable to",
                        "ERROR:",
                        "FAIL",
                    )
                ) or re.search(r"^\s+\S+\.go:\d+:", b):
                    context_lines.append(b)
            if not context_lines:
                context_lines = [b for b in buffer[:-1] if b.strip()][-5:]
            failures.append((test_name, "\n".join(context_lines)))

    return failures


def run(cmd: list[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    """Run a command. By default, fail on non-zero exit and stream output."""
    return subprocess.run(
        cmd,
        check=check,
        capture_output=capture,
        text=True,
    )


def run_ignore_failure(cmd: list[str]) -> None:
    """Run a command and ignore failures (e.g. cleanup when container may not exist)."""
    subprocess.run(cmd, capture_output=True, text=True)


def generate_tls_certs_and_prepare_yb_dir(pgx_dir: Path, go_image: str) -> Path:
    """
    Generate TLS certs via testsetup/generate_certs.go into .testdb/, then create
    .yb_certs/ with names YugabyteDB expects (ca.crt, node.crt, node.key) for
    --certs_for_client_dir. Returns path to .yb_certs.
    """
    testdb = pgx_dir / ".testdb"
    yb_certs = pgx_dir / ".yb_certs"
    testdb.mkdir(exist_ok=True)
    run([
        "docker", "run", "--rm",
        "-v", f"{pgx_dir}:/pgx",
        "-w", "/pgx",
        go_image,
        "sh", "-c", "mkdir -p .testdb && cd .testdb && go run ../testsetup/generate_certs.go",
    ])
    yb_certs.mkdir(exist_ok=True)
    shutil.copy(testdb / "ca.pem", yb_certs / "ca.crt")
    shutil.copy(testdb / "localhost.crt", yb_certs / "node.crt")
    shutil.copy(testdb / "localhost.key", yb_certs / "node.key")
    return yb_certs


def setup_contributing_users(pgx_dir: Path, yb_container: str) -> None:
    """
    Run testsetup/postgresql_setup.sql to create users and the tricky test user
    (CONTRIBUTING.md). Extensions and domain are already created in step 4, so we
    run only the user-creation part to avoid duplicate extension/domain errors.
    """
    setup_sql = pgx_dir / "testsetup" / "postgresql_setup.sql"
    if not setup_sql.exists():
        return
    run_ignore_failure([
        "docker", "exec", yb_container,
        "bin/ysqlsh", "-h", yb_container, "-d", "pgx_test",
        "-c", "CREATE ROLE postgres WITH SUPERUSER LOGIN;",
    ])
    run_ignore_failure(["docker", "cp", str(setup_sql), f"{yb_container}:/tmp/postgresql_setup.sql"])
    # Run full setup; extensions/domain may already exist (step 4) - ignore so users still get created
    run_ignore_failure([
        "docker", "exec", yb_container,
        "bin/ysqlsh", "-h", yb_container, "-d", "pgx_test", "--no-psqlrc",
        "-f", "/tmp/postgresql_setup.sql",
    ])
    # Ensure users exist even if setup file failed on earlier lines (e.g. extension already exists)
    users_sql = """
    SET password_encryption = md5;
    CREATE USER pgx_md5 WITH SUPERUSER PASSWORD 'secret';
    SET password_encryption = 'scram-sha-256';
    CREATE USER pgx_scram WITH SUPERUSER PASSWORD 'secret';
    CREATE USER pgx_pw WITH SUPERUSER PASSWORD 'secret';
    CREATE USER pgx_ssl WITH SUPERUSER PASSWORD 'secret';
    CREATE USER pgx_sslcert WITH SUPERUSER PASSWORD 'secret';
    """
    for stmt in users_sql.strip().split(";"):
        stmt = stmt.strip()
        if not stmt or stmt.startswith("SET"):
            continue
        run_ignore_failure([
            "docker", "exec", yb_container,
            "bin/ysqlsh", "-h", yb_container, "-d", "pgx_test",
            "-c", stmt + ";",
        ])
    # Tricky test user (aclitem test); run from file to avoid shell escaping
    # Identifier:  tricky, ' } " \ test user  (double-quote and backslash in name)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write("CREATE USER \" tricky, ' } \"\" \\\\ test user \" SUPERUSER PASSWORD 'secret';\n")
        tmp = f.name
    try:
        run_ignore_failure(["docker", "cp", tmp, f"{yb_container}:/tmp/tricky_user.sql"])
        run_ignore_failure([
            "docker", "exec", yb_container,
            "bin/ysqlsh", "-h", yb_container, "-d", "pgx_test",
            "-f", "/tmp/tricky_user.sql",
        ])
    finally:
        os.unlink(tmp)


def main() -> int:
    # Config (override via env if needed)
    pgx_dir = Path(os.environ.get("PGX_DIR", str(Path(__file__).resolve().parent)))
    log_file_path = os.environ.get("PGX_TEST_OUTPUT_LOG", str(pgx_dir / "pgx_test_output.log"))
    network_name = os.environ.get("NETWORK_NAME", "pgx-test-net")
    yb_container = os.environ.get("YB_CONTAINER", "yugabyte")
    yb_image = os.environ.get("YB_IMAGE", "yugabytedb/yugabyte:latest")
    # With TLS enabled, YSQL often needs 50–90s to bind to 5433; retries (step 4) cover extra wait
    yb_wait_sec = int(os.environ.get("YB_WAIT_SEC", "55"))
    go_image = os.environ.get("GO_IMAGE", "golang:1.23")
    enable_tls = os.environ.get("YB_ENABLE_TLS", "0").lower() in ("1", "true", "yes")

    print("=== PGX Docker Test Runner ===")
    print(f"PGX_DIR={pgx_dir}")
    print(f"Log file: {log_file_path}")
    print()

    # --- Step 0: Cleanup any previous run ---
    print("[0/9] Cleaning up any existing containers/network...")
    run_ignore_failure(["docker", "rm", "-f", "pgx-tests", yb_container])
    run_ignore_failure(["docker", "network", "rm", network_name])
    print()

    # --- Step 1: Create network ---
    print(f"[1/9] Creating Docker network: {network_name}")
    run(["docker", "network", "create", network_name])
    print()

    # --- Step 2: Generate TLS certs only when TLS is enabled ---
    yb_certs_dir = None
    if enable_tls:
        print("[2/9] Generating TLS certs (.testdb/ and .yb_certs/)...")
        yb_certs_dir = generate_tls_certs_and_prepare_yb_dir(pgx_dir, go_image)
    else:
        print("[2/9] TLS disabled (YB_ENABLE_TLS=0), skipping cert generation.")
    print()

    # --- Step 3: Start YugabyteDB ---
    # Connection manager OFF by default. TLS only when YB_ENABLE_TLS=1 (can prevent YSQL from starting).
    enable_conn_mgr = os.environ.get("YB_ENABLE_YSQL_CONN_MGR", "0")
    yugabyted_args = ["bin/yugabyted", "start", "--background=false"]
    if enable_conn_mgr and enable_conn_mgr.lower() not in ("0", "false", "no"):
        tserver_flags = "enable_ysql_conn_mgr=true"
        if enable_tls and yb_certs_dir is not None:
            tserver_flags += ",use_client_to_server_encryption=true,certs_for_client_dir=/yugabyte-certs"
        yugabyted_args.append(f"--tserver_flags={tserver_flags}")
        print("  (YSQL Connection Manager enabled)")
    elif enable_tls and yb_certs_dir is not None:
        yugabyted_args.append(
            "--tserver_flags=use_client_to_server_encryption=true,certs_for_client_dir=/yugabyte-certs"
        )
        print("  (TLS enabled for client-to-server)")
    else:
        print("  (YSQL Connection Manager and TLS disabled)")
    print(f"[3/9] Starting YugabyteDB container: {yb_container}")
    docker_run = [
        "docker", "run", "-d",
        "--name", yb_container,
        "--network", network_name,
        "-p", "5433:5433", "-p", "7000:7000", "-p", "9000:9000",
        yb_image,
        *yugabyted_args,
    ]
    if enable_tls and yb_certs_dir is not None:
        idx = docker_run.index("-p")
        docker_run.insert(idx, f"{yb_certs_dir}:/yugabyte-certs:ro")
        docker_run.insert(idx, "-v")
    run(docker_run)
    print()

    # --- Step 4: Wait for YugabyteDB ---
    # With TLS enabled, YSQL can take longer to bind to 5433; use initial wait + retries.
    yb_verify_retries = int(os.environ.get("YB_VERIFY_RETRIES", "24"))  # 24 * 5s = 2 min of retries
    print(f"[4/9] Waiting {yb_wait_sec}s for YugabyteDB, then verifying YSQL (up to {yb_verify_retries} retries)...")
    time.sleep(yb_wait_sec)
    for attempt in range(1, yb_verify_retries + 1):
        result = subprocess.run(
            ["docker", "exec", yb_container, "bin/ysqlsh", "-h", yb_container, "-c", "SELECT version();"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            print("YSQL is ready.")
            break
        if attempt < yb_verify_retries:
            print(f"  YSQL not ready yet ({attempt}/{yb_verify_retries}), retrying in 5s...")
            time.sleep(5)
        else:
            print(f"YugabyteDB YSQL did not become ready. Try increasing YB_WAIT_SEC (current: {yb_wait_sec}) or YB_VERIFY_RETRIES.")
            print(result.stderr or result.stdout)
            print("  Check: docker logs", yb_container)
            return 1
    print()

    # --- Step 5: Create test database and extensions ---
    print("[5/9] Creating pgx_test database and extensions...")
    run_ignore_failure(["docker", "exec", yb_container, "bin/ysqlsh", "-h", yb_container, "-c", "CREATE DATABASE pgx_test;"])
    run(["docker", "exec", yb_container, "bin/ysqlsh", "-h", yb_container, "-d", "pgx_test", "-c", "CREATE EXTENSION IF NOT EXISTS hstore;"])
    run(["docker", "exec", yb_container, "bin/ysqlsh", "-h", yb_container, "-d", "pgx_test", "-c", "CREATE EXTENSION IF NOT EXISTS ltree;"])
    run_ignore_failure(["docker", "exec", yb_container, "bin/ysqlsh", "-h", yb_container, "-d", "pgx_test", "-c", "CREATE DOMAIN uint64 AS numeric(20,0);"])
    print()

    # --- Step 6: CONTRIBUTING.md users (pgx_md5, pgx_scram, pgx_pw, pgx_ssl, pgx_sslcert, tricky user) ---
    print("[6/9] Creating CONTRIBUTING users (pgx_md5, pgx_scram, pgx_pw, pgx_ssl, pgx_sslcert, tricky user)...")
    setup_contributing_users(pgx_dir, yb_container)
    print()

    # --- Step 7: Run pgx tests; stream to terminal and write full output to log file ---
    # Connection env vars; add TLS vars only when TLS was enabled (otherwise TLS tests skip).
    print("[7/9] Running pgx tests (output below; full log written to file)...")
    print()
    base_conn = f"host={yb_container} port=5433 database=pgx_test sslmode=disable"
    pgx_test_db = f"{base_conn} user=yugabyte password=yugabyte"
    pgx_md5_conn = f"{base_conn} user=pgx_md5 password=secret"
    pgx_scram_conn = f"{base_conn} user=pgx_scram password=secret"
    pgx_pw_conn = f"host={yb_container} port=5433 user=pgx_pw password=secret database=pgx_test sslmode=disable"
    pgx_env = [
        ("PGX_TEST_DATABASE", pgx_test_db),
        ("PGX_TEST_TCP_CONN_STRING", pgx_test_db),
        ("PGX_TEST_MD5_PASSWORD_CONN_STRING", pgx_md5_conn),
        ("PGX_TEST_SCRAM_PASSWORD_CONN_STRING", pgx_scram_conn),
        ("PGX_TEST_PLAIN_PASSWORD_CONN_STRING", pgx_pw_conn),
        ("PGX_TEST_PGBOUNCER_CONN_STRING", pgx_test_db),
        ("PGX_TEST_CRATEDB_CONN_STRING", pgx_test_db),
    ]
    if enable_tls:
        testdb_path = "/pgx/.testdb"
        tls_conn = f"host={yb_container} port=5433 user=pgx_ssl password=secret database=pgx_test sslmode=require sslrootcert={testdb_path}/ca.pem"
        tls_client_conn = f"host={yb_container} port=5433 user=pgx_sslcert database=pgx_test sslmode=require sslrootcert={testdb_path}/ca.pem sslcert={testdb_path}/pgx_sslcert.crt sslkey={testdb_path}/pgx_sslcert.key"
        pgx_env.extend([
            ("PGX_TEST_TLS_CONN_STRING", tls_conn),
            ("PGX_TEST_TLS_CLIENT_CONN_STRING", tls_client_conn),
            ("PGX_SSL_PASSWORD", "certpw"),
        ])
    cmd = [
        "docker", "run", "--rm",
        "--name", "pgx-tests",
        "--network", network_name,
        *[arg for pair in pgx_env for arg in ("-e", f"{pair[0]}={pair[1]}")],
        "-v", f"{pgx_dir}:/pgx",
        "-w", "/pgx",
        go_image,
        "go", "test", "./...", "-v", "-count=1",
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    with open(log_file_path, "w", encoding="utf-8") as logf:
        for line in proc.stdout:
            logf.write(line)
            logf.flush()
            print(line, end="")
    result = proc.wait()

    # Failure summary from log file
    try:
        with open(log_file_path, encoding="utf-8") as f:
            captured = f.read()
    except OSError:
        captured = ""
    failures = extract_failures_from_output(captured)
    print()
    print("=" * 70)
    print(f"Full test log saved to: {log_file_path}")
    print("=" * 70)
    if failures:
        print()
        print("FAILURE SUMMARY")
        print("-" * 70)
        for idx, (test_name, msg) in enumerate(failures, 1):
            print(f"\n  {idx}. {test_name}")
            if msg.strip():
                for m in msg.splitlines():
                    print(f"     {m}")
            else:
                print("     (no message captured)")
        print()
        print("-" * 70)
        print(f"Total failures: {len(failures)} — see log file for full output.")
    print()

    # --- Step 8: Cleanup ---
    print("[8/9] Cleaning up containers and network...")
    run_ignore_failure(["docker", "rm", "-f", yb_container])
    run_ignore_failure(["docker", "network", "rm", network_name])
    print("Done.")

    return result


if __name__ == "__main__":
    sys.exit(main())
