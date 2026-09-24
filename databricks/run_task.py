"""Databricks entry wrapper for the pipeline scripts.

Runs one of the pure-Python ``pipelines/*.py`` scripts unchanged inside a
Databricks job task. The scripts and ``src/`` don't know about Databricks
(the GitHub Actions workflows run the very same files); this wrapper is the
only Databricks-specific glue:

1. Sets the run-mode env vars the code reads at import (``STAGE`` selects the
   ocha-stratus data plane — DEV vs PROD DB + blob — plus any ``--env`` extras
   such as ``EMAIL_BACKEND`` / ``MONITORING_DATE``). Credentials are NOT set here: the Job Compute
   policy injects ``DSCI_AZ_*`` from the ``dsci`` secret scope.
2. Copies ``src`` + ``pipelines`` from the wsfs git checkout onto local disk
   and runs from there. Importing packages straight off the workspace FUSE
   mount is unreliable (the import machinery's probing of non-existent
   candidate files raises hard filesystem errors intermittently), so the
   checkout is taken off the import path entirely.
3. Shells out to the script with ``PYTHONPATH`` at the copied repo root so
   ``from src ...`` resolves without ``pip install -e .``.

Usage (as the ``spark_python_task`` parameters):

    run_task.py pipelines/check_forecasts.py --stage prod --env EMAIL_BACKEND=ses
    run_task.py pipelines/export_monitoring_status.py --stage prod -- riverine
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile

_COPY_DIRS = ("src", "pipelines")


def _find_script_dir() -> str:
    """spark_python_task's exec context doesn't always define __file__."""
    try:
        return os.path.dirname(os.path.abspath(__file__))  # noqa: F821
    except NameError:
        pass
    if sys.argv and sys.argv[0]:
        return os.path.dirname(os.path.abspath(sys.argv[0]))
    return os.getcwd()


def _parse(argv):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("script", help="pipeline script, relative to repo root")
    ap.add_argument(
        "--stage",
        required=True,
        choices=["dev", "prod"],
        help="ocha-stratus data plane (DEV vs PROD DB + blob)",
    )
    ap.add_argument(
        "--env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="extra env var for the script (repeatable)",
    )
    ap.epilog = "Everything after a literal `--` is passed through to the script."
    # Split on the first literal "--" ourselves: an argparse REMAINDER
    # positional would swallow --stage/--env as soon as it sees the script.
    if "--" in argv:
        i = argv.index("--")
        argv, script_args = argv[:i], argv[i + 1 :]
    else:
        script_args = []
    args = ap.parse_args(argv)
    args.script_args = script_args
    return args


def main(argv=None):
    args = _parse(sys.argv[1:] if argv is None else argv)
    repo_root = os.path.abspath(os.path.join(_find_script_dir(), ".."))
    local_root = os.path.join(
        "/local_disk0" if os.path.isdir("/local_disk0") else tempfile.gettempdir(),
        "nga_monitor_run",
    )
    for sub in _COPY_DIRS:
        shutil.copytree(
            os.path.join(repo_root, sub),
            os.path.join(local_root, sub),
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )

    env = dict(os.environ)
    env["STAGE"] = args.stage
    for kv in args.env:
        key, _, value = kv.partition("=")
        if not key:
            raise ValueError(f"bad --env {kv!r}; expected KEY=VALUE")
        env[key] = value
    env["PYTHONPATH"] = local_root + os.pathsep + env.get("PYTHONPATH", "")
    # Unbuffered so the script's prints interleave correctly in the run log.
    env["PYTHONUNBUFFERED"] = "1"
    env["MPLCONFIGDIR"] = "/tmp/mplconfig"

    cmd = [sys.executable, os.path.join(local_root, args.script), *args.script_args]
    shown = {k: env[k] for k in ["STAGE", *[kv.partition("=")[0] for kv in args.env]]}
    print(f"[run_task] script={args.script} env={shown} args={args.script_args}")
    rc = subprocess.run(cmd, cwd=local_root, env=env, check=False).returncode
    # Databricks treats a top-level SystemExit (even code 0) as a task failure;
    # raise only on non-zero and let success return naturally.
    if rc != 0:
        raise RuntimeError(f"{args.script} exited with code {rc}")
    print("[run_task] OK")


if __name__ == "__main__":
    main()
