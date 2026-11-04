from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path

PACKAGE = "phoenix_channels_python_client"
COMMAND_TIMEOUT_S = 120
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONSUMER_FIXTURES = Path(__file__).parent / "fixtures" / "package_typing"


@dataclass(frozen=True)
class PackageArtifacts:
    wheel: Path
    sdist: Path


@dataclass(frozen=True)
class InstalledPackage:
    root: Path
    python: Path


def run_command(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    for variable in ("PYTHONPATH", "PYTHONHOME"):
        environment.pop(variable, None)
    return subprocess.run(  # noqa: S603 -- fixed tooling and temporary paths
        args,
        cwd=cwd,
        env=environment,
        capture_output=True,
        text=True,
        timeout=COMMAND_TIMEOUT_S,
        check=False,
    )


def succeeded(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode == 0, result.stdout + result.stderr


def uv_path() -> str:
    executable = shutil.which("uv")
    assert executable is not None, "Package typing acceptance requires uv"
    return executable


def build_package(destination: Path) -> PackageArtifacts:
    succeeded(
        run_command(
            uv_path(),
            "build",
            "--out-dir",
            str(destination),
            str(PROJECT_ROOT),
            cwd=destination,
        )
    )
    (wheel,) = destination.glob("*.whl")
    (sdist,) = destination.glob("*.tar.gz")
    return PackageArtifacts(wheel, sdist)


def install_package(artifacts: PackageArtifacts, destination: Path) -> InstalledPackage:
    uv = uv_path()
    environment = destination / "venv"
    succeeded(
        run_command(
            uv, "venv", "--python", sys.executable, str(environment), cwd=destination
        )
    )
    executable = environment / (
        "Scripts/python.exe" if sys.platform == "win32" else "bin/python"
    )
    succeeded(
        run_command(
            uv,
            "pip",
            "install",
            "--python",
            str(executable),
            str(artifacts.wheel),
            f"pyrefly=={version('pyrefly')}",
            cwd=destination,
        )
    )
    return InstalledPackage(environment, executable)


def check_consumer(
    package: InstalledPackage, consumer: Path, diagnostics: Path
) -> subprocess.CompletedProcess[str]:
    return run_command(
        str(package.python),
        "-I",
        "-m",
        "pyrefly",
        "check",
        str(consumer),
        "--output-format=json",
        "--output",
        str(diagnostics),
        "--search-path",
        str(consumer.parent),
        "--disable-search-path-heuristics",
        "--python-interpreter-path",
        str(package.python),
        cwd=consumer.parent,
    )
