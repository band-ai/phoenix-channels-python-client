from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tarfile
from importlib.metadata import version
from pathlib import Path
from zipfile import ZipFile

import pytest

PACKAGE = "phoenix_channels_python_client"
PACKAGING_TEST_ENV = "PHOENIX_PACKAGE_TEST"
COMMAND_TIMEOUT_S = 120
PROJECT_ROOT = Path(__file__).resolve().parents[1]

pytestmark = pytest.mark.skipif(
    os.environ.get(PACKAGING_TEST_ENV) != "1",
    reason="installed-wheel acceptance runs in the dedicated Package Typing CI job",
)


def run(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    for variable in ("PYTHONPATH", "PYTHONHOME"):
        environment.pop(variable, None)
    return subprocess.run(  # noqa: S603 -- arguments are fixed tooling and temporary paths
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


def test_the_installed_wheel_exposes_checked_public_types(tmp_path: Path) -> None:
    uv = shutil.which("uv")
    assert uv is not None
    artifacts = tmp_path / "dist"
    succeeded(
        run(uv, "build", "--out-dir", str(artifacts), str(PROJECT_ROOT), cwd=tmp_path)
    )
    (wheel,) = artifacts.glob("*.whl")
    (sdist,) = artifacts.glob("*.tar.gz")
    marker = f"{PACKAGE}/py.typed"
    with ZipFile(wheel) as archive:
        assert marker in archive.namelist()
    with tarfile.open(sdist) as archive:
        assert any(name.endswith(f"/{marker}") for name in archive.getnames())

    environment = tmp_path / "venv"
    succeeded(
        run(uv, "venv", "--python", sys.executable, str(environment), cwd=tmp_path)
    )
    executable = environment / (
        "Scripts/python.exe" if sys.platform == "win32" else "bin/python"
    )
    succeeded(
        run(
            uv,
            "pip",
            "install",
            "--python",
            str(executable),
            str(wheel),
            f"pyrefly=={version('pyrefly')}",
            cwd=tmp_path,
        )
    )
    imported = run(
        str(executable),
        "-I",
        "-c",
        f"import {PACKAGE} as package; print(package.__file__)",
        cwd=tmp_path,
    )
    succeeded(imported)
    installed_module = Path(imported.stdout.strip())
    assert installed_module.resolve().is_relative_to(environment.resolve())
    assert installed_module.with_name("py.typed").is_file()

    consumer = tmp_path / "consumer.py"
    consumer.write_text(
        "from typing import assert_type\n"
        "from phoenix_channels_python_client import Client, Message, ProtocolVersion\n"
        "client = Client('ws://localhost/socket', api_key=None, "
        "protocol_version=ProtocolVersion.V2)\n"
        "assert_type(client, Client)\n"
        "assert_type(client.channel_socket_url, str)\n"
        "assert_type(ProtocolVersion.V2.vsn, str)\n"
        "def receive(message: Message) -> str:\n"
        "    return message.topic\n"
        "async def stop() -> None:\n"
        "    await client.shutdown()\n",
        encoding="utf-8",
    )
    diagnostics = tmp_path / "diagnostics.json"
    command = (
        str(executable),
        "-I",
        "-m",
        "pyrefly",
        "check",
        str(consumer),
        "--output-format=json",
        "--output",
        str(diagnostics),
        "--search-path",
        str(tmp_path),
        "--disable-search-path-heuristics",
        "--python-interpreter-path",
        str(executable),
    )
    succeeded(run(*command, cwd=tmp_path))

    consumer.write_text(
        "from phoenix_channels_python_client import Client\n"
        "Client('ws://localhost/socket', api_key=42)\n"
        "Client('ws://localhost/socket', 'secret')\n",
        encoding="utf-8",
    )
    rejected = run(*command, cwd=tmp_path)
    assert rejected.returncode == 1, rejected.stdout + rejected.stderr
    errors = json.loads(diagnostics.read_text(encoding="utf-8"))["errors"]
    assert [(error["line"], error["name"]) for error in errors] == [
        (2, "bad-argument-type"),
        (3, "bad-argument-count"),
    ]
    assert all(
        (tmp_path / error["path"]).resolve() == consumer.resolve() for error in errors
    ), errors
