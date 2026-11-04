from __future__ import annotations

import json
import os
import shutil
import tarfile
from pathlib import Path
from zipfile import ZipFile

import pytest

from tests.package_typing_support import (
    CONSUMER_FIXTURES,
    PACKAGE,
    InstalledPackage,
    PackageArtifacts,
    check_consumer,
    run_command,
    succeeded,
)

PACKAGING_TEST_ENV = "PHOENIX_PACKAGE_TEST"

pytestmark = pytest.mark.skipif(
    os.environ.get(PACKAGING_TEST_ENV) != "1",
    reason="installed-wheel acceptance runs in the dedicated Package Typing CI job",
)


def test_distributions_include_the_typing_marker(
    package_artifacts: PackageArtifacts,
) -> None:
    marker = f"{PACKAGE}/py.typed"
    with ZipFile(package_artifacts.wheel) as archive:
        assert marker in archive.namelist()
    with tarfile.open(package_artifacts.sdist) as archive:
        assert any(name.endswith(f"/{marker}") for name in archive.getnames())


def test_installed_import_stays_outside_the_checkout(
    installed_package: InstalledPackage, tmp_path: Path
) -> None:
    imported = run_command(
        str(installed_package.python),
        "-I",
        "-c",
        f"import {PACKAGE} as package; print(package.__file__)",
        cwd=tmp_path,
    )
    succeeded(imported)
    installed_module = Path(imported.stdout.strip())
    assert installed_module.resolve().is_relative_to(installed_package.root.resolve())
    assert installed_module.with_name("py.typed").is_file()


def test_installed_public_types_accept_valid_calls(
    installed_package: InstalledPackage, tmp_path: Path
) -> None:
    consumer = tmp_path / "valid.py"
    shutil.copyfile(CONSUMER_FIXTURES / "valid.py.txt", consumer)

    succeeded(
        check_consumer(installed_package, consumer, tmp_path / "diagnostics.json")
    )


def test_installed_signatures_reject_invalid_calls(
    installed_package: InstalledPackage, tmp_path: Path
) -> None:
    consumer = tmp_path / "invalid.py"
    shutil.copyfile(CONSUMER_FIXTURES / "invalid.py.txt", consumer)
    diagnostics = tmp_path / "diagnostics.json"

    rejected = check_consumer(installed_package, consumer, diagnostics)

    assert rejected.returncode == 1, rejected.stdout + rejected.stderr
    errors = json.loads(diagnostics.read_text(encoding="utf-8"))["errors"]
    assert [(error["line"], error["name"]) for error in errors] == [
        (2, "bad-argument-type"),
        (3, "bad-argument-count"),
    ]
    assert all(
        (tmp_path / error["path"]).resolve() == consumer.resolve() for error in errors
    ), errors
