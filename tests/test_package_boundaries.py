from __future__ import annotations

import ast
import tomllib
from pathlib import Path


def test_pipeline_imports_strava_source_from_shared_package() -> None:
    tree = ast.parse(Path("strava_dlt/pipeline.py").read_text(encoding="utf-8"))
    imports = [
        node
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.names[0].name == "strava_source"
    ]

    assert imports
    assert imports[0].module == "tailor_made_dlt_sources.strava"


def test_project_depends_on_tailor_made_dlt_sources_package() -> None:
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    dependencies = pyproject["project"]["dependencies"]

    assert any(
        dependency.startswith("tailor-made-dlt-sources @ git+https://github.com/kingfink/dlt-sources")
        for dependency in dependencies
    )
    assert not any(dependency.startswith("kingfink-dlt-sources") for dependency in dependencies)
    assert pyproject["tool"]["hatch"]["metadata"]["allow-direct-references"] is True


def test_running_dlt_does_not_vendor_shared_strava_source_files() -> None:
    assert not Path("strava_dlt/source.py").exists()
    assert not Path("strava_dlt/auth.py").exists()
