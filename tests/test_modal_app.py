from __future__ import annotations

import ast
import logging
from pathlib import Path

from modal_app import _configure_logging


def test_modal_image_installs_git_before_pyproject_dependencies():
    tree = ast.parse(Path("modal_app.py").read_text(encoding="utf-8"))
    image_assignment = next(
        node for node in tree.body if isinstance(node, ast.Assign) and node.targets[0].id == "image"
    )
    calls = _chained_call_names(image_assignment.value)

    assert calls.index("apt_install") < calls.index("pip_install_from_pyproject")


def _chained_call_names(node: ast.AST) -> list[str]:
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return []

    return [*_chained_call_names(node.func.value), node.func.attr]


def test_configure_logging_lowers_existing_modal_handler_to_info():
    root = logging.getLogger()
    original_level = root.level
    original_handlers = list(root.handlers)
    handler = logging.Handler()
    handler.setLevel(logging.WARNING)

    try:
        root.handlers = [handler]
        root.setLevel(logging.WARNING)

        _configure_logging()

        assert root.level == logging.INFO
        assert handler.level == logging.INFO
    finally:
        root.handlers = original_handlers
        root.setLevel(original_level)
