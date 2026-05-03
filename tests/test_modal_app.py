from __future__ import annotations

import logging

from modal_app import _configure_logging


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
