from __future__ import annotations

import unittest

from PySide6.QtWidgets import QApplication, QWidget


class QtWidgetTestCase(unittest.TestCase):
    _app: QApplication

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._app = QApplication.instance() or QApplication([])

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            cls._app.processEvents()
            cls._app.quit()
            cls._app.processEvents()
        finally:
            super().tearDownClass()

    @classmethod
    def dispose_widget(cls, widget: QWidget) -> None:
        widget.close()
        widget.deleteLater()
        cls._app.processEvents()
