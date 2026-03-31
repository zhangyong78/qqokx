from unittest import TestCase

from okx_quant.app_meta import APP_VERSION, build_app_title, build_version_info_text


class AppMetaTest(TestCase):
    def test_build_app_title_contains_version(self) -> None:
        self.assertIn(APP_VERSION, build_app_title())

    def test_build_version_info_text_contains_version(self) -> None:
        text = build_version_info_text()
        self.assertIn(f"版本：v{APP_VERSION}", text)
        self.assertIn("pyproject.toml", text)
