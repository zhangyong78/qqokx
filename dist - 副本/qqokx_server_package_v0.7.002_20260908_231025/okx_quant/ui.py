from __future__ import annotations

import sys as _sys

from okx_quant import ui_shell as _ui_shell
from okx_quant.ui_shell import *  # noqa: F401,F403

_sys.modules[__name__] = _ui_shell
_pkg = _sys.modules.get(__package__)
if _pkg is not None:
    setattr(_pkg, "ui", _ui_shell)
