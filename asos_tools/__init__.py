"""ASOS Tools — Python port of dmhuehol/ASOS-Tools with 1-minute support.

Public API
----------
.. code-block:: python

    from asos_tools import fetch_1min, fetch_metars, stations
    from asos_tools.report import build_report, build_maintenance_report, \\
        build_comparison_report
"""

from asos_tools.fetch import (
    DEFAULT_VARS_1MIN,
    fetch_1min,
    normalize_station,
)
from asos_tools.metars import (
    fetch_metars,
    has_maintenance_flag,
)
from asos_tools import stations  # noqa: F401  (subpackage export)

__version__ = "0.2.0"
__all__ = [
    "fetch_1min",
    "fetch_metars",
    "has_maintenance_flag",
    "DEFAULT_VARS_1MIN",
    "normalize_station",
    "stations",
    "__version__",
]
