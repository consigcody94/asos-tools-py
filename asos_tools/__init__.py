"""ASOS Tools — Python port of dmhuehol/ASOS-Tools with 1-minute support.

Public API:

    from asos_tools import fetch_1min, DEFAULT_VARS_1MIN

"""

from asos_tools.fetch import (
    DEFAULT_VARS_1MIN,
    fetch_1min,
    normalize_station,
)

__version__ = "0.1.0"
__all__ = ["fetch_1min", "DEFAULT_VARS_1MIN", "normalize_station", "__version__"]
