"""obsd - minimal metrics ingestion, alerting and SLO backend (stdlib only)."""

from .alerts import AlertEngine, ObsError
from .http_app import create_server
from .tsdb import SeriesStore

__all__ = ["SeriesStore", "AlertEngine", "create_server"]

__version__ = "0.1.0"
