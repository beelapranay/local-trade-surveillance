"""Local market ingestion, simulated internal activity and surveillance."""

from .models import Event
from .detect import Detector, detect

__all__ = ["Event", "Detector", "detect"]
