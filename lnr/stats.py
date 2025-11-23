"""Statistics tracking for message handlers.

This module provides a flexible statistics counter based on a dict of values
that can be set, incremented, or reset. Metrics are defined dynamically as
they are used in handlers.
"""

import logging
from collections import defaultdict
from typing import Dict, Any, Union

logger = logging.getLogger(__name__)


class StatsCounter:
    """Flexible statistics counter with dict-based metrics.

    Supports three operations:
    - set(key, value): Set a metric to a specific value
    - increment(key, delta): Increment a metric by a delta (default 1)
    - reset(key): Reset a metric to 0

    Metrics are created on-the-fly as they are accessed.
    """

    def __init__(self):
        self._metrics: Dict[str, Union[int, float, bool, str]] = defaultdict(int)
        logger.info("StatsCounter initialized")

    def set(self, key: str, value: Any) -> None:
        """Set a metric to a specific value.

        Args:
            key: Metric name (e.g., "bridge.connected", "dedup.duplicates")
            value: Value to set (int, float, bool, str, etc.)
        """
        self._metrics[key] = value
        logger.debug(f"Stats: {key} = {value}")

    def increment(self, key: str, delta: Union[int, float] = 1) -> None:
        """Increment a metric by a delta.

        Args:
            key: Metric name
            delta: Amount to increment by (default 1)
        """
        # Ensure the metric exists and is numeric
        current = self._metrics.get(key, 0)
        if not isinstance(current, (int, float)):
            logger.warning(
                f"Cannot increment non-numeric metric {key} (current value: {current})"
            )
            return

        self._metrics[key] = current + delta
        logger.debug(f"Stats: {key} += {delta} (now {self._metrics[key]})")

    def reset(self, key: str) -> None:
        """Reset a metric to 0.

        Args:
            key: Metric name
        """
        self._metrics[key] = 0
        logger.debug(f"Stats: {key} reset to 0")

    def get(self, key: str, default: Any = 0) -> Any:
        """Get a metric value.

        Args:
            key: Metric name
            default: Default value if metric doesn't exist

        Returns:
            The metric value or default
        """
        return self._metrics.get(key, default)

    def get_all(self) -> Dict[str, Any]:
        """Get all metrics as a dictionary.

        Returns:
            Dictionary of all metrics
        """
        return dict(self._metrics)

    def get_filtered(self, prefix: str) -> Dict[str, Any]:
        """Get all metrics with a specific prefix.

        Args:
            prefix: Prefix to filter by (e.g., "bridge." returns all bridge metrics)

        Returns:
            Dictionary of matching metrics
        """
        return {k: v for k, v in self._metrics.items() if k.startswith(prefix)}


# Global stats counter instance
stats_counter = StatsCounter()
