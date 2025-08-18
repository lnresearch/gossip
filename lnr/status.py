from enum import Enum
from datetime import datetime
from typing import Dict, Optional
from dataclasses import dataclass
import asyncio


class ServiceStatus(Enum):
    """Service status enumeration."""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    ERROR = "error"


@dataclass
class ServiceInfo:
    """Information about a service."""
    name: str
    status: ServiceStatus
    last_update: datetime
    message_count: int = 0
    error_message: Optional[str] = None
    last_message_time: Optional[datetime] = None


class StatusManager:
    """Manages the status of all services in the pipeline."""
    
    def __init__(self):
        self._services: Dict[str, ServiceInfo] = {
            "glbridge": ServiceInfo("glbridge", ServiceStatus.STOPPED, datetime.now()),
            "dedup": ServiceInfo("dedup", ServiceStatus.STOPPED, datetime.now()),
            "archiver": ServiceInfo("archiver", ServiceStatus.STOPPED, datetime.now()),
        }
        self._lock = asyncio.Lock()
    
    async def update_service_status(
        self,
        service_name: str,
        status: ServiceStatus,
        error_message: Optional[str] = None
    ) -> None:
        """Update the status of a service."""
        async with self._lock:
            if service_name in self._services:
                self._services[service_name].status = status
                self._services[service_name].last_update = datetime.now()
                self._services[service_name].error_message = error_message
    
    async def increment_message_count(self, service_name: str) -> None:
        """Increment the message count for a service."""
        async with self._lock:
            if service_name in self._services:
                self._services[service_name].message_count += 1
                self._services[service_name].last_message_time = datetime.now()
    
    async def get_service_info(self, service_name: str) -> Optional[ServiceInfo]:
        """Get information about a specific service."""
        async with self._lock:
            return self._services.get(service_name)
    
    async def get_all_services(self) -> Dict[str, ServiceInfo]:
        """Get information about all services."""
        async with self._lock:
            return self._services.copy()


# Global status manager instance
status_manager = StatusManager()