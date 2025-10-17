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
    
    # Archiver-specific fields
    current_archive_messages: int = 0
    current_archive_bytes: int = 0

    # Syncer-specific fields
    annex_total_files: int = 0
    annex_local_files: int = 0
    annex_remote_files: int = 0
    annex_total_size: int = 0
    last_sync_time: Optional[datetime] = None


class StatusManager:
    """Manages the status of all services in the pipeline."""
    
    def __init__(self):
        self._services: Dict[str, ServiceInfo] = {
            "glbridge": ServiceInfo("glbridge", ServiceStatus.STOPPED, datetime.now()),
            "dedup": ServiceInfo("dedup", ServiceStatus.STOPPED, datetime.now()),
            "archiver": ServiceInfo("archiver", ServiceStatus.STOPPED, datetime.now()),
            "syncer": ServiceInfo("syncer", ServiceStatus.STOPPED, datetime.now()),
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
    
    async def update_archiver_stats(self, messages: int, bytes_count: int) -> None:
        """Update archiver-specific statistics."""
        async with self._lock:
            if "archiver" in self._services:
                self._services["archiver"].current_archive_messages = messages
                self._services["archiver"].current_archive_bytes = bytes_count

    async def update_syncer_stats(
        self,
        total_files: int,
        local_files: int,
        remote_files: int,
        total_size: int,
        last_sync_time: Optional[datetime] = None
    ) -> None:
        """Update syncer-specific statistics."""
        async with self._lock:
            if "syncer" in self._services:
                self._services["syncer"].annex_total_files = total_files
                self._services["syncer"].annex_local_files = local_files
                self._services["syncer"].annex_remote_files = remote_files
                self._services["syncer"].annex_total_size = total_size
                if last_sync_time:
                    self._services["syncer"].last_sync_time = last_sync_time
    
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