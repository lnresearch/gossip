import asyncio
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional
from ..config import Config
from ..status import status_manager, ServiceStatus

logger = logging.getLogger(__name__)


class SyncerService:
    """Service that manages git-annex synchronization and reports statistics."""

    def __init__(self, config: Config):
        self.config = config
        self.running = False
        self.last_sync_time: Optional[datetime] = None
        self.annex_stats: Dict[str, Any] = {}

        # Ensure git-annex directory exists
        Path(config.git_annex_directory).mkdir(parents=True, exist_ok=True)

    async def start(self) -> None:
        """Start the syncer service."""
        logger.info("Starting syncer service")
        await status_manager.update_service_status("syncer", ServiceStatus.STARTING)

        try:
            # Initialize git-annex repository if needed
            await self._ensure_git_annex_init()

            # Get initial stats
            await self._update_annex_stats()

            self.running = True
            await status_manager.update_service_status("syncer", ServiceStatus.RUNNING)
            logger.info("Syncer service started successfully")

        except Exception as e:
            logger.error(f"Failed to start syncer service: {e}")
            await status_manager.update_service_status(
                "syncer",
                ServiceStatus.ERROR,
                str(e)
            )
            raise

    async def _ensure_git_annex_init(self) -> None:
        """Ensure git-annex repository is initialized."""
        annex_dir = Path(self.config.git_annex_directory)
        git_dir = annex_dir / ".git"

        if not git_dir.exists():
            logger.info(f"Initializing git repository in {annex_dir}")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._init_git_repo)

        # Check if git-annex is initialized
        annex_uuid_file = git_dir / "annex" / "uuid"
        if not annex_uuid_file.exists():
            logger.info(f"Initializing git-annex in {annex_dir}")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._init_git_annex)

    def _init_git_repo(self) -> None:
        """Initialize git repository (blocking operation)."""
        try:
            annex_dir = Path(self.config.git_annex_directory)
            original_cwd = Path.cwd()
            import os
            os.chdir(annex_dir)

            try:
                # Initialize git repository
                subprocess.run(
                    ["git", "init"],
                    capture_output=True,
                    text=True,
                    check=True
                )

                # Set up basic git config if not already set
                try:
                    subprocess.run(
                        ["git", "config", "user.name", "LNR Gossip Pipeline"],
                        capture_output=True,
                        text=True,
                        check=True
                    )
                    subprocess.run(
                        ["git", "config", "user.email", "lnr-gossip@example.com"],
                        capture_output=True,
                        text=True,
                        check=True
                    )
                except subprocess.CalledProcessError:
                    # Config might already be set globally
                    pass

                logger.info("Git repository initialized")

            finally:
                os.chdir(original_cwd)

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to initialize git repository: {e.stderr}")
            raise

    def _init_git_annex(self) -> None:
        """Initialize git-annex (blocking operation)."""
        try:
            annex_dir = Path(self.config.git_annex_directory)
            original_cwd = Path.cwd()
            import os
            os.chdir(annex_dir)

            try:
                # Initialize git-annex
                subprocess.run(
                    ["git", "annex", "init", "lnr-gossip-pipeline"],
                    capture_output=True,
                    text=True,
                    check=True
                )

                logger.info("Git-annex initialized")

            finally:
                os.chdir(original_cwd)

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to initialize git-annex: {e.stderr}")
            raise

    async def _sync_with_remotes(self) -> None:
        """Synchronize with git-annex remotes."""
        try:
            logger.info("Starting git-annex sync")

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._run_git_annex_sync)

            self.last_sync_time = datetime.now(timezone.utc)
            logger.info("Git-annex sync completed successfully")

        except Exception as e:
            logger.error(f"Git-annex sync failed: {e}")
            await status_manager.update_service_status(
                "syncer",
                ServiceStatus.ERROR,
                f"Sync failed: {e}"
            )

    def _run_git_annex_sync(self) -> None:
        """Run git-annex sync (blocking operation)."""
        try:
            annex_dir = Path(self.config.git_annex_directory)
            original_cwd = Path.cwd()
            import os
            os.chdir(annex_dir)

            try:
                # Sync with remotes
                subprocess.run(
                    ["git", "annex", "sync"],
                    capture_output=True,
                    text=True,
                    check=True
                )

                # Also pull any new changes
                subprocess.run(
                    ["git", "pull"],
                    capture_output=True,
                    text=True,
                    check=False  # Don't fail if no remote is configured
                )

            finally:
                os.chdir(original_cwd)

        except subprocess.CalledProcessError as e:
            logger.error(f"Git-annex sync command failed: {e.stderr}")
            raise

    async def _update_annex_stats(self) -> None:
        """Update git-annex statistics."""
        try:
            loop = asyncio.get_event_loop()
            stats = await loop.run_in_executor(None, self._get_annex_stats)
            self.annex_stats = stats

            # Update status manager with annex stats
            await status_manager.update_syncer_stats(
                total_files=stats.get("total_files", 0),
                local_files=stats.get("local_files", 0),
                remote_files=stats.get("remote_files", 0),
                total_size=stats.get("total_size", 0),
                last_sync_time=self.last_sync_time
            )

            await status_manager.increment_message_count("syncer")

        except Exception as e:
            logger.error(f"Failed to update annex stats: {e}")

    def _get_annex_stats(self) -> Dict[str, Any]:
        """Get git-annex statistics (blocking operation)."""
        try:
            annex_dir = Path(self.config.git_annex_directory)
            original_cwd = Path.cwd()
            import os
            os.chdir(annex_dir)

            stats = {
                "total_files": 0,
                "local_files": 0,
                "remote_files": 0,
                "total_size": 0,
                "repository_description": "lnr-gossip-pipeline"
            }

            try:
                # Get file count and status
                result = subprocess.run(
                    ["git", "annex", "info", "--fast"],
                    capture_output=True,
                    text=True,
                    check=True
                )

                # Parse the output for basic stats
                lines = result.stdout.split('\n')
                for line in lines:
                    line = line.strip()
                    if 'annexed files in working tree:' in line:
                        stats["total_files"] = int(line.split(':')[1].strip())
                    elif 'size of annexed files in working tree:' in line:
                        size_str = line.split(':')[1].strip()
                        stats["total_size"] = self._parse_size_string(size_str)

                # Get whereis info for local vs remote files
                try:
                    whereis_result = subprocess.run(
                        ["git", "annex", "find", "--format=${file}\\n"],
                        capture_output=True,
                        text=True,
                        check=True
                    )

                    files = [f.strip() for f in whereis_result.stdout.split('\n') if f.strip()]
                    stats["total_files"] = len(files)

                    # Count local files (this is a simplified check)
                    local_count = 0
                    for filename in files:
                        file_path = annex_dir / filename
                        if file_path.exists() and not file_path.is_symlink():
                            local_count += 1

                    stats["local_files"] = local_count
                    stats["remote_files"] = stats["total_files"] - local_count

                except subprocess.CalledProcessError:
                    # If find fails, fall back to basic counts
                    pass

            finally:
                os.chdir(original_cwd)

            return stats

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get annex stats: {e.stderr}")
            return {
                "total_files": 0,
                "local_files": 0,
                "remote_files": 0,
                "total_size": 0,
                "repository_description": "lnr-gossip-pipeline",
                "error": str(e)
            }
        except Exception as e:
            logger.error(f"Failed to get annex stats: {e}")
            return {
                "total_files": 0,
                "local_files": 0,
                "remote_files": 0,
                "total_size": 0,
                "repository_description": "lnr-gossip-pipeline",
                "error": str(e)
            }

    def _parse_size_string(self, size_str: str) -> int:
        """Parse size string like '1.2 MB' to bytes."""
        try:
            parts = size_str.split()
            if len(parts) != 2:
                return 0

            value = float(parts[0])
            unit = parts[1].upper()

            multipliers = {
                'B': 1,
                'KB': 1024,
                'MB': 1024 * 1024,
                'GB': 1024 * 1024 * 1024,
                'TB': 1024 * 1024 * 1024 * 1024,
            }

            return int(value * multipliers.get(unit, 1))

        except (ValueError, IndexError):
            return 0

    def get_stats(self) -> Dict[str, Any]:
        """Get current annex statistics."""
        stats = self.annex_stats.copy()
        stats["last_sync_time"] = self.last_sync_time.isoformat() if self.last_sync_time else None
        return stats

    async def stop(self) -> None:
        """Stop the syncer service."""
        logger.info("Stopping syncer service")
        self.running = False

        await status_manager.update_service_status("syncer", ServiceStatus.STOPPED)
        logger.info("Syncer service stopped")

    async def run(self) -> None:
        """Run the syncer service."""
        await self.start()

        try:
            sync_interval_seconds = self.config.sync_interval_hours * 3600
            last_sync_check = 0
            stats_update_interval = 300  # Update stats every 5 minutes
            last_stats_update = 0

            while self.running:
                await asyncio.sleep(1)

                last_sync_check += 1
                last_stats_update += 1

                # Update stats periodically
                if last_stats_update >= stats_update_interval:
                    await self._update_annex_stats()
                    last_stats_update = 0

                # Sync periodically
                if last_sync_check >= sync_interval_seconds:
                    await self._sync_with_remotes()
                    last_sync_check = 0

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            await self.stop()