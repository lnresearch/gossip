"""Uploader handler for gossip archive files.

This handler periodically scans the annex directory for .gsp files,
compresses them, uploads to GCS, and adds them to git-annex.
"""

import asyncio
import logging
from pathlib import Path

from lnr.config import Config
from lnr.services.uploader import UploaderService
from lnr.status import status_manager, ServiceStatus
from lnr.stats import stats_counter

logger = logging.getLogger(__name__)


class UploaderState:
    """State container for uploader handler."""

    def __init__(self, config: Config):
        self.config = config
        self.uploader = UploaderService(config, dry_run=False)
        self.running = False
        logger.info("Uploader state initialized")

    async def start(self) -> None:
        """Start the uploader background task."""
        self.running = True
        asyncio.create_task(self._upload_loop())
        logger.info("Uploader started")

    async def stop(self) -> None:
        """Stop the uploader background task."""
        self.running = False
        logger.info("Uploader stopped")

    async def _upload_loop(self) -> None:
        """Periodically scan and upload files from annex directory."""
        while self.running:
            try:
                await self._process_unannexed_files()
                # Wait before next scan (configurable, default 60 seconds)
                await asyncio.sleep(self.config.upload_interval_seconds)
            except Exception as e:
                logger.error(f"Error in upload loop: {e}")
                stats_counter.increment("uploader.errors")
                await status_manager.update_service_status(
                    "uploader", ServiceStatus.ERROR, f"Upload loop error: {e}"
                )
                # Wait a bit before retrying
                await asyncio.sleep(30)

    async def _process_unannexed_files(self) -> None:
        """Process all unannexed files in the annex directory."""
        try:
            # Find unannexed files
            loop = asyncio.get_event_loop()
            unannexed_files = await loop.run_in_executor(
                None, self.uploader.find_unannexed_files
            )

            if not unannexed_files:
                logger.debug("No unannexed files found")
                stats_counter.set("uploader.pending_files", 0)
                return

            logger.info(f"Found {len(unannexed_files)} unannexed file(s) to process")
            stats_counter.set("uploader.pending_files", len(unannexed_files))

            # Process each file
            success_count = 0
            failed_count = 0

            for file_path in unannexed_files:
                try:
                    logger.info(f"Processing file: {file_path.name}")
                    success, error = await loop.run_in_executor(
                        None, self.uploader.process_file, file_path
                    )

                    if success:
                        success_count += 1
                        stats_counter.increment("uploader.files_uploaded")
                        logger.info(f"Successfully processed: {file_path.name}")
                    else:
                        failed_count += 1
                        stats_counter.increment("uploader.upload_failures")
                        logger.error(f"Failed to process {file_path.name}: {error}")

                except Exception as e:
                    failed_count += 1
                    stats_counter.increment("uploader.upload_failures")
                    logger.error(f"Exception processing {file_path.name}: {e}")

            # Sync annex if we had any successes
            if success_count > 0:
                try:
                    logger.info("Syncing git-annex...")
                    await loop.run_in_executor(None, self.uploader.sync_annex)
                    logger.info("Sync completed")
                except Exception as e:
                    logger.error(f"Failed to sync git-annex: {e}")

            # Push to remotes if we had any successes
            if success_count > 0:
                try:
                    logger.info("Pushing to remotes...")
                    await loop.run_in_executor(None, self.uploader.push_to_remotes)
                    logger.info("Push completed")
                except Exception as e:
                    logger.error(f"Failed to push to remotes: {e}")

            # Update status
            if failed_count == 0:
                await status_manager.update_service_status("uploader", ServiceStatus.RUNNING)
            else:
                await status_manager.update_service_status(
                    "uploader",
                    ServiceStatus.ERROR,
                    f"Processed {success_count} files, {failed_count} failed"
                )

            logger.info(f"Upload batch complete: {success_count} succeeded, {failed_count} failed")

        except Exception as e:
            logger.error(f"Error processing unannexed files: {e}")
            stats_counter.increment("uploader.errors")
            await status_manager.update_service_status(
                "uploader", ServiceStatus.ERROR, f"Error processing files: {e}"
            )
            raise
