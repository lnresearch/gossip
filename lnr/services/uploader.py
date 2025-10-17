import logging
import subprocess
import bz2
import shutil
from pathlib import Path
from typing import List, Tuple
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from ..config import Config

logger = logging.getLogger(__name__)
console = Console()


class UploaderService:
    """Service for uploading unannexed files to GCS and adding them to git-annex."""

    def __init__(self, config: Config, dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run
        # Convert to absolute path to avoid issues with relative paths
        self.annex_dir = Path(config.git_annex_directory).resolve()
        self._setup_ssh_env()

    def _setup_ssh_env(self) -> None:
        """Setup SSH environment for git operations."""
        import os

        # Configure GIT_SSH_COMMAND if SSH key is specified
        if self.config.git_ssh_key_path:
            ssh_command = f"ssh -i {self.config.git_ssh_key_path}"

            # Add known_hosts if specified
            if self.config.git_ssh_known_hosts_path:
                ssh_command += f" -o UserKnownHostsFile={self.config.git_ssh_known_hosts_path}"
            else:
                # Disable strict host key checking if no known_hosts provided
                ssh_command += " -o StrictHostKeyChecking=no"

            os.environ['GIT_SSH_COMMAND'] = ssh_command
            logger.info(f"Configured GIT_SSH_COMMAND: {ssh_command}")

    def find_unannexed_files(self) -> List[Path]:
        """Find unannexed files (both .gsp and .gsp.bz2) in annex directory.

        Returns:
            List of Path objects for unannexed files (.gsp files to compress and .gsp.bz2 files not in annex)
        """
        daily_dir = self.annex_dir / "daily"
        if not daily_dir.exists():
            logger.warning(f"Directory does not exist: {daily_dir}")
            return []

        unannexed_files = []

        # Find .gsp files (need compression)
        for file_path in sorted(daily_dir.glob("gossip-*.gsp")):
            if file_path.is_file() and not file_path.is_symlink():
                unannexed_files.append(file_path)

        # Find .gsp.bz2 files that aren't already in git-annex
        for file_path in sorted(daily_dir.glob("gossip-*.gsp.bz2")):
            if file_path.is_file() and not file_path.is_symlink():
                unannexed_files.append(file_path)

        return unannexed_files

    def compress_file(self, file_path: Path) -> Path:
        """Compress file with bzip2 using command-line tool.

        Args:
            file_path: Path to file to compress

        Returns:
            Path to compressed file
        """
        file_path = file_path.resolve()  # Convert to absolute path
        compressed_path = file_path.with_suffix(file_path.suffix + '.bz2')

        if self.dry_run:
            console.print(f"[dim]Would compress: {file_path} -> {compressed_path}[/dim]")
            return compressed_path

        logger.info(f"Compressing {file_path} to {compressed_path}")

        try:
            # Store original size before compression (file will be deleted by bzip2)
            original_size = file_path.stat().st_size

            # Use command-line bzip2 for better performance
            # bzip2 -9 creates file.bz2 and removes original file
            result = subprocess.run(
                ['bzip2', '-9', str(file_path)],
                stderr=subprocess.PIPE,
                text=False,
                check=True
            )

            compressed_size = compressed_path.stat().st_size
            compression_ratio = compressed_size / original_size if original_size > 0 else 1.0

            logger.info(f"Compression completed: {self._format_bytes(original_size)} -> "
                       f"{self._format_bytes(compressed_size)} "
                       f"(ratio: {compression_ratio:.2%})")

            return compressed_path

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to compress file {file_path}: {e.stderr.decode() if e.stderr else str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to compress file {file_path}: {e}")
            raise

    def upload_to_gcs(self, file_path: Path, gcs_path: str) -> str:
        """Upload file to GCS.

        Args:
            file_path: Local file to upload
            gcs_path: Destination path in GCS (e.g., "daily/filename.gsp.bz2")

        Returns:
            GCS URL
        """
        bucket_name = "lnresearch"
        gcs_url = f"https://storage.googleapis.com/{bucket_name}/{gcs_path}"

        if self.dry_run:
            console.print(f"[dim]Would upload: {file_path} -> {gcs_url}[/dim]")
            return gcs_url

        logger.info(f"Uploading {file_path} to {gcs_url}")

        try:
            from google.cloud import storage

            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(gcs_path)

            with open(file_path, 'rb') as f:
                blob.upload_from_file(f)

            logger.info(f"Successfully uploaded to {gcs_url}")
            return gcs_url

        except ImportError:
            logger.error("google-cloud-storage not installed. Please install it: pip install google-cloud-storage")
            raise
        except Exception as e:
            logger.error(f"GCS upload failed: {e}")
            raise

    def add_to_git_annex(self, gcs_url: str, annex_filename: str, local_file_path: Path = None) -> None:
        """Add GCS URL to git-annex.

        Args:
            gcs_url: GCS URL to add
            annex_filename: Filename in annex (e.g., "daily/gossip-202510120000.gsp.bz2")
            local_file_path: Optional local file to remove before adding (to avoid conflicts)
        """
        if self.dry_run:
            console.print(f"[dim]Would add to git-annex: {gcs_url} as {annex_filename}[/dim]")
            if local_file_path:
                console.print(f"[dim]Would remove local file first: {local_file_path}[/dim]")
            return

        logger.info(f"Adding {annex_filename} to git-annex with URL {gcs_url}")

        try:
            original_cwd = Path.cwd()
            import os
            os.chdir(self.annex_dir)

            try:
                # Remove local file if it exists (git-annex will create a symlink)
                full_path = self.annex_dir / annex_filename
                if full_path.exists():
                    logger.info(f"Removing existing file: {full_path}")
                    full_path.unlink()

                # Add URL to git-annex with --relaxed to skip size check
                result = subprocess.run(
                    ["git", "annex", "addurl", gcs_url, "--file", annex_filename, "--relaxed"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                logger.info(f"Added {annex_filename} to git-annex")

            finally:
                os.chdir(original_cwd)

        except subprocess.CalledProcessError as e:
            logger.error(f"git-annex addurl failed: {e.stderr}")
            raise

    def commit_changes(self, filename: str) -> None:
        """Commit git-annex changes.

        Args:
            filename: Name of file being committed
        """
        if self.dry_run:
            console.print(f"[dim]Would commit: Add {filename} archive[/dim]")
            return

        logger.info(f"Committing changes for {filename}")

        try:
            original_cwd = Path.cwd()
            import os
            os.chdir(self.annex_dir)

            try:
                # Stage changes
                subprocess.run(
                    ["git", "add", f"daily/{filename}"],
                    capture_output=True,
                    text=True,
                    check=True
                )

                # Commit
                commit_msg = f"Add {filename} archive\n\nAutomatic upload from lnr uploader service"
                subprocess.run(
                    ["git", "commit", "-m", commit_msg],
                    capture_output=True,
                    text=True,
                    check=True
                )

                logger.info(f"Committed {filename}")

            finally:
                os.chdir(original_cwd)

        except subprocess.CalledProcessError as e:
            logger.error(f"git commit failed: {e.stderr}")
            raise

    def drop_local_copy(self, annex_filename: str) -> None:
        """Drop local copy of file from git-annex.

        Args:
            annex_filename: Filename in annex (e.g., "daily/gossip-202510120000.gsp.bz2")
        """
        if self.dry_run:
            console.print(f"[dim]Would drop local copy: {annex_filename}[/dim]")
            return

        logger.info(f"Dropping local copy of {annex_filename}")

        try:
            original_cwd = Path.cwd()
            import os
            os.chdir(self.annex_dir)

            try:
                # Drop local content
                subprocess.run(
                    ["git", "annex", "drop", "--force", annex_filename],
                    capture_output=True,
                    text=True,
                    check=True
                )
                logger.info(f"Dropped local copy of {annex_filename}")

            finally:
                os.chdir(original_cwd)

        except subprocess.CalledProcessError as e:
            logger.error(f"git annex drop failed: {e.stderr}")
            raise

    def sync_annex(self) -> None:
        """Run git annex sync --content."""
        if self.dry_run:
            console.print(f"[dim]Would run: git annex sync --content[/dim]")
            return

        logger.info("Running git annex sync --content")

        try:
            original_cwd = Path.cwd()
            import os
            os.chdir(self.annex_dir)

            try:
                subprocess.run(
                    ["git", "annex", "sync", "--content"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                logger.info("Git annex sync completed")

            finally:
                os.chdir(original_cwd)

        except subprocess.CalledProcessError as e:
            logger.error(f"git annex sync failed: {e.stderr}")
            raise

    def push_to_remotes(self) -> None:
        """Push commits to all remotes."""
        if self.dry_run:
            console.print(f"[dim]Would push to remote: {self.config.github_remote}[/dim]")
            return

        logger.info(f"Pushing to remote: {self.config.github_remote}")

        try:
            original_cwd = Path.cwd()
            import os
            os.chdir(self.annex_dir)

            try:
                subprocess.run(
                    ["git", "push", self.config.github_remote],
                    capture_output=True,
                    text=True,
                    check=True
                )
                logger.info(f"Pushed to {self.config.github_remote}")

            finally:
                os.chdir(original_cwd)

        except subprocess.CalledProcessError as e:
            logger.error(f"git push failed: {e.stderr}")
            raise

    def process_file(self, file_path: Path) -> Tuple[bool, str]:
        """Process a single unannexed file (.gsp or .gsp.bz2).

        Args:
            file_path: Path to file to process

        Returns:
            Tuple of (success, error_message)
        """
        try:
            filename = file_path.name
            console.print(f"\n[bold cyan]Processing: {filename}[/bold cyan]")

            # Handle .gsp files (need compression)
            if filename.endswith('.gsp'):
                console.print(f"  [yellow]→[/yellow] Compressing...")
                compressed_path = self.compress_file(file_path)
                compressed_filename = compressed_path.name
            # Handle .gsp.bz2 files (already compressed)
            elif filename.endswith('.gsp.bz2'):
                console.print(f"  [yellow]→[/yellow] Using pre-compressed file...")
                compressed_path = file_path
                compressed_filename = filename
            else:
                return False, f"Unknown file type: {filename}"

            # Step 2: Upload to GCS
            console.print(f"  [yellow]→[/yellow] Uploading to GCS...")
            gcs_path = f"daily/{compressed_filename}"
            gcs_url = self.upload_to_gcs(compressed_path, gcs_path)

            # Step 3: Add to git-annex (this will remove the local compressed file and create a symlink)
            console.print(f"  [yellow]→[/yellow] Adding to git-annex...")
            annex_filename = f"daily/{compressed_filename}"
            self.add_to_git_annex(gcs_url, annex_filename, local_file_path=compressed_path)

            # Step 4: Commit changes
            console.print(f"  [yellow]→[/yellow] Committing...")
            self.commit_changes(compressed_filename)

            # Step 5: Drop local copy (remove the symlink content, keep the pointer)
            console.print(f"  [yellow]→[/yellow] Dropping local copy...")
            self.drop_local_copy(annex_filename)

            console.print(f"  [bold green]✓[/bold green] Success")
            return True, ""

        except Exception as e:
            error_msg = str(e)
            console.print(f"  [bold red]✗[/bold red] Failed: {error_msg}")
            logger.error(f"Failed to process {file_path}: {e}")
            return False, error_msg

    def run(self) -> None:
        """Run the uploader service to process all unannexed files."""
        console.print("[bold]LNR Uploader Service[/bold]")
        console.print(f"Annex directory: {self.annex_dir}")

        if self.dry_run:
            console.print("[yellow]DRY RUN MODE - No changes will be made[/yellow]\n")

        # Find unannexed files
        console.print(f"\n[bold]Scanning for unannexed files (.gsp and .gsp.bz2)...[/bold]")
        unannexed_files = self.find_unannexed_files()

        if not unannexed_files:
            console.print("[green]No unannexed files found![/green]")
            return

        console.print(f"Found {len(unannexed_files)} unannexed file(s):\n")
        for file_path in unannexed_files:
            size = file_path.stat().st_size
            console.print(f"  • {file_path.name} ({self._format_bytes(size)})")

        # Process each file
        success_count = 0
        failed_files = []

        for file_path in unannexed_files:
            success, error = self.process_file(file_path)
            if success:
                success_count += 1
            else:
                failed_files.append((file_path.name, error))

        # Sync annex after processing all files
        if success_count > 0 and not self.dry_run:
            console.print(f"\n[bold]Syncing git-annex...[/bold]")
            try:
                self.sync_annex()
                console.print(f"  [bold green]✓[/bold green] Sync completed")
            except Exception as e:
                console.print(f"  [bold red]✗[/bold red] Sync failed: {e}")

        # Push to remotes after processing all files
        if success_count > 0 and not self.dry_run:
            console.print(f"\n[bold]Pushing to remotes...[/bold]")
            try:
                self.push_to_remotes()
                console.print(f"  [bold green]✓[/bold green] Push completed")
            except Exception as e:
                console.print(f"  [bold red]✗[/bold red] Push failed: {e}")

        # Summary
        console.print(f"\n[bold]Summary:[/bold]")
        console.print(f"  Total files: {len(unannexed_files)}")
        console.print(f"  [green]Successful: {success_count}[/green]")
        console.print(f"  [red]Failed: {len(failed_files)}[/red]")

        if failed_files:
            console.print(f"\n[bold red]Failed files:[/bold red]")
            for filename, error in failed_files:
                console.print(f"  • {filename}: {error}")

    @staticmethod
    def _format_bytes(bytes_count: int) -> str:
        """Format bytes into human readable form."""
        if bytes_count < 1024:
            return f"{bytes_count} B"
        elif bytes_count < 1024 * 1024:
            return f"{bytes_count / 1024:.1f} KB"
        elif bytes_count < 1024 * 1024 * 1024:
            return f"{bytes_count / (1024 * 1024):.1f} MB"
        else:
            return f"{bytes_count / (1024 * 1024 * 1024):.1f} GB"
