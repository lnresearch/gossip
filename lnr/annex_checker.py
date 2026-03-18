"""Git-annex checker to verify if files are tracked in the annex."""
import logging
import asyncio
from pathlib import Path
from typing import Optional, Set

logger = logging.getLogger(__name__)


class AnnexChecker:
    """Checks if files are part of git-annex by maintaining a checkout in /tmp."""
    
    def __init__(self, repo_url: str = "https://github.com/lnresearch/gossip.git"):
        self.repo_url = repo_url
        self.checkout_path = Path("/tmp/lnr-annex-checkout")
        self._initialized = False
        self._annexed_files: Optional[Set[str]] = None
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> None:
        """Initialize the git-annex checkout in /tmp."""
        if self._initialized:
            return
        
        async with self._lock:
            if self._initialized:
                return
            
            try:
                await self._do_initialize()
                self._initialized = True
                logger.info(f"Git-annex checkout initialized at {self.checkout_path}")
            except Exception as e:
                logger.error(f"Failed to initialize git-annex checkout: {e}")
                raise
    
    async def _do_initialize(self) -> None:
        """Perform the actual initialization (async operation)."""
        # Check if directory already exists
        if self.checkout_path.exists():
            logger.info(f"Checkout already exists at {self.checkout_path}, updating...")
            try:
                # Try to pull latest changes
                proc = await asyncio.create_subprocess_exec(
                    "git", "pull",
                    cwd=self.checkout_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
                
                if proc.returncode == 0:
                    logger.info("Updated existing checkout")
                    return
                else:
                    logger.warning(f"Failed to update existing checkout: {stderr.decode()}")
                    logger.info("Removing and re-cloning...")
                    import shutil
                    shutil.rmtree(self.checkout_path)
                    
            except asyncio.TimeoutError:
                logger.warning("Git pull timed out, removing and re-cloning...")
                import shutil
                shutil.rmtree(self.checkout_path)
        
        # Clone the repository
        logger.info(f"Cloning {self.repo_url} to {self.checkout_path}...")
        self.checkout_path.parent.mkdir(parents=True, exist_ok=True)
        
        proc = await asyncio.create_subprocess_exec(
            "git", "clone", self.repo_url, str(self.checkout_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
        
        if proc.returncode != 0:
            raise RuntimeError(f"Git clone failed: {stderr.decode()}")
        
        # Initialize git-annex
        logger.info("Initializing git-annex...")
        proc = await asyncio.create_subprocess_exec(
            "git", "annex", "init", "lnr-web-checker",
            cwd=self.checkout_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
        
        if proc.returncode != 0:
            raise RuntimeError(f"Git annex init failed: {stderr.decode()}")
    
    async def refresh_file_list(self) -> None:
        """Refresh the list of annexed files."""
        await self.initialize()
        
        async with self._lock:
            try:
                annexed_files = await self._get_annexed_files()
                self._annexed_files = annexed_files
                logger.info(f"Refreshed annexed file list: {len(annexed_files)} files")
            except Exception as e:
                logger.error(f"Failed to refresh file list: {e}")
                self._annexed_files = set()
    
    async def _get_annexed_files(self) -> Set[str]:
        """Get the set of all annexed files (async operation)."""
        try:
            # First, pull latest changes
            proc = await asyncio.create_subprocess_exec(
                "git", "pull",
                cwd=self.checkout_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await asyncio.wait_for(proc.communicate(), timeout=60)
            
            # List all annexed files
            proc = await asyncio.create_subprocess_exec(
                "git", "annex", "list", "--allrepos",
                cwd=self.checkout_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
            
            if proc.returncode != 0:
                logger.error(f"Failed to list annexed files: {stderr.decode()}")
                return set()
            
            # Parse output - format is typically "X filename" where X indicates location
            annexed_files = set()
            for line in stdout.decode().strip().split('\n'):
                if not line.strip():
                    continue
                # Skip the location indicators and get just the filename
                parts = line.strip().split(None, 1)
                if len(parts) >= 2:
                    filename = parts[1]
                    annexed_files.add(filename)
            
            return annexed_files
            
        except asyncio.TimeoutError:
            logger.error("Timeout while listing annexed files")
            return set()
        except Exception as e:
            logger.error(f"Error getting annexed files: {e}")
            return set()
    
    async def is_annexed(self, filename: str) -> bool:
        """Check if a file is tracked in git-annex.
        
        Args:
            filename: Relative path to file (e.g., "daily/gossip-2025010100.gsp.bz2")
        
        Returns:
            True if file is in git-annex
        """
        # Ensure we have the file list
        if self._annexed_files is None:
            await self.refresh_file_list()
        
        return filename in (self._annexed_files or set())
    
    async def get_file_info(self, filename: str) -> Optional[dict]:
        """Get detailed info about a file from git-annex.
        
        Args:
            filename: Relative path to file
        
        Returns:
            Dictionary with file info or None if not found
        """
        await self.initialize()
        
        try:
            proc = await asyncio.create_subprocess_exec(
                "git", "annex", "info", filename, "--json",
                cwd=self.checkout_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
            
            if proc.returncode != 0:
                return None
            
            import json
            return json.loads(stdout.decode())
            
        except Exception as e:
            logger.error(f"Failed to get file info for {filename}: {e}")
            return None


# Global instance
_annex_checker: Optional[AnnexChecker] = None


def get_annex_checker() -> AnnexChecker:
    """Get the global AnnexChecker instance."""
    global _annex_checker
    if _annex_checker is None:
        _annex_checker = AnnexChecker()
    return _annex_checker
