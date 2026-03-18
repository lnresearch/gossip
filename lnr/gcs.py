"""GCS storage helper for listing and managing files in the bucket."""
import logging
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field, computed_field

logger = logging.getLogger(__name__)


class GCSFile(BaseModel):
    """Represents a file in GCS with metadata."""
    
    name: str = Field(..., description="Full path to file in bucket")
    size: int = Field(..., description="File size in bytes")
    updated: datetime = Field(..., description="Last updated timestamp")
    md5_hash: str = Field(default="", description="MD5 hash of file content")
    
    @computed_field
    @property
    def filename(self) -> str:
        """Just the filename without path."""
        return self.name.split('/')[-1]
    
    @computed_field
    @property
    def size_human(self) -> str:
        """Human-readable file size."""
        return self._format_bytes(self.size)
    
    @computed_field
    @property
    def url(self) -> str:
        """Public URL to the file."""
        return f"https://storage.googleapis.com/lnresearch/{self.name}"
    
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
            return f"{bytes_count / (1024 * 1024 * 1024):.2f} GB"


class GCSFileWithAnnexStatus(BaseModel):
    """GCS file with git-annex status."""
    
    name: str = Field(..., description="Full path to file in bucket")
    filename: str = Field(..., description="Just the filename")
    size: int = Field(..., description="File size in bytes")
    size_human: str = Field(..., description="Human-readable file size")
    updated: datetime = Field(..., description="Last updated timestamp")
    url: str = Field(..., description="Public URL to the file")
    md5_hash: str = Field(default="", description="MD5 hash of file content")
    is_annexed: bool = Field(..., description="Whether file is tracked in git-annex")
    
    @classmethod
    def from_gcs_file(cls, gcs_file: GCSFile, is_annexed: bool) -> "GCSFileWithAnnexStatus":
        """Create from a GCSFile and annex status."""
        return cls(
            name=gcs_file.name,
            filename=gcs_file.filename,
            size=gcs_file.size,
            size_human=gcs_file.size_human,
            updated=gcs_file.updated,
            url=gcs_file.url,
            md5_hash=gcs_file.md5_hash,
            is_annexed=is_annexed
        )


class GCSFileListResponse(BaseModel):
    """Response model for file list API."""
    
    files: List[GCSFileWithAnnexStatus] = Field(..., description="List of files")
    next_page_token: Optional[str] = Field(None, description="Token for next page")
    count: int = Field(..., description="Number of files in this response")


class GCSStorage:
    """Helper for interacting with GCS bucket."""
    
    def __init__(self, bucket_name: str = "lnresearch", credentials_path: Optional[str] = None):
        self.bucket_name = bucket_name
        self.credentials_path = credentials_path
        self._client = None
        self._bucket = None
    
    def _ensure_client(self):
        """Lazily initialize GCS client."""
        if self._client is None:
            try:
                from google.cloud import storage
                
                if self.credentials_path:
                    # Use explicit credentials file
                    from google.oauth2 import service_account
                    credentials = service_account.Credentials.from_service_account_file(
                        self.credentials_path
                    )
                    self._client = storage.Client(credentials=credentials)
                    logger.info(f"Using GCS credentials from: {self.credentials_path}")
                else:
                    # Use default credentials (application default or env var)
                    self._client = storage.Client()
                
                self._bucket = self._client.bucket(self.bucket_name)
            except ImportError:
                logger.error("google-cloud-storage not installed")
                raise
            except Exception as e:
                logger.error(f"Failed to initialize GCS client: {e}")
                raise
    
    def list_files(
        self, 
        prefix: str = "daily/"
    ) -> List[GCSFile]:
        """List all files in GCS bucket.
        
        Args:
            prefix: Prefix to filter files (e.g., "daily/")
        
        Returns:
            List of all files
        """
        self._ensure_client()
        
        try:
            # List all blobs (no pagination limit)
            blobs = self._client.list_blobs(
                self.bucket_name,
                prefix=prefix
            )
            
            # Collect files
            files = []
            for blob in blobs:
                # Skip directory markers
                if blob.name.endswith('/'):
                    continue
                
                files.append(GCSFile(
                    name=blob.name,
                    size=blob.size or 0,
                    updated=blob.updated or datetime.now(),
                    md5_hash=blob.md5_hash or ""
                ))
            
            # Sort by name in reverse order (newest first)
            files.sort(key=lambda f: f.name, reverse=True)
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to list GCS files: {e}")
            raise
    
    def file_exists(self, file_path: str) -> bool:
        """Check if a file exists in GCS.
        
        Args:
            file_path: Path to file in bucket (e.g., "daily/gossip-2025010100.gsp.bz2")
        
        Returns:
            True if file exists
        """
        self._ensure_client()
        
        try:
            blob = self._bucket.blob(file_path)
            return blob.exists()
        except Exception as e:
            logger.error(f"Failed to check if file exists: {e}")
            return False
