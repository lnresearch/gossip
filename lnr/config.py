from pathlib import Path
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    """Configuration for the Lightning Network gossip processing pipeline."""

    # Data directory configuration
    data_dir: str = Field(
        default="/data",
        description="Base directory for all data storage"
    )

    # RabbitMQ configuration
    rabbitmq_url: str = Field(
        default="amqp://guest:guest@localhost:35672/",
        description="RabbitMQ connection URL"
    )

    # Upstream RabbitMQ for glbridge
    upstream_rabbitmq_url: str = Field(
        default="amqp://guest:guest@upstream:5672/",
        description="Upstream RabbitMQ connection URL for glbridge"
    )
    upstream_queue_name: str = Field(
        default="router.gossip",
        description="Upstream exchange name to subscribe to"
    )

    # Archive configuration
    archive_rotation: str = Field(
        default="hourly",
        description="Archive rotation: 'hourly' or 'daily'"
    )

    # GCS configuration
    gcs_bucket_url: str = Field(
        default="https://storage.googleapis.com/lnresearch/daily/",
        description="GCS bucket URL for uploading archive files"
    )

    # Git annex configuration
    github_remote: str = Field(
        default="origin",
        description="GitHub remote name for pushing annex commits"
    )
    git_ssh_key_path: str = Field(
        default="",
        description="Path to SSH private key for git operations (empty = use default SSH)"
    )
    git_ssh_known_hosts_path: str = Field(
        default="",
        description="Path to SSH known_hosts file (empty = use default)"
    )

    # Computed paths based on data_dir
    @computed_field
    @property
    def database_path(self) -> str:
        """SQLite database path for message deduplication: {data_dir}/db/gossip.db"""
        return str(Path(self.data_dir) / "db" / "gossip.db")

    @computed_field
    @property
    def temp_directory(self) -> str:
        """Temporary directory for active archive files: {data_dir}/temp"""
        return str(Path(self.data_dir) / "temp")

    @computed_field
    @property
    def annex_directory(self) -> str:
        """Git annex base directory: {data_dir}/annex"""
        return str(Path(self.data_dir) / "annex")

    @computed_field
    @property
    def annex_daily_directory(self) -> str:
        """Git annex daily snapshots directory: {data_dir}/annex/daily"""
        return str(Path(self.data_dir) / "annex" / "daily")

    # Syncer configuration
    sync_interval_hours: int = Field(
        default=1,
        description="Interval in hours for syncer operations"
    )

    # Uploader configuration
    upload_interval_seconds: int = Field(
        default=60,
        description="Interval in seconds for uploader to scan for unannexed files"
    )
    
    # Web server configuration
    web_host: str = Field(
        default="0.0.0.0",
        description="Web server host"
    )
    web_port: int = Field(
        default=8008,
        description="Web server port"
    )
    
    # Logging configuration
    log_level: str = Field(
        default="INFO",
        description="Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
