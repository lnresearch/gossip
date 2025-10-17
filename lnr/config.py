from pydantic import Field
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    """Configuration for the Lightning Network gossip processing pipeline."""
    
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
    
    # Database configuration
    database_path: str = Field(
        default="gossip.db",
        description="SQLite database path for message deduplication"
    )
    
    # Archive configuration
    archive_temp_directory: str = Field(
        default="/data/temp",
        description="Temporary directory for active archive files"
    )
    archive_directory: str = Field(
        default="annex/dailies",
        description="Directory for archiving gossip snapshots"
    )
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
    git_annex_directory: str = Field(
        default="/data/annex/daily",
        description="Git annex directory for storing file references"
    )
    github_remote: str = Field(
        default="origin",
        description="GitHub remote name for pushing annex commits"
    )

    # Syncer configuration
    sync_interval_hours: int = Field(
        default=1,
        description="Interval in hours for syncer operations"
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
