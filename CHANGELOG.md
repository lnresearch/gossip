# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Fixed

- GLBridge reconnection failure: Changed from exclusive queue to durable queue with Single Active Consumer. This fixes the RESOURCE_LOCKED error loop that prevented reconnection after connection drops.

### Added

- Dashboard footer showing application uptime and git commit hash via `/api/info` endpoint

### Changed

- Updated upstream RabbitMQ IP from 10.168.0.9 to 10.64.4.6 in Kubernetes deployment (glbridge service)

## Previous

### Added

- New 'lnr upload' CLI command to automate uploading unannexed files to GCS and managing them through git-annex
- Support for --dry-run flag to preview upload operations without executing them
- Automatic bzip2 compression before GCS upload
- Automatic git-annex integration with local content dropping after successful upload

### Changed

- Archive filename format to gossip-{YYYYMMDDHHMM}.gsp for consistency and hourly flexibility
