# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- New 'lnr upload' CLI command to automate uploading unannexed files to GCS and managing them through git-annex
- Support for --dry-run flag to preview upload operations without executing them
- Automatic bzip2 compression before GCS upload
- Automatic git-annex integration with local content dropping after successful upload

### Changed

- Archive filename format to gossip-{YYYYMMDDHHMM}.gsp for consistency and hourly flexibility
