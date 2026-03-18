# gossipcli - Gossip Archive Management CLI

`gossipcli` is a command-line tool for managing the synchronization between Google Cloud Storage (GCS) and git-annex for Lightning Network gossip archives.

## Overview

The Lightning Network gossip processing pipeline archives messages to GCS and tracks them using git-annex. This tool helps you:

- **Discover** files in GCS that haven't been added to git-annex yet
- **Add** GCS files to git-annex by URL (without downloading)
- **Monitor** sync status between GCS and git-annex
- **Manage** the annex repository efficiently

## Installation

The tool is installed automatically when you install the `lnr` package:

```bash
uv sync
```

This registers the `gossipcli` command in your environment.

## Commands

### `gossipcli status`

Show the overall sync status between GCS and git-annex.

```bash
gossipcli status
```

**Output:**
- Total files in GCS
- Total files in git-annex
- Files synced (in both)
- Files missing from annex
- Sync percentage

**Example:**
```
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
┃ Metric               ┃  Count ┃ Details             ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
│ Files in GCS         │    150 │ 45.2 GB total       │
│ Files in git-annex   │    142 │                     │
│ Files synced         │    142 │ ✓                   │
│ Missing from annex   │      8 │ 2.1 GB              │
└──────────────────────┴────────┴─────────────────────┘

Sync status: 94.7% complete
```

### `gossipcli list-missing`

List all files in GCS that are not yet tracked in git-annex.

```bash
# Show all missing files
gossipcli list-missing

# Limit to first 10 files
gossipcli list-missing --limit 10
```

**Output:**
```
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━┓
┃ Filename                     ┃    Size ┃ Updated         ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━┩
│ gossip-202512311600.gsp.bz2  │ 512.3 KB│ 2025-12-31 16:00│
│ gossip-202512311700.gsp.bz2  │ 498.1 KB│ 2025-12-31 17:00│
│ gossip-202512311800.gsp.bz2  │ 505.7 KB│ 2025-12-31 18:00│
└──────────────────────────────┴─────────┴─────────────────┘
```

### `gossipcli list-gcs`

List all files currently in the GCS bucket.

```bash
# Show first 50 files (default)
gossipcli list-gcs

# Show first 100 files
gossipcli list-gcs --limit 100
```

### `gossipcli annex-add`

Add files from GCS to git-annex by registering their URLs.

**Important:** This command uses `git annex addurl` to add files without downloading them. The files will appear as symlinks in the annex repository.

#### Add specific files

```bash
# Add a single file
gossipcli annex-add daily/gossip-202501010000.gsp.bz2

# Add multiple files
gossipcli annex-add daily/gossip-202501010000.gsp.bz2 daily/gossip-202501010100.gsp.bz2

# Short filename (prefix added automatically)
gossipcli annex-add gossip-202501010000.gsp.bz2
```

#### Add all missing files

```bash
# Add all files missing from annex
gossipcli annex-add --all

# Preview what would be added (dry run)
gossipcli annex-add --all --dry-run

# Add up to 10 missing files
gossipcli annex-add --all --limit 10
```

**Output:**
```
Adding 3 file(s) to git-annex:

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Filename                     ┃    Size ┃ URL                           ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ gossip-202512311600.gsp.bz2  │ 512.3 KB│ https://storage.googleapis... │
│ gossip-202512311700.gsp.bz2  │ 498.1 KB│ https://storage.googleapis... │
│ gossip-202512311800.gsp.bz2  │ 505.7 KB│ https://storage.googleapis... │
└──────────────────────────────┴─────────┴─────────────────────────────────┘

✓ Successfully added 3 file(s)

Committing changes...
✓ Changes committed

Remember to push changes with: git push
```

**After adding files, remember to push:**
```bash
cd /data/annex  # or your annex directory
git push
```

## Global Options

All commands support these options:

- `--log-level <LEVEL>` - Set logging level (DEBUG, INFO, WARNING, ERROR)
- `--bucket <NAME>` - GCS bucket name (default: `lnresearch`)
- `--prefix <PATH>` - GCS prefix to filter files (default: `daily/`)

**Example:**
```bash
# Use debug logging
gossipcli --log-level DEBUG status

# Use different bucket
gossipcli --bucket my-bucket --prefix archives/ list-gcs
```

## Configuration

The tool uses the same configuration as the main `lnr` application, loaded from environment variables or `.env` file:

- `DATA_DIR` - Base directory (default: `/data`)
  - Annex directory is `{DATA_DIR}/annex`
- `GCS_BUCKET_URL` - GCS bucket URL

## How It Works

### GCS to Git-Annex Workflow

1. **Archive Service** creates `.gsp` files in `/data/temp/`
2. **Uploader Service** compresses and uploads to GCS
3. **Uploader Service** adds to git-annex and pushes
4. **gossipcli** helps manage files that may have been missed

### Git-Annex Integration

The tool maintains a checkout of the git-annex repository in `/tmp/lnr-annex-checkout` to check file status. This allows it to:

- List annexed files without modifying your working directory
- Check if files are already tracked
- Verify sync status between GCS and annex

When you run `annex-add`, the tool:

1. Validates files exist in GCS
2. Changes to your annex directory (`/data/annex`)
3. Runs `git annex addurl <GCS_URL> --file <filename>`
4. Commits the changes
5. Prompts you to push

## Use Cases

### Recovering from Upload Failures

If the uploader service failed or was interrupted, some files may be in GCS but not in git-annex:

```bash
# Check what's missing
gossipcli list-missing

# Add all missing files
gossipcli annex-add --all

# Push changes
cd /data/annex
git push
```

### Bulk Import Historical Data

If you uploaded historical archives to GCS manually:

```bash
# Preview what would be added
gossipcli annex-add --all --dry-run

# Add in batches
gossipcli annex-add --all --limit 100

# Check progress
gossipcli status
```

### Regular Monitoring

Check sync status periodically:

```bash
# Quick status check
gossipcli status

# Detailed view of missing files
gossipcli list-missing
```

## Troubleshooting

### "403 Permission Denied" from GCS

Ensure you have GCS credentials configured:

```bash
# Set up application default credentials
gcloud auth application-default login
```

Or use a service account key:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

### "Git annex not found"

Ensure git-annex is installed:

```bash
# Ubuntu/Debian
sudo apt-get install git-annex

# macOS
brew install git-annex
```

### Files not showing up in git-annex

The tool caches the annex file list. If files were recently added:

```bash
# The tool refreshes automatically, but you can also check directly
cd /data/annex
git pull
git annex list
```

### Annex directory not found

Check your `DATA_DIR` environment variable:

```bash
echo $DATA_DIR  # Should show your data directory path

# Or set it
export DATA_DIR=/path/to/data
```

## Examples

### Daily Workflow

```bash
# Morning check
gossipcli status

# Add any missing files
gossipcli annex-add --all --limit 50

# Push if files were added
cd /data/annex
git push
```

### Recovery Workflow

```bash
# Find what's missing
gossipcli list-missing --limit 20

# Add specific important files first
gossipcli annex-add gossip-202501010000.gsp.bz2 gossip-202501020000.gsp.bz2

# Then add the rest
gossipcli annex-add --all

# Verify
gossipcli status
```

## See Also

- `lnr` - Main Lightning Network Research CLI
- `lnr-web` - Web interface for monitoring services
- Git-annex documentation: https://git-annex.branchable.com/
