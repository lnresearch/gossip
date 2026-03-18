#!/usr/bin/env python3
"""CLI tool for managing GCS files and git-annex integration."""

import asyncio
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Set

import click
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from .config import Config
from .gcs import GCSFile, GCSStorage

console = Console()


def setup_logging(log_level: str = "INFO") -> None:
    """Setup rich logging."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)]
    )


@click.group()
@click.option('--log-level', default='INFO', help='Log level (DEBUG, INFO, WARNING, ERROR)')
@click.option('--bucket', default='lnresearch', help='GCS bucket name')
@click.option('--prefix', default='daily/', help='GCS prefix to filter files')
@click.option('--service-account', default='service-account.json', help='Path to service account JSON file')
@click.pass_context
def cli(ctx, log_level, bucket, prefix, service_account):
    """Gossip CLI - Manage GCS files and git-annex integration.
    
    This tool helps you:
    - List files in GCS that are not yet added to git-annex
    - Add GCS files to git-annex without downloading them
    - Check sync status between GCS and git-annex
    """
    ctx.ensure_object(dict)
    ctx.obj['config'] = Config()
    ctx.obj['bucket'] = bucket
    ctx.obj['prefix'] = prefix
    ctx.obj['service_account'] = service_account
    setup_logging(log_level)


async def _get_gcs_files(bucket: str, prefix: str, credentials_path: Optional[str] = None) -> List[GCSFile]:
    """Get all files from GCS."""
    storage = GCSStorage(bucket_name=bucket, credentials_path=credentials_path)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Fetching files from GCS...", total=None)
        files = storage.list_files(prefix=prefix)
        progress.update(task, description=f"Fetched {len(files)} files from GCS...")

    return files


async def _get_annexed_files(annex_dir: Path) -> Set[str]:
    """Get set of files already in git-annex from local annex directory."""
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Checking git-annex repository...", total=None)

        # List all annexed files from the local annex directory
        proc = await asyncio.create_subprocess_exec(
            "git", "annex", "list", "--allrepos",
            cwd=annex_dir,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)

        annexed_files: Set[str] = set()
        if proc.returncode == 0:
            for line in stdout.decode().strip().split('\n'):
                if not line.strip():
                    continue
                # Format is "X filename" where X indicates location
                parts = line.strip().split(None, 1)
                if len(parts) >= 2:
                    annexed_files.add(parts[1])

        progress.update(task, description=f"Found {len(annexed_files)} annexed files")

    return annexed_files


@cli.command('list-missing')
@click.option('--limit', default=None, type=int, help='Limit number of results')
@click.pass_context
def list_missing(ctx, limit):
    """List GCS files that are not yet in git-annex.
    
    Shows files that exist in GCS but are not tracked in the git-annex repository.
    Useful for identifying which files need to be added.
    """
    bucket = ctx.obj['bucket']
    prefix = ctx.obj['prefix']
    service_account = ctx.obj.get('service_account')
    config = ctx.obj['config']
    annex_dir = Path(config.annex_directory)

    async def _run():
        # Get files from both sources
        gcs_files = await _get_gcs_files(bucket, prefix, service_account)
        annexed_files = await _get_annexed_files(annex_dir)

        # Find missing files
        missing = []
        for gcs_file in gcs_files:
            if gcs_file.name not in annexed_files:
                missing.append(gcs_file)

        # Display results
        if not missing:
            console.print("\n[green]✓ All GCS files are already in git-annex![/green]\n")
            return

        console.print(f"\n[yellow]Found {len(missing)} files in GCS not yet in git-annex:[/yellow]\n")

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Filename", style="cyan")
        table.add_column("Size", justify="right", style="green")
        table.add_column("Updated", style="yellow")

        display_count = len(missing) if limit is None else min(limit, len(missing))
        for gcs_file in missing[:display_count]:
            table.add_row(
                gcs_file.filename,
                gcs_file.size_human,
                gcs_file.updated.strftime("%Y-%m-%d %H:%M")
            )

        console.print(table)

        if limit and len(missing) > limit:
            console.print(f"\n[dim]Showing {limit} of {len(missing)} total missing files[/dim]\n")
        else:
            console.print()

    asyncio.run(_run())


@cli.command('status')
@click.pass_context
def status(ctx):
    """Show sync status between GCS and git-annex.
    
    Displays:
    - Total files in GCS
    - Total files in git-annex
    - Files in GCS but not in annex
    - Files in annex but not in GCS (if any)
    """
    bucket = ctx.obj['bucket']
    prefix = ctx.obj['prefix']
    service_account = ctx.obj.get('service_account')
    config = ctx.obj['config']
    annex_dir = Path(config.annex_directory)

    async def _run():
        # Get files from both sources
        gcs_files = await _get_gcs_files(bucket, prefix, service_account)
        annexed_files = await _get_annexed_files(annex_dir)

        gcs_names = {f.name for f in gcs_files}

        # Calculate differences
        in_gcs_not_annex = gcs_names - annexed_files
        in_annex_not_gcs = annexed_files - gcs_names
        in_both = gcs_names & annexed_files

        # Calculate total sizes
        total_gcs_size = sum(f.size for f in gcs_files)
        missing_size = sum(f.size for f in gcs_files if f.name in in_gcs_not_annex)

        # Display status
        console.print("\n[bold]GCS ↔ Git-Annex Sync Status[/bold]\n")

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Metric", style="white")
        table.add_column("Count", justify="right", style="yellow")
        table.add_column("Details", style="dim")

        table.add_row(
            "Files in GCS",
            str(len(gcs_files)),
            f"{GCSFile._format_bytes(total_gcs_size)} total"
        )
        table.add_row(
            "Files in git-annex",
            str(len(annexed_files)),
            ""
        )
        table.add_row(
            "Files synced (in both)",
            str(len(in_both)),
            "[green]✓[/green]" if len(in_both) > 0 else ""
        )

        if in_gcs_not_annex:
            table.add_row(
                "Missing from annex",
                f"[yellow]{len(in_gcs_not_annex)}[/yellow]",
                f"[yellow]{GCSFile._format_bytes(missing_size)}[/yellow]"
            )
        else:
            table.add_row(
                "Missing from annex",
                "[green]0[/green]",
                "[green]✓ All synced[/green]"
            )

        if in_annex_not_gcs:
            table.add_row(
                "In annex but not GCS",
                f"[red]{len(in_annex_not_gcs)}[/red]",
                "[red]⚠ Unexpected[/red]"
            )

        console.print(table)
        console.print()

        # Sync percentage
        if len(gcs_files) > 0:
            sync_pct = (len(in_both) / len(gcs_files)) * 100
            if sync_pct == 100:
                console.print("[green]✓ 100% synced - all GCS files are in git-annex![/green]\n")
            else:
                console.print(f"[yellow]Sync status: {sync_pct:.1f}% complete[/yellow]\n")

    asyncio.run(_run())


@cli.command('annex-add')
@click.argument('files', nargs=-1)
@click.option('--all', is_flag=True, help='Add all missing files from GCS')
@click.option('--dry-run', is_flag=True, help='Show what would be done without doing it')
@click.option('--limit', default=None, type=int, help='Limit number of files to add (with --all)')
@click.option('--push', is_flag=True, help='Push changes after committing')
@click.pass_context
def annex_add(ctx, files, all, dry_run, limit, push):
    """Add GCS files to git-annex by URL.
    
    Examples:
    
        # Add specific files
        gossipcli annex-add daily/gossip-202501010000.gsp.bz2
        
        # Add all missing files
        gossipcli annex-add --all
        
        # Preview what would be added
        gossipcli annex-add --all --dry-run
        
        # Add up to 10 missing files
        gossipcli annex-add --all --limit 10
    
    This command uses 'git annex addurl' to register files by their GCS URL
    without downloading them. The files will appear as symlinks in the annex.
    """
    bucket = ctx.obj['bucket']
    prefix = ctx.obj['prefix']
    service_account = ctx.obj.get('service_account')
    config = ctx.obj['config']
    annex_dir = Path(config.annex_directory)

    if not all and not files:
        console.print("[red]Error: Provide either file names or use --all[/red]")
        sys.exit(1)

    if all and files:
        console.print("[red]Error: Cannot use both --all and specific files[/red]")
        sys.exit(1)

    async def _run():
        # Determine which files to add
        files_to_add = []
        
        if all:
            # Get all missing files
            gcs_files = await _get_gcs_files(bucket, prefix, service_account)
            annexed_files = await _get_annexed_files(annex_dir)

            for gcs_file in gcs_files:
                if gcs_file.name not in annexed_files and gcs_file.size >= 1024:
                    files_to_add.append(gcs_file)

            if limit and len(files_to_add) > limit:
                files_to_add = files_to_add[:limit]
                console.print(f"[yellow]Limited to {limit} files[/yellow]")
        else:
            # Use specific files provided
            for filename in files:
                # Ensure prefix is included
                if not filename.startswith(prefix):
                    filename = f"{prefix}{filename}"

                # Get file info from GCS
                all_gcs_files = await _get_gcs_files(bucket, prefix, service_account)
                matching = [f for f in all_gcs_files if f.name == filename]

                if matching:
                    files_to_add.append(matching[0])
                else:
                    console.print(f"[yellow]Warning: {filename} not found in GCS[/yellow]")

        if not files_to_add:
            console.print("\n[green]No files to add![/green]\n")
            return

        # Display what will be added
        console.print(f"\n[bold]{'Would add' if dry_run else 'Adding'} {len(files_to_add)} file(s) to git-annex:[/bold]\n")

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Filename", style="cyan")
        table.add_column("Size", justify="right", style="green")
        table.add_column("URL", style="dim", overflow="fold")

        for gcs_file in files_to_add:
            table.add_row(
                gcs_file.filename,
                gcs_file.size_human,
                gcs_file.url
            )

        console.print(table)
        console.print()

        if dry_run:
            console.print("[dim]Dry run - no changes made[/dim]\n")
            return

        # Actually add files
        if not annex_dir.exists():
            console.print(f"[red]Error: Annex directory not found: {annex_dir}[/red]")
            sys.exit(1)

        # Change to annex directory
        original_dir = Path.cwd()

        try:
            import os
            os.chdir(annex_dir)

            # Pull latest changes
            console.print(f"[bold]Pulling latest changes...[/bold] [dim](cwd: {Path.cwd()})[/dim]")
            try:
                result = subprocess.run(
                    ["git", "pull", "origin", "main"],
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                if result.returncode == 0:
                    console.print("[green]✓ Pulled latest changes[/green]\n")
                else:
                    console.print(f"[yellow]Warning: Failed to pull: {result.stderr}[/yellow]\n")
            except Exception as e:
                console.print(f"[yellow]Warning: Failed to pull: {e}[/yellow]\n")

            success_count = 0
            error_count = 0

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Adding files to git-annex...", total=len(files_to_add))

                for gcs_file in files_to_add:
                    try:
                        # Extract year from filename (gossip-YYYY...) and build path with year directory
                        year_match = re.search(r'gossip-(\d{4})', gcs_file.filename)
                        if year_match:
                            year = year_match.group(1)
                            # Convert daily/gossip-*.gsp.bz2 to daily/YYYY/gossip-*.gsp.bz2
                            file_path = f"daily/{year}/{gcs_file.filename}"
                            # Ensure year directory exists
                            year_dir = annex_dir / "daily" / year
                            year_dir.mkdir(parents=True, exist_ok=True)
                        else:
                            file_path = gcs_file.name

                        # Use git annex addurl to add by URL
                        # --no-check-gitignore: ignore gitignore rules from parent repo
                        result = subprocess.run(
                            ["git", "annex", "addurl", gcs_file.url, "--file", file_path, "--no-check-gitignore"],
                            capture_output=True,
                            text=True,
                            timeout=60
                        )

                        if result.returncode == 0:
                            success_count += 1
                            progress.update(task, advance=1, description=f"Added {gcs_file.filename}")
                        else:
                            error_count += 1
                            console.print(f"[red]Failed to add {gcs_file.filename}: {result.stderr}[/red]")
                            progress.update(task, advance=1)
                    except subprocess.TimeoutExpired:
                        error_count += 1
                        console.print(f"[red]Timeout adding {gcs_file.filename}[/red]")
                        progress.update(task, advance=1)
                    except Exception as e:
                        error_count += 1
                        console.print(f"[red]Error adding {gcs_file.filename}: {e}[/red]")
                        progress.update(task, advance=1)

            console.print()
            console.print(f"[green]✓ Successfully added {success_count} file(s)[/green]")

            if error_count > 0:
                console.print(f"[red]✗ Failed to add {error_count} file(s)[/red]")

            # Commit changes if any files were added
            if success_count > 0:
                console.print("\n[bold]Committing changes...[/bold]")

                try:
                    # Git commit
                    commit_msg = f"Add {success_count} file(s) from GCS to annex"
                    result = subprocess.run(
                        ["git", "commit", "-m", commit_msg],
                        capture_output=True,
                        text=True,
                        timeout=30
                    )

                    if result.returncode == 0:
                        console.print("[green]✓ Changes committed[/green]")
                    else:
                        console.print(f"[yellow]Note: {result.stderr}[/yellow]")
                except Exception as e:
                    console.print(f"[yellow]Warning: Failed to commit: {e}[/yellow]")

                # Push changes if requested
                if push:
                    console.print("\n[bold]Pushing changes...[/bold]")
                    try:
                        result = subprocess.run(
                            ["git", "push", "origin", "main"],
                            capture_output=True,
                            text=True,
                            timeout=60
                        )

                        if result.returncode == 0:
                            console.print("[green]✓ Changes pushed[/green]")
                        else:
                            console.print(f"[yellow]Warning: Failed to push: {result.stderr}[/yellow]")
                    except Exception as e:
                        console.print(f"[yellow]Warning: Failed to push: {e}[/yellow]")

            console.print()

        finally:
            os.chdir(original_dir)

    asyncio.run(_run())


@cli.command('list-gcs')
@click.option('--limit', default=50, type=int, help='Limit number of results')
@click.pass_context
def list_gcs(ctx, limit):
    """List all files in GCS bucket.
    
    Shows files currently stored in Google Cloud Storage with their metadata.
    """
    bucket = ctx.obj['bucket']
    prefix = ctx.obj['prefix']
    service_account = ctx.obj.get('service_account')

    async def _run():
        files = await _get_gcs_files(bucket, prefix, service_account)

        if not files:
            console.print(f"\n[yellow]No files found in gs://{bucket}/{prefix}[/yellow]\n")
            return

        console.print(f"\n[bold]Files in gs://{bucket}/{prefix}[/bold]\n")

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Filename", style="cyan")
        table.add_column("Size", justify="right", style="green")
        table.add_column("Updated", style="yellow")

        display_count = min(limit, len(files))
        for gcs_file in files[:display_count]:
            table.add_row(
                gcs_file.filename,
                gcs_file.size_human,
                gcs_file.updated.strftime("%Y-%m-%d %H:%M")
            )

        console.print(table)

        if len(files) > limit:
            console.print(f"\n[dim]Showing {limit} of {len(files)} total files[/dim]")

        # Summary
        total_size = sum(f.size for f in files)
        console.print(f"\n[bold]Total: {len(files)} files, {GCSFile._format_bytes(total_size)}[/bold]\n")

    asyncio.run(_run())


if __name__ == "__main__":
    cli()
