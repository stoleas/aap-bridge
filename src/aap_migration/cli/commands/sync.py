"""Experimental organization-scoped add/update sync, with a read-only plan default."""

import asyncio
import json
from contextlib import nullcontext
from pathlib import Path

import click

from aap_migration.cli.decorators import handle_errors, pass_context, requires_config
from aap_migration.org_sync import SyncError, synchronize
from aap_migration.sync_checkpoint import CheckpointError, SyncCheckpoint, public_endpoint


@click.command("sync")
@click.option("--organization", required=True, help="Exact source and target organization name")
@click.option(
    "--apply", is_flag=True, help="Apply creates/updates; requires dry_run: false in config"
)
@click.option(
    "--incremental",
    "--modified-on",
    is_flag=True,
    help="Reuse a successful modified_on baseline to skip unchanged resources",
)
@click.option(
    "--sync-state",
    type=click.Path(path_type=Path),
    help="Incremental checkpoint database (default: .aap-bridge-sync/checkpoints.sqlite3)",
)
@click.option(
    "--full-resync",
    is_flag=True,
    help="With --incremental, recheck every resource and rebuild the successful baseline",
)
@pass_context
@requires_config
@handle_errors
def sync(
    ctx,
    organization,
    apply,
    incremental=False,
    sync_state=None,
    full_resync=False,
):
    """Plan or apply adds/updates for one organization's supported resources.

    Experimental lab path: static inventories/hosts, credential-free Git projects
    and job templates. No deletes, job
    launches, RBAC, schedules, or execution-environment migration.
    """
    if apply and ctx.config.dry_run:
        raise click.ClickException("--apply requires dry_run: false; use the demo sync config.")
    if (sync_state is not None or full_resync) and not incremental:
        raise click.ClickException("--sync-state and --full-resync require --incremental.")

    async def run(checkpoint):
        source, target = ctx.source_client, ctx.target_client
        try:
            kwargs = {}
            if checkpoint is not None:
                kwargs["checkpoint"] = checkpoint
            return await synchronize(source, target, organization, apply=apply, **kwargs)
        finally:
            await source.client.aclose()
            await target.client.aclose()

    try:
        context = nullcontext(None)
        if incremental:
            scope = {
                "source": public_endpoint(ctx.config.source.url),
                "target": public_endpoint(ctx.config.target.url),
                "organization": organization,
                "with_credentials": False,
            }
            context = SyncCheckpoint(
                sync_state or Path(".aap-bridge-sync/checkpoints.sqlite3"),
                scope,
                write=apply,
                full_resync=full_resync,
            )
        with context as checkpoint:
            report = asyncio.run(run(checkpoint))
        if checkpoint is not None:
            report["incremental"]["checkpoint_saved"] = checkpoint.committed
    except (SyncError, CheckpointError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(json.dumps(report, indent=2))
