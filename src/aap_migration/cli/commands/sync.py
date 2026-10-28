"""Experimental organization-scoped add/update sync, with a read-only plan default."""

import asyncio
import json
from contextlib import nullcontext
from pathlib import Path

import click

from aap_migration.cli.decorators import handle_errors, pass_context, requires_config
from aap_migration.credential_sync import (
    CredentialSession,
    CredentialSyncError,
    credential_reader,
)
from aap_migration.org_sync import SyncError, synchronize
from aap_migration.sync_checkpoint import CheckpointError, SyncCheckpoint, public_endpoint


@click.command("sync")
@click.option("--organization", required=True, help="Exact source and target organization name")
@click.option(
    "--apply", is_flag=True, help="Apply creates/updates; requires dry_run: false in config"
)
@click.option(
    "--with-credentials",
    is_flag=True,
    help="Sync scoped credential inputs using Controller-local keys via OpenShift or SSH",
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
    with_credentials=False,
    incremental=False,
    sync_state=None,
    full_resync=False,
):
    """Plan or apply adds/updates for one organization's supported resources.

    Experimental lab path: static inventories/hosts, Git projects, job templates,
    and optional scoped credentials via --with-credentials. No deletes, job
    launches, RBAC, schedules, or execution-environment migration.
    """
    if apply and ctx.config.dry_run:
        raise click.ClickException("--apply requires dry_run: false; use the demo sync config.")
    if (sync_state is not None or full_resync) and not incremental:
        raise click.ClickException("--sync-state and --full-resync require --incremental.")
    credential_session = None
    if with_credentials:
        settings = ctx.config.credential_sync
        if settings is None:
            raise click.ClickException(
                "Configure credential_sync source/target Controller access first."
            )
        if ctx.config.logging.log_payloads:
            raise click.ClickException("Credential sync requires logging.log_payloads: false.")
        readers = [
            credential_reader(access, settings)
            for access in (settings.source, settings.target)
        ]
        credential_session = CredentialSession(*readers)

    async def run(checkpoint):
        source, target = ctx.source_client, ctx.target_client
        try:
            kwargs = {"credential_session": credential_session} if with_credentials else {}
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
                "with_credentials": with_credentials,
            }
            if with_credentials:
                scope["credential_access"] = {
                    "server": public_endpoint(settings.server) if settings.server else None,
                    "source": settings.source.model_dump(),
                    "target": settings.target.model_dump(),
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
    except (SyncError, CredentialSyncError, CheckpointError) as error:
        raise click.ClickException(str(error)) from error
    except Exception:
        if with_credentials:
            raise click.ClickException(
                "Credential-aware sync failed; diagnostic details withheld to protect inputs."
            ) from None
        raise
    click.echo(json.dumps(report, indent=2))
