"""Experimental organization-scoped add/update sync, with a read-only plan default."""

import asyncio
import json

import click

from aap_migration.cli.decorators import handle_errors, pass_context, requires_config
from aap_migration.credential_sync import (
    CredentialSession,
    CredentialSyncError,
    PodCredentialReader,
)
from aap_migration.org_sync import SyncError, synchronize


@click.command("sync")
@click.option("--organization", required=True, help="Exact source and target organization name")
@click.option(
    "--apply", is_flag=True, help="Apply creates/updates; requires dry_run: false in config"
)
@click.option(
    "--with-credentials",
    is_flag=True,
    help="Sync scoped credential inputs using mounted Controller keys via oc exec",
)
@pass_context
@requires_config
@handle_errors
def sync(ctx, organization, apply, with_credentials=False):
    """Plan or apply adds/updates for one organization's supported resources.

    Experimental lab path: static inventories/hosts, Git projects, job templates,
    and optional scoped credentials via --with-credentials. No deletes, job
    launches, RBAC, schedules, or execution-environment migration.
    """
    if apply and ctx.config.dry_run:
        raise click.ClickException("--apply requires dry_run: false; use the demo sync config.")
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
            PodCredentialReader(access, settings.oc_binary, settings.server, settings.verify_ssl)
            for access in (settings.source, settings.target)
        ]
        credential_session = CredentialSession(*readers)

    async def run():
        source, target = ctx.source_client, ctx.target_client
        try:
            kwargs = {"credential_session": credential_session} if with_credentials else {}
            return await synchronize(source, target, organization, apply=apply, **kwargs)
        finally:
            await source.client.aclose()
            await target.client.aclose()

    try:
        report = asyncio.run(run())
    except (SyncError, CredentialSyncError) as error:
        raise click.ClickException(str(error)) from error
    except Exception:
        if with_credentials:
            raise click.ClickException(
                "Credential-aware sync failed; diagnostic details withheld to protect inputs."
            ) from None
        raise
    click.echo(json.dumps(report, indent=2))
