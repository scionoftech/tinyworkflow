"""
CLI tool for managing workflows using Click.
"""

import click
import asyncio
import json
import os
from typing import Optional
from datetime import datetime
from functools import wraps
from tabulate import tabulate

from tinyworkflow.client import TinyWorkflowClient
from tinyworkflow.models import WorkflowStatus


def async_command(f):
    """Decorator to run async commands."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


@click.group()
@click.option(
    "--db",
    default=None,
    envvar="TINYWORKFLOW_DATABASE_URL",
    help="Database URL (can also be set via TINYWORKFLOW_DATABASE_URL env var)",
    show_default=True,
)
@click.pass_context
def cli(ctx, db):
    """TinyWorkflow - Durable workflow orchestration CLI."""
    ctx.ensure_object(dict)
    # Use provided db, or environment variable, or default
    ctx.obj["db_url"] = db or os.getenv("TINYWORKFLOW_DATABASE_URL", "sqlite+aiosqlite:///tinyworkflow.db")


@cli.command()
@click.argument("workflow_name")
@click.option("--input", "-i", help="Input data as JSON string")
@click.option("--wait", is_flag=True, help="Wait for workflow completion")
@click.pass_context
@async_command
async def start(ctx, workflow_name, input, wait):
    """Start a workflow execution."""
    db_url = ctx.obj["db_url"]

    # Parse input data
    input_data = None
    if input:
        try:
            input_data = json.loads(input)
        except json.JSONDecodeError:
            click.echo("Error: Invalid JSON input", err=True)
            return

    async with TinyWorkflowClient(db_url) as client:
        try:
            run_id = await client.start_workflow(workflow_name, input_data=input_data, wait=wait)
            click.echo(f"Workflow started: {run_id}")

            if wait:
                workflow = await client.get_workflow_status(run_id)
                if workflow:
                    click.echo(f"Status: {workflow.status}")
                    if workflow.output_data:
                        click.echo(f"Output: {json.dumps(workflow.output_data, indent=2)}")
                    if workflow.error:
                        click.echo(f"Error: {workflow.error}", err=True)

        except Exception as e:
            click.echo(f"Error: {e}", err=True)


@cli.command()
@click.argument("run_id")
@click.pass_context
@async_command
async def status(ctx, run_id):
    """Get workflow execution status."""
    db_url = ctx.obj["db_url"]

    async with TinyWorkflowClient(db_url) as client:
        workflow = await client.get_workflow_status(run_id)
        if not workflow:
            click.echo(f"Workflow not found: {run_id}", err=True)
            return

        click.echo(f"\nWorkflow: {workflow.workflow_name}")
        click.echo(f"Run ID: {workflow.run_id}")
        click.echo(f"Status: {workflow.status}")
        click.echo(f"Created: {workflow.created_at}")

        if workflow.started_at:
            click.echo(f"Started: {workflow.started_at}")
        if workflow.completed_at:
            click.echo(f"Completed: {workflow.completed_at}")

        if workflow.input_data:
            click.echo(f"\nInput: {json.dumps(workflow.input_data, indent=2)}")

        if workflow.output_data:
            click.echo(f"\nOutput: {json.dumps(workflow.output_data, indent=2)}")

        if workflow.error:
            click.echo(f"\nError: {workflow.error}")

        click.echo(f"\nRetries: {workflow.retry_count}/{workflow.max_retries}")


@cli.command()
@click.option("--status", "-s", help="Filter by status")
@click.option("--workflow", "-w", help="Filter by workflow name")
@click.option("--limit", "-l", default=20, help="Maximum number of results", show_default=True)
@click.pass_context
@async_command
async def list(ctx, status, workflow, limit):
    """List workflow executions."""
    db_url = ctx.obj["db_url"]

    # Parse status
    status_filter = None
    if status:
        try:
            status_filter = WorkflowStatus(status.lower())
        except ValueError:
            click.echo(f"Error: Invalid status '{status}'", err=True)
            return

    async with TinyWorkflowClient(db_url) as client:
        workflows = await client.list_workflow_executions(
            status=status_filter, workflow_name=workflow, limit=limit
        )

        if not workflows:
            click.echo("No workflows found")
            return

        # Format as table
        headers = ["Run ID", "Workflow", "Status", "Created", "Completed"]
        rows = []
        for w in workflows:
            rows.append(
                [
                    w.run_id[:16] + "...",
                    w.workflow_name,
                    w.status,
                    w.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    w.completed_at.strftime("%Y-%m-%d %H:%M:%S") if w.completed_at else "-",
                ]
            )

        click.echo(tabulate(rows, headers=headers, tablefmt="grid"))


@cli.command()
@click.argument("run_id")
@click.pass_context
@async_command
async def cancel(ctx, run_id):
    """Cancel a workflow execution."""
    db_url = ctx.obj["db_url"]

    async with TinyWorkflowClient(db_url) as client:
        try:
            await client.cancel_workflow(run_id)
            click.echo(f"Workflow cancelled: {run_id}")
        except Exception as e:
            click.echo(f"Error: {e}", err=True)


@cli.command()
@click.argument("run_id")
@click.option("--limit", "-l", default=50, help="Maximum number of events", show_default=True)
@click.pass_context
@async_command
async def events(ctx, run_id, limit):
    """Show workflow event history."""
    db_url = ctx.obj["db_url"]

    async with TinyWorkflowClient(db_url) as client:
        events = await client.get_workflow_events(run_id, limit=limit)

        if not events:
            click.echo("No events found")
            return

        # Format as table
        headers = ["Timestamp", "Event Type", "Data"]
        rows = []
        for event in reversed(events):  # Show oldest first
            data_str = json.dumps(event.event_data) if event.event_data else "-"
            if len(data_str) > 50:
                data_str = data_str[:47] + "..."
            rows.append(
                [event.timestamp.strftime("%Y-%m-%d %H:%M:%S"), event.event_type, data_str]
            )

        click.echo(tabulate(rows, headers=headers, tablefmt="grid"))


@cli.command()
@click.argument("workflow_name")
@click.argument("cron_expression")
@click.option("--input", "-i", help="Input data as JSON string")
@click.pass_context
@async_command
async def schedule(ctx, workflow_name, cron_expression, input):
    """Schedule a workflow with cron expression."""
    db_url = ctx.obj["db_url"]

    # Parse input data
    input_data = None
    if input:
        try:
            input_data = json.loads(input)
        except json.JSONDecodeError:
            click.echo("Error: Invalid JSON input", err=True)
            return

    async with TinyWorkflowClient(db_url) as client:
        try:
            job_id = await client.schedule_workflow(workflow_name, cron_expression, input_data)
            click.echo(f"Workflow scheduled: {job_id}")
            click.echo(f"Cron: {cron_expression}")
        except Exception as e:
            click.echo(f"Error: {e}", err=True)


@cli.command()
@click.pass_context
@async_command
async def approvals(ctx):
    """List workflows pending approval."""
    db_url = ctx.obj["db_url"]

    async with TinyWorkflowClient(db_url) as client:
        workflows = await client.get_pending_approvals()

        if not workflows:
            click.echo("No pending approvals")
            return

        # Format as table
        headers = ["Run ID", "Workflow", "Approval Key", "Created"]
        rows = []
        for w in workflows:
            rows.append(
                [
                    w.run_id[:16] + "...",
                    w.workflow_name,
                    w.approval_required or "-",
                    w.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                ]
            )

        click.echo(tabulate(rows, headers=headers, tablefmt="grid"))


@cli.command()
@click.argument("run_id")
@click.option("--approve", is_flag=True, help="Approve the workflow")
@click.option("--reject", is_flag=True, help="Reject the workflow")
@click.pass_context
@async_command
async def approve(ctx, run_id, approve, reject):
    """Approve or reject a workflow."""
    db_url = ctx.obj["db_url"]

    if approve == reject:
        click.echo("Error: Must specify either --approve or --reject", err=True)
        return

    async with TinyWorkflowClient(db_url) as client:
        try:
            if approve:
                await client.approve_workflow(run_id)
                click.echo(f"Workflow approved: {run_id}")
            else:
                await client.reject_workflow(run_id)
                click.echo(f"Workflow rejected: {run_id}")
        except Exception as e:
            click.echo(f"Error: {e}", err=True)


@cli.command()
@click.pass_context
@async_command
async def workflows(ctx):
    """List all registered workflows."""
    db_url = ctx.obj["db_url"]

    async with TinyWorkflowClient(db_url) as client:
        workflow_names = client.list_registered_workflows()

        if not workflow_names:
            click.echo("No workflows registered")
            return

        click.echo("Registered workflows:")
        for name in workflow_names:
            click.echo(f"  - {name}")


@cli.command()
@click.option("--host", default="0.0.0.0", help="Server host", show_default=True)
@click.option("--port", default=8080, help="Server port", show_default=True)
@click.option(
    "--import-workflows",
    "-w",
    help="Python module path to import (e.g., 'examples.app' or 'myproject.workflows')",
)
@click.pass_context
def server(ctx, host, port, import_workflows):
    """Start the web UI server."""
    db_url = ctx.obj["db_url"]

    click.echo(f"Starting TinyWorkflow server on http://{host}:{port}")
    click.echo(f"Database: {db_url}")

    # Import workflows module if specified
    if import_workflows:
        click.echo(f"Importing workflows from: {import_workflows}")
        try:
            import importlib
            import sys

            # Add current directory to Python path to allow local imports
            current_dir = os.getcwd()
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)

            importlib.import_module(import_workflows)

            # Show registered workflows
            from tinyworkflow.client import TinyWorkflowClient

            temp_client = TinyWorkflowClient(db_url)
            workflows = temp_client.list_registered_workflows()
            click.echo(f"Registered {len(workflows)} workflows:")
            for wf in workflows:
                click.echo(f"  - {wf}")
        except Exception as e:
            click.echo(f"Warning: Failed to import workflows: {e}", err=True)
            click.echo("Hint: Make sure you're running from the project root directory.")
            click.echo("Server will start but no workflows will be available.")

    # Import here to avoid circular dependency
    from tinyworkflow.server import create_app
    import uvicorn

    app = create_app(db_url)
    uvicorn.run(app, host=host, port=port)


@cli.command()
@click.option(
    "--import-workflows",
    "-w",
    help="Python module path to import (e.g., 'examples.app' or 'myproject.workflows')",
)
@click.pass_context
@async_command
async def worker(ctx, import_workflows):
    """Start the background worker."""
    db_url = ctx.obj["db_url"]

    click.echo("Starting TinyWorkflow worker...")
    click.echo(f"Database: {db_url}")

    # Import workflows module if specified
    if import_workflows:
        click.echo(f"Importing workflows from: {import_workflows}")
        try:
            import importlib
            import sys

            # Add current directory to Python path to allow local imports
            current_dir = os.getcwd()
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)

            importlib.import_module(import_workflows)

            # Show registered workflows
            from tinyworkflow.client import TinyWorkflowClient

            temp_client = TinyWorkflowClient(db_url)
            workflows = temp_client.list_registered_workflows()
            click.echo(f"Registered {len(workflows)} workflows:")
            for wf in workflows:
                click.echo(f"  - {wf}")
        except Exception as e:
            click.echo(f"Warning: Failed to import workflows: {e}", err=True)
            click.echo("Hint: Make sure you're running from the project root directory.")

    async with TinyWorkflowClient(db_url, auto_start_worker=True) as client:
        click.echo("Worker running. Press Ctrl+C to stop.")
        try:
            # Keep running
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            click.echo("\nStopping worker...")


if __name__ == "__main__":
    cli()
