"""
Client API for workflow management and execution.
"""

import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime

from tinyworkflow.state import StateManager
from tinyworkflow.workflow import WorkflowEngine, list_workflows
from tinyworkflow.activity import list_activities
from tinyworkflow.scheduler import WorkflowScheduler
from tinyworkflow.worker import WorkflowWorker
from tinyworkflow.models import WorkflowExecution, WorkflowStatus, WorkflowEvent


class TinyWorkflowClient:
    """
    Main client for interacting with TinyWorkflow.

    This is the primary interface for submitting workflows, managing execution,
    and querying workflow state.
    """

    def __init__(
        self,
        database_url: str = "sqlite+aiosqlite:///tinyworkflow.db",
        auto_start_worker: bool = False,
    ):
        """
        Initialize the TinyWorkflow client.

        Args:
            database_url: Database URL for state persistence
            auto_start_worker: Whether to automatically start the background worker
        """
        self.state_manager = StateManager(database_url)
        self.workflow_engine = WorkflowEngine(self.state_manager)
        self.scheduler = WorkflowScheduler(self.state_manager, self.workflow_engine)
        self.worker = WorkflowWorker(
            self.state_manager, self.workflow_engine, self.scheduler
        )
        self._initialized = False
        self._auto_start_worker = auto_start_worker
        self._worker_task = None

    async def initialize(self):
        """Initialize the client and database."""
        if not self._initialized:
            await self.state_manager.initialize()
            self._initialized = True

            if self._auto_start_worker:
                await self.start_worker()

    async def close(self):
        """Close the client and clean up resources."""
        if self._worker_task:
            await self.stop_worker()
        await self.state_manager.close()

    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    # Workflow execution
    async def start_workflow(
        self,
        workflow_name: str,
        input_data: Optional[Dict[str, Any]] = None,
        workflow_id: Optional[str] = None,
        wait: bool = False,
    ) -> str:
        """
        Start a workflow execution.

        Args:
            workflow_name: Name of the workflow to execute
            input_data: Input data for the workflow
            workflow_id: Optional workflow ID (for grouping related runs)
            wait: Whether to wait for workflow completion

        Returns:
            Run ID of the workflow execution

        Example:
            client = TinyWorkflowClient()
            await client.initialize()

            run_id = await client.start_workflow(
                "process_order",
                input_data={"order_id": "12345"}
            )
        """
        if not self._initialized:
            await self.initialize()

        if wait:
            # Execute immediately and wait
            result = await self.workflow_engine.execute_workflow(
                workflow_name=workflow_name,
                workflow_id=workflow_id,
                input_data=input_data,
            )
            # Get the run_id from the last created workflow
            workflows = await self.state_manager.list_workflows(
                workflow_name=workflow_name, limit=1
            )
            return workflows[0].run_id if workflows else None
        else:
            # Create workflow record and let worker process it
            import uuid

            run_id = f"{workflow_name}_{uuid.uuid4().hex[:12]}"
            await self.state_manager.create_workflow(
                workflow_id=workflow_id or workflow_name,
                run_id=run_id,
                workflow_name=workflow_name,
                input_data=input_data,
            )
            return run_id

    async def get_workflow_status(self, run_id: str) -> Optional[WorkflowExecution]:
        """
        Get workflow execution status.

        Args:
            run_id: Workflow run ID

        Returns:
            Workflow execution object or None if not found
        """
        if not self._initialized:
            await self.initialize()

        return await self.state_manager.get_workflow(run_id)

    async def cancel_workflow(self, run_id: str):
        """
        Cancel a workflow execution.

        Args:
            run_id: Workflow run ID
        """
        if not self._initialized:
            await self.initialize()

        await self.state_manager.update_workflow_status(run_id, WorkflowStatus.CANCELLED)

    async def list_workflow_executions(
        self,
        status: Optional[WorkflowStatus] = None,
        workflow_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[WorkflowExecution]:
        """
        List workflow executions.

        Args:
            status: Filter by status
            workflow_name: Filter by workflow name
            limit: Maximum number of results
            offset: Offset for pagination

        Returns:
            List of workflow executions
        """
        if not self._initialized:
            await self.initialize()

        return await self.state_manager.list_workflows(status, workflow_name, limit, offset)

    async def get_workflow_events(self, run_id: str, limit: int = 100) -> List[WorkflowEvent]:
        """
        Get workflow events for audit trail.

        Args:
            run_id: Workflow run ID
            limit: Maximum number of events

        Returns:
            List of workflow events
        """
        if not self._initialized:
            await self.initialize()

        return await self.state_manager.get_workflow_events(run_id, limit)

    # Scheduling
    async def schedule_workflow(
        self,
        workflow_name: str,
        cron_expression: str,
        input_data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Schedule a workflow to run on a cron schedule.

        Args:
            workflow_name: Name of the workflow to execute
            cron_expression: Cron expression (e.g., "0 9 * * *" for daily at 9am)
            input_data: Input data for the workflow

        Returns:
            Schedule job ID

        Example:
            # Run daily at 9am
            await client.schedule_workflow("daily_report", "0 9 * * *")
        """
        if not self._initialized:
            await self.initialize()

        return await self.scheduler.add_cron_workflow(workflow_name, cron_expression, input_data)

    async def schedule_delayed_workflow(
        self,
        workflow_name: str,
        delay_seconds: float,
        input_data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Schedule a workflow to run after a delay.

        Args:
            workflow_name: Name of the workflow to execute
            delay_seconds: Delay in seconds before execution
            input_data: Input data for the workflow

        Returns:
            Schedule job ID
        """
        if not self._initialized:
            await self.initialize()

        return await self.scheduler.add_delayed_workflow(workflow_name, delay_seconds, input_data)

    # Human-in-the-loop
    async def approve_workflow(self, run_id: str):
        """
        Approve a workflow waiting for human approval.

        Args:
            run_id: Workflow run ID
        """
        if not self._initialized:
            await self.initialize()

        await self.state_manager.grant_approval(run_id, approved=True)

    async def reject_workflow(self, run_id: str):
        """
        Reject a workflow waiting for human approval.

        Args:
            run_id: Workflow run ID
        """
        if not self._initialized:
            await self.initialize()

        await self.state_manager.grant_approval(run_id, approved=False)

    async def get_pending_approvals(self) -> List[WorkflowExecution]:
        """
        Get workflows pending approval.

        Returns:
            List of workflows waiting for approval
        """
        if not self._initialized:
            await self.initialize()

        return await self.state_manager.get_pending_approvals()

    # Worker management
    async def start_worker(self):
        """Start the background worker."""
        if not self._initialized:
            await self.initialize()

        if not self._worker_task:
            self._worker_task = asyncio.create_task(self.worker.start())

    async def stop_worker(self):
        """Stop the background worker."""
        if self._worker_task:
            await self.worker.stop()
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None

    # Discovery
    def list_registered_workflows(self) -> List[str]:
        """
        List all registered workflow names.

        Returns:
            List of workflow names
        """
        return list_workflows()

    def list_registered_activities(self) -> List[str]:
        """
        List all registered activity names.

        Returns:
            List of activity names
        """
        return list_activities()

    # Utility
    async def cleanup_old_workflows(self, days: int = 30):
        """
        Clean up old completed/failed workflows.

        Args:
            days: Delete workflows older than this many days
        """
        # TODO: Implement cleanup logic
        pass
