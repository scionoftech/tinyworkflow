"""
Background worker for async workflow execution.
"""

import asyncio
from typing import Optional
from datetime import datetime

from tinyworkflow.state import StateManager
from tinyworkflow.workflow import WorkflowEngine
from tinyworkflow.scheduler import WorkflowScheduler
from tinyworkflow.models import WorkflowStatus


class WorkflowWorker:
    """Background worker that processes pending workflows."""

    def __init__(
        self,
        state_manager: StateManager,
        workflow_engine: WorkflowEngine,
        scheduler: Optional[WorkflowScheduler] = None,
        poll_interval: float = 1.0,
        max_concurrent_workflows: int = 10,
    ):
        """
        Initialize the worker.

        Args:
            state_manager: State manager for persistence
            workflow_engine: Workflow engine for execution
            scheduler: Optional scheduler for cron jobs
            poll_interval: Interval in seconds to poll for pending workflows
            max_concurrent_workflows: Maximum number of concurrent workflows
        """
        self.state_manager = state_manager
        self.workflow_engine = workflow_engine
        self.scheduler = scheduler
        self.poll_interval = poll_interval
        self.max_concurrent_workflows = max_concurrent_workflows
        self._running = False
        self._tasks = set()

    async def start(self):
        """Start the worker."""
        if self._running:
            return

        self._running = True

        # Start scheduler if provided
        if self.scheduler:
            await self.scheduler.start()

        # Start processing loop
        await self._process_loop()

    async def stop(self):
        """Stop the worker gracefully."""
        self._running = False

        # Stop scheduler
        if self.scheduler:
            await self.scheduler.stop()

        # Wait for all tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _process_loop(self):
        """Main processing loop."""
        while self._running:
            try:
                # Clean up completed tasks
                self._tasks = {task for task in self._tasks if not task.done()}

                # Check if we can process more workflows
                if len(self._tasks) < self.max_concurrent_workflows:
                    # Get pending workflows
                    pending_workflows = await self.state_manager.list_workflows(
                        status=WorkflowStatus.PENDING, limit=self.max_concurrent_workflows
                    )

                    # Also get workflows scheduled to run now
                    all_workflows = await self.state_manager.list_workflows(
                        limit=self.max_concurrent_workflows * 2
                    )

                    scheduled_now = [
                        w
                        for w in all_workflows
                        if w.status == WorkflowStatus.PENDING
                        and w.scheduled_time
                        and w.scheduled_time <= datetime.utcnow()
                    ]

                    # Combine and deduplicate
                    workflows_to_process = {w.run_id: w for w in pending_workflows + scheduled_now}

                    # Process workflows
                    for workflow in list(workflows_to_process.values())[
                        : self.max_concurrent_workflows - len(self._tasks)
                    ]:
                        task = asyncio.create_task(self._execute_workflow(workflow.run_id))
                        self._tasks.add(task)

                # Sleep before next poll
                await asyncio.sleep(self.poll_interval)

            except Exception as e:
                # Log error but keep running
                print(f"Error in worker loop: {e}")
                await asyncio.sleep(self.poll_interval)

    async def _execute_workflow(self, run_id: str):
        """Execute a single workflow."""
        try:
            # Get workflow details
            workflow = await self.state_manager.get_workflow(run_id)
            if not workflow:
                return

            # Execute workflow
            await self.workflow_engine.execute_workflow(
                workflow_name=workflow.workflow_name,
                workflow_id=workflow.workflow_id,
                input_data=workflow.input_data,
                run_id=run_id,
            )

        except Exception as e:
            # Error is already handled in workflow engine
            print(f"Error executing workflow {run_id}: {e}")


class WorkflowQueue:
    """Simple in-memory queue for workflow execution (alternative to database polling)."""

    def __init__(self):
        self._queue = asyncio.Queue()
        self._running = False

    async def enqueue(self, workflow_name: str, input_data: Optional[dict] = None):
        """Add a workflow to the queue."""
        await self._queue.put((workflow_name, input_data))

    async def process(self, workflow_engine: WorkflowEngine, max_workers: int = 10):
        """Process workflows from the queue."""
        self._running = True
        tasks = set()

        while self._running:
            try:
                # Clean up completed tasks
                tasks = {task for task in tasks if not task.done()}

                # Process new workflows if we have capacity
                if len(tasks) < max_workers and not self._queue.empty():
                    workflow_name, input_data = await asyncio.wait_for(
                        self._queue.get(), timeout=1.0
                    )

                    task = asyncio.create_task(
                        workflow_engine.execute_workflow(workflow_name, input_data=input_data)
                    )
                    tasks.add(task)
                else:
                    await asyncio.sleep(0.1)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"Error in queue processor: {e}")

        # Wait for remaining tasks
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def stop(self):
        """Stop processing the queue."""
        self._running = False

    def size(self) -> int:
        """Get queue size."""
        return self._queue.qsize()
