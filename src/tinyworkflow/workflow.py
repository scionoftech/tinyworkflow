"""
Workflow decorator and execution engine with state persistence and parallel execution support.
"""

import asyncio
import inspect
import uuid
from functools import wraps
from typing import Callable, Optional, Any, Dict, List
from dataclasses import dataclass, field
from datetime import datetime

from tinyworkflow.retry import RetryPolicy, RetryExecutor
from tinyworkflow.state import StateManager
from tinyworkflow.models import WorkflowStatus, ActivityStatus
from tinyworkflow.activity import get_activity_metadata


@dataclass
class WorkflowMetadata:
    """Metadata for a workflow."""

    name: str
    retry_policy: Optional[RetryPolicy]
    timeout: Optional[float]
    is_async: bool


@dataclass
class WorkflowContext:
    """
    Context passed to workflow functions.

    Provides access to workflow state, parallel execution, and human-in-the-loop features.
    """

    workflow_id: str
    run_id: str
    workflow_name: str
    input_data: Optional[Dict[str, Any]]
    retry_count: int = 0
    _state_manager: Optional[StateManager] = field(default=None, repr=False)
    _activity_sequence: int = field(default=0, init=False)

    async def execute_activity(
        self, activity_func: Callable, *args, **kwargs
    ) -> Any:
        """
        Execute an activity within the workflow.

        Args:
            activity_func: Activity function to execute
            *args: Positional arguments for the activity
            **kwargs: Keyword arguments for the activity

        Returns:
            Activity result
        """
        # Get activity metadata
        activity_name = getattr(activity_func, "__activity_name__", activity_func.__name__)
        activity_metadata = get_activity_metadata(activity_name)

        self._activity_sequence += 1
        activity_id = f"{self.run_id}_{activity_name}_{self._activity_sequence}"

        # Create activity record
        if self._state_manager:
            await self._state_manager.create_activity(
                workflow_run_id=self.run_id,
                activity_id=activity_id,
                activity_name=activity_name,
                sequence_number=self._activity_sequence,
                input_data={"args": args, "kwargs": kwargs},
                max_retries=activity_metadata.retry_policy.max_retries
                if activity_metadata
                else 3,
            )

            await self._state_manager.update_activity_status(
                activity_id, self.run_id, ActivityStatus.RUNNING
            )

        try:
            # Execute with retry policy
            retry_policy = (
                activity_metadata.retry_policy if activity_metadata else RetryPolicy()
            )

            result = await RetryExecutor.execute_with_retry(
                activity_func,
                *args,
                retry_policy=retry_policy,
                **kwargs,
            )

            # Update activity as completed
            if self._state_manager:
                await self._state_manager.update_activity_status(
                    activity_id, self.run_id, ActivityStatus.COMPLETED, output_data={"result": result}
                )

            return result

        except Exception as e:
            # Update activity as failed
            if self._state_manager:
                await self._state_manager.update_activity_status(
                    activity_id, self.run_id, ActivityStatus.FAILED, error=str(e)
                )
            raise

    async def execute_parallel(
        self, *activities: tuple[Callable, tuple, dict]
    ) -> List[Any]:
        """
        Execute multiple activities in parallel.

        Args:
            *activities: Tuples of (activity_func, args, kwargs)

        Returns:
            List of results in the same order as input activities

        Example:
            results = await ctx.execute_parallel(
                (fetch_user, (user_id,), {}),
                (fetch_orders, (user_id,), {}),
                (fetch_profile, (user_id,), {})
            )
        """
        tasks = []
        for activity_func, args, kwargs in activities:
            task = self.execute_activity(activity_func, *args, **kwargs)
            tasks.append(task)

        return await asyncio.gather(*tasks)

    async def wait_for_approval(self, approval_key: str, timeout: Optional[float] = None) -> bool:
        """
        Pause workflow for human approval.

        Args:
            approval_key: Unique key for this approval checkpoint
            timeout: Optional timeout in seconds

        Returns:
            True if approved, False if rejected

        Raises:
            TimeoutError: If timeout is reached
        """
        if not self._state_manager:
            raise RuntimeError("State manager not available for approval workflow")

        # Pause workflow
        await self._state_manager.pause_for_approval(self.run_id, approval_key)

        # Poll for approval (in a real system, this would be event-driven)
        start_time = datetime.utcnow()
        while True:
            workflow = await self._state_manager.get_workflow(self.run_id)
            if workflow.approval_granted:
                return workflow.approval_granted == "approved"

            # Check timeout
            if timeout:
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                if elapsed > timeout:
                    raise TimeoutError(f"Approval timeout after {timeout} seconds")

            await asyncio.sleep(1)  # Poll every second

    def get_input(self, key: str, default: Any = None) -> Any:
        """Get input data by key."""
        if self.input_data is None:
            return default
        return self.input_data.get(key, default)


# Registry of all workflows
_workflow_registry: Dict[str, Callable] = {}
_workflow_metadata: Dict[str, WorkflowMetadata] = {}


def workflow(
    name: Optional[str] = None,
    retry_policy: Optional[RetryPolicy] = None,
    timeout: Optional[float] = None,
):
    """
    Decorator to define a durable workflow.

    Workflows are automatically persisted and can recover from failures.
    They support retries, timeouts, and parallel execution.

    Args:
        name: Workflow name (defaults to function name)
        retry_policy: Retry policy for the entire workflow
        timeout: Timeout in seconds for workflow execution

    Example:
        @workflow(name="process_order", retry_policy=RetryPolicy(max_retries=3))
        async def process_order_workflow(ctx: WorkflowContext):
            order_id = ctx.get_input("order_id")

            # Execute activities
            order = await ctx.execute_activity(fetch_order, order_id)
            payment = await ctx.execute_activity(process_payment, order)

            # Parallel execution
            results = await ctx.execute_parallel(
                (send_email, (order,), {}),
                (update_inventory, (order,), {})
            )

            return {"status": "completed", "order_id": order_id}
    """

    def decorator(func: Callable) -> Callable:
        workflow_name = name or func.__name__
        is_async = asyncio.iscoroutinefunction(func)

        if not is_async:
            raise ValueError(f"Workflow '{workflow_name}' must be an async function")

        # Store metadata
        _workflow_metadata[workflow_name] = WorkflowMetadata(
            name=workflow_name,
            retry_policy=retry_policy or RetryPolicy(),
            timeout=timeout,
            is_async=is_async,
        )

        # Store in registry
        _workflow_registry[workflow_name] = func

        @wraps(func)
        async def wrapper(*args, **kwargs):
            """Wrapper for direct workflow execution (not recommended, use client instead)."""
            return await func(*args, **kwargs)

        # Mark as workflow
        wrapper.__workflow_name__ = workflow_name
        wrapper.__is_workflow__ = True
        wrapper.__workflow_metadata__ = _workflow_metadata[workflow_name]

        return wrapper

    return decorator


def get_workflow(name: str) -> Optional[Callable]:
    """Get a workflow by name from the registry."""
    return _workflow_registry.get(name)


def get_workflow_metadata(name: str) -> Optional[WorkflowMetadata]:
    """Get workflow metadata by name."""
    return _workflow_metadata.get(name)


def list_workflows() -> list[str]:
    """List all registered workflow names."""
    return list(_workflow_registry.keys())


class WorkflowEngine:
    """Executes workflows with state persistence and recovery."""

    def __init__(self, state_manager: StateManager):
        """
        Initialize the workflow engine.

        Args:
            state_manager: State manager for persistence
        """
        self.state_manager = state_manager

    async def execute_workflow(
        self,
        workflow_name: str,
        workflow_id: Optional[str] = None,
        input_data: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
    ) -> Any:
        """
        Execute a workflow.

        Args:
            workflow_name: Name of the workflow to execute
            workflow_id: Optional workflow ID (for grouping related runs)
            input_data: Input data for the workflow
            run_id: Optional run ID (for resuming workflows)

        Returns:
            Workflow result

        Raises:
            ValueError: If workflow not found
        """
        # Get workflow function
        workflow_func = get_workflow(workflow_name)
        if not workflow_func:
            raise ValueError(f"Workflow '{workflow_name}' not found")

        metadata = get_workflow_metadata(workflow_name)

        # Generate IDs
        if not workflow_id:
            workflow_id = workflow_name
        if not run_id:
            run_id = f"{workflow_name}_{uuid.uuid4().hex[:12]}"

        # Create or get existing workflow record
        workflow_record = await self.state_manager.get_workflow(run_id)
        if not workflow_record:
            # Create new workflow record only if it doesn't exist
            workflow_record = await self.state_manager.create_workflow(
                workflow_id=workflow_id,
                run_id=run_id,
                workflow_name=workflow_name,
                input_data=input_data,
                max_retries=metadata.retry_policy.max_retries,
            )

        # Create context
        ctx = WorkflowContext(
            workflow_id=workflow_id,
            run_id=run_id,
            workflow_name=workflow_name,
            input_data=input_data,
            _state_manager=self.state_manager,
        )

        try:
            # Update status to running
            await self.state_manager.update_workflow_status(run_id, WorkflowStatus.RUNNING)

            # Execute workflow with timeout
            if metadata.timeout:
                result = await asyncio.wait_for(
                    workflow_func(ctx), timeout=metadata.timeout
                )
            else:
                result = await workflow_func(ctx)

            # Update as completed
            await self.state_manager.update_workflow_status(
                run_id, WorkflowStatus.COMPLETED, output_data={"result": result}
            )

            return result

        except Exception as e:
            # Check if we should retry
            retry_count = await self.state_manager.increment_workflow_retry(run_id)

            if retry_count <= metadata.retry_policy.max_retries:
                # Calculate delay and retry
                delay = metadata.retry_policy.calculate_delay(retry_count - 1)
                await asyncio.sleep(delay)

                # Retry execution
                ctx.retry_count = retry_count
                try:
                    await self.state_manager.update_workflow_status(
                        run_id, WorkflowStatus.RUNNING
                    )

                    if metadata.timeout:
                        result = await asyncio.wait_for(
                            workflow_func(ctx), timeout=metadata.timeout
                        )
                    else:
                        result = await workflow_func(ctx)

                    await self.state_manager.update_workflow_status(
                        run_id, WorkflowStatus.COMPLETED, output_data={"result": result}
                    )
                    return result

                except Exception as retry_error:
                    await self.state_manager.update_workflow_status(
                        run_id, WorkflowStatus.FAILED, error=str(retry_error)
                    )
                    raise
            else:
                # All retries exhausted
                await self.state_manager.update_workflow_status(
                    run_id, WorkflowStatus.FAILED, error=str(e)
                )
                raise
