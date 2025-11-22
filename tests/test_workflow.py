"""
Basic tests for workflow functionality.
"""

import pytest
import asyncio
from tinyworkflow import workflow, activity, WorkflowContext, TinyWorkflowClient, RetryPolicy


@activity(name="add_numbers")
async def add_numbers(a: int, b: int):
    """Simple addition activity."""
    return a + b


@activity(name="multiply_numbers")
async def multiply_numbers(a: int, b: int):
    """Simple multiplication activity."""
    return a * b


@workflow(name="simple_workflow")
async def simple_workflow(ctx: WorkflowContext):
    """Simple test workflow."""
    a = ctx.get_input("a", 0)
    b = ctx.get_input("b", 0)

    result = await ctx.execute_activity(add_numbers, a, b)
    return {"result": result}


@workflow(name="parallel_workflow")
async def parallel_workflow(ctx: WorkflowContext):
    """Test parallel execution."""
    a = ctx.get_input("a", 2)
    b = ctx.get_input("b", 3)
    c = ctx.get_input("c", 4)

    # Execute in parallel
    r1, r2 = await ctx.execute_parallel(
        (add_numbers, (a, b), {}),
        (multiply_numbers, (b, c), {}),
    )

    return {"sum": r1, "product": r2}


@pytest.mark.asyncio
async def test_workflow_execution():
    """Test basic workflow execution."""
    async with TinyWorkflowClient(database_url="sqlite+aiosqlite:///:memory:") as client:
        run_id = await client.start_workflow(
            "simple_workflow",
            input_data={"a": 5, "b": 3},
            wait=True
        )

        wf = await client.get_workflow_status(run_id)
        assert wf is not None
        assert wf.status == "completed"
        assert wf.output_data["result"]["result"] == 8


@pytest.mark.asyncio
async def test_parallel_execution():
    """Test parallel activity execution."""
    async with TinyWorkflowClient(database_url="sqlite+aiosqlite:///:memory:") as client:
        run_id = await client.start_workflow(
            "parallel_workflow",
            input_data={"a": 2, "b": 3, "c": 4},
            wait=True
        )

        wf = await client.get_workflow_status(run_id)
        assert wf is not None
        assert wf.status == "completed"
        assert wf.output_data["result"]["sum"] == 5
        assert wf.output_data["result"]["product"] == 12


@pytest.mark.asyncio
async def test_workflow_state_persistence():
    """Test workflow state is persisted."""
    db_url = "sqlite+aiosqlite:///test_tinyworkflow.db"

    # Create and run workflow
    async with TinyWorkflowClient(database_url=db_url) as client:
        run_id = await client.start_workflow(
            "simple_workflow",
            input_data={"a": 10, "b": 20},
            wait=True
        )

    # Verify state persists in new client
    async with TinyWorkflowClient(database_url=db_url) as client:
        wf = await client.get_workflow_status(run_id)
        assert wf is not None
        assert wf.output_data["result"]["result"] == 30

    # Cleanup
    import os
    if os.path.exists("test_tinyworkflow.db"):
        os.remove("test_tinyworkflow.db")


@pytest.mark.asyncio
async def test_retry_policy():
    """Test retry policy."""
    call_count = 0

    @activity(name="flaky_activity", retry_policy=RetryPolicy(max_retries=2))
    async def flaky_activity():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ValueError("Simulated failure")
        return {"success": True}

    @workflow(name="retry_test_workflow")
    async def retry_test_wf(ctx: WorkflowContext):
        result = await ctx.execute_activity(flaky_activity)
        return result

    async with TinyWorkflowClient(database_url="sqlite+aiosqlite:///:memory:") as client:
        run_id = await client.start_workflow("retry_test_workflow", wait=True)

        wf = await client.get_workflow_status(run_id)
        assert wf is not None
        assert wf.status == "completed"
        assert call_count == 2  # Failed once, succeeded on retry


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
