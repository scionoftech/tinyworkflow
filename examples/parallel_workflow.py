"""
Parallel workflow example demonstrating concurrent activity execution.
"""

import asyncio
from tinyworkflow import workflow, activity, WorkflowContext, TinyWorkflowClient, RetryPolicy


# Define activities
@activity(name="fetch_user")
async def fetch_user(user_id: str):
    """Fetch user data."""
    print(f"Fetching user {user_id}...")
    await asyncio.sleep(2)
    return {"user_id": user_id, "name": "John Doe", "email": "john@example.com"}


@activity(name="fetch_orders")
async def fetch_orders(user_id: str):
    """Fetch user orders."""
    print(f"Fetching orders for user {user_id}...")
    await asyncio.sleep(2)
    return {"user_id": user_id, "orders": [{"id": "order1"}, {"id": "order2"}]}


@activity(name="fetch_preferences")
async def fetch_preferences(user_id: str):
    """Fetch user preferences."""
    print(f"Fetching preferences for user {user_id}...")
    await asyncio.sleep(2)
    return {"user_id": user_id, "theme": "dark", "notifications": True}


@activity(name="generate_report")
async def generate_report(user: dict, orders: dict, preferences: dict):
    """Generate a comprehensive user report."""
    print("Generating report...")
    await asyncio.sleep(1)
    return {
        "report_id": "report_123",
        "user": user,
        "order_count": len(orders["orders"]),
        "preferences": preferences,
    }


# Define workflow with parallel execution
@workflow(name="user_report", retry_policy=RetryPolicy(max_retries=2))
async def user_report_workflow(ctx: WorkflowContext):
    """
    Generate a user report by fetching data in parallel.
    """
    user_id = ctx.get_input("user_id", "user_123")

    # Execute activities in parallel
    print("Fetching data in parallel...")
    user, orders, preferences = await ctx.execute_parallel(
        (fetch_user, (user_id,), {}),
        (fetch_orders, (user_id,), {}),
        (fetch_preferences, (user_id,), {}),
    )

    # Generate report with all data
    report = await ctx.execute_activity(generate_report, user, orders, preferences)

    return {"status": "completed", "report_id": report["report_id"]}


async def main():
    """Run the parallel workflow example."""
    async with TinyWorkflowClient() as client:
        print("Starting user report workflow with parallel execution...")

        import time

        start_time = time.time()

        # Start workflow
        run_id = await client.start_workflow(
            "user_report", input_data={"user_id": "user_456"}, wait=True
        )

        elapsed = time.time() - start_time

        print(f"\nWorkflow completed in {elapsed:.2f} seconds")
        print(f"Run ID: {run_id}")

        # Get status
        workflow = await client.get_workflow_status(run_id)
        print(f"Output: {workflow.output_data}")


if __name__ == "__main__":
    asyncio.run(main())
