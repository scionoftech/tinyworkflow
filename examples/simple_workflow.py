"""
Simple workflow example demonstrating basic features.
"""

import asyncio
from tinyworkflow import workflow, activity, WorkflowContext, TinyWorkflowClient, RetryPolicy


# Define activities
@activity(name="fetch_data", retry_policy=RetryPolicy(max_retries=3))
async def fetch_data(url: str):
    """Simulate fetching data from an API."""
    print(f"Fetching data from {url}...")
    await asyncio.sleep(1)
    return {"data": f"Data from {url}", "count": 42}


@activity(name="process_data")
async def process_data(data: dict):
    """Process the fetched data."""
    print(f"Processing data: {data}")
    await asyncio.sleep(0.5)
    return {"processed": True, "result": data["count"] * 2}


@activity(name="save_result")
async def save_result(result: dict):
    """Save the processed result."""
    print(f"Saving result: {result}")
    await asyncio.sleep(0.3)
    return {"saved": True, "id": "result_123"}


# Define workflow
@workflow(name="simple_etl", retry_policy=RetryPolicy(max_retries=2))
async def simple_etl_workflow(ctx: WorkflowContext):
    """
    Simple ETL workflow: Extract, Transform, Load.
    """
    url = ctx.get_input("url", "https://api.example.com/data")

    # Extract: Fetch data
    data = await ctx.execute_activity(fetch_data, url)

    # Transform: Process data
    processed = await ctx.execute_activity(process_data, data)

    # Load: Save result
    saved = await ctx.execute_activity(save_result, processed)

    return {"status": "completed", "result_id": saved["id"]}


async def main():
    """Run the simple workflow example."""
    # Initialize client
    async with TinyWorkflowClient() as client:
        print("Starting simple ETL workflow...")

        # Start workflow
        run_id = await client.start_workflow(
            "simple_etl", input_data={"url": "https://api.example.com/users"}, wait=True
        )

        print(f"\nWorkflow completed: {run_id}")

        # Get status
        workflow = await client.get_workflow_status(run_id)
        print(f"Final status: {workflow.status}")
        print(f"Output: {workflow.output_data}")


if __name__ == "__main__":
    asyncio.run(main())
