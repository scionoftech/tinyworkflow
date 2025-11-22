"""
Retry workflow example demonstrating:
- Activity retry policies
- Workflow retry policies
- Exponential backoff
- Custom retry configuration
- Simulated failures and recovery
"""

import asyncio
import random
from datetime import datetime
from tinyworkflow import workflow, activity, WorkflowContext, TinyWorkflowClient, RetryPolicy


# Global counters to track retry behavior
attempt_counters = {}


def reset_counter(name: str):
    """Reset attempt counter for an activity."""
    attempt_counters[name] = 0


def get_attempt(name: str) -> int:
    """Get current attempt number."""
    if name not in attempt_counters:
        attempt_counters[name] = 0
    attempt_counters[name] += 1
    return attempt_counters[name]


# Activities with different retry behaviors
@activity(
    name="flaky_api_call",
    retry_policy=RetryPolicy(
        max_retries=5,
        initial_delay=1.0,
        max_delay=10.0,
        backoff_multiplier=2.0,
        jitter=True
    )
)
async def flaky_api_call(endpoint: str):
    """
    Simulates a flaky API that fails the first 2 times.
    Demonstrates retry with exponential backoff.
    """
    attempt = get_attempt("flaky_api")
    print(f"[Attempt {attempt}] Calling API endpoint: {endpoint}")

    # Fail first 2 attempts, succeed on 3rd
    if attempt <= 2:
        print(f"  API call failed (simulated error)")
        raise Exception(f"API Error: Connection timeout (attempt {attempt})")

    print(f"  API call succeeded!")
    return {
        "status": "success",
        "data": {"id": 123, "value": "API response data"},
        "attempts": attempt
    }


@activity(
    name="network_request",
    retry_policy=RetryPolicy(max_retries=3, initial_delay=0.5)
)
async def network_request(url: str):
    """
    Simulates a network request that succeeds after 2 retries.
    """
    attempt = get_attempt("network")
    print(f"[Attempt {attempt}] Fetching: {url}")

    if attempt < 2:
        print(f"  Network error (simulated)")
        raise Exception(f"NetworkError: Request failed")

    print(f"  Request succeeded!")
    return {"url": url, "status_code": 200, "attempts": attempt}


@activity(
    name="database_operation",
    retry_policy=RetryPolicy(
        max_retries=4,
        initial_delay=2.0,
        backoff_multiplier=1.5
    )
)
async def database_operation(query: str):
    """
    Simulates a database operation with connection issues.
    """
    attempt = get_attempt("database")
    print(f"[Attempt {attempt}] Executing query: {query[:50]}...")

    # Random failure for first few attempts
    if attempt < 3 and random.random() < 0.7:
        print(f"  Database connection failed")
        raise Exception("DBError: Connection pool exhausted")

    print(f"  Query executed successfully!")
    return {"rows_affected": 42, "query_time_ms": 156, "attempts": attempt}


@activity(
    name="external_service_call",
    retry_policy=RetryPolicy(
        max_retries=6,
        initial_delay=1.0,
        max_delay=30.0,
        backoff_multiplier=3.0
    )
)
async def external_service_call(service: str):
    """
    Simulates calling an external service with aggressive backoff.
    """
    attempt = get_attempt("external_service")
    print(f"[Attempt {attempt}] Calling service: {service}")

    if attempt < 4:
        print(f"  Service unavailable (503)")
        raise Exception(f"ServiceError: Service temporarily unavailable")

    print(f"  Service call succeeded!")
    return {"service": service, "response": "OK", "attempts": attempt}


@activity(name="always_succeeds")
async def always_succeeds(data: str):
    """Activity that always succeeds - no retries needed."""
    print(f"Processing: {data}")
    await asyncio.sleep(0.2)
    return {"processed": True, "data": data}


@activity(
    name="critical_operation",
    retry_policy=RetryPolicy(max_retries=3, initial_delay=1.0)
)
async def critical_operation(operation_id: str):
    """
    Simulates a critical operation that must eventually succeed.
    """
    attempt = get_attempt("critical")
    print(f"[Attempt {attempt}] Critical operation: {operation_id}")

    if attempt == 1:
        print(f"  Transient error occurred")
        raise Exception("TransientError: Resource temporarily locked")

    print(f"  Critical operation completed!")
    return {"operation_id": operation_id, "status": "completed", "attempts": attempt}


# Workflows demonstrating retry behavior
@workflow(name="api_workflow_with_retry")
async def api_workflow_with_retry(ctx: WorkflowContext):
    """
    Workflow demonstrating activity-level retries.
    """
    endpoint = ctx.get_input("endpoint", "/api/data")

    print(f"\n=== API Workflow with Retry ===")
    print(f"Target endpoint: {endpoint}")
    print(f"Retry policy: max_retries=5, exponential backoff\n")

    # Reset counter for this run
    reset_counter("flaky_api")

    # This will retry automatically on failure
    result = await ctx.execute_activity(flaky_api_call, endpoint)

    print(f"\nWorkflow completed after {result['attempts']} attempts")

    return {
        "status": "completed",
        "result": result
    }


@workflow(name="multi_step_retry_workflow")
async def multi_step_retry_workflow(ctx: WorkflowContext):
    """
    Workflow with multiple activities, each with its own retry policy.
    """
    print(f"\n=== Multi-Step Retry Workflow ===\n")

    # Reset counters
    reset_counter("network")
    reset_counter("database")
    reset_counter("critical")

    # Step 1: Network request (will retry)
    print("Step 1: Network Request")
    network_result = await ctx.execute_activity(
        network_request,
        "https://api.example.com/users"
    )
    print(f"Step 1 completed after {network_result['attempts']} attempts\n")

    # Step 2: Database operation (will retry)
    print("Step 2: Database Operation")
    db_result = await ctx.execute_activity(
        database_operation,
        "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
    )
    print(f"Step 2 completed after {db_result['attempts']} attempts\n")

    # Step 3: Critical operation (will retry once)
    print("Step 3: Critical Operation")
    critical_result = await ctx.execute_activity(
        critical_operation,
        "finalize_transaction_123"
    )
    print(f"Step 3 completed after {critical_result['attempts']} attempts\n")

    # Step 4: Always succeeds (no retry needed)
    print("Step 4: Final Processing")
    final_result = await ctx.execute_activity(
        always_succeeds,
        "final_data"
    )
    print("Step 4 completed\n")

    return {
        "status": "completed",
        "steps": {
            "network": network_result,
            "database": db_result,
            "critical": critical_result,
            "final": final_result
        }
    }


@workflow(
    name="workflow_level_retry",
    retry_policy=RetryPolicy(max_retries=2, initial_delay=3.0)
)
async def workflow_level_retry(ctx: WorkflowContext):
    """
    Workflow demonstrating workflow-level retry.
    If the entire workflow fails, it will be retried.
    """
    print(f"\n=== Workflow-Level Retry ===")
    print(f"Workflow retry attempt: {ctx.retry_count + 1}")

    # Reset counter
    reset_counter("external_service")

    # Call external service (will fail a few times)
    result = await ctx.execute_activity(
        external_service_call,
        "payment-gateway"
    )

    return {
        "status": "completed",
        "workflow_attempt": ctx.retry_count + 1,
        "service_result": result
    }


@workflow(name="parallel_with_retries")
async def parallel_with_retries(ctx: WorkflowContext):
    """
    Workflow with parallel activities, each having independent retry logic.
    """
    print(f"\n=== Parallel Execution with Retries ===\n")

    # Reset all counters
    reset_counter("flaky_api")
    reset_counter("network")
    reset_counter("database")

    print("Executing 3 activities in parallel (each will retry independently)...\n")

    # Execute all in parallel - each will retry independently
    api_result, network_result, db_result = await ctx.execute_parallel(
        (flaky_api_call, ("/api/users",), {}),
        (network_request, ("https://api.example.com/products",), {}),
        (database_operation, ("SELECT * FROM orders WHERE status='pending'",), {})
    )

    print("\nAll parallel activities completed!")
    print(f"  API attempts: {api_result['attempts']}")
    print(f"  Network attempts: {network_result['attempts']}")
    print(f"  Database attempts: {db_result['attempts']}")

    return {
        "status": "completed",
        "results": {
            "api": api_result,
            "network": network_result,
            "database": db_result
        }
    }


async def example_activity_retry():
    """Example: Activity-level retry with exponential backoff."""
    print("\n" + "=" * 70)
    print(" Example 1: Activity-Level Retry with Exponential Backoff")
    print("=" * 70)

    async with TinyWorkflowClient() as client:
        run_id = await client.start_workflow(
            "api_workflow_with_retry",
            input_data={"endpoint": "/api/v2/data"},
            wait=True
        )

        result = await client.get_workflow_status(run_id)
        print(f"\nFinal Result: {result.output_data}")


async def example_multi_step_retry():
    """Example: Multi-step workflow with different retry policies."""
    print("\n\n" + "=" * 70)
    print(" Example 2: Multi-Step Workflow with Independent Retry Policies")
    print("=" * 70)

    async with TinyWorkflowClient() as client:
        run_id = await client.start_workflow(
            "multi_step_retry_workflow",
            wait=True
        )

        result = await client.get_workflow_status(run_id)
        print(f"Workflow Status: {result.status}")


async def example_workflow_retry():
    """Example: Workflow-level retry."""
    print("\n\n" + "=" * 70)
    print(" Example 3: Workflow-Level Retry")
    print("=" * 70)

    async with TinyWorkflowClient() as client:
        run_id = await client.start_workflow(
            "workflow_level_retry",
            wait=True
        )

        result = await client.get_workflow_status(run_id)
        print(f"\nWorkflow Result: {result.output_data}")


async def example_parallel_retry():
    """Example: Parallel activities with independent retries."""
    print("\n\n" + "=" * 70)
    print(" Example 4: Parallel Execution with Independent Retries")
    print("=" * 70)

    async with TinyWorkflowClient() as client:
        run_id = await client.start_workflow(
            "parallel_with_retries",
            wait=True
        )

        result = await client.get_workflow_status(run_id)
        print(f"\nFinal Status: {result.status}")


async def main():
    """Run all retry examples."""
    print("\n" + "=" * 70)
    print(" TinyWorkflow Retry Policy Examples")
    print("=" * 70)

    # Run all examples
    await example_activity_retry()
    await example_multi_step_retry()
    await example_workflow_retry()
    await example_parallel_retry()

    # Summary
    print("\n\n" + "=" * 70)
    print(" Retry Policy Configuration Options")
    print("=" * 70)
    print("\n RetryPolicy Parameters:")
    print("   max_retries       - Maximum number of retry attempts (default: 3)")
    print("   initial_delay     - Initial delay in seconds (default: 1.0)")
    print("   max_delay         - Maximum delay between retries (default: 60.0)")
    print("   backoff_multiplier- Exponential backoff multiplier (default: 2.0)")
    print("   jitter            - Add randomness to delay (default: True)")
    print("   jitter_factor     - Jitter randomness factor (default: 0.1)")
    print("\n Retry Strategies:")
    print("   Activity-level - Each activity can have its own retry policy")
    print("   Workflow-level - Entire workflow retries on failure")
    print("   Exponential backoff - Delay increases: 1s, 2s, 4s, 8s...")
    print("   Jitter - Adds randomness to prevent thundering herd")
    print("\n Best Practices:")
    print("   Use higher max_retries for transient errors (network, API)")
    print("   Use aggressive backoff for rate-limited services")
    print("   Set appropriate timeouts to prevent hanging")
    print("   Consider idempotency for retry safety")
    print("   Log retry attempts for debugging")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
