"""
Example: Database Configuration
Shows how to use TinyWorkflow with different database backends: SQLite, PostgreSQL, and MySQL.
"""

import asyncio
from tinyworkflow import workflow, activity, WorkflowContext, TinyWorkflowClient


# Define a simple activity
@activity(name="process_data")
async def process_data(value: int):
    """Simple data processing activity."""
    return {"result": value * 2}


# Define a simple workflow
@workflow(name="simple_workflow")
async def simple_workflow(ctx: WorkflowContext):
    """Simple workflow to test database configuration."""
    value = ctx.get_input("value", 10)
    result = await ctx.execute_activity(process_data, value)
    return result


async def example_sqlite():
    """Example: Using SQLite (default)"""
    print("\n=== SQLite Example ===")

    # Use default SQLite database
    async with TinyWorkflowClient() as client:
        run_id = await client.start_workflow("simple_workflow", input_data={"value": 5})
        print(f"Workflow started with SQLite: {run_id}")

        workflow = await client.get_workflow_status(run_id)
        print(f"Status: {workflow.status}")

    # Use custom SQLite path
    import tempfile
    import os
    custom_db = os.path.join(tempfile.gettempdir(), "custom_tinyworkflow.db")
    async with TinyWorkflowClient(
        database_url=f"sqlite+aiosqlite:///{custom_db}"
    ) as client:
        run_id = await client.start_workflow("simple_workflow", input_data={"value": 7})
        print(f"Workflow started with custom SQLite: {run_id}")


async def example_postgresql():
    """Example: Using PostgreSQL"""
    print("\n=== PostgreSQL Example ===")

    # Make sure you have:
    # 1. Created database: createdb tinyworkflow
    # 2. PostgreSQL server is running

    try:
        async with TinyWorkflowClient(
            database_url="postgresql+asyncpg://user:password@localhost:5432/tinyworkflow"
        ) as client:
            run_id = await client.start_workflow(
                "simple_workflow", input_data={"value": 10}
            )
            print(f"Workflow started with PostgreSQL: {run_id}")

            workflow = await client.get_workflow_status(run_id)
            print(f"Status: {workflow.status}")
    except Exception as e:
        print(f"PostgreSQL connection failed: {e}")
        print("Make sure PostgreSQL is running and database exists:")
        print("  createdb tinyworkflow")


async def example_mysql():
    """Example: Using MySQL"""
    print("\n=== MySQL Example ===")

    # Make sure you have:
    # 1. Created database: CREATE DATABASE tinyworkflow;
    # 2. MySQL server is running

    try:
        async with TinyWorkflowClient(
            database_url="mysql+asyncmy://user:password@localhost:3306/tinyworkflow"
        ) as client:
            run_id = await client.start_workflow(
                "simple_workflow", input_data={"value": 15}
            )
            print(f"Workflow started with MySQL: {run_id}")

            workflow = await client.get_workflow_status(run_id)
            print(f"Status: {workflow.status}")
    except Exception as e:
        print(f"MySQL connection failed: {e}")
        print("Make sure MySQL is running and database exists:")
        print("  mysql -u root -p -e 'CREATE DATABASE tinyworkflow;'")


async def example_from_environment():
    """Example: Using database URL from environment variable"""
    print("\n=== Environment Variable Example ===")

    import os

    # Set environment variable (in real usage, set this in your shell)
    # os.environ["TINYWORKFLOW_DATABASE_URL"] = "postgresql+asyncpg://user:pass@localhost/tinyworkflow"

    db_url = os.getenv(
        "TINYWORKFLOW_DATABASE_URL", "sqlite+aiosqlite:///tinyworkflow.db"
    )

    async with TinyWorkflowClient(database_url=db_url) as client:
        run_id = await client.start_workflow("simple_workflow", input_data={"value": 20})
        print(f"Workflow started with env var database: {run_id}")
        print(f"Using database: {db_url}")


async def main():
    """Run all database examples."""
    print("TinyWorkflow Database Configuration Examples")
    print("=" * 50)

    # SQLite always works
    await example_sqlite()

    # PostgreSQL and MySQL examples (will show error messages if not configured)
    await example_postgresql()
    await example_mysql()

    # Environment variable example
    await example_from_environment()

    print("\n" + "=" * 50)
    print("Examples completed!")
    print("\nTo use PostgreSQL:")
    print("  createdb tinyworkflow")
    print("  tinyworkflow --db 'postgresql+asyncpg://user:pass@localhost/tinyworkflow' server")
    print("\nTo use MySQL:")
    print("  mysql -e 'CREATE DATABASE tinyworkflow;'")
    print("  tinyworkflow --db 'mysql+asyncmy://user:pass@localhost/tinyworkflow' server")


if __name__ == "__main__":
    asyncio.run(main())
