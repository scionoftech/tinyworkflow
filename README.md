![Tinyworkflow](https://github.com/scionoftech/tinyworkflow/blob/main/tinyworkflow.png?width=200&height=150)

[![PyPI version](https://badge.fury.io/py/tinyworkflow.svg)](https://pypi.org/project/tinyworkflow/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Lightweight Workflow Library for Learning and Experimentation**

TinyWorkflow is a simple, Python-first workflow library designed for **learning workflow concepts**, **prototyping**, and **lightweight task orchestration**. Perfect for AI experimentation, small projects, and understanding workflow patterns before moving to production systems.

> **⚠️ Important**: TinyWorkflow is designed for learning and lightweight use cases. For production-grade durable workflows with full fault tolerance, use [Temporal](https://temporal.io/), [Azure Durable Functions](https://learn.microsoft.com/en-us/azure/azure-functions/durable/), or [DBOS](https://www.dbos.dev/).

## 🌟 Key Features

**Perfect for learning and lightweight workflows:**

- **🎯 Pure Python** - Simple decorator-based API, no DSL to learn
- **💾 State Persistence** - SQLite, PostgreSQL, or MySQL (basic state tracking)
- **🔄 Retry Logic** - Exponential backoff with jitter for failed activities
- **⚡ Async/Await** - Modern Python async for high-performance
- **🔀 Parallel Execution** - Run activities concurrently (fan-out/fan-in)
- **👥 Human-in-the-Loop** - Basic approval workflows
- **📅 Scheduling** - Cron expressions and delayed execution
- **📊 Event Sourcing** - Audit trail for observability
- **🖥️ Web UI** - Simple workflow monitoring interface
- **🛠️ CLI Tool** - Command-line interface for operations
- **🚀 Zero Setup** - No external services required (SQLite default)
- **📚 Easy to Learn** - Small codebase (~2000 LOC), great for education

## 🚀 Quick Start

### Installation

```bash
# Install from PyPI (once published)
# Includes support for SQLite, PostgreSQL, and MySQL
pip install tinyworkflow

# Or install from source
git clone https://github.com/scionoftech/tinyworkflow
cd tinyworkflow
pip install -e .
```

### Basic Example

```python
import asyncio
from tinyworkflow import workflow, activity, WorkflowContext, TinyWorkflowClient

# Define activities
@activity(name="fetch_data")
async def fetch_data(url: str):
    # Your code here
    return {"data": "..."}

@activity(name="process_data")
async def process_data(data: dict):
    # Your code here
    return {"result": "..."}

# Define workflow
@workflow(name="etl_pipeline")
async def etl_workflow(ctx: WorkflowContext):
    url = ctx.get_input("url")

    # Execute activities
    data = await ctx.execute_activity(fetch_data, url)
    result = await ctx.execute_activity(process_data, data)

    return result

# Run workflow
async def main():
    async with TinyWorkflowClient() as client:
        run_id = await client.start_workflow(
            "etl_pipeline",
            input_data={"url": "https://api.example.com"}
        )
        print(f"Workflow started: {run_id}")

asyncio.run(main())
```

### Try the Web UI

```bash
# IMPORTANT: Run from project root directory
cd /path/to/tinyworkflow

# Start server with example workflows
tinyworkflow server --import-workflows examples.workflows

# Open browser to http://localhost:8080
# You'll see all 20 example workflows ready to run!
```

**Common Error:** If you see "No module named 'examples'", make sure you're running from the project root directory (the directory containing the `examples/` folder).

## 📖 Core Concepts

### Activities

Activities are reusable tasks that perform a single unit of work. They support automatic retries and timeouts.

```python
from tinyworkflow import activity, RetryPolicy

@activity(
    name="fetch_user",
    retry_policy=RetryPolicy(max_retries=5, initial_delay=1.0),
    timeout=30.0
)
async def fetch_user(user_id: str):
    # Activity code
    return {"id": user_id, "name": "John"}
```

### Workflows

Workflows orchestrate multiple activities and define the business logic. They are automatically persisted and can recover from failures.

```python
from tinyworkflow import workflow, WorkflowContext, RetryPolicy

@workflow(
    name="user_onboarding",
    retry_policy=RetryPolicy(max_retries=3)
)
async def user_onboarding_workflow(ctx: WorkflowContext):
    user_id = ctx.get_input("user_id")

    # Sequential execution
    user = await ctx.execute_activity(fetch_user, user_id)
    await ctx.execute_activity(send_welcome_email, user)

    return {"status": "completed"}
```

### Parallel Execution

Execute multiple activities concurrently for better performance:

```python
@workflow(name="parallel_example")
async def parallel_workflow(ctx: WorkflowContext):
    user_id = ctx.get_input("user_id")

    # Run activities in parallel
    user, orders, preferences = await ctx.execute_parallel(
        (fetch_user, (user_id,), {}),
        (fetch_orders, (user_id,), {}),
        (fetch_preferences, (user_id,), {})
    )

    return {"user": user, "orders": orders, "preferences": preferences}
```

### Human-in-the-Loop

Pause workflows for manual approval:

```python
@workflow(name="expense_approval")
async def expense_workflow(ctx: WorkflowContext):
    amount = ctx.get_input("amount")

    if amount > 1000:
        # Wait for manager approval
        approved = await ctx.wait_for_approval("manager_approval", timeout=3600)

        if not approved:
            return {"status": "rejected"}

    # Process payment
    result = await ctx.execute_activity(process_payment, amount)
    return result
```

## 🛠️ CLI Usage

TinyWorkflow includes a powerful CLI for workflow management:

```bash
# Start the web UI server (with workflow imports)
tinyworkflow server --import-workflows examples.workflows --port 8080

# Start a background worker (with workflow imports)
tinyworkflow worker --import-workflows examples.workflows

# Start a workflow
tinyworkflow start my_workflow --input '{"key": "value"}'

# Check workflow status
tinyworkflow status <run_id>

# List all workflows
tinyworkflow list --status running

# View workflow events (audit trail)
tinyworkflow events <run_id>

# Schedule a workflow (cron)
tinyworkflow schedule my_workflow "0 9 * * *"

# List pending approvals
tinyworkflow approvals

# Approve a workflow
tinyworkflow approve <run_id> --approve

# List registered workflows
tinyworkflow workflows

# Cancel a workflow
tinyworkflow cancel <run_id>
```

## 🖥️ Web UI

Start the web interface to manage workflows visually:

```bash
# IMPORTANT: Run from project root directory
cd /path/to/tinyworkflow

# Start server with workflow imports
tinyworkflow server --import-workflows examples.workflows --port 8080
```

Then open http://localhost:8080 in your browser. Features include:

- 📊 Dashboard with workflow statistics
- ▶️ Start new workflows with custom input
- 📋 List and filter workflow executions
- 🔍 View detailed workflow status and events
- ⏰ Schedule workflows with cron expressions
- ✅ Approve/reject pending workflows
- 📚 Browse registered workflows and activities

**⚠️ Requirements:**
- Must use `--import-workflows` to make workflows available
- Must run from project root directory
- See [Workflow Registration](#-workflow-registration) for troubleshooting

## 📅 Scheduling

### Cron-based Scheduling

```python
async with TinyWorkflowClient() as client:
    # Run daily at 9am
    await client.schedule_workflow("daily_report", "0 9 * * *")

    # Run every 5 minutes
    await client.schedule_workflow("health_check", "*/5 * * * *")
```

### Delayed Execution

```python
async with TinyWorkflowClient() as client:
    # Run after 5 minutes
    await client.schedule_delayed_workflow(
        "cleanup_job",
        delay_seconds=300,
        input_data={"resource_id": "abc123"}
    )
```

## 🎯 Use Cases

### AI/ML Workflows

Perfect for multi-step AI pipelines with automatic retries and state management:

```python
@workflow(name="ai_content_pipeline")
async def ai_content_pipeline(ctx: WorkflowContext):
    prompt = ctx.get_input("prompt")

    # Generate content with retry logic
    content = await ctx.execute_activity(generate_ai_content, prompt)

    # Parallel analysis: sentiment, moderation, keywords
    sentiment, moderation, keywords = await ctx.execute_parallel(
        (analyze_sentiment, (content,), {}),
        (moderate_content, (content,), {}),
        (extract_keywords, (content,), {})
    )

    # Check moderation
    if moderation["flagged"]:
        return {"status": "rejected", "reason": "content_moderation"}

    # Translate to multiple languages
    translations = await ctx.execute_parallel(
        (translate, (content, "es"), {}),
        (translate, (content, "fr"), {}),
        (translate, (content, "de"), {})
    )

    # Save results with full audit trail
    await ctx.execute_activity(save_results, {
        "content": content,
        "sentiment": sentiment,
        "translations": translations
    })

    return {"status": "completed", "content": content}
```

**Real-world AI use cases:**
- Content generation and moderation pipelines
- Document processing and extraction
- Sentiment analysis workflows
- Multi-language translation pipelines
- Image/video processing workflows
- ML model inference pipelines
- Data labeling and annotation workflows

### Data Processing

ETL and data pipelines:

```python
@workflow(name="etl")
async def etl_workflow(ctx: WorkflowContext):
    # Extract
    data = await ctx.execute_activity(extract_from_source)

    # Transform
    transformed = await ctx.execute_activity(transform_data, data)

    # Load
    await ctx.execute_activity(load_to_destination, transformed)

    return {"status": "success"}
```

### Approval Workflows

Business processes requiring human approval:

```python
@workflow(name="purchase_order")
async def purchase_order_workflow(ctx: WorkflowContext):
    order = await ctx.execute_activity(create_order, ctx.get_input("items"))

    # Require approval for large orders
    if order["total"] > 10000:
        approved = await ctx.wait_for_approval("purchase_approval")
        if not approved:
            return {"status": "rejected"}

    await ctx.execute_activity(process_order, order)
    return {"status": "completed", "order_id": order["id"]}
```

## 🏗️ Architecture

TinyWorkflow is designed as a **simple workflow library** with these components:

- **State Manager** - SQLAlchemy-based persistence (SQLite/PostgreSQL/MySQL)
- **Workflow Engine** - Executes workflows with state tracking
- **Activity Executor** - Runs activities with retry logic
- **Scheduler** - Cron and delayed jobs (APScheduler)
- **Worker** - Background processor for async execution
- **Client API** - Python API for workflow management
- **CLI** - Command-line interface (Click)
- **Web UI** - FastAPI-based web interface

### ⚠️ Current Limitations

**What TinyWorkflow does NOT provide (by design):**

1. **No Workflow Replay** - Failed workflows retry from scratch, not from the failure point
2. **No Deterministic Execution** - Can use `datetime.now()`, `uuid.uuid4()`, `random()` freely
3. **No Durable Timers** - Using `asyncio.sleep()` loses timer state on crash
4. **No Signal System** - Cannot send external events to running workflows
5. **No Saga/Compensation** - No automatic rollback on failures
6. **No Workflow Versioning** - Changing code may break in-flight workflows

**These limitations are intentional** - implementing them would significantly increase complexity. For workflows requiring these features, use production systems like Temporal or DBOS.

**What TinyWorkflow DOES provide:**

✅ State persistence (workflows/activities stored in database)
✅ Retry policies (exponential backoff with jitter)
✅ Parallel execution (fan-out/fan-in patterns)
✅ Event sourcing (audit trail)
✅ Human-in-the-loop (basic approval workflows)
✅ Scheduling (cron expressions)
✅ Web UI (workflow monitoring)
✅ Multi-database support (SQLite/PostgreSQL/MySQL)

## 🔧 Configuration

### Database Configuration

TinyWorkflow supports SQLite (default), PostgreSQL, and MySQL for state persistence.

#### SQLite (Default)

No additional setup required. Perfect for development and small deployments:

```python
from tinyworkflow import TinyWorkflowClient

# Use default SQLite database (tinyworkflow.db in current directory)
async with TinyWorkflowClient() as client:
    pass

# Or specify custom SQLite path
async with TinyWorkflowClient(
    database_url="sqlite+aiosqlite:///path/to/custom.db"
) as client:
    pass
```

#### PostgreSQL

Configure PostgreSQL connection (driver included by default):

```python
from tinyworkflow import TinyWorkflowClient

# Connect to PostgreSQL
async with TinyWorkflowClient(
    database_url="postgresql+asyncpg://user:password@localhost:5432/tinyworkflow"
) as client:
    pass

```

**Setup PostgreSQL database:**

```bash
# Create database
createdb tinyworkflow

# Or using psql
psql -c "CREATE DATABASE tinyworkflow;"
```

#### MySQL

Configure MySQL connection (driver included by default):

```python
from tinyworkflow import TinyWorkflowClient

# Connect to MySQL
async with TinyWorkflowClient(
    database_url="mysql+asyncmy://user:password@localhost:3306/tinyworkflow"
) as client:
    pass

# With charset specification
async with TinyWorkflowClient(
    database_url="mysql+asyncmy://user:password@localhost:3306/tinyworkflow?charset=utf8mb4"
) as client:
    pass
```

**Setup MySQL database:**

```bash
# Create database
mysql -u root -p -e "CREATE DATABASE tinyworkflow CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
```

#### CLI with Custom Database

You can specify the database URL when using the CLI:

```bash
# PostgreSQL
tinyworkflow --db "postgresql+asyncpg://user:pass@localhost/tinyworkflow" server

# MySQL
tinyworkflow --db "mysql+asyncmy://user:pass@localhost/tinyworkflow" worker

# Custom SQLite path
tinyworkflow --db "sqlite+aiosqlite:///custom/path/db.sqlite" server
```

#### Environment Variables

Set database URL via environment variable:

```bash
export TINYWORKFLOW_DATABASE_URL="postgresql+asyncpg://user:pass@localhost/tinyworkflow"
tinyworkflow server
```

#### Connection Pooling

PostgreSQL and MySQL use connection pooling by default:

- **Pool Size**: 10 connections
- **Max Overflow**: 20 additional connections
- **Pool Pre-Ping**: Enabled (verifies connections before use)
- **Pool Recycle**: 3600 seconds (1 hour)

These settings are optimized for most use cases and applied automatically.

### Retry Policies

Customize retry behavior:

```python
from tinyworkflow import RetryPolicy

retry_policy = RetryPolicy(
    max_retries=5,
    initial_delay=1.0,      # seconds
    max_delay=60.0,         # seconds
    backoff_multiplier=2.0,  # exponential backoff
    jitter=True,            # add randomness
    jitter_factor=0.1       # 10% jitter
)

@activity(name="flaky_task", retry_policy=retry_policy)
async def flaky_task():
    # May fail and will be retried
    pass
```

### Worker Configuration

```python
client = TinyWorkflowClient(auto_start_worker=True)

# Or manually configure
worker = WorkflowWorker(
    state_manager=state_manager,
    workflow_engine=engine,
    poll_interval=1.0,
    max_concurrent_workflows=10
)
```

## 📊 Monitoring & Observability

### Event Sourcing

Every state change is recorded:

```python
events = await client.get_workflow_events(run_id)
for event in events:
    print(f"{event.timestamp}: {event.event_type}")
```

### Workflow Status

```python
workflow = await client.get_workflow_status(run_id)
print(f"Status: {workflow.status}")
print(f"Created: {workflow.created_at}")
print(f"Retries: {workflow.retry_count}/{workflow.max_retries}")
```

## 📦 Workflow Registration

**Important:** Workflows must be explicitly imported to be available in the CLI and web UI.

### Quick Start

```bash
# IMPORTANT: Run from project root directory
cd /path/to/tinyworkflow

# Start server with example workflows
tinyworkflow server --import-workflows examples.workflows

# Start worker with your project workflows
tinyworkflow worker --import-workflows myproject.workflows
```

**Troubleshooting:** If you get "No module named 'examples'" error:
1. Verify you're in the project root directory: `pwd` or `cd`
2. Check that `examples/__init__.py` exists
3. Try: `python -c "import examples.workflows"` to test imports

### Create a Workflow Registry

```python
# myproject/workflows.py
"""Workflow registry - imports all workflow modules"""

from myproject.orders import order_workflow
from myproject.payments import payment_workflow
from myproject.notifications import notification_workflow
```

Then start the server:

```bash
tinyworkflow server --import-workflows myproject.workflows
```

### Why This Is Needed

Workflows are registered when their Python modules are imported via the `@workflow` decorator. Without explicit imports:
- ❌ Web UI shows "No workflows registered"
- ❌ Cannot start or schedule workflows
- ❌ Registry appears empty

**📖 See [WORKFLOW_REGISTRATION.md](WORKFLOW_REGISTRATION.md) for detailed guide**

## 🧪 Testing

Run the test suite:

```bash
pytest tests/ -v
```

## 📝 Examples

Check the `examples/` directory for complete examples:

- `simple_workflow.py` - Basic ETL workflow
- `parallel_workflow.py` - Parallel activity execution
- `approval_workflow.py` - Human-in-the-loop approval
- `retry_workflow.py` - **Retry policies and failure handling**
- `scheduling_workflow.py` - **Cron scheduling and delayed execution**
- `ai_content_pipeline.py` - **AI content generation with sentiment analysis and moderation**
- `ai_document_processor.py` - **AI document processing with parallel analysis**
- `database_configuration.py` - Multi-database configuration examples

### Core Workflow Examples

Run the core examples to understand TinyWorkflow's features:

```bash
# Retry Policies - Handle failures with automatic retries
python examples/retry_workflow.py

# Scheduling - Cron expressions and delayed execution
python examples/scheduling_workflow.py

# Approval Workflows - Human-in-the-loop patterns
python examples/approval_workflow.py
```

### AI Workflow Examples

Run the AI examples to see TinyWorkflow in action with AI workloads:

```bash
# AI Content Pipeline - Generate, analyze, and moderate content
python examples/ai_content_pipeline.py

# AI Document Processor - Extract, classify, and analyze documents
python examples/ai_document_processor.py
```

**Core features demonstrated:**
- ✅ Retry policies with exponential backoff
- ✅ Activity-level and workflow-level retries
- ✅ Cron-based scheduling (daily, weekly, monthly jobs)
- ✅ Delayed workflow execution
- ✅ Human-in-the-loop approval workflows
- ✅ Parallel execution patterns

**AI/ML features demonstrated:**
- ✅ AI/ML task orchestration
- ✅ Content generation and moderation pipelines
- ✅ Document processing with parallel analysis
- ✅ Sentiment analysis workflows
- ✅ State persistence and recovery
- ✅ Event sourcing for audit trails
- ✅ Batch processing of multiple documents

## ✅ When to Use TinyWorkflow

**Perfect for:**
- 📚 **Learning** workflow orchestration concepts
- 🧪 **Prototyping** and experimenting with workflow patterns
- 🎓 **Educational** projects and tutorials
- 🚀 **Quick demos** and POCs
- 📊 **Simple data pipelines** (< 1 hour execution)
- 🤖 **AI experimentation** with LLM chains
- 🛠️ **Small internal tools** and automation scripts
- 📅 **Lightweight scheduled jobs**

**Key advantages:**
- Zero infrastructure setup (SQLite by default)
- Simple decorator-based API
- Easy to understand codebase (~2000 LOC)
- Great for learning before Temporal

## ⚠️ When NOT to Use TinyWorkflow

**Use production systems instead for:**
- ❌ **Critical business processes** requiring guaranteed execution
- ❌ **Long-running workflows** (hours/days) with crash recovery
- ❌ **High-scale production** workloads (1000s of workflows/sec)
- ❌ **Distributed transactions** requiring saga patterns
- ❌ **Complex compensations** and rollback logic
- ❌ **Mission-critical** systems where downtime costs money

**For production, use:**
- [**Temporal**](https://temporal.io/) - Full-featured durable execution
- [**Azure Durable Functions**](https://learn.microsoft.com/en-us/azure/azure-functions/durable/) - Serverless workflows
- [**DBOS**](https://www.dbos.dev/) - Database-backed workflows
- [**Prefect**](https://www.prefect.io/) - Data engineering workflows
- [**Airflow**](https://airflow.apache.org/) - Batch data pipelines

## 🆚 Comparison

| Feature | TinyWorkflow | Temporal | Azure Durable | DBOS |
|---------|-------------|----------|---------------|------|
| **Setup Complexity** | ⭐ Very Simple | ⭐⭐⭐ Complex | ⭐⭐ Moderate | ⭐⭐ Moderate |
| **Target Use Case** | Learning/Small | Production | Production | Production |
| **Workflow Replay** | ❌ | ✅ | ✅ | ✅ |
| **Deterministic Execution** | ❌ | ✅ | ✅ | ✅ |
| **Fault Tolerance** | ⚠️ Basic | ✅ Full | ✅ Full | ✅ Full |
| **Durable Timers** | ❌ | ✅ | ✅ | ✅ |
| **Signals/Events** | ❌ | ✅ | ✅ | ✅ |
| **State Persistence** | ✅ SQLite/Postgres/MySQL | ✅ | ✅ | ✅ |
| **Retry Policies** | ✅ | ✅ | ✅ | ✅ |
| **Parallel Execution** | ✅ | ✅ | ✅ | ✅ |
| **Learning Curve** | Low | High | Medium | Medium |
| **Best For** | Learning & Prototypes | Production Scale | Azure Ecosystem | DB-Centric Apps |

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

MIT License - see LICENSE file for details.

## 🙏 Acknowledgments

Inspired by:
- [Temporal](https://temporal.io/) - Durable execution primitives
- [Prefect](https://www.prefect.io/) - Modern workflow orchestration
- [DBOS](https://www.dbos.dev/) - Durable execution with databases

## 📚 Documentation

- [Quick Start Guide](QUICKSTART.md) - Get started in 5 minutes
- [Workflow Registration](WORKFLOW_REGISTRATION.md) - How to register workflows
- [Limitations](LIMITATIONS.md) - What TinyWorkflow does and doesn't provide

## 💬 Support

- GitHub Issues: [Report bugs](https://github.com/scionoftech/tinyworkflow/issues)
- Discussions: [Ask questions](https://github.com/scionoftech/tinyworkflow/discussions)

---
