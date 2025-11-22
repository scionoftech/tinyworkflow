# TinyWorkflow Quick Start Guide

## Installation

```bash
# Clone repository
git clone https://github.com/scionoftech/tinyworkflow
cd tinyworkflow

# Install in development mode
pip install -e .
```

## Running the Web UI

**Step 1: Navigate to project root**
```bash
cd /path/to/tinyworkflow
```

**Step 2: Start server with workflows**
```bash
tinyworkflow server --import-workflows examples.workflows
```

**Step 3: Open browser**
```
http://localhost:8080
```

You should see **20 workflows** ready to run!

## Running Example Workflows

### From Python

```bash
# Run any example directly
python examples/simple_workflow.py
python examples/retry_workflow.py
python examples/approval_workflow.py
python examples/scheduling_workflow.py
python examples/ai_content_pipeline.py
```

### From CLI

```bash
# List registered workflows
tinyworkflow workflows

# Start a workflow
tinyworkflow start simple_etl --input '{"url": "https://api.example.com"}'

# Check status
tinyworkflow status <run_id>

# Schedule a workflow
tinyworkflow schedule daily_backup "0 2 * * *"
```

## Common Issues

### ❌ "No module named 'examples'"

**Problem:** Running from wrong directory

**Solution:**
```bash
# Check current directory
pwd

# Should show: /path/to/tinyworkflow
# NOT: /path/to/tinyworkflow/src

# Navigate to project root
cd /path/to/tinyworkflow
```

### ❌ "No workflows registered" in Web UI

**Problem:** Started server without `--import-workflows`

**Solution:**
```bash
# Stop server (Ctrl+C)

# Restart with workflow import
tinyworkflow server --import-workflows examples.workflows
```

### ❌ Empty workflow dropdowns

**Problem:** Workflows not imported

**Solution:**
```bash
# Always use --import-workflows
tinyworkflow server --import-workflows examples.workflows

# Verify workflows are registered (should show 20)
tinyworkflow workflows
```

## Next Steps

1. **Explore Examples**
   - Check `examples/` directory for 20+ workflow examples
   - Run them to see different patterns

2. **Create Your Own Workflows**
   ```python
   # myproject/workflows/orders.py
   from tinyworkflow import workflow, activity, WorkflowContext

   @activity(name="create_order")
   async def create_order(order_id: str):
       # Your code
       return {"order_id": order_id}

   @workflow(name="order_workflow")
   async def order_workflow(ctx: WorkflowContext):
       order_id = ctx.get_input("order_id")
       result = await ctx.execute_activity(create_order, order_id)
       return result
   ```

3. **Create Workflow Registry**
   ```python
   # myproject/workflows.py
   from myproject.workflows.orders import *
   ```

4. **Start Server with Your Workflows**
   ```bash
   cd /path/to/myproject
   tinyworkflow server --import-workflows myproject.workflows
   ```

## Architecture Overview

```
TinyWorkflow Project
├── examples/              # Example workflows (20 workflows)
│   ├── __init__.py       # Makes it a package
│   ├── workflows.py      # Registry module
│   ├── simple_workflow.py
│   ├── retry_workflow.py
│   ├── scheduling_workflow.py
│   └── ...
├── src/
│   └── tinyworkflow/      # Core library
│       ├── cli.py        # Command-line interface
│       ├── server.py     # Web UI server
│       ├── workflow.py   # Workflow engine
│       └── ...
├── tests/                # Test suite
├── README.md            # Main documentation
├── LIMITATIONS.md       # Architecture limitations
└── WORKFLOW_REGISTRATION.md  # Detailed registration guide
```

## Key Commands

```bash
# Server
tinyworkflow server --import-workflows examples.workflows

# Worker
tinyworkflow worker --import-workflows examples.workflows

# List workflows
tinyworkflow workflows

# Start workflow
tinyworkflow start <workflow_name> --input '{"key": "value"}'

# Check status
tinyworkflow status <run_id>

# List executions
tinyworkflow list

# View events
tinyworkflow events <run_id>

# Approvals
tinyworkflow approvals
tinyworkflow approve <run_id> --approve
```

## Database Configuration

```bash
# SQLite (default)
tinyworkflow server --import-workflows examples.workflows

# PostgreSQL
tinyworkflow server \
  --db "postgresql+asyncpg://user:pass@localhost/tinyworkflow" \
  --import-workflows myproject.workflows

# MySQL
tinyworkflow server \
  --db "mysql+asyncmy://user:pass@localhost/tinyworkflow" \
  --import-workflows myproject.workflows

# Environment variable
export TINYWORKFLOW_DATABASE_URL="postgresql+asyncpg://user:pass@localhost/tinyworkflow"
tinyworkflow server --import-workflows examples.workflows
```

## Development Workflow

```bash
# 1. Create workflows
# myproject/workflows/my_workflow.py

# 2. Create registry
# myproject/workflows.py

# 3. Test import
python -c "import myproject.workflows; from tinyworkflow.workflow import list_workflows; print(list_workflows())"

# 4. Start server
tinyworkflow server --import-workflows myproject.workflows

# 5. Open browser
# http://localhost:8080

# 6. Test workflow via UI or CLI
tinyworkflow start my_workflow --input '{"test": true}'
```

## Summary

**✅ DO:**
- Run from project root directory
- Use `--import-workflows` option
- Create workflow registry module
- Check `tinyworkflow workflows` to verify

**❌ DON'T:**
- Run from subdirectories
- Forget `--import-workflows`
- Expect auto-discovery of workflows
- Use relative paths in module names

**Need Help?**
- See [WORKFLOW_REGISTRATION.md](WORKFLOW_REGISTRATION.md) for detailed guide
- Check [LIMITATIONS.md](LIMITATIONS.md) to understand design choices
- Review [README.md](README.md) for full documentation
