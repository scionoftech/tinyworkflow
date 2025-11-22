# Workflow Registration Guide

## Problem

When you start the TinyWorkflow web server or worker, workflows from your application aren't automatically available. This is because **workflows are only registered when their Python modules are imported**.

The web UI shows empty lists for:
- Start Workflow (no workflows to select)
- Schedule (no workflows to schedule)
- Registry (no registered workflows)

## Why This Happens

TinyWorkflow uses Python decorators to register workflows:

```python
@workflow(name="my_workflow")
async def my_workflow(ctx: WorkflowContext):
    # Workflow logic
    pass
```

The `@workflow` decorator registers the workflow into a global registry **when the Python module is imported**. If you start the server without importing your workflow modules, the registry is empty.

## Solutions

There are **three ways** to make your workflows available:

### Solution 1: Use the `--import-workflows` Option (Recommended)

Start the server or worker with the `--import-workflows` (or `-w`) option:

```bash
# IMPORTANT: Run from the project root directory
cd /path/to/tinyworkflow

# Start server with workflows
tinyworkflow server --import-workflows examples.workflows

# Start worker with workflows
tinyworkflow worker --import-workflows examples.workflows

# Use short form
tinyworkflow server -w myproject.workflows
```

**How it works:**
1. CLI adds current directory to Python path
2. Imports the specified module
3. Workflows are registered via decorators
4. Web UI shows all workflows

**Requirements:**
- Must run from project root directory
- Module must be a valid Python package (has `__init__.py`)
- For `examples.workflows`, run from directory containing `examples/` folder

### Solution 2: Create a Workflow Registry Module

Create a `workflows.py` file that imports all your workflow modules:

```python
# myproject/workflows.py
"""
Workflow registry - imports all workflow modules
"""

from myproject.orders import order_workflow
from myproject.payments import payment_workflow
from myproject.notifications import notification_workflow

# All workflows are now registered!
```

Then start the server:

```bash
tinyworkflow server --import-workflows myproject.workflows
```

### Solution 3: Create a Custom Application Entry Point

Create an `app.py` that imports workflows and starts the server:

```python
# app.py
import sys
import os

# Import all your workflow modules
from myproject import orders, payments, notifications

if __name__ == "__main__":
    from tinyworkflow.server import create_app
    import uvicorn

    # Start server
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=8080)
```

Then run:

```bash
python app.py
```

## Example Setup

### Directory Structure

```
myproject/
├── workflows/
│   ├── __init__.py
│   ├── orders.py        # Contains @workflow decorators
│   ├── payments.py      # Contains @workflow decorators
│   └── notifications.py # Contains @workflow decorators
├── workflows.py         # Registry module (imports all workflows)
└── app.py              # Optional: custom entry point
```

### workflows.py (Registry)

```python
# myproject/workflows.py
"""Workflow registry"""

# Import all workflow modules
from myproject.workflows.orders import *
from myproject.workflows.payments import *
from myproject.workflows.notifications import *
```

### Usage

```bash
# Option 1: CLI with import
tinyworkflow server --import-workflows myproject.workflows

# Option 2: Custom app.py
python app.py
```

## Using with Examples

TinyWorkflow includes an example workflow registry:

```bash
# Start server with all example workflows
tinyworkflow server --import-workflows examples.workflows

# Start worker with all example workflows
tinyworkflow worker --import-workflows examples.workflows
```

This registers **20 example workflows**:

**Core workflows:**
- simple_etl
- user_report
- expense_approval
- api_workflow_with_retry
- multi_step_retry_workflow
- workflow_level_retry
- parallel_with_retries
- daily_backup
- weekly_report
- hourly_health_check
- monthly_cleanup
- delayed_task

**AI workflows:**
- sentiment_analysis
- batch_sentiment_analysis
- simple_ai_pipeline
- advanced_ai_pipeline
- ai_content_with_approval
- process_document
- advanced_document_processing
- batch_document_processing

## Environment Variable Support

You can also set the `TINYWORKFLOW_WORKFLOWS_MODULE` environment variable:

```bash
# Linux/Mac
export TINYWORKFLOW_WORKFLOWS_MODULE=myproject.workflows
tinyworkflow server

# Windows
set TINYWORKFLOW_WORKFLOWS_MODULE=myproject.workflows
tinyworkflow server
```

**Note:** This feature is not yet implemented but can be added if needed.

## Troubleshooting

### "No module named 'examples'" or "No module named 'myproject'"

**Problem:** `ModuleNotFoundError: No module named 'examples'`

**Solutions:**

1. **Run from the correct directory:**
   ```bash
   # Wrong - running from src/
   cd src
   tinyworkflow server --import-workflows examples.workflows  # ❌ Won't work

   # Correct - run from project root
   cd /path/to/tinyworkflow
   tinyworkflow server --import-workflows examples.workflows  # ✅ Works
   ```

2. **Verify package structure:**
   ```bash
   # Check that __init__.py exists
   ls examples/__init__.py  # Should exist
   ```

3. **Install project in development mode:**
   ```bash
   pip install -e .
   # Now you can run from anywhere
   ```

### "No workflows registered"

**Problem:** Web UI shows "No workflows registered"

**Solutions:**
1. Verify you're using `--import-workflows` option
2. Check that the module path is correct (e.g., `examples.workflows` not `examples/workflows.py`)
3. Run from the project root directory

```bash
# Check current directory
pwd  # Should show project root

# Check if module is importable
python -c "import examples.workflows"
```

### Import errors

**Problem:** `ModuleNotFoundError: No module named 'myproject'`

**Solutions:**
1. Add your project to Python path:
   ```bash
   export PYTHONPATH=/path/to/your/project:$PYTHONPATH
   ```

2. Install your project in development mode:
   ```bash
   pip install -e .
   ```

3. Run from the correct directory:
   ```bash
   cd /path/to/your/project
   tinyworkflow server --import-workflows myproject.workflows
   ```

### Workflows not appearing after import

**Problem:** Module imports successfully but workflows still don't appear

**Solutions:**
1. Verify workflows are using the `@workflow` decorator
2. Check that workflow functions are defined at module level (not inside classes)
3. Ensure workflow modules are actually imported in your registry file

```python
# ❌ Wrong - doesn't actually import workflows
import myproject.workflows.orders

# ✅ Correct - imports everything including decorators
from myproject.workflows.orders import *
```

## Best Practices

### 1. Create a Dedicated Registry Module

```python
# workflows.py - single source of truth
from myproject.workflows.orders import *
from myproject.workflows.payments import *
```

### 2. Use Explicit Imports for Production

```python
# Better for production - explicit and clear
from myproject.workflows.orders import (
    create_order_workflow,
    cancel_order_workflow,
    refund_order_workflow,
)
```

### 3. Separate Workflows by Domain

```
workflows/
├── orders.py
├── payments.py
├── inventory.py
└── notifications.py
```

### 4. Document Required Workflows

```python
# workflows.py
"""
Workflow Registry

Registered workflows:
- order_workflows: create_order, cancel_order, refund_order
- payment_workflows: process_payment, refund_payment
- notification_workflows: send_email, send_sms
"""
```

## Summary

**Quick Start:**

```bash
# 1. Create workflow registry
# myproject/workflows.py
from myproject.orders import *

# 2. Start server with import
tinyworkflow server --import-workflows myproject.workflows

# 3. Open web UI
# http://localhost:8080
```

**Key Points:**
- ✅ Workflows must be imported to be registered
- ✅ Use `--import-workflows` option when starting server/worker
- ✅ Create a `workflows.py` registry module for your project
- ✅ Workflows appear immediately in web UI after import
- ❌ Don't expect auto-discovery - explicit imports required
