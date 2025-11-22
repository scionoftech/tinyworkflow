"""
Example application file that imports all workflows.
This file should be run to start the TinyWorkflow server with all workflows registered.

Usage:
    python examples/app.py

Or use the CLI with the --import option:
    tinyworkflow server --import examples.app
"""

import sys
import os

# Add parent directory to path so we can import examples
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import all workflow modules to register them
print("Registering workflows...")

# Core workflows
from examples import simple_workflow
from examples import parallel_workflow
from examples import approval_workflow
from examples import retry_workflow
from examples import scheduling_workflow

# AI workflows
from examples import ai_workflow
from examples import ai_content_pipeline
from examples import ai_document_processor

print("All workflows registered!")
print("\nRegistered workflows:")
from tinyworkflow import TinyWorkflowClient

client = TinyWorkflowClient()
workflows = client.list_registered_workflows()
for wf in workflows:
    print(f"  - {wf}")

print(f"\nTotal: {len(workflows)} workflows")
print("\nStarting server...")
print("Open http://localhost:8080 in your browser")
print("=" * 70)

# Start the server
if __name__ == "__main__":
    from tinyworkflow.server import create_app
    import uvicorn

    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=8080)
