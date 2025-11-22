"""
Workflow registry module.
Import this module to register all example workflows.

This file imports all workflow modules so their @workflow decorators
are executed and workflows are registered in the global registry.
"""

# Core workflow examples
from examples.simple_workflow import *
from examples.parallel_workflow import *
from examples.approval_workflow import *
from examples.retry_workflow import *
from examples.scheduling_workflow import *

# AI workflow examples
from examples.ai_workflow import *
from examples.ai_content_pipeline import *
from examples.ai_document_processor import *

# Print registered workflows when imported
if __name__ != "__main__":
    from tinyworkflow.workflow import list_workflows

    workflows = list_workflows()
    print(f"Registered {len(workflows)} workflows: {', '.join(workflows)}")
