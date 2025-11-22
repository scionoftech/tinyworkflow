"""
FastAPI server with web UI for workflow management.
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime

from tinyworkflow.client import TinyWorkflowClient
from tinyworkflow.models import WorkflowStatus


# Pydantic models for API
class WorkflowStartRequest(BaseModel):
    workflow_name: str
    input_data: Optional[Dict[str, Any]] = None
    wait: bool = False


class WorkflowScheduleRequest(BaseModel):
    workflow_name: str
    cron_expression: str
    input_data: Optional[Dict[str, Any]] = None


class ApprovalRequest(BaseModel):
    approved: bool


# Global client instance
_client: Optional[TinyWorkflowClient] = None


def create_app(database_url: str = "sqlite+aiosqlite:///tinyworkflow.db") -> FastAPI:
    """Create FastAPI application."""
    app = FastAPI(title="TinyWorkflow", description="Durable Workflow Orchestration", version="0.1.0")

    global _client

    @app.on_event("startup")
    async def startup():
        global _client
        _client = TinyWorkflowClient(database_url, auto_start_worker=True)
        await _client.initialize()

    @app.on_event("shutdown")
    async def shutdown():
        global _client
        if _client:
            await _client.close()

    # API routes
    @app.post("/api/workflows/start")
    async def start_workflow(request: WorkflowStartRequest):
        """Start a workflow execution."""
        try:
            run_id = await _client.start_workflow(
                workflow_name=request.workflow_name,
                input_data=request.input_data,
                wait=request.wait,
            )
            return {"run_id": run_id, "status": "started"}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/api/workflows/{run_id}")
    async def get_workflow(run_id: str):
        """Get workflow status."""
        workflow = await _client.get_workflow_status(run_id)
        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")

        return {
            "run_id": workflow.run_id,
            "workflow_id": workflow.workflow_id,
            "workflow_name": workflow.workflow_name,
            "status": workflow.status,
            "input_data": workflow.input_data,
            "output_data": workflow.output_data,
            "error": workflow.error,
            "created_at": workflow.created_at.isoformat(),
            "started_at": workflow.started_at.isoformat() if workflow.started_at else None,
            "completed_at": workflow.completed_at.isoformat() if workflow.completed_at else None,
            "retry_count": workflow.retry_count,
            "max_retries": workflow.max_retries,
        }

    @app.get("/api/workflows")
    async def list_workflows(
        status: Optional[str] = None, workflow_name: Optional[str] = None, limit: int = 100
    ):
        """List workflow executions."""
        status_filter = None
        if status:
            try:
                status_filter = WorkflowStatus(status.lower())
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

        workflows = await _client.list_workflow_executions(
            status=status_filter, workflow_name=workflow_name, limit=limit
        )

        return {
            "workflows": [
                {
                    "run_id": w.run_id,
                    "workflow_name": w.workflow_name,
                    "status": w.status,
                    "created_at": w.created_at.isoformat(),
                    "completed_at": w.completed_at.isoformat() if w.completed_at else None,
                    "error": w.error,
                }
                for w in workflows
            ]
        }

    @app.delete("/api/workflows/{run_id}")
    async def cancel_workflow(run_id: str):
        """Cancel a workflow."""
        await _client.cancel_workflow(run_id)
        return {"status": "cancelled"}

    @app.get("/api/workflows/{run_id}/events")
    async def get_workflow_events(run_id: str, limit: int = 100):
        """Get workflow events."""
        events = await _client.get_workflow_events(run_id, limit=limit)
        return {
            "events": [
                {
                    "event_type": e.event_type,
                    "event_data": e.event_data,
                    "timestamp": e.timestamp.isoformat(),
                }
                for e in events
            ]
        }

    @app.post("/api/schedules")
    async def schedule_workflow(request: WorkflowScheduleRequest):
        """Schedule a workflow."""
        try:
            job_id = await _client.schedule_workflow(
                workflow_name=request.workflow_name,
                cron_expression=request.cron_expression,
                input_data=request.input_data,
            )
            return {"job_id": job_id, "status": "scheduled"}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/api/approvals")
    async def get_pending_approvals():
        """Get pending approvals."""
        workflows = await _client.get_pending_approvals()
        return {
            "approvals": [
                {
                    "run_id": w.run_id,
                    "workflow_name": w.workflow_name,
                    "approval_required": w.approval_required,
                    "created_at": w.created_at.isoformat(),
                }
                for w in workflows
            ]
        }

    @app.post("/api/workflows/{run_id}/approve")
    async def approve_workflow(run_id: str, request: ApprovalRequest):
        """Approve or reject a workflow."""
        if request.approved:
            await _client.approve_workflow(run_id)
        else:
            await _client.reject_workflow(run_id)
        return {"status": "approved" if request.approved else "rejected"}

    @app.get("/api/registry/workflows")
    async def list_registered_workflows():
        """List registered workflows."""
        return {"workflows": _client.list_registered_workflows()}

    @app.get("/api/registry/activities")
    async def list_registered_activities():
        """List registered activities."""
        return {"activities": _client.list_registered_activities()}

    # Web UI
    @app.get("/", response_class=HTMLResponse)
    async def index():
        """Serve web UI."""
        return get_html_ui()

    return app


def get_html_ui() -> str:
    """Generate simple HTML UI."""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TinyWorkflow - Workflow Orchestration</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: #f5f7fa;
            color: #333;
            line-height: 1.6;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }
        header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px 0;
            margin-bottom: 30px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        h1 { font-size: 2.5em; margin-bottom: 10px; }
        h2 { color: #667eea; margin: 20px 0 15px; }
        h3 { margin: 15px 0 10px; color: #555; }

        .tabs {
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
            border-bottom: 2px solid #e0e0e0;
        }
        .tab {
            padding: 12px 24px;
            background: #e8eaf6;
            border: 2px solid transparent;
            cursor: pointer;
            font-size: 16px;
            font-weight: 500;
            color: #5568d3;
            border-top-left-radius: 8px;
            border-top-right-radius: 8px;
            transition: all 0.3s;
            position: relative;
        }
        .tab:hover {
            background: #c5cae9;
            transform: translateY(-2px);
        }
        .tab.active {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-color: #667eea;
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
            font-weight: 600;
        }
        .tab.active::after {
            content: '';
            position: absolute;
            bottom: -2px;
            left: 0;
            right: 0;
            height: 2px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }

        .tab-content {
            display: none;
            background: white;
            padding: 25px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        .tab-content.active { display: block; }

        .form-group {
            margin-bottom: 20px;
        }
        label {
            display: block;
            margin-bottom: 5px;
            font-weight: 600;
            color: #555;
        }
        input, textarea, select {
            width: 100%;
            padding: 12px;
            border: 1px solid #ddd;
            border-radius: 6px;
            font-size: 14px;
            transition: border 0.3s;
        }
        input:focus, textarea:focus, select:focus {
            outline: none;
            border-color: #667eea;
        }
        textarea { min-height: 100px; font-family: monospace; }

        button {
            background: #667eea;
            color: white;
            padding: 12px 24px;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-size: 16px;
            transition: background 0.3s;
        }
        button:hover { background: #5568d3; }
        button:disabled {
            background: #ccc;
            cursor: not-allowed;
        }

        .workflow-list {
            display: grid;
            gap: 15px;
            margin-top: 20px;
        }
        .workflow-item {
            background: #f9f9f9;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #667eea;
            transition: transform 0.2s;
        }
        .workflow-item:hover {
            transform: translateX(5px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }
        .workflow-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }
        .workflow-name {
            font-size: 18px;
            font-weight: 600;
            color: #333;
        }
        .status {
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
        }
        .status.completed { background: #d4edda; color: #155724; }
        .status.running { background: #cce5ff; color: #004085; }
        .status.failed { background: #f8d7da; color: #721c24; }
        .status.pending { background: #fff3cd; color: #856404; }
        .status.paused { background: #e7e8ea; color: #383d41; }

        .workflow-details {
            font-size: 14px;
            color: #666;
        }
        .workflow-actions {
            margin-top: 10px;
            display: flex;
            gap: 10px;
        }
        .btn-small {
            padding: 6px 12px;
            font-size: 14px;
        }
        .btn-danger {
            background: #dc3545;
        }
        .btn-danger:hover {
            background: #c82333;
        }
        .btn-success {
            background: #28a745;
        }
        .btn-success:hover {
            background: #218838;
        }

        .message {
            padding: 15px;
            margin: 15px 0;
            border-radius: 6px;
        }
        .message.success {
            background: #d4edda;
            color: #155724;
        }
        .message.error {
            background: #f8d7da;
            color: #721c24;
        }

        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            text-align: center;
        }
        .stat-value {
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
        }
        .stat-label {
            color: #666;
            margin-top: 5px;
        }

        .refresh-btn {
            float: right;
            padding: 8px 16px;
            font-size: 14px;
        }

        code {
            background: #f4f4f4;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: 'Courier New', monospace;
        }

        .event-list {
            max-height: 400px;
            overflow-y: auto;
            background: #f9f9f9;
            padding: 15px;
            border-radius: 6px;
        }
        .event-item {
            padding: 10px;
            margin-bottom: 10px;
            background: white;
            border-radius: 4px;
            border-left: 3px solid #667eea;
        }
        .event-time {
            font-size: 12px;
            color: #999;
        }
        .event-type {
            font-weight: 600;
            color: #667eea;
        }
    </style>
</head>
<body>
    <header>
        <div class="container">
            <h1>⚡ TinyWorkflow</h1>
            <p>Lightweight Durable Workflow Orchestration</p>
        </div>
    </header>

    <div class="container">
        <div class="tabs">
            <button class="tab active" onclick="showTab('workflows')">Workflows</button>
            <button class="tab" onclick="showTab('start')">Start Workflow</button>
            <button class="tab" onclick="showTab('schedule')">Schedule</button>
            <button class="tab" onclick="showTab('approvals')">Approvals</button>
            <button class="tab" onclick="showTab('registry')">Registry</button>
        </div>

        <!-- Workflows Tab -->
        <div id="workflows" class="tab-content active">
            <h2>Workflow Executions</h2>
            <button class="refresh-btn" onclick="loadWorkflows()">Refresh</button>
            <div class="stats" id="stats"></div>
            <div class="workflow-list" id="workflow-list"></div>
        </div>

        <!-- Start Workflow Tab -->
        <div id="start" class="tab-content">
            <h2>Start New Workflow</h2>
            <div id="start-message"></div>
            <form onsubmit="startWorkflow(event)">
                <div class="form-group">
                    <label for="workflow-name">Workflow Name:</label>
                    <select id="workflow-name" required>
                        <option value="">Select a workflow...</option>
                    </select>
                </div>
                <div class="form-group">
                    <label for="input-data">Input Data (JSON):</label>
                    <textarea id="input-data" placeholder='{"key": "value"}'></textarea>
                </div>
                <div class="form-group">
                    <label>
                        <input type="checkbox" id="wait-completion"> Wait for completion
                    </label>
                </div>
                <button type="submit">Start Workflow</button>
            </form>
        </div>

        <!-- Schedule Tab -->
        <div id="schedule" class="tab-content">
            <h2>Schedule Workflow</h2>
            <div id="schedule-message"></div>
            <form onsubmit="scheduleWorkflow(event)">
                <div class="form-group">
                    <label for="schedule-workflow-name">Workflow Name:</label>
                    <select id="schedule-workflow-name" required>
                        <option value="">Select a workflow...</option>
                    </select>
                </div>
                <div class="form-group">
                    <label for="cron-expression">Cron Expression:</label>
                    <input type="text" id="cron-expression" placeholder="0 9 * * *" required>
                    <small>Examples: "0 9 * * *" (daily at 9am), "*/5 * * * *" (every 5 min)</small>
                </div>
                <div class="form-group">
                    <label for="schedule-input-data">Input Data (JSON):</label>
                    <textarea id="schedule-input-data" placeholder='{"key": "value"}'></textarea>
                </div>
                <button type="submit">Schedule Workflow</button>
            </form>
        </div>

        <!-- Approvals Tab -->
        <div id="approvals" class="tab-content">
            <h2>Pending Approvals</h2>
            <button class="refresh-btn" onclick="loadApprovals()">Refresh</button>
            <div class="workflow-list" id="approval-list"></div>
        </div>

        <!-- Registry Tab -->
        <div id="registry" class="tab-content">
            <h2>Registered Components</h2>
            <h3>Workflows</h3>
            <div id="registry-workflows"></div>
            <h3>Activities</h3>
            <div id="registry-activities"></div>
        </div>
    </div>

    <script>
        // Tab switching
        function showTab(tabName) {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            event.target.classList.add('active');
            document.getElementById(tabName).classList.add('active');

            if (tabName === 'workflows') loadWorkflows();
            if (tabName === 'approvals') loadApprovals();
            if (tabName === 'registry') loadRegistry();
        }

        // Load workflows
        async function loadWorkflows() {
            try {
                const response = await fetch('/api/workflows?limit=50');
                const data = await response.json();

                const list = document.getElementById('workflow-list');
                if (data.workflows.length === 0) {
                    list.innerHTML = '<p>No workflows found</p>';
                    return;
                }

                // Calculate stats
                const stats = {
                    total: data.workflows.length,
                    running: data.workflows.filter(w => w.status === 'running').length,
                    completed: data.workflows.filter(w => w.status === 'completed').length,
                    failed: data.workflows.filter(w => w.status === 'failed').length,
                };

                document.getElementById('stats').innerHTML = `
                    <div class="stat-card">
                        <div class="stat-value">${stats.total}</div>
                        <div class="stat-label">Total</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">${stats.running}</div>
                        <div class="stat-label">Running</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">${stats.completed}</div>
                        <div class="stat-label">Completed</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">${stats.failed}</div>
                        <div class="stat-label">Failed</div>
                    </div>
                `;

                list.innerHTML = data.workflows.map(w => `
                    <div class="workflow-item">
                        <div class="workflow-header">
                            <div class="workflow-name">${w.workflow_name}</div>
                            <span class="status ${w.status}">${w.status}</span>
                        </div>
                        <div class="workflow-details">
                            <div><strong>Run ID:</strong> <code>${w.run_id}</code></div>
                            <div><strong>Created:</strong> ${new Date(w.created_at).toLocaleString()}</div>
                            ${w.completed_at ? `<div><strong>Completed:</strong> ${new Date(w.completed_at).toLocaleString()}</div>` : ''}
                            ${w.error ? `<div><strong>Error:</strong> ${w.error}</div>` : ''}
                        </div>
                        <div class="workflow-actions">
                            <button class="btn-small" onclick="viewWorkflow('${w.run_id}')">View Details</button>
                            ${w.status === 'running' || w.status === 'pending' ?
                                `<button class="btn-small btn-danger" onclick="cancelWorkflow('${w.run_id}')">Cancel</button>` : ''}
                        </div>
                    </div>
                `).join('');
            } catch (error) {
                console.error('Error loading workflows:', error);
            }
        }

        // View workflow details
        async function viewWorkflow(runId) {
            try {
                const response = await fetch(`/api/workflows/${runId}`);
                const workflow = await response.json();

                const eventsResponse = await fetch(`/api/workflows/${runId}/events`);
                const eventsData = await eventsResponse.json();

                alert(`Workflow Details:\n\n` +
                    `Name: ${workflow.workflow_name}\n` +
                    `Run ID: ${workflow.run_id}\n` +
                    `Status: ${workflow.status}\n` +
                    `Created: ${new Date(workflow.created_at).toLocaleString()}\n` +
                    `\nInput: ${JSON.stringify(workflow.input_data, null, 2)}\n` +
                    `\nOutput: ${JSON.stringify(workflow.output_data, null, 2)}\n` +
                    `\nEvents: ${eventsData.events.length}`
                );
            } catch (error) {
                alert('Error loading workflow details: ' + error);
            }
        }

        // Cancel workflow
        async function cancelWorkflow(runId) {
            if (!confirm('Are you sure you want to cancel this workflow?')) return;

            try {
                await fetch(`/api/workflows/${runId}`, { method: 'DELETE' });
                loadWorkflows();
            } catch (error) {
                alert('Error cancelling workflow: ' + error);
            }
        }

        // Start workflow
        async function startWorkflow(event) {
            event.preventDefault();
            const messageDiv = document.getElementById('start-message');

            try {
                const inputData = document.getElementById('input-data').value;
                const payload = {
                    workflow_name: document.getElementById('workflow-name').value,
                    input_data: inputData ? JSON.parse(inputData) : null,
                    wait: document.getElementById('wait-completion').checked
                };

                const response = await fetch('/api/workflows/start', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });

                const data = await response.json();
                messageDiv.innerHTML = `<div class="message success">Workflow started! Run ID: ${data.run_id}</div>`;

                // Reset form
                document.getElementById('input-data').value = '';
                document.getElementById('wait-completion').checked = false;
            } catch (error) {
                messageDiv.innerHTML = `<div class="message error">Error: ${error.message}</div>`;
            }
        }

        // Schedule workflow
        async function scheduleWorkflow(event) {
            event.preventDefault();
            const messageDiv = document.getElementById('schedule-message');

            try {
                const inputData = document.getElementById('schedule-input-data').value;
                const payload = {
                    workflow_name: document.getElementById('schedule-workflow-name').value,
                    cron_expression: document.getElementById('cron-expression').value,
                    input_data: inputData ? JSON.parse(inputData) : null
                };

                const response = await fetch('/api/schedules', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });

                const data = await response.json();
                messageDiv.innerHTML = `<div class="message success">Workflow scheduled! Job ID: ${data.job_id}</div>`;

                // Reset form
                document.getElementById('cron-expression').value = '';
                document.getElementById('schedule-input-data').value = '';
            } catch (error) {
                messageDiv.innerHTML = `<div class="message error">Error: ${error.message}</div>`;
            }
        }

        // Load approvals
        async function loadApprovals() {
            try {
                const response = await fetch('/api/approvals');
                const data = await response.json();

                const list = document.getElementById('approval-list');
                if (data.approvals.length === 0) {
                    list.innerHTML = '<p>No pending approvals</p>';
                    return;
                }

                list.innerHTML = data.approvals.map(w => `
                    <div class="workflow-item">
                        <div class="workflow-header">
                            <div class="workflow-name">${w.workflow_name}</div>
                            <span class="status paused">Pending Approval</span>
                        </div>
                        <div class="workflow-details">
                            <div><strong>Run ID:</strong> <code>${w.run_id}</code></div>
                            <div><strong>Approval Key:</strong> ${w.approval_required}</div>
                            <div><strong>Created:</strong> ${new Date(w.created_at).toLocaleString()}</div>
                        </div>
                        <div class="workflow-actions">
                            <button class="btn-small btn-success" onclick="approveWorkflow('${w.run_id}', true)">Approve</button>
                            <button class="btn-small btn-danger" onclick="approveWorkflow('${w.run_id}', false)">Reject</button>
                        </div>
                    </div>
                `).join('');
            } catch (error) {
                console.error('Error loading approvals:', error);
            }
        }

        // Approve/reject workflow
        async function approveWorkflow(runId, approved) {
            try {
                await fetch(`/api/workflows/${runId}/approve`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ approved })
                });
                loadApprovals();
            } catch (error) {
                alert('Error: ' + error);
            }
        }

        // Load registry
        async function loadRegistry() {
            try {
                const [workflowsRes, activitiesRes] = await Promise.all([
                    fetch('/api/registry/workflows'),
                    fetch('/api/registry/activities')
                ]);

                const workflows = await workflowsRes.json();
                const activities = await activitiesRes.json();

                document.getElementById('registry-workflows').innerHTML =
                    workflows.workflows.length > 0
                        ? '<ul>' + workflows.workflows.map(w => `<li><code>${w}</code></li>`).join('') + '</ul>'
                        : '<p>No workflows registered</p>';

                document.getElementById('registry-activities').innerHTML =
                    activities.activities.length > 0
                        ? '<ul>' + activities.activities.map(a => `<li><code>${a}</code></li>`).join('') + '</ul>'
                        : '<p>No activities registered</p>';

                // Populate workflow dropdowns
                const workflowOptions = workflows.workflows.map(w =>
                    `<option value="${w}">${w}</option>`
                ).join('');
                document.getElementById('workflow-name').innerHTML =
                    '<option value="">Select a workflow...</option>' + workflowOptions;
                document.getElementById('schedule-workflow-name').innerHTML =
                    '<option value="">Select a workflow...</option>' + workflowOptions;
            } catch (error) {
                console.error('Error loading registry:', error);
            }
        }

        // Initialize
        loadWorkflows();
        loadRegistry();

        // Auto-refresh every 5 seconds
        setInterval(() => {
            const activeTab = document.querySelector('.tab-content.active');
            if (activeTab.id === 'workflows') loadWorkflows();
            if (activeTab.id === 'approvals') loadApprovals();
        }, 5000);
    </script>
</body>
</html>
"""
