"""
Tests for database configuration and multi-database support.
"""

import pytest
import tempfile
import os
from sqlalchemy.ext.asyncio import AsyncSession

from tinyworkflow.state import StateManager, _get_engine_params
from tinyworkflow.models import WorkflowExecution, ActivityExecution, WorkflowStatus


class TestDatabaseURLValidation:
    """Test database URL validation."""

    def test_valid_sqlite_url(self):
        """Test valid SQLite URL."""
        state_manager = StateManager("sqlite+aiosqlite:///test.db")
        assert state_manager.database_url == "sqlite+aiosqlite:///test.db"

    def test_valid_postgresql_url(self):
        """Test valid PostgreSQL URL."""
        state_manager = StateManager(
            "postgresql+asyncpg://user:pass@localhost:5432/test"
        )
        assert "postgresql+asyncpg" in state_manager.database_url

    def test_valid_mysql_url(self):
        """Test valid MySQL URL."""
        state_manager = StateManager("mysql+asyncmy://user:pass@localhost:3306/test")
        assert "mysql+asyncmy" in state_manager.database_url

    def test_invalid_empty_url(self):
        """Test empty database URL."""
        with pytest.raises(ValueError, match="must be a non-empty string"):
            StateManager("")

    def test_invalid_none_url(self):
        """Test None database URL."""
        with pytest.raises(ValueError, match="must be a non-empty string"):
            StateManager(None)

    def test_unsupported_postgresql_sync_driver(self):
        """Test PostgreSQL with synchronous driver."""
        with pytest.raises(ValueError, match="PostgreSQL requires an async driver"):
            StateManager("postgresql://user:pass@localhost/test")

    def test_unsupported_postgresql_psycopg_driver(self):
        """Test PostgreSQL with psycopg driver."""
        with pytest.raises(ValueError, match="PostgreSQL requires an async driver"):
            StateManager("postgresql+psycopg://user:pass@localhost/test")

    def test_unsupported_mysql_sync_driver(self):
        """Test MySQL with synchronous driver."""
        with pytest.raises(ValueError, match="MySQL requires an async driver"):
            StateManager("mysql://user:pass@localhost/test")

    def test_unsupported_mysql_pymysql_driver(self):
        """Test MySQL with pymysql driver."""
        with pytest.raises(ValueError, match="MySQL requires an async driver"):
            StateManager("mysql+pymysql://user:pass@localhost/test")

    def test_unsupported_sqlite_sync_driver(self):
        """Test SQLite without async driver."""
        with pytest.raises(ValueError, match="SQLite requires an async driver"):
            StateManager("sqlite:///test.db")

    def test_completely_unsupported_database(self):
        """Test completely unsupported database."""
        with pytest.raises(ValueError, match="Unsupported database driver"):
            StateManager("oracle://user:pass@localhost/test")


class TestEngineParameters:
    """Test database-specific engine parameters."""

    def test_sqlite_engine_params(self):
        """Test SQLite engine parameters."""
        params = _get_engine_params("sqlite+aiosqlite:///test.db")
        assert params["echo"] == False
        assert "connect_args" in params
        assert params["connect_args"]["check_same_thread"] == False

    def test_postgresql_engine_params(self):
        """Test PostgreSQL engine parameters."""
        params = _get_engine_params("postgresql+asyncpg://user:pass@localhost/test")
        assert params["echo"] == False
        assert params["pool_size"] == 10
        assert params["max_overflow"] == 20
        assert params["pool_pre_ping"] == True
        assert params["pool_recycle"] == 3600

    def test_mysql_engine_params(self):
        """Test MySQL engine parameters."""
        params = _get_engine_params("mysql+asyncmy://user:pass@localhost/test")
        assert params["echo"] == False
        assert params["pool_size"] == 10
        assert params["max_overflow"] == 20
        assert params["pool_pre_ping"] == True
        assert params["pool_recycle"] == 3600


class TestStateManagerInitialization:
    """Test StateManager initialization with different databases."""

    def test_sqlite_state_manager_init(self):
        """Test StateManager initialization with SQLite."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            state_manager = StateManager(f"sqlite+aiosqlite:///{db_path}")
            assert state_manager.database_url == f"sqlite+aiosqlite:///{db_path}"
            assert state_manager.engine is not None
            assert state_manager.async_session_maker is not None
            assert state_manager._initialized == False
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)

    def test_default_sqlite_state_manager(self):
        """Test StateManager with default SQLite URL."""
        state_manager = StateManager()
        assert state_manager.database_url == "sqlite+aiosqlite:///tinyworkflow.db"

    def test_postgresql_state_manager_init(self):
        """Test StateManager initialization with PostgreSQL URL."""
        # Just test initialization, don't connect
        state_manager = StateManager(
            "postgresql+asyncpg://user:pass@localhost:5432/test"
        )
        assert "postgresql+asyncpg" in state_manager.database_url
        assert state_manager.engine is not None

    def test_mysql_state_manager_init(self):
        """Test StateManager initialization with MySQL URL."""
        # Just test initialization, don't connect
        state_manager = StateManager("mysql+asyncmy://user:pass@localhost:3306/test")
        assert "mysql+asyncmy" in state_manager.database_url
        assert state_manager.engine is not None


@pytest.mark.asyncio
class TestStateManagerOperations:
    """Test StateManager operations with SQLite."""

    async def test_initialize_database(self):
        """Test database initialization."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            state_manager = StateManager(f"sqlite+aiosqlite:///{db_path}")
            await state_manager.initialize()
            assert state_manager._initialized == True

            # Initialize again should not fail
            await state_manager.initialize()
            assert state_manager._initialized == True

            await state_manager.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_create_workflow(self):
        """Test creating a workflow."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            state_manager = StateManager(f"sqlite+aiosqlite:///{db_path}")
            await state_manager.initialize()

            workflow = await state_manager.create_workflow(
                workflow_id="wf-1",
                run_id="run-1",
                workflow_name="test_workflow",
                input_data={"key": "value"},
                max_retries=3,
            )

            assert workflow is not None
            assert workflow.workflow_id == "wf-1"
            assert workflow.run_id == "run-1"
            assert workflow.workflow_name == "test_workflow"
            assert workflow.input_data == {"key": "value"}
            assert workflow.status == WorkflowStatus.PENDING
            assert workflow.max_retries == 3

            await state_manager.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_get_workflow(self):
        """Test retrieving a workflow."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            state_manager = StateManager(f"sqlite+aiosqlite:///{db_path}")
            await state_manager.initialize()

            # Create workflow
            await state_manager.create_workflow(
                workflow_id="wf-2",
                run_id="run-2",
                workflow_name="test_workflow",
            )

            # Get workflow
            workflow = await state_manager.get_workflow("run-2")
            assert workflow is not None
            assert workflow.run_id == "run-2"
            assert workflow.workflow_name == "test_workflow"

            # Get non-existent workflow
            workflow = await state_manager.get_workflow("non-existent")
            assert workflow is None

            await state_manager.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_update_workflow_status(self):
        """Test updating workflow status."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            state_manager = StateManager(f"sqlite+aiosqlite:///{db_path}")
            await state_manager.initialize()

            # Create workflow
            await state_manager.create_workflow(
                workflow_id="wf-3",
                run_id="run-3",
                workflow_name="test_workflow",
            )

            # Update to RUNNING
            await state_manager.update_workflow_status(
                run_id="run-3", status=WorkflowStatus.RUNNING
            )
            workflow = await state_manager.get_workflow("run-3")
            assert workflow.status == WorkflowStatus.RUNNING
            assert workflow.started_at is not None

            # Update to COMPLETED
            await state_manager.update_workflow_status(
                run_id="run-3",
                status=WorkflowStatus.COMPLETED,
                output_data={"result": "success"},
            )
            workflow = await state_manager.get_workflow("run-3")
            assert workflow.status == WorkflowStatus.COMPLETED
            assert workflow.completed_at is not None
            assert workflow.output_data == {"result": "success"}

            # Update to FAILED with error
            await state_manager.update_workflow_status(
                run_id="run-3", status=WorkflowStatus.FAILED, error="Test error"
            )
            workflow = await state_manager.get_workflow("run-3")
            assert workflow.status == WorkflowStatus.FAILED
            assert workflow.error == "Test error"

            await state_manager.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_create_activity(self):
        """Test creating an activity."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            state_manager = StateManager(f"sqlite+aiosqlite:///{db_path}")
            await state_manager.initialize()

            # Create workflow first
            await state_manager.create_workflow(
                workflow_id="wf-4",
                run_id="run-4",
                workflow_name="test_workflow",
            )

            # Create activity
            activity = await state_manager.create_activity(
                workflow_run_id="run-4",
                activity_id="act-1",
                activity_name="test_activity",
                sequence_number=1,
                input_data={"input": "data"},
            )

            assert activity is not None
            assert activity.workflow_run_id == "run-4"
            assert activity.activity_id == "act-1"
            assert activity.activity_name == "test_activity"
            assert activity.sequence_number == 1
            assert activity.input_data == {"input": "data"}

            await state_manager.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_list_workflows(self):
        """Test listing workflows."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            state_manager = StateManager(f"sqlite+aiosqlite:///{db_path}")
            await state_manager.initialize()

            # Create multiple workflows
            await state_manager.create_workflow(
                workflow_id="wf-5", run_id="run-5", workflow_name="workflow_a"
            )
            await state_manager.create_workflow(
                workflow_id="wf-6", run_id="run-6", workflow_name="workflow_b"
            )
            await state_manager.update_workflow_status("run-6", WorkflowStatus.RUNNING)

            # List all workflows
            workflows = await state_manager.list_workflows()
            assert len(workflows) >= 2

            # List by status
            workflows = await state_manager.list_workflows(status=WorkflowStatus.PENDING)
            assert len(workflows) >= 1
            assert all(w.status == WorkflowStatus.PENDING for w in workflows)

            # List by workflow name
            workflows = await state_manager.list_workflows(workflow_name="workflow_a")
            assert len(workflows) >= 1
            assert all(w.workflow_name == "workflow_a" for w in workflows)

            await state_manager.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_workflow_events(self):
        """Test workflow events (audit trail)."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            state_manager = StateManager(f"sqlite+aiosqlite:///{db_path}")
            await state_manager.initialize()

            # Create workflow
            await state_manager.create_workflow(
                workflow_id="wf-7", run_id="run-7", workflow_name="test_workflow"
            )

            # Update status (creates events)
            await state_manager.update_workflow_status(
                run_id="run-7", status=WorkflowStatus.RUNNING
            )
            await state_manager.update_workflow_status(
                run_id="run-7", status=WorkflowStatus.COMPLETED
            )

            # Get events
            events = await state_manager.get_workflow_events("run-7")
            assert len(events) >= 3  # created, running, completed
            assert any(e.event_type == "workflow_created" for e in events)
            assert any(e.event_type == "workflow_running" for e in events)
            assert any(e.event_type == "workflow_completed" for e in events)

            await state_manager.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
