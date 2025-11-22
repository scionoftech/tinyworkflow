"""
State management and persistence layer using SQLAlchemy with async support.
"""

import asyncio
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select, and_, or_, update
from sqlalchemy.orm import selectinload

from tinyworkflow.models import (
    Base,
    WorkflowExecution,
    ActivityExecution,
    WorkflowEvent,
    ScheduledWorkflow,
    WorkflowStatus,
    ActivityStatus,
)


def _get_engine_params(database_url: str) -> Dict[str, Any]:
    """
    Get database-specific engine parameters.

    Args:
        database_url: SQLAlchemy database URL

    Returns:
        Dictionary of engine parameters
    """
    params = {"echo": False}

    # PostgreSQL specific settings
    if "postgresql" in database_url:
        params.update({
            "pool_size": 10,
            "max_overflow": 20,
            "pool_pre_ping": True,  # Verify connections before using
            "pool_recycle": 3600,   # Recycle connections after 1 hour
        })

    # MySQL specific settings
    elif "mysql" in database_url:
        params.update({
            "pool_size": 10,
            "max_overflow": 20,
            "pool_pre_ping": True,
            "pool_recycle": 3600,
        })

    # SQLite specific settings
    elif "sqlite" in database_url:
        params.update({
            "connect_args": {"check_same_thread": False},
        })

    return params


class StateManager:
    """Manages workflow and activity state persistence with async support."""

    def __init__(self, database_url: str = "sqlite+aiosqlite:///tinyworkflow.db"):
        """
        Initialize the state manager.

        Args:
            database_url: SQLAlchemy database URL. Supported formats:
                - SQLite: sqlite+aiosqlite:///path/to/database.db
                - PostgreSQL: postgresql+asyncpg://user:password@host:port/database
                - MySQL: mysql+asyncmy://user:password@host:port/database

        Raises:
            ValueError: If database URL format is invalid or driver is not supported
        """
        self.database_url = database_url
        self._validate_database_url(database_url)

        # Get database-specific engine parameters
        engine_params = _get_engine_params(database_url)

        self.engine = create_async_engine(database_url, **engine_params)
        self.async_session_maker = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )
        self._initialized = False

    def _validate_database_url(self, database_url: str):
        """
        Validate database URL format and check for supported async drivers.

        Args:
            database_url: SQLAlchemy database URL

        Raises:
            ValueError: If URL is invalid or uses unsupported driver
        """
        if not database_url or not isinstance(database_url, str):
            raise ValueError("database_url must be a non-empty string")

        # Check for async driver support
        supported_drivers = {
            "sqlite+aiosqlite": "SQLite with aiosqlite",
            "postgresql+asyncpg": "PostgreSQL with asyncpg",
            "mysql+asyncmy": "MySQL with asyncmy",
            "mysql+aiomysql": "MySQL with aiomysql (alternative)",
        }

        # Check if using a supported async driver
        is_supported = any(driver in database_url for driver in supported_drivers.keys())

        if not is_supported:
            # Check if using a synchronous driver and provide helpful error
            if database_url.startswith("postgresql://") or database_url.startswith("postgresql+psycopg"):
                raise ValueError(
                    "PostgreSQL requires an async driver. "
                    "Use 'postgresql+asyncpg://...' instead."
                )
            elif database_url.startswith("mysql://") or database_url.startswith("mysql+pymysql"):
                raise ValueError(
                    "MySQL requires an async driver. "
                    "Use 'mysql+asyncmy://...' instead."
                )
            elif "sqlite" in database_url and "+aiosqlite" not in database_url:
                raise ValueError(
                    "SQLite requires an async driver. "
                    "Use 'sqlite+aiosqlite://...' instead."
                )
            else:
                supported_list = "\n  - ".join(supported_drivers.values())
                raise ValueError(
                    f"Unsupported database driver in URL: {database_url}\n"
                    f"Supported drivers:\n  - {supported_list}"
                )

    async def initialize(self):
        """Initialize database schema."""
        if not self._initialized:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            self._initialized = True

    async def close(self):
        """Close database connections."""
        await self.engine.dispose()

    @asynccontextmanager
    async def session(self):
        """Context manager for database sessions."""
        async with self.async_session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    # Workflow methods
    async def create_workflow(
        self,
        workflow_id: str,
        run_id: str,
        workflow_name: str,
        input_data: Optional[Dict[str, Any]] = None,
        scheduled_time: Optional[datetime] = None,
        cron_expression: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        max_retries: int = 3,
    ) -> WorkflowExecution:
        """Create a new workflow execution."""
        async with self.session() as session:
            workflow = WorkflowExecution(
                workflow_id=workflow_id,
                run_id=run_id,
                workflow_name=workflow_name,
                input_data=input_data,
                scheduled_time=scheduled_time,
                cron_expression=cron_expression,
                parent_run_id=parent_run_id,
                max_retries=max_retries,
                status=WorkflowStatus.PENDING,
            )
            session.add(workflow)
            await session.flush()

            # Record event
            await self._record_event(
                session, run_id, "workflow_created", {"workflow_name": workflow_name}
            )

            return workflow

    async def get_workflow(self, run_id: str) -> Optional[WorkflowExecution]:
        """Get workflow by run_id."""
        async with self.session() as session:
            result = await session.execute(
                select(WorkflowExecution)
                .options(selectinload(WorkflowExecution.activities))
                .where(WorkflowExecution.run_id == run_id)
            )
            return result.scalar_one_or_none()

    async def update_workflow_status(
        self,
        run_id: str,
        status: WorkflowStatus,
        output_data: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ):
        """Update workflow status."""
        async with self.session() as session:
            update_data = {"status": status, "updated_at": datetime.utcnow()}

            if status == WorkflowStatus.RUNNING and not output_data:
                update_data["started_at"] = datetime.utcnow()
            elif status in [WorkflowStatus.COMPLETED, WorkflowStatus.FAILED]:
                update_data["completed_at"] = datetime.utcnow()

            if output_data is not None:
                update_data["output_data"] = output_data
            if error is not None:
                update_data["error"] = error

            await session.execute(
                update(WorkflowExecution)
                .where(WorkflowExecution.run_id == run_id)
                .values(**update_data)
            )

            await self._record_event(
                session, run_id, f"workflow_{status.lower()}", {"error": error} if error else {}
            )

    async def increment_workflow_retry(self, run_id: str) -> int:
        """Increment workflow retry count and return new count."""
        async with self.session() as session:
            result = await session.execute(
                select(WorkflowExecution.retry_count).where(WorkflowExecution.run_id == run_id)
            )
            current_count = result.scalar_one()
            new_count = current_count + 1

            await session.execute(
                update(WorkflowExecution)
                .where(WorkflowExecution.run_id == run_id)
                .values(retry_count=new_count, status=WorkflowStatus.RETRYING)
            )

            await self._record_event(session, run_id, "workflow_retry", {"retry_count": new_count})
            return new_count

    async def list_workflows(
        self,
        status: Optional[WorkflowStatus] = None,
        workflow_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[WorkflowExecution]:
        """List workflows with optional filtering."""
        async with self.session() as session:
            query = select(WorkflowExecution).order_by(WorkflowExecution.created_at.desc())

            if status:
                query = query.where(WorkflowExecution.status == status)
            if workflow_name:
                query = query.where(WorkflowExecution.workflow_name == workflow_name)

            query = query.limit(limit).offset(offset)
            result = await session.execute(query)
            return list(result.scalars().all())

    # Activity methods
    async def create_activity(
        self,
        workflow_run_id: str,
        activity_id: str,
        activity_name: str,
        sequence_number: int,
        input_data: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
    ) -> ActivityExecution:
        """Create a new activity execution."""
        async with self.session() as session:
            activity = ActivityExecution(
                workflow_run_id=workflow_run_id,
                activity_id=activity_id,
                activity_name=activity_name,
                sequence_number=sequence_number,
                input_data=input_data,
                max_retries=max_retries,
                status=ActivityStatus.PENDING,
            )
            session.add(activity)
            await session.flush()

            await self._record_event(
                session,
                workflow_run_id,
                "activity_created",
                {"activity_name": activity_name, "sequence": sequence_number},
            )

            return activity

    async def update_activity_status(
        self,
        activity_id: str,
        workflow_run_id: str,
        status: ActivityStatus,
        output_data: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ):
        """Update activity status."""
        async with self.session() as session:
            update_data = {"status": status}

            if status == ActivityStatus.RUNNING:
                update_data["started_at"] = datetime.utcnow()
            elif status in [ActivityStatus.COMPLETED, ActivityStatus.FAILED]:
                update_data["completed_at"] = datetime.utcnow()

            if output_data is not None:
                update_data["output_data"] = output_data
            if error is not None:
                update_data["error"] = error

            await session.execute(
                update(ActivityExecution)
                .where(
                    and_(
                        ActivityExecution.activity_id == activity_id,
                        ActivityExecution.workflow_run_id == workflow_run_id,
                    )
                )
                .values(**update_data)
            )

            await self._record_event(
                session,
                workflow_run_id,
                f"activity_{status.lower()}",
                {"activity_id": activity_id, "error": error} if error else {"activity_id": activity_id},
            )

    async def get_activities(self, workflow_run_id: str) -> List[ActivityExecution]:
        """Get all activities for a workflow."""
        async with self.session() as session:
            result = await session.execute(
                select(ActivityExecution)
                .where(ActivityExecution.workflow_run_id == workflow_run_id)
                .order_by(ActivityExecution.sequence_number)
            )
            return list(result.scalars().all())

    # Event sourcing
    async def _record_event(
        self, session: AsyncSession, workflow_run_id: str, event_type: str, event_data: Dict[str, Any]
    ):
        """Record a workflow event for audit trail."""
        event = WorkflowEvent(
            workflow_run_id=workflow_run_id, event_type=event_type, event_data=event_data
        )
        session.add(event)

    async def get_workflow_events(
        self, workflow_run_id: str, limit: int = 100
    ) -> List[WorkflowEvent]:
        """Get workflow events for audit trail."""
        async with self.session() as session:
            result = await session.execute(
                select(WorkflowEvent)
                .where(WorkflowEvent.workflow_run_id == workflow_run_id)
                .order_by(WorkflowEvent.timestamp.desc())
                .limit(limit)
            )
            return list(result.scalars().all())

    # Scheduling
    async def create_scheduled_workflow(
        self,
        workflow_name: str,
        cron_expression: str,
        input_data: Optional[Dict[str, Any]] = None,
    ) -> ScheduledWorkflow:
        """Create a scheduled workflow."""
        async with self.session() as session:
            scheduled = ScheduledWorkflow(
                workflow_name=workflow_name, cron_expression=cron_expression, input_data=input_data
            )
            session.add(scheduled)
            return scheduled

    async def get_enabled_schedules(self) -> List[ScheduledWorkflow]:
        """Get all enabled scheduled workflows."""
        async with self.session() as session:
            result = await session.execute(
                select(ScheduledWorkflow).where(ScheduledWorkflow.enabled == "true")
            )
            return list(result.scalars().all())

    async def update_schedule_run_time(self, schedule_id: int, last_run: datetime, next_run: datetime):
        """Update scheduled workflow run times."""
        async with self.session() as session:
            await session.execute(
                update(ScheduledWorkflow)
                .where(ScheduledWorkflow.id == schedule_id)
                .values(last_run_at=last_run, next_run_at=next_run)
            )

    # Human-in-the-loop
    async def pause_for_approval(self, run_id: str, approval_key: str):
        """Pause workflow for human approval."""
        async with self.session() as session:
            await session.execute(
                update(WorkflowExecution)
                .where(WorkflowExecution.run_id == run_id)
                .values(status=WorkflowStatus.PAUSED, approval_required=approval_key)
            )
            await self._record_event(
                session, run_id, "workflow_paused_for_approval", {"approval_key": approval_key}
            )

    async def grant_approval(self, run_id: str, approved: bool):
        """Grant or deny workflow approval."""
        async with self.session() as session:
            approval_status = "approved" if approved else "rejected"
            await session.execute(
                update(WorkflowExecution)
                .where(WorkflowExecution.run_id == run_id)
                .values(
                    approval_granted=approval_status,
                    status=WorkflowStatus.RUNNING if approved else WorkflowStatus.FAILED,
                )
            )
            await self._record_event(
                session, run_id, "workflow_approval_granted", {"approved": approved}
            )

    async def get_pending_approvals(self) -> List[WorkflowExecution]:
        """Get workflows pending approval."""
        async with self.session() as session:
            result = await session.execute(
                select(WorkflowExecution).where(WorkflowExecution.status == WorkflowStatus.PAUSED)
            )
            return list(result.scalars().all())
