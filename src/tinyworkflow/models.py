"""
Database models for tinyworkflow state persistence.
"""

from datetime import datetime
from enum import Enum
from typing import Optional
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    ForeignKey,
    JSON,
    Index,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class WorkflowStatus(str, Enum):
    """Workflow execution status."""

    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"  # For human-in-the-loop
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class ActivityStatus(str, Enum):
    """Activity execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    SKIPPED = "skipped"


class WorkflowExecution(Base):
    """Represents a workflow execution instance."""

    __tablename__ = "workflow_executions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    workflow_id = Column(String(255), nullable=False, index=True)
    run_id = Column(String(255), nullable=False, unique=True, index=True)
    workflow_name = Column(String(255), nullable=False, index=True)
    status = Column(String(50), nullable=False, default=WorkflowStatus.PENDING)

    # Input/Output
    input_data = Column(JSON, nullable=True)
    output_data = Column(JSON, nullable=True)
    error = Column(Text, nullable=True)

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Retry tracking
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)

    # Scheduling
    scheduled_time = Column(DateTime, nullable=True)
    cron_expression = Column(String(255), nullable=True)

    # Human-in-the-loop
    approval_required = Column(String(255), nullable=True)
    approval_granted = Column(String(50), nullable=True)

    # Parent workflow (for sub-workflows)
    parent_run_id = Column(String(255), nullable=True, index=True)

    # Relationships
    activities = relationship(
        "ActivityExecution", back_populates="workflow", cascade="all, delete-orphan"
    )
    events = relationship(
        "WorkflowEvent", back_populates="workflow", cascade="all, delete-orphan"
    )

    # Indexes for common queries
    __table_args__ = (
        Index("idx_workflow_status_created", "status", "created_at"),
        Index("idx_workflow_name_status", "workflow_name", "status"),
    )


class ActivityExecution(Base):
    """Represents an activity execution within a workflow."""

    __tablename__ = "activity_executions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    workflow_run_id = Column(String(255), ForeignKey("workflow_executions.run_id"), nullable=False)
    activity_id = Column(String(255), nullable=False, index=True)
    activity_name = Column(String(255), nullable=False)
    status = Column(String(50), nullable=False, default=ActivityStatus.PENDING)

    # Input/Output
    input_data = Column(JSON, nullable=True)
    output_data = Column(JSON, nullable=True)
    error = Column(Text, nullable=True)

    # Execution tracking
    sequence_number = Column(Integer, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Retry tracking
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)

    # Relationship
    workflow = relationship("WorkflowExecution", back_populates="activities")

    __table_args__ = (Index("idx_activity_workflow_sequence", "workflow_run_id", "sequence_number"),)


class WorkflowEvent(Base):
    """Event sourcing - records all state transitions for audit trail."""

    __tablename__ = "workflow_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    workflow_run_id = Column(String(255), ForeignKey("workflow_executions.run_id"), nullable=False)
    event_type = Column(String(100), nullable=False)
    event_data = Column(JSON, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    # Relationship
    workflow = relationship("WorkflowExecution", back_populates="events")

    __table_args__ = (Index("idx_event_workflow_timestamp", "workflow_run_id", "timestamp"),)


class ScheduledWorkflow(Base):
    """Scheduled workflow definitions."""

    __tablename__ = "scheduled_workflows"

    id = Column(Integer, primary_key=True, autoincrement=True)
    workflow_name = Column(String(255), nullable=False)
    cron_expression = Column(String(255), nullable=False)
    input_data = Column(JSON, nullable=True)
    enabled = Column(String(10), default="true")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_run_at = Column(DateTime, nullable=True)
    next_run_at = Column(DateTime, nullable=True)

    __table_args__ = (Index("idx_scheduled_enabled_next", "enabled", "next_run_at"),)
