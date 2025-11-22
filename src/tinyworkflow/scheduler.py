"""
Scheduler for cron-like and delayed job execution using APScheduler.
"""

import asyncio
from typing import Optional, Dict, Any, Callable
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from tinyworkflow.state import StateManager
from tinyworkflow.workflow import WorkflowEngine, get_workflow


class WorkflowScheduler:
    """Schedules and executes workflows based on cron expressions or delays."""

    def __init__(self, state_manager: StateManager, workflow_engine: WorkflowEngine):
        """
        Initialize the scheduler.

        Args:
            state_manager: State manager for persistence
            workflow_engine: Workflow engine for execution
        """
        self.state_manager = state_manager
        self.workflow_engine = workflow_engine
        self.scheduler = AsyncIOScheduler()
        self._running = False

    async def start(self):
        """Start the scheduler."""
        if not self._running:
            # Load scheduled workflows from database
            await self._load_schedules()
            self.scheduler.start()
            self._running = True

    async def stop(self):
        """Stop the scheduler."""
        if self._running:
            self.scheduler.shutdown(wait=True)
            self._running = False

    async def _load_schedules(self):
        """Load scheduled workflows from database."""
        schedules = await self.state_manager.get_enabled_schedules()
        for schedule in schedules:
            await self.add_cron_workflow(
                workflow_name=schedule.workflow_name,
                cron_expression=schedule.cron_expression,
                input_data=schedule.input_data,
                schedule_id=schedule.id,
            )

    async def add_cron_workflow(
        self,
        workflow_name: str,
        cron_expression: str,
        input_data: Optional[Dict[str, Any]] = None,
        schedule_id: Optional[int] = None,
    ) -> str:
        """
        Schedule a workflow to run on a cron schedule.

        Args:
            workflow_name: Name of the workflow to execute
            cron_expression: Cron expression (e.g., "0 9 * * *" for daily at 9am)
            input_data: Input data for the workflow
            schedule_id: Optional schedule ID from database

        Returns:
            Job ID

        Example:
            # Run daily at 9am
            await scheduler.add_cron_workflow("daily_report", "0 9 * * *")

            # Run every 5 minutes
            await scheduler.add_cron_workflow("health_check", "*/5 * * * *")
        """
        # Verify workflow exists
        if not get_workflow(workflow_name):
            raise ValueError(f"Workflow '{workflow_name}' not found")

        # Create schedule in database if not exists
        if schedule_id is None:
            schedule = await self.state_manager.create_scheduled_workflow(
                workflow_name=workflow_name,
                cron_expression=cron_expression,
                input_data=input_data,
            )
            schedule_id = schedule.id

        # Create cron trigger
        trigger = CronTrigger.from_crontab(cron_expression)

        # Add job to scheduler
        job = self.scheduler.add_job(
            self._execute_scheduled_workflow,
            trigger=trigger,
            args=[workflow_name, input_data, schedule_id],
            id=f"cron_{schedule_id}",
            replace_existing=True,
        )

        return job.id

    async def add_delayed_workflow(
        self,
        workflow_name: str,
        delay_seconds: float,
        input_data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Schedule a workflow to run after a delay.

        Args:
            workflow_name: Name of the workflow to execute
            delay_seconds: Delay in seconds before execution
            input_data: Input data for the workflow

        Returns:
            Job ID

        Example:
            # Run after 5 minutes
            await scheduler.add_delayed_workflow("cleanup", delay_seconds=300)
        """
        # Verify workflow exists
        if not get_workflow(workflow_name):
            raise ValueError(f"Workflow '{workflow_name}' not found")

        # Calculate run time
        run_time = datetime.utcnow() + timedelta(seconds=delay_seconds)

        # Create date trigger
        trigger = DateTrigger(run_date=run_time)

        # Add job to scheduler
        job = self.scheduler.add_job(
            self._execute_scheduled_workflow,
            trigger=trigger,
            args=[workflow_name, input_data, None],
            id=f"delayed_{workflow_name}_{run_time.timestamp()}",
            replace_existing=False,
        )

        return job.id

    async def add_interval_workflow(
        self,
        workflow_name: str,
        interval_seconds: float,
        input_data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Schedule a workflow to run at regular intervals.

        Args:
            workflow_name: Name of the workflow to execute
            interval_seconds: Interval in seconds between executions
            input_data: Input data for the workflow

        Returns:
            Job ID

        Example:
            # Run every 30 seconds
            await scheduler.add_interval_workflow("monitor", interval_seconds=30)
        """
        # Verify workflow exists
        if not get_workflow(workflow_name):
            raise ValueError(f"Workflow '{workflow_name}' not found")

        # Create interval trigger
        trigger = IntervalTrigger(seconds=interval_seconds)

        # Add job to scheduler
        job = self.scheduler.add_job(
            self._execute_scheduled_workflow,
            trigger=trigger,
            args=[workflow_name, input_data, None],
            id=f"interval_{workflow_name}",
            replace_existing=True,
        )

        return job.id

    async def _execute_scheduled_workflow(
        self,
        workflow_name: str,
        input_data: Optional[Dict[str, Any]],
        schedule_id: Optional[int],
    ):
        """Execute a scheduled workflow."""
        try:
            # Execute workflow
            await self.workflow_engine.execute_workflow(
                workflow_name=workflow_name, input_data=input_data
            )

            # Update schedule run time if it's a recurring schedule
            if schedule_id:
                next_run = self.scheduler.get_job(f"cron_{schedule_id}").next_run_time
                await self.state_manager.update_schedule_run_time(
                    schedule_id=schedule_id,
                    last_run=datetime.utcnow(),
                    next_run=next_run if next_run else datetime.utcnow(),
                )

        except Exception as e:
            # Log error (in production, use proper logging)
            print(f"Error executing scheduled workflow '{workflow_name}': {e}")

    async def remove_job(self, job_id: str):
        """Remove a scheduled job."""
        self.scheduler.remove_job(job_id)

    def get_jobs(self) -> list:
        """Get all scheduled jobs."""
        return self.scheduler.get_jobs()

    def get_job(self, job_id: str):
        """Get a specific job by ID."""
        return self.scheduler.get_job(job_id)
