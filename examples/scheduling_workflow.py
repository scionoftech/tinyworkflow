"""
Scheduling workflow example demonstrating:
- Cron-based scheduling
- Delayed execution
- Scheduled job management
"""

import asyncio
from datetime import datetime
from tinyworkflow import workflow, activity, WorkflowContext, TinyWorkflowClient


# Define activities
@activity(name="backup_database")
async def backup_database():
    """Simulate database backup."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting database backup...")
    await asyncio.sleep(1)

    backup_id = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"Database backup completed: {backup_id}")

    return {
        "backup_id": backup_id,
        "timestamp": datetime.now().isoformat(),
        "size_mb": 125.4
    }


@activity(name="send_report")
async def send_report(period: str):
    """Generate and send report."""
    print(f"Generating {period} report...")
    await asyncio.sleep(0.5)

    report = {
        "period": period,
        "total_workflows": 152,
        "completed": 148,
        "failed": 4,
        "success_rate": "97.4%"
    }

    print(f"Report generated: {report}")
    return report


@activity(name="cleanup_old_data")
async def cleanup_old_data(days: int):
    """Clean up old workflow data."""
    print(f"Cleaning up data older than {days} days...")
    await asyncio.sleep(0.8)

    deleted_count = 47
    print(f"Cleanup completed: {deleted_count} records deleted")

    return {
        "deleted_count": deleted_count,
        "days_threshold": days
    }


@activity(name="health_check")
async def health_check():
    """Perform system health check."""
    print("Running health check...")
    await asyncio.sleep(0.3)

    health = {
        "status": "healthy",
        "database": "ok",
        "memory_usage": "45%",
        "disk_usage": "62%",
        "timestamp": datetime.now().isoformat()
    }

    print(f"Health check: {health['status']}")
    return health


@activity(name="send_notification")
async def send_notification(message: str):
    """Send notification."""
    print(f"Notification: {message}")
    await asyncio.sleep(0.2)
    return {"sent": True}


# Define workflows
@workflow(name="daily_backup")
async def daily_backup_workflow(ctx: WorkflowContext):
    """
    Daily backup workflow - typically scheduled with cron.
    Cron: "0 2 * * *" (runs at 2 AM daily)
    """
    print("\n=== Daily Backup Workflow ===")

    # Perform backup
    backup_result = await ctx.execute_activity(backup_database)

    # Send notification
    await ctx.execute_activity(
        send_notification,
        f"Daily backup completed: {backup_result['backup_id']}"
    )

    return {
        "status": "completed",
        "backup": backup_result
    }


@workflow(name="weekly_report")
async def weekly_report_workflow(ctx: WorkflowContext):
    """
    Weekly report workflow.
    Cron: "0 9 * * 1" (runs at 9 AM every Monday)
    """
    print("\n=== Weekly Report Workflow ===")

    # Generate report
    report = await ctx.execute_activity(send_report, "weekly")

    # Send notification
    await ctx.execute_activity(
        send_notification,
        f"Weekly report ready: {report['success_rate']} success rate"
    )

    return {
        "status": "completed",
        "report": report
    }


@workflow(name="hourly_health_check")
async def hourly_health_check_workflow(ctx: WorkflowContext):
    """
    Hourly health check workflow.
    Cron: "0 * * * *" (runs every hour)
    """
    print("\n=== Hourly Health Check ===")

    # Perform health check
    health = await ctx.execute_activity(health_check)

    # If unhealthy, send alert
    if health["status"] != "healthy":
        await ctx.execute_activity(
            send_notification,
            f"ALERT: System health issue detected!"
        )

    return {
        "status": "completed",
        "health": health
    }


@workflow(name="monthly_cleanup")
async def monthly_cleanup_workflow(ctx: WorkflowContext):
    """
    Monthly cleanup workflow.
    Cron: "0 3 1 * *" (runs at 3 AM on the 1st of every month)
    """
    print("\n=== Monthly Cleanup Workflow ===")

    # Clean up old data (90 days)
    cleanup_result = await ctx.execute_activity(cleanup_old_data, 90)

    # Send notification
    await ctx.execute_activity(
        send_notification,
        f"Monthly cleanup: {cleanup_result['deleted_count']} records removed"
    )

    return {
        "status": "completed",
        "cleanup": cleanup_result
    }


@workflow(name="delayed_task")
async def delayed_task_workflow(ctx: WorkflowContext):
    """
    Workflow for delayed execution.
    Example: Schedule a reminder or follow-up task.
    """
    task_name = ctx.get_input("task_name", "reminder")
    delay_message = ctx.get_input("message", "Time to follow up!")

    print(f"\n=== Delayed Task: {task_name} ===")

    # Execute the delayed task
    await ctx.execute_activity(send_notification, delay_message)

    return {
        "status": "completed",
        "task_name": task_name,
        "executed_at": datetime.now().isoformat()
    }


async def example_cron_scheduling():
    """Example: Schedule workflows with cron expressions."""
    print("\n" + "=" * 70)
    print(" Example 1: Cron-based Scheduling")
    print("=" * 70)

    async with TinyWorkflowClient(auto_start_worker=True) as client:
        print("\nScheduling workflows with cron expressions...")

        # Schedule daily backup at 2 AM
        job1 = await client.schedule_workflow(
            "daily_backup",
            cron_expression="0 2 * * *",
            input_data={}
        )
        print(f"  Daily backup scheduled (2 AM daily): {job1}")

        # Schedule weekly report on Mondays at 9 AM
        job2 = await client.schedule_workflow(
            "weekly_report",
            cron_expression="0 9 * * 1",
            input_data={}
        )
        print(f"  Weekly report scheduled (9 AM Monday): {job2}")

        # Schedule hourly health check
        job3 = await client.schedule_workflow(
            "hourly_health_check",
            cron_expression="0 * * * *",
            input_data={}
        )
        print(f"  Hourly health check scheduled: {job3}")

        # Schedule monthly cleanup on 1st at 3 AM
        job4 = await client.schedule_workflow(
            "monthly_cleanup",
            cron_expression="0 3 1 * *",
            input_data={}
        )
        print(f"  Monthly cleanup scheduled (1st @ 3 AM): {job4}")

        # Schedule every 5 minutes (for demo)
        job5 = await client.schedule_workflow(
            "hourly_health_check",
            cron_expression="*/5 * * * *",
            input_data={}
        )
        print(f"  Quick health check (every 5 min): {job5}")

        print("\n All scheduled jobs are now active!")
        print(" They will run automatically based on their cron expressions.")


async def example_delayed_execution():
    """Example: Schedule workflows for delayed execution."""
    print("\n\n" + "=" * 70)
    print(" Example 2: Delayed Execution")
    print("=" * 70)

    async with TinyWorkflowClient(auto_start_worker=True) as client:
        print("\nScheduling delayed workflows...")

        # Delay 10 seconds
        run_id1 = await client.schedule_delayed_workflow(
            "delayed_task",
            delay_seconds=10,
            input_data={
                "task_name": "follow_up_email",
                "message": "Time to send follow-up email!"
            }
        )
        print(f"  Task 1 scheduled (10 seconds): {run_id1}")

        # Delay 30 seconds
        run_id2 = await client.schedule_delayed_workflow(
            "delayed_task",
            delay_seconds=30,
            input_data={
                "task_name": "reminder",
                "message": "Reminder: Check on customer request"
            }
        )
        print(f"  Task 2 scheduled (30 seconds): {run_id2}")

        # Delay 60 seconds (1 minute)
        run_id3 = await client.schedule_delayed_workflow(
            "delayed_task",
            delay_seconds=60,
            input_data={
                "task_name": "backup_check",
                "message": "Verify backup completed successfully"
            }
        )
        print(f"  Task 3 scheduled (60 seconds): {run_id3}")

        print("\n Delayed workflows will execute after their delay period.")
        print(" Worker will automatically pick them up when scheduled time arrives.")


async def example_immediate_execution():
    """Example: Execute scheduled workflows immediately for testing."""
    print("\n\n" + "=" * 70)
    print(" Example 3: Immediate Execution (Testing Scheduled Workflows)")
    print("=" * 70)

    async with TinyWorkflowClient(auto_start_worker=True) as client:
        print("\nExecuting scheduled workflows immediately for demo...\n")

        # Execute daily backup
        run_id = await client.start_workflow("daily_backup", wait=False)
        print(f"Daily backup started: {run_id}")

        await asyncio.sleep(2)

        # Execute weekly report
        run_id = await client.start_workflow("weekly_report", wait=False)
        print(f"Weekly report started: {run_id}")

        await asyncio.sleep(1)

        # Execute health check
        run_id = await client.start_workflow("hourly_health_check", wait=False)
        print(f"Health check started: {run_id}")

        await asyncio.sleep(1)

        # Execute cleanup
        run_id = await client.start_workflow("monthly_cleanup", wait=False)
        print(f"Monthly cleanup started: {run_id}")

        # Wait for all to complete
        print("\nWaiting for workflows to complete...")
        await asyncio.sleep(3)

        print("\n All workflows completed!")


async def main():
    """Run scheduling examples."""
    print("\n" + "=" * 70)
    print(" TinyWorkflow Scheduling Examples")
    print("=" * 70)

    # Example 1: Cron scheduling
    await example_cron_scheduling()

    # Example 2: Delayed execution
    await example_delayed_execution()

    # Example 3: Immediate execution (for testing)
    await example_immediate_execution()

    print("\n" + "=" * 70)
    print(" Scheduling Examples Summary")
    print("=" * 70)
    print("\n Cron Expression Examples:")
    print("   '0 2 * * *'      - Every day at 2:00 AM")
    print("   '0 9 * * 1'      - Every Monday at 9:00 AM")
    print("   '0 * * * *'      - Every hour at minute 0")
    print("   '*/5 * * * *'    - Every 5 minutes")
    print("   '0 3 1 * *'      - 1st of every month at 3:00 AM")
    print("   '0 0 * * 0'      - Every Sunday at midnight")
    print("   '30 14 * * 1-5'  - Every weekday at 2:30 PM")
    print("\n Use Cases:")
    print("   Daily backups")
    print("   Weekly/monthly reports")
    print("   Periodic health checks")
    print("   Data cleanup jobs")
    print("   Scheduled notifications")
    print("   Delayed reminders/follow-ups")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
