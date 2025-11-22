# TinyWorkflow Limitations

## Overview

TinyWorkflow is designed as a **learning tool and lightweight workflow library**, not a production-grade durable workflow orchestration system. This document explains what it does and doesn't provide.

## What TinyWorkflow IS

✅ **A workflow library for:**
- Learning workflow orchestration concepts
- Prototyping and experimentation
- Small internal tools and scripts
- AI/LLM experimentation
- Educational projects
- Quick demos and POCs

## What TinyWorkflow IS NOT

❌ **Not a production durable workflow system**

TinyWorkflow lacks critical features that define true durable workflow orchestration systems like Temporal, Azure Durable Functions, or DBOS.

---

## Critical Limitations

### 1. No Workflow Replay ❌

**What this means:**
When a workflow fails and retries, it re-executes ALL activities from scratch, including ones that already completed successfully.

**Example:**
```python
@workflow(name="example")
async def my_workflow(ctx):
    result1 = await ctx.execute_activity(step1)  # Succeeds
    result2 = await ctx.execute_activity(step2)  # Succeeds
    result3 = await ctx.execute_activity(step3)  # FAILS!

    # On retry, step1 and step2 run AGAIN (wasteful, potentially dangerous)
```

**Production systems (Temporal/DBOS):**
- Skip completed activities
- Resume from failure point
- Only re-execute failed activities

**Impact:**
- Wasteful resource usage
- Risk of duplicate side effects (charging customer twice, etc.)
- Not suitable for long-running workflows

---

### 2. No Deterministic Execution ❌

**What this means:**
Workflows can use non-deterministic operations that would break replay in production systems.

**Example - This is ALLOWED but DANGEROUS:**
```python
@workflow(name="bad_example")
async def non_deterministic_workflow(ctx):
    # These would break replay in Temporal!
    user_id = str(uuid.uuid4())  # Different on each execution
    timestamp = datetime.now()    # Different on each execution
    random_num = random.randint(1, 100)  # Different on each execution

    return await ctx.execute_activity(save_user, user_id, timestamp)
```

**Production systems:**
- Enforce deterministic code
- Provide special APIs for non-deterministic operations (`workflow.uuid()`, `workflow.now()`)
- Ensure replay produces identical results

**Impact:**
- Cannot guarantee consistent results
- Replay would produce different outcomes
- Not suitable for financial transactions or critical processes

---

### 3. No Durable Timers ❌

**What this means:**
Using `asyncio.sleep()` in workflows loses timer state if the process crashes.

**Example - Timer is NOT persisted:**
```python
@workflow(name="timer_example")
async def workflow_with_timer(ctx):
    await ctx.execute_activity(send_email)

    # If process crashes during this sleep, timer is LOST
    await asyncio.sleep(3600)  # Sleep for 1 hour - NOT DURABLE!

    await ctx.execute_activity(send_reminder)
```

**Production systems:**
- Timers stored in database
- Survive process restarts
- Can wait for days/weeks reliably

**Impact:**
- Cannot reliably wait long periods
- Not suitable for delayed notifications, scheduled tasks, or long polling

---

### 4. No Signal/Event System ❌

**What this means:**
Cannot send external events to running workflows. Current `wait_for_approval()` uses inefficient database polling.

**What's missing:**
```python
# This API doesn't exist:
@workflow(name="waiting")
async def workflow_waiting_for_signal(ctx):
    # Wait for external event - NOT IMPLEMENTED
    approval_data = await ctx.wait_for_signal("approval_signal")
    return approval_data

# Client cannot do this:
client.send_signal(run_id, "approval_signal", {"approved": True})
```

**Production systems:**
- Event-driven signal delivery
- Efficient external event handling
- No polling overhead

**Impact:**
- Inefficient polling instead of event-driven
- Cannot integrate with webhooks efficiently
- Higher database load

---

### 5. No Saga/Compensation Pattern ❌

**What this means:**
No automatic rollback or compensation on workflow failure.

**Example - Manual rollback required:**
```python
@workflow(name="booking")
async def booking_workflow(ctx):
    hotel = await ctx.execute_activity(book_hotel)
    flight = await ctx.execute_activity(book_flight)

    try:
        payment = await ctx.execute_activity(charge_customer)
    except Exception:
        # Must manually cancel everything!
        await ctx.execute_activity(cancel_flight)
        await ctx.execute_activity(cancel_hotel)
        raise
```

**Production systems:**
- Automatic compensation on failure
- Saga pattern support
- Rollback in reverse order

**Impact:**
- Manual error handling required
- Risk of inconsistent state
- Not suitable for distributed transactions

---

### 6. No Workflow Versioning ❌

**What this means:**
Changing workflow code can break in-flight executions.

**Example - Breaking change:**
```python
# Version 1 - Deployed with 100 running workflows
@workflow(name="my_workflow")
async def my_workflow_v1(ctx):
    step1 = await ctx.execute_activity(activity_a)
    step2 = await ctx.execute_activity(activity_b)
    return {"result": step1 + step2}

# Version 2 - Changing the workflow breaks in-flight executions!
@workflow(name="my_workflow")
async def my_workflow_v2(ctx):
    # Added new step - breaks existing workflows
    step0 = await ctx.execute_activity(activity_new)
    step1 = await ctx.execute_activity(activity_a)
    step2 = await ctx.execute_activity(activity_b)
    return {"result": step0 + step1 + step2}
```

**Production systems:**
- Version tracking
- Multiple versions coexist
- Gradual migration

**Impact:**
- Cannot safely update workflow code
- Must wait for all workflows to complete before deploying
- Risky deployments

---

## Additional Limitations

### 7. Basic Retry Only

**Current behavior:**
- Retries entire workflow from scratch
- Fixed retry policy per workflow
- No intelligent recovery

**Production systems:**
- Activity-level retries
- Workflow-level retries
- Configurable per-activity
- Intelligent backoff

### 8. No Continue-As-New

**What's missing:**
Cannot reset workflow state for very long-running workflows (weeks/months).

**Production systems:**
- `continue_as_new()` API
- Prevents unbounded history growth
- Essential for infinite loops

### 9. No Child Workflow Execution

**What's missing:**
Cannot spawn sub-workflows and wait for results.

**Database schema exists** (`parent_run_id` field) but no API implemented.

### 10. No Activity Idempotency Checks

**Current behavior:**
Activities re-execute even if previously completed.

**Production systems:**
- Check activity history
- Skip completed activities
- Return cached results

---

## What TinyWorkflow DOES Provide

Despite these limitations, TinyWorkflow is useful for learning and lightweight use cases:

### ✅ State Persistence
- Workflows and activities stored in database
- SQLite/PostgreSQL/MySQL support
- Query workflow status and history

### ✅ Retry Policies
- Exponential backoff with jitter
- Configurable max retries
- Per-workflow and per-activity

### ✅ Parallel Execution
- `execute_parallel()` for fan-out/fan-in
- Uses `asyncio.gather()`
- Works well for concurrent tasks

### ✅ Event Sourcing (Audit Trail)
- All state transitions logged
- Queryable event history
- Good for observability (not replay)

### ✅ Scheduling
- Cron expressions
- Delayed execution
- Interval-based triggers

### ✅ Human-in-the-Loop (Basic)
- Approval workflows
- Database polling mechanism
- Basic pause/resume

### ✅ Web UI
- Workflow monitoring
- Start/cancel workflows
- View execution history

---

## Comparison: When to Use What

### Use TinyWorkflow When:
- ✅ Learning workflow concepts
- ✅ Prototyping ideas quickly
- ✅ Simple automation (< 1 hour)
- ✅ Educational projects
- ✅ AI experimentation
- ✅ Zero infrastructure is important

### Use Temporal When:
- 🎯 Critical business processes
- 🎯 Long-running workflows (days/weeks)
- 🎯 Need guaranteed execution
- 🎯 Complex orchestrations
- 🎯 Production scale

### Use Azure Durable Functions When:
- 🎯 Already on Azure
- 🎯 Serverless workflows
- 🎯 Event-driven architectures
- 🎯 Production reliability

### Use DBOS When:
- 🎯 Database-centric applications
- 🎯 Transactional workflows
- 🎯 Want database as source of truth
- 🎯 Production guarantees

### Use Airflow When:
- 🎯 Batch data pipelines
- 🎯 ETL workflows
- 🎯 Data engineering tasks
- 🎯 Scheduled jobs at scale

---

## Migration Path

**Learning progression:**

1. **Start with TinyWorkflow** - Learn concepts, build prototypes
2. **Understand limitations** - Know when you need more
3. **Graduate to production** - Move to Temporal/DBOS when ready

TinyWorkflow is intentionally simple to help you understand workflow orchestration without overwhelming complexity. When you need production features, you'll better appreciate what systems like Temporal provide.

---

## Summary

**TinyWorkflow is honest about what it is:**
- ✅ A learning tool
- ✅ A prototyping library
- ✅ A lightweight orchestrator
- ❌ NOT a production durable workflow system

**For production workflows, use:**
- [Temporal](https://temporal.io/)
- [Azure Durable Functions](https://learn.microsoft.com/en-us/azure/azure-functions/durable/)
- [DBOS](https://www.dbos.dev/)
- [Prefect](https://www.prefect.io/)
- [Airflow](https://airflow.apache.org/)

---

*This honest assessment helps users make informed decisions and sets appropriate expectations.*
