"""
Approval workflow example demonstrating human-in-the-loop.
"""

import asyncio
from tinyworkflow import workflow, activity, WorkflowContext, TinyWorkflowClient


# Define activities
@activity(name="create_expense_claim")
async def create_expense_claim(amount: float, description: str):
    """Create an expense claim."""
    print(f"Creating expense claim: ${amount} - {description}")
    await asyncio.sleep(0.5)
    return {"claim_id": "claim_123", "amount": amount, "description": description}


@activity(name="notify_manager")
async def notify_manager(claim: dict):
    """Notify manager about pending approval."""
    print(f"Notifying manager about claim {claim['claim_id']}...")
    await asyncio.sleep(0.3)
    return {"notified": True}


@activity(name="process_payment")
async def process_payment(claim: dict):
    """Process the approved payment."""
    print(f"Processing payment for claim {claim['claim_id']}: ${claim['amount']}")
    await asyncio.sleep(1)
    return {"payment_id": "pay_456", "status": "processed"}


@activity(name="send_confirmation")
async def send_confirmation(claim: dict, payment: dict):
    """Send confirmation to employee."""
    print(f"Sending confirmation for claim {claim['claim_id']}")
    await asyncio.sleep(0.3)
    return {"confirmed": True}


# Define approval workflow
@workflow(name="expense_approval")
async def expense_approval_workflow(ctx: WorkflowContext):
    """
    Expense approval workflow with human-in-the-loop.
    """
    amount = ctx.get_input("amount", 0)
    description = ctx.get_input("description", "")

    # Create expense claim
    claim = await ctx.execute_activity(create_expense_claim, amount, description)

    # If amount is over $1000, require manager approval
    if amount > 1000:
        print(f"\nExpense requires manager approval (amount: ${amount})")

        # Notify manager
        await ctx.execute_activity(notify_manager, claim)

        # Wait for approval
        print("Waiting for manager approval...")
        print("(In a real system, manager would approve via UI)")
        print(f"To approve: tinyworkflow approve {ctx.run_id} --approve")
        print(f"To reject: tinyworkflow approve {ctx.run_id} --reject")

        # This will pause the workflow until approval is granted
        approved = await ctx.wait_for_approval(f"manager_approval_{claim['claim_id']}", timeout=300)

        if not approved:
            print("Expense claim was rejected")
            return {"status": "rejected", "claim_id": claim["claim_id"]}

        print("Expense claim approved!")

    # Process payment
    payment = await ctx.execute_activity(process_payment, claim)

    # Send confirmation
    await ctx.execute_activity(send_confirmation, claim, payment)

    return {
        "status": "completed",
        "claim_id": claim["claim_id"],
        "payment_id": payment["payment_id"],
    }


async def example_with_auto_approval():
    """Example that automatically approves small expenses."""
    async with TinyWorkflowClient() as client:
        print("Example 1: Small expense (auto-approved)")
        print("=" * 50)

        run_id = await client.start_workflow(
            "expense_approval",
            input_data={"amount": 500, "description": "Team lunch"},
            wait=True,
        )

        workflow = await client.get_workflow_status(run_id)
        print(f"Result: {workflow.output_data}\n")


async def example_with_manual_approval():
    """Example that requires manual approval."""
    async with TinyWorkflowClient(auto_start_worker=True) as client:
        print("\nExample 2: Large expense (requires approval)")
        print("=" * 50)

        # Start workflow (don't wait)
        run_id = await client.start_workflow(
            "expense_approval",
            input_data={"amount": 2500, "description": "New laptop"},
            wait=False,
        )

        print(f"Workflow started: {run_id}")
        print("Workflow will pause for approval...")

        # Wait a bit for workflow to reach approval checkpoint
        await asyncio.sleep(2)

        # Check status
        workflow = await client.get_workflow_status(run_id)
        print(f"Current status: {workflow.status}")

        # Simulate manager approval
        print("\nSimulating manager approval...")
        # await client.approve_workflow(run_id)

        # Wait for workflow to complete
        await asyncio.sleep(3)

        # Check final status
        workflow = await client.get_workflow_status(run_id)
        print(f"Final status: {workflow.status}")
        print(f"Result: {workflow.output_data}")


async def main():
    """Run approval workflow examples."""
    # await example_with_auto_approval()
    await example_with_manual_approval()


if __name__ == "__main__":
    asyncio.run(main())
