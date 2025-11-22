"""
AI Content Pipeline Example
Demonstrates using TinyWorkflow for AI/ML workloads with:
- Text generation with OpenAI/local models
- Sentiment analysis
- Content moderation
- Parallel processing
- Retry policies for API calls
"""

import asyncio
import json
from typing import Dict, Any
from tinyworkflow import workflow, activity, WorkflowContext, TinyWorkflowClient, RetryPolicy


# Simulated AI services (replace with real API calls)
class AIServices:
    """Simulated AI services - replace with actual API calls."""

    @staticmethod
    async def generate_text(prompt: str, max_tokens: int = 100) -> str:
        """Simulate text generation (replace with OpenAI, Anthropic, etc.)"""
        await asyncio.sleep(0.5)  # Simulate API latency
        return f"Generated content based on: {prompt[:50]}... (simulated response)"

    @staticmethod
    async def analyze_sentiment(text: str) -> Dict[str, Any]:
        """Simulate sentiment analysis."""
        await asyncio.sleep(0.3)
        # Simulate sentiment detection
        return {
            "sentiment": "positive",
            "confidence": 0.85,
            "scores": {"positive": 0.85, "negative": 0.10, "neutral": 0.05},
        }

    @staticmethod
    async def moderate_content(text: str) -> Dict[str, Any]:
        """Simulate content moderation."""
        await asyncio.sleep(0.2)
        return {
            "is_safe": True,
            "categories": {
                "hate_speech": 0.01,
                "violence": 0.02,
                "adult": 0.03,
                "spam": 0.01,
            },
            "flagged": False,
        }

    @staticmethod
    async def extract_keywords(text: str) -> list:
        """Simulate keyword extraction."""
        await asyncio.sleep(0.2)
        return ["AI", "workflow", "automation", "content", "pipeline"]

    @staticmethod
    async def translate_text(text: str, target_lang: str) -> str:
        """Simulate translation."""
        await asyncio.sleep(0.4)
        return f"[{target_lang.upper()}] {text[:30]}... (translated)"

    @staticmethod
    async def summarize_text(text: str, max_length: int = 50) -> str:
        """Simulate text summarization."""
        await asyncio.sleep(0.3)
        return f"Summary: {text[:max_length]}..."


# Define activities with retry policies for API calls
@activity(
    name="generate_content",
    retry_policy=RetryPolicy(max_retries=3, initial_delay=1.0, backoff_multiplier=2.0),
)
async def generate_content(prompt: str, max_tokens: int = 200) -> Dict[str, Any]:
    """Generate AI content with retry logic for API failures."""
    try:
        content = await AIServices.generate_text(prompt, max_tokens)
        return {
            "success": True,
            "content": content,
            "tokens_used": len(content.split()),
            "prompt": prompt,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@activity(name="analyze_sentiment")
async def analyze_sentiment(content: str) -> Dict[str, Any]:
    """Analyze sentiment of generated content."""
    return await AIServices.analyze_sentiment(content)


@activity(name="moderate_content")
async def moderate_content(content: str) -> Dict[str, Any]:
    """Check content for safety and moderation."""
    return await AIServices.moderate_content(content)


@activity(name="extract_keywords")
async def extract_keywords(content: str) -> Dict[str, Any]:
    """Extract keywords from content."""
    keywords = await AIServices.extract_keywords(content)
    return {"keywords": keywords, "count": len(keywords)}


@activity(name="translate_content")
async def translate_content(content: str, target_language: str) -> Dict[str, Any]:
    """Translate content to target language."""
    translated = await AIServices.translate_text(content, target_language)
    return {"translated": translated, "language": target_language}


@activity(name="summarize_content")
async def summarize_content(content: str, max_length: int = 100) -> Dict[str, Any]:
    """Create a summary of the content."""
    summary = await AIServices.summarize_text(content, max_length)
    return {"summary": summary, "original_length": len(content)}


@activity(name="save_to_database")
async def save_to_database(data: Dict[str, Any]) -> Dict[str, Any]:
    """Simulate saving results to database."""
    await asyncio.sleep(0.2)
    return {"saved": True, "id": "content_12345", "timestamp": "2025-01-20T12:00:00Z"}


# Workflow 1: Simple AI Content Generation Pipeline
@workflow(name="simple_ai_pipeline")
async def simple_ai_pipeline(ctx: WorkflowContext):
    """
    Simple AI content generation workflow:
    1. Generate content
    2. Analyze sentiment
    3. Moderate content
    4. Save results
    """
    prompt = ctx.get_input("prompt", "Write a blog post about AI workflows")

    print(f"[START] AI pipeline for prompt: {prompt}")

    # Step 1: Generate content
    print("[1/4] Generating content...")
    generation_result = await ctx.execute_activity(generate_content, prompt)

    if not generation_result["success"]:
        return {"error": "Content generation failed", "details": generation_result}

    content = generation_result["content"]
    print(f"[DONE] Content generated: {content[:100]}...")

    # Step 2: Analyze sentiment
    print("[2/4] Analyzing sentiment...")
    sentiment = await ctx.execute_activity(analyze_sentiment, content)
    print(f"[DONE] Sentiment: {sentiment['sentiment']} (confidence: {sentiment['confidence']})")

    # Step 3: Moderate content
    print("[3/4] Moderating content...")
    moderation = await ctx.execute_activity(moderate_content, content)

    if moderation["flagged"]:
        print("[WARN] Content flagged by moderation")
        return {
            "status": "rejected",
            "reason": "Content moderation failed",
            "moderation": moderation,
        }

    print("[DONE] Content passed moderation")

    # Step 4: Save results
    print("[4/4] Saving to database...")
    save_result = await ctx.execute_activity(
        save_to_database,
        {
            "content": content,
            "sentiment": sentiment,
            "moderation": moderation,
            "prompt": prompt,
        },
    )

    print("[COMPLETE] Pipeline completed!")

    return {
        "status": "completed",
        "content": content,
        "sentiment": sentiment,
        "moderation": moderation,
        "database_id": save_result["id"],
    }


# Workflow 2: Advanced AI Pipeline with Parallel Processing
@workflow(name="advanced_ai_pipeline")
async def advanced_ai_pipeline(ctx: WorkflowContext):
    """
    Advanced AI pipeline with parallel processing:
    1. Generate content
    2. Run sentiment, moderation, and keyword extraction in parallel
    3. Translate to multiple languages in parallel
    4. Summarize
    5. Save all results
    """
    prompt = ctx.get_input("prompt", "Write about artificial intelligence")
    languages = ctx.get_input("languages", ["es", "fr", "de"])

    print(f"[START] Starting advanced AI pipeline")

    # Step 1: Generate content
    print(" Generating content...")
    generation_result = await ctx.execute_activity(generate_content, prompt, 300)

    if not generation_result["success"]:
        return {"error": "Content generation failed"}

    content = generation_result["content"]
    print(f" Content generated")

    # Step 2: Parallel analysis (sentiment, moderation, keywords)
    print("[PARALLEL] Running parallel analysis...")
    sentiment, moderation, keywords = await ctx.execute_parallel(
        (analyze_sentiment, (content,), {}),
        (moderate_content, (content,), {}),
        (extract_keywords, (content,), {}),
    )
    print(f" Analysis complete - Keywords: {keywords['keywords']}")

    if moderation["flagged"]:
        return {"status": "rejected", "reason": "Content moderation failed"}

    # Step 3: Parallel translations
    print(f" Translating to {len(languages)} languages...")
    translation_tasks = [(translate_content, (content, lang), {}) for lang in languages]
    translations = await ctx.execute_parallel(*translation_tasks)
    print(f" Translations complete")

    # Step 4: Generate summary
    print(" Generating summary...")
    summary = await ctx.execute_activity(summarize_content, content, 150)
    print(f" Summary created")

    # Step 5: Save everything
    print(" Saving results...")
    final_data = {
        "original_content": content,
        "sentiment": sentiment,
        "moderation": moderation,
        "keywords": keywords,
        "translations": translations,
        "summary": summary,
        "prompt": prompt,
    }
    save_result = await ctx.execute_activity(save_to_database, final_data)

    print(" Advanced pipeline completed!")

    return {
        "status": "completed",
        "content": content,
        "sentiment": sentiment["sentiment"],
        "keywords": keywords["keywords"],
        "translations_count": len(translations),
        "summary": summary["summary"],
        "database_id": save_result["id"],
    }


# Workflow 3: Multi-step Content Creation with Human Approval
@workflow(name="ai_content_with_approval")
async def ai_content_with_approval(ctx: WorkflowContext):
    """
    AI content workflow with human-in-the-loop approval:
    1. Generate content
    2. Analyze and moderate
    3. Wait for human approval if needed
    4. Finalize and save
    """
    prompt = ctx.get_input("prompt", "Write marketing copy")
    require_approval = ctx.get_input("require_approval", True)

    print(f"[START] Starting AI content with approval workflow")

    # Generate and analyze
    generation_result = await ctx.execute_activity(generate_content, prompt)
    content = generation_result["content"]

    sentiment, moderation = await ctx.execute_parallel(
        (analyze_sentiment, (content,), {}), (moderate_content, (content,), {})
    )

    # Check if approval is needed
    if require_approval and sentiment["sentiment"] != "positive":
        print(" Content requires human approval (non-positive sentiment)")

        # In a real scenario, this would pause and wait for approval
        # For demo, we'll just flag it
        return {
            "status": "pending_approval",
            "content": content,
            "sentiment": sentiment,
            "message": "Content requires manual review",
        }

    # Save approved content
    save_result = await ctx.execute_activity(
        save_to_database, {"content": content, "sentiment": sentiment, "approved": True}
    )

    print(" Content approved and saved!")

    return {
        "status": "completed",
        "content": content,
        "sentiment": sentiment,
        "database_id": save_result["id"],
    }


async def main():
    """Run example AI workflows."""
    print("=" * 70)
    print("AI WORKFLOW: TinyWorkflow AI Content Pipeline Examples")
    print("=" * 70)

    async with TinyWorkflowClient() as client:
        # Example 1: Simple AI Pipeline
        print("\n Example 1: Simple AI Content Generation Pipeline")
        print("-" * 70)
        run_id = await client.start_workflow(
            "simple_ai_pipeline",
            input_data={"prompt": "Write a blog post about workflow automation in AI"},
            wait=True,
        )

        result = await client.get_workflow_status(run_id)
        print(f"\n Workflow completed!")
        print(f"Status: {result.status}")
        print(f"Result: {json.dumps(result.output_data, indent=2)}")

        # Example 2: Advanced Pipeline with Parallel Processing
        print("\n\n Example 2: Advanced AI Pipeline (Parallel Processing)")
        print("-" * 70)
        run_id = await client.start_workflow(
            "advanced_ai_pipeline",
            input_data={
                "prompt": "Explain the benefits of durable workflows",
                "languages": ["es", "fr", "de", "ja"],
            },
            wait=True,
        )

        result = await client.get_workflow_status(run_id)
        print(f"\n Workflow completed!")
        print(f"Status: {result.status}")
        if result.output_data:
            output = result.output_data.get("result", {})
            print(f"Sentiment: {output.get('sentiment')}")
            print(f"Keywords: {output.get('keywords')}")
            print(f"Translations: {output.get('translations_count')} languages")

        # Example 3: Content with Approval
        print("\n\n Example 3: AI Content with Human Approval")
        print("-" * 70)
        run_id = await client.start_workflow(
            "ai_content_with_approval",
            input_data={
                "prompt": "Create a social media post about AI",
                "require_approval": True,
            },
            wait=True,
        )

        result = await client.get_workflow_status(run_id)
        print(f"\n Workflow completed!")
        print(f"Status: {result.status}")

        # Show workflow events (audit trail)
        print("\n\n Workflow Event History (Audit Trail):")
        print("-" * 70)
        events = await client.get_workflow_events(run_id, limit=10)
        for event in events[:5]:
            print(f"   {event.event_type} at {event.timestamp}")

    print("\n" + "=" * 70)
    print(" All AI workflow examples completed!")
    print("=" * 70)
    print("\n Key Features Demonstrated:")
    print("   AI/ML task orchestration")
    print("   Retry policies for API failures")
    print("   Parallel execution for performance")
    print("   Content moderation and safety checks")
    print("   State persistence and recovery")
    print("   Event sourcing for audit trails")
    print("   Human-in-the-loop workflows")
    print("\n Replace AIServices with real API calls (OpenAI, Anthropic, etc.)")
    print("   to use in production!")


if __name__ == "__main__":
    asyncio.run(main())
