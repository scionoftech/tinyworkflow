"""
AI workflow example demonstrating a multi-step AI pipeline.
"""

import asyncio
import random
from tinyworkflow import workflow, activity, WorkflowContext, TinyWorkflowClient, RetryPolicy


# Simulated AI activities
@activity(name="preprocess_data", retry_policy=RetryPolicy(max_retries=3))
async def preprocess_data(raw_data: str):
    """Preprocess raw text data."""
    print(f"Preprocessing data: {raw_data[:50]}...")
    await asyncio.sleep(1)

    # Simulate preprocessing
    tokens = raw_data.lower().split()
    return {
        "tokens": tokens,
        "token_count": len(tokens),
        "preprocessed": True,
    }


@activity(name="generate_embeddings")
async def generate_embeddings(tokens: list):
    """Generate embeddings for tokens."""
    print(f"Generating embeddings for {len(tokens)} tokens...")
    await asyncio.sleep(2)

    # Simulate embedding generation
    embeddings = [[random.random() for _ in range(384)] for _ in tokens[:10]]
    return {"embeddings": embeddings, "dimension": 384}


@activity(name="run_inference")
async def run_inference(embeddings: dict):
    """Run AI model inference."""
    print("Running model inference...")
    await asyncio.sleep(2)

    # Simulate inference
    predictions = [
        {"label": "positive", "confidence": 0.87},
        {"label": "neutral", "confidence": 0.45},
        {"label": "negative", "confidence": 0.23},
    ]
    return {"predictions": predictions, "model": "sentiment-analyzer-v1"}


@activity(name="post_process_results")
async def post_process_results(predictions: dict):
    """Post-process model predictions."""
    print("Post-processing results...")
    await asyncio.sleep(0.5)

    # Get top prediction
    top_prediction = max(predictions["predictions"], key=lambda x: x["confidence"])

    return {
        "sentiment": top_prediction["label"],
        "confidence": top_prediction["confidence"],
        "model": predictions["model"],
    }


@activity(name="store_results")
async def store_results(text: str, sentiment: dict):
    """Store results in database."""
    print(f"Storing results: {sentiment['sentiment']} ({sentiment['confidence']:.2f})")
    await asyncio.sleep(0.5)
    return {"stored": True, "result_id": f"result_{random.randint(1000, 9999)}"}


# Define AI workflow
@workflow(name="sentiment_analysis", retry_policy=RetryPolicy(max_retries=2))
async def sentiment_analysis_workflow(ctx: WorkflowContext):
    """
    Sentiment analysis AI workflow.
    """
    text = ctx.get_input("text", "")

    if not text:
        raise ValueError("No text provided for analysis")

    # Step 1: Preprocess
    preprocessed = await ctx.execute_activity(preprocess_data, text)

    # Step 2 & 3: Generate embeddings and run inference in parallel
    embeddings_result, inference_result = await ctx.execute_parallel(
        (generate_embeddings, (preprocessed["tokens"],), {}),
        # Simulate a parallel task
        (run_inference, ({"embeddings": []},), {}),
    )

    # Step 4: Post-process (use the real inference result)
    # In practice, we'd pass embeddings to inference, but this is a simulation
    final_result = await ctx.execute_activity(post_process_results, inference_result)

    # Step 5: Store results
    stored = await ctx.execute_activity(store_results, text, final_result)

    return {
        "sentiment": final_result["sentiment"],
        "confidence": final_result["confidence"],
        "result_id": stored["result_id"],
        "token_count": preprocessed["token_count"],
    }


@workflow(name="batch_sentiment_analysis")
async def batch_sentiment_analysis_workflow(ctx: WorkflowContext):
    """
    Batch sentiment analysis workflow - process multiple texts.
    """
    texts = ctx.get_input("texts", [])

    if not texts:
        raise ValueError("No texts provided for batch analysis")

    print(f"Processing {len(texts)} texts in batch...")

    # Process all texts in parallel
    tasks = [(preprocess_data, (text,), {}) for text in texts]
    preprocessed_results = await ctx.execute_parallel(*tasks)

    # Run inference on all preprocessed data in parallel
    inference_tasks = [
        (run_inference, ({"embeddings": []},), {}) for _ in preprocessed_results
    ]
    inference_results = await ctx.execute_parallel(*inference_tasks)

    # Post-process all results in parallel
    postprocess_tasks = [(post_process_results, (result,), {}) for result in inference_results]
    final_results = await ctx.execute_parallel(*postprocess_tasks)

    # Aggregate results
    sentiments = {
        "positive": sum(1 for r in final_results if r["sentiment"] == "positive"),
        "neutral": sum(1 for r in final_results if r["sentiment"] == "neutral"),
        "negative": sum(1 for r in final_results if r["sentiment"] == "negative"),
    }

    return {
        "total_processed": len(texts),
        "sentiment_distribution": sentiments,
        "results": final_results,
    }


async def main():
    """Run AI workflow examples."""
    async with TinyWorkflowClient() as client:
        # Example 1: Single text analysis
        print("Example 1: Single Text Sentiment Analysis")
        print("=" * 50)

        run_id = await client.start_workflow(
            "sentiment_analysis",
            input_data={"text": "I absolutely love this product! It's amazing and works perfectly."},
            wait=True,
        )

        workflow = await client.get_workflow_status(run_id)
        print(f"\nResult: {workflow.output_data}\n")

        # Example 2: Batch analysis
        print("\nExample 2: Batch Sentiment Analysis")
        print("=" * 50)

        texts = [
            "This is the best thing ever!",
            "I'm not sure how I feel about this.",
            "Terrible experience, very disappointing.",
            "It's okay, nothing special.",
            "Absolutely fantastic, highly recommend!",
        ]

        run_id = await client.start_workflow(
            "batch_sentiment_analysis", input_data={"texts": texts}, wait=True
        )

        workflow = await client.get_workflow_status(run_id)
        print(f"\nResult: {workflow.output_data}")


if __name__ == "__main__":
    asyncio.run(main())
