"""
AI Document Processing Workflow
Real-world example: Process documents with AI for extraction, classification, and insights

Use case: Automated document processing pipeline
- Extract text from documents
- Classify document type
- Extract structured data
- Generate insights/summary
- Handle failures with retries
"""

import asyncio
import json
import hashlib
from typing import Dict, Any, List
from datetime import datetime
from tinyworkflow import workflow, activity, WorkflowContext, TinyWorkflowClient, RetryPolicy


# Simulated document processing services
class DocumentProcessor:
    """Simulated document processing - replace with real services."""

    @staticmethod
    async def extract_text(document_path: str) -> Dict[str, Any]:
        """Extract text from document (PDF, DOCX, images via OCR)."""
        await asyncio.sleep(0.5)  # Simulate processing time

        # Simulate extracted text
        return {
            "text": f"Sample document content from {document_path}. "
            "This contains information about quarterly sales, "
            "revenue projections, and market analysis.",
            "page_count": 5,
            "word_count": 1250,
            "language": "en",
        }

    @staticmethod
    async def classify_document(text: str) -> Dict[str, Any]:
        """Classify document type using AI."""
        await asyncio.sleep(0.3)

        # Simulate classification
        classifications = {
            "invoice": 0.15,
            "contract": 0.10,
            "report": 0.65,
            "email": 0.05,
            "other": 0.05,
        }

        primary_type = max(classifications, key=classifications.get)

        return {
            "document_type": primary_type,
            "confidence": classifications[primary_type],
            "all_scores": classifications,
        }

    @staticmethod
    async def extract_entities(text: str) -> Dict[str, Any]:
        """Extract named entities (dates, amounts, organizations, etc.)."""
        await asyncio.sleep(0.4)

        return {
            "entities": {
                "dates": ["2024-Q4", "December 2024"],
                "amounts": ["$1.2M", "$850K"],
                "organizations": ["Acme Corp", "Tech Industries"],
                "people": ["John Smith", "Jane Doe"],
                "locations": ["New York", "San Francisco"],
            },
            "entity_count": 9,
        }

    @staticmethod
    async def generate_summary(text: str, max_sentences: int = 3) -> str:
        """Generate AI summary of document."""
        await asyncio.sleep(0.5)

        return (
            "This quarterly report shows strong revenue growth of 25% YoY. "
            "Key markets include technology and healthcare sectors. "
            "Projected targets for next quarter remain optimistic."
        )

    @staticmethod
    async def extract_key_points(text: str) -> List[str]:
        """Extract key bullet points from document."""
        await asyncio.sleep(0.3)

        return [
            "Revenue increased 25% year-over-year",
            "Technology sector shows strongest growth",
            "Healthcare market expansion planned for Q1",
            "Operating expenses reduced by 12%",
            "Customer acquisition cost decreased 18%",
        ]

    @staticmethod
    async def perform_sentiment_analysis(text: str) -> Dict[str, Any]:
        """Analyze sentiment/tone of document."""
        await asyncio.sleep(0.2)

        return {
            "overall_sentiment": "positive",
            "confidence": 0.82,
            "tone": ["professional", "optimistic", "data-driven"],
        }

    @staticmethod
    async def check_compliance(text: str, document_type: str) -> Dict[str, Any]:
        """Check document for compliance issues."""
        await asyncio.sleep(0.4)

        return {
            "compliant": True,
            "issues": [],
            "warnings": ["Missing signature date field"],
            "score": 0.95,
        }


# Define activities
@activity(
    name="extract_document_text",
    retry_policy=RetryPolicy(max_retries=3, initial_delay=2.0),
)
async def extract_document_text(document_path: str) -> Dict[str, Any]:
    """Extract text from document with retry logic."""
    print(f"   Extracting text from: {document_path}")
    result = await DocumentProcessor.extract_text(document_path)
    print(f"   Extracted {result['word_count']} words from {result['page_count']} pages")
    return result


@activity(name="classify_document_type")
async def classify_document_type(text: str) -> Dict[str, Any]:
    """Classify the document type."""
    print(f"   Classifying document...")
    result = await DocumentProcessor.classify_document(text)
    print(f"   Classified as: {result['document_type']} ({result['confidence']:.0%} confidence)")
    return result


@activity(name="extract_document_entities")
async def extract_document_entities(text: str) -> Dict[str, Any]:
    """Extract named entities from document."""
    print(f"   Extracting entities...")
    result = await DocumentProcessor.extract_entities(text)
    print(f"   Found {result['entity_count']} entities")
    return result


@activity(name="generate_document_summary")
async def generate_document_summary(text: str) -> Dict[str, Any]:
    """Generate summary of document."""
    print(f"   Generating summary...")
    summary = await DocumentProcessor.generate_summary(text)
    print(f"   Summary generated")
    return {"summary": summary, "length": len(summary)}


@activity(name="extract_document_key_points")
async def extract_document_key_points(text: str) -> Dict[str, Any]:
    """Extract key points from document."""
    print(f"   Extracting key points...")
    points = await DocumentProcessor.extract_key_points(text)
    print(f"   Extracted {len(points)} key points")
    return {"key_points": points, "count": len(points)}


@activity(name="analyze_document_sentiment")
async def analyze_document_sentiment(text: str) -> Dict[str, Any]:
    """Analyze document sentiment."""
    print(f"   Analyzing sentiment...")
    result = await DocumentProcessor.perform_sentiment_analysis(text)
    print(f"   Sentiment: {result['overall_sentiment']}")
    return result


@activity(name="check_document_compliance")
async def check_document_compliance(text: str, document_type: str) -> Dict[str, Any]:
    """Check document compliance."""
    print(f"   Checking compliance...")
    result = await DocumentProcessor.check_compliance(text, document_type)
    print(f"   Compliance score: {result['score']:.0%}")
    return result


@activity(name="store_processed_document")
async def store_processed_document(data: Dict[str, Any]) -> Dict[str, Any]:
    """Store processed document results."""
    await asyncio.sleep(0.2)

    # Generate document ID
    doc_id = hashlib.md5(data["document_path"].encode()).hexdigest()[:12]

    print(f"   Storing document: {doc_id}")

    return {
        "document_id": doc_id,
        "stored_at": datetime.utcnow().isoformat(),
        "status": "success",
    }


# Workflow: Basic Document Processing
@workflow(name="process_document")
async def process_document(ctx: WorkflowContext):
    """
    Basic document processing workflow:
    1. Extract text
    2. Classify document
    3. Extract entities
    4. Generate summary
    5. Store results
    """
    document_path = ctx.get_input("document_path", "documents/quarterly_report.pdf")

    print(f"\n Processing document: {document_path}")
    print("=" * 60)

    # Step 1: Extract text
    extraction = await ctx.execute_activity(extract_document_text, document_path)
    text = extraction["text"]

    # Step 2: Classify document
    classification = await ctx.execute_activity(classify_document_type, text)

    # Step 3: Extract entities
    entities = await ctx.execute_activity(extract_document_entities, text)

    # Step 4: Generate summary
    summary = await ctx.execute_activity(generate_document_summary, text)

    # Step 5: Store results
    storage_result = await ctx.execute_activity(
        store_processed_document,
        {
            "document_path": document_path,
            "classification": classification,
            "entities": entities,
            "summary": summary,
            "extraction": extraction,
        },
    )

    print("=" * 60)
    print(f" Document processing completed!\n")

    return {
        "document_id": storage_result["document_id"],
        "document_type": classification["document_type"],
        "summary": summary["summary"],
        "entity_count": entities["entity_count"],
        "word_count": extraction["word_count"],
        "status": "completed",
    }


# Workflow: Advanced Document Processing with Parallel Analysis
@workflow(name="advanced_document_processing")
async def advanced_document_processing(ctx: WorkflowContext):
    """
    Advanced document processing with parallel analysis:
    1. Extract text
    2. Run multiple analyses in parallel
    3. Compliance check
    4. Store comprehensive results
    """
    document_path = ctx.get_input("document_path", "documents/contract.pdf")

    print(f"\n Advanced processing: {document_path}")
    print("=" * 60)

    # Step 1: Extract text
    extraction = await ctx.execute_activity(extract_document_text, document_path)
    text = extraction["text"]

    # Step 2: Parallel analysis (classification, entities, sentiment, key points)
    print("\n Running parallel analysis...")
    classification, entities, sentiment, key_points = await ctx.execute_parallel(
        (classify_document_type, (text,), {}),
        (extract_document_entities, (text,), {}),
        (analyze_document_sentiment, (text,), {}),
        (extract_document_key_points, (text,), {}),
    )

    # Step 3: Generate summary
    summary = await ctx.execute_activity(generate_document_summary, text)

    # Step 4: Compliance check
    compliance = await ctx.execute_activity(
        check_document_compliance, text, classification["document_type"]
    )

    # Step 5: Store comprehensive results
    storage_result = await ctx.execute_activity(
        store_processed_document,
        {
            "document_path": document_path,
            "extraction": extraction,
            "classification": classification,
            "entities": entities,
            "sentiment": sentiment,
            "key_points": key_points,
            "summary": summary,
            "compliance": compliance,
        },
    )

    print("=" * 60)
    print(f" Advanced processing completed!\n")

    return {
        "document_id": storage_result["document_id"],
        "document_type": classification["document_type"],
        "summary": summary["summary"],
        "key_points": key_points["key_points"],
        "sentiment": sentiment["overall_sentiment"],
        "compliance_score": compliance["score"],
        "entity_count": entities["entity_count"],
        "status": "completed",
    }


# Workflow: Batch Document Processing
@workflow(name="batch_document_processing")
async def batch_document_processing(ctx: WorkflowContext):
    """
    Process multiple documents in a batch.
    """
    documents = ctx.get_input(
        "documents",
        [
            "documents/report1.pdf",
            "documents/report2.pdf",
            "documents/report3.pdf",
        ],
    )

    print(f"\n Batch processing {len(documents)} documents")
    print("=" * 60)

    results = []

    for i, doc_path in enumerate(documents, 1):
        print(f"\n Processing document {i}/{len(documents)}: {doc_path}")

        # Extract and classify
        extraction = await ctx.execute_activity(extract_document_text, doc_path)
        classification = await ctx.execute_activity(classify_document_type, extraction["text"])

        results.append(
            {
                "document": doc_path,
                "type": classification["document_type"],
                "word_count": extraction["word_count"],
            }
        )

    print("\n" + "=" * 60)
    print(f" Batch processing completed! Processed {len(results)} documents\n")

    return {"documents_processed": len(results), "results": results, "status": "completed"}


async def main():
    """Run AI document processing examples."""
    print("\n" + "=" * 70)
    print(" TinyWorkflow AI Document Processing Examples")
    print("=" * 70)

    async with TinyWorkflowClient() as client:
        # Example 1: Basic document processing
        print("\n\n Example 1: Basic Document Processing")
        print("-" * 70)

        run_id = await client.start_workflow(
            "process_document",
            input_data={"document_path": "documents/quarterly_report.pdf"},
            wait=True,
        )

        result = await client.get_workflow_status(run_id)
        if result.output_data:
            output = result.output_data.get("result", {})
            print(f"\n Results:")
            print(f"  Document ID: {output.get('document_id')}")
            print(f"  Type: {output.get('document_type')}")
            print(f"  Summary: {output.get('summary')}")

        # Example 2: Advanced processing with parallel analysis
        print("\n\n Example 2: Advanced Document Processing (Parallel)")
        print("-" * 70)

        run_id = await client.start_workflow(
            "advanced_document_processing",
            input_data={"document_path": "documents/legal_contract.pdf"},
            wait=True,
        )

        result = await client.get_workflow_status(run_id)
        if result.output_data:
            output = result.output_data.get("result", {})
            print(f"\n Results:")
            print(f"  Document ID: {output.get('document_id')}")
            print(f"  Type: {output.get('document_type')}")
            print(f"  Sentiment: {output.get('sentiment')}")
            print(f"  Compliance: {output.get('compliance_score'):.0%}")
            print(f"  Key Points: {len(output.get('key_points', []))}")

        # Example 3: Batch processing
        print("\n\n Example 3: Batch Document Processing")
        print("-" * 70)

        run_id = await client.start_workflow(
            "batch_document_processing",
            input_data={
                "documents": [
                    "documents/report_jan.pdf",
                    "documents/report_feb.pdf",
                    "documents/report_mar.pdf",
                ]
            },
            wait=True,
        )

        result = await client.get_workflow_status(run_id)
        if result.output_data:
            output = result.output_data.get("result", {})
            print(f"\n Batch Results:")
            print(f"  Documents processed: {output.get('documents_processed')}")

    print("\n" + "=" * 70)
    print(" All document processing examples completed!")
    print("=" * 70)
    print("\n Use Cases:")
    print("   Automated invoice processing")
    print("   Legal document analysis")
    print("   Research paper summarization")
    print("   Contract review and compliance")
    print("   Resume screening and extraction")
    print("\n Integration Options:")
    print("   OpenAI GPT-4 for text analysis")
    print("   Google Cloud Vision for OCR")
    print("   AWS Textract for document extraction")
    print("   Azure Cognitive Services")
    print("   Local models (Llama, Mistral, etc.)")


if __name__ == "__main__":
    asyncio.run(main())
