"""
Streaming Ops Demo

Demonstrates true streaming with ops and dynamic outputs for large BAM files.
This shows how sensors can trigger jobs that process data without buffering.
"""

import time

import dagster
from dagster import (
    DynamicOut,
    DynamicOutput,
    asset,
    job,
    op,
    run_status_sensor,
    sensor,
)

from .components.stream_bam import stream_bam_chunks
from .components.types import BamChunk


@op(out=DynamicOut())
def stream_bam_chunks_op(context, bam_url: str, chunk_size: int = 1000):
    """
    Op that streams BAM chunks without buffering.

    Yields each chunk as soon as it's read, enabling true streaming.
    """
    context.log.info(f"🚀 Starting streaming from: {bam_url}")

    chunk_count = 0
    for chunk_reads in stream_bam_chunks(bam_url, chunk_size):
        chunk_count += 1

        # Create BamChunk (same as asset version)
        bam_chunk = BamChunk(
            chunk_id=chunk_count,
            total_chunks=None,  # We'll update this later if needed
            reads=chunk_reads,
            bam_url=bam_url,
        )

        context.log.info(f"📦 Yielding chunk {chunk_count} ({len(chunk_reads)} reads)")

        # Yield immediately - no buffering!
        yield DynamicOutput(
            value=bam_chunk,
            mapping_key=f"chunk_{chunk_count}",
            metadata={"chunk_id": chunk_count, "read_count": len(chunk_reads)},
        )

    context.log.info(f"✅ Streaming complete! Yielded {chunk_count} chunks")


@op
def process_bam_chunk_op(context, bam_chunk: BamChunk):
    """
    Process a single BAM chunk.

    This op runs for each chunk yielded by the streamer.
    """
    chunk_id = bam_chunk.chunk_id
    read_count = len(bam_chunk.reads)

    context.log.info(f"🔄 Processing chunk {chunk_id} with {read_count} reads")

    # Simulate processing time
    time.sleep(0.01)  # Small delay to show parallel processing

    # Calculate some stats
    mapped_count = sum(1 for read in bam_chunk.reads if not (read.flag & 4))
    avg_quality = sum(read.mapping_quality for read in bam_chunk.reads) / read_count

    context.log.info(
        f"✅ Chunk {chunk_id}: {mapped_count}/{read_count} mapped, "
        f"avg quality: {avg_quality:.1f}"
    )

    return {
        "chunk_id": chunk_id,
        "read_count": read_count,
        "mapped_count": mapped_count,
        "avg_quality": round(avg_quality, 2),
    }


@op
def collect_chunk_results_op(context, chunk_results: list):
    """
    Collect and summarize results from all processed chunks.
    """
    total_chunks = len(chunk_results)
    total_reads = sum(r["read_count"] for r in chunk_results)
    total_mapped = sum(r["mapped_count"] for r in chunk_results)
    avg_quality = sum(r["avg_quality"] for r in chunk_results) / total_chunks

    summary = {
        "total_chunks": total_chunks,
        "total_reads": total_reads,
        "total_mapped": total_mapped,
        "mapping_rate": total_mapped / total_reads if total_reads > 0 else 0,
        "avg_quality": round(avg_quality, 2),
    }

    context.log.info("📊 Final Summary:")
    context.log.info(f"   Chunks processed: {total_chunks}")
    context.log.info(f"   Total reads: {total_reads:,}")
    context.log.info(
        f"   Mapped reads: {total_mapped:,} ({summary['mapping_rate']:.1%})"
    )
    context.log.info(f"   Average quality: {summary['avg_quality']}")

    return summary


# Job that combines the ops
@job
def streaming_bam_job():
    """
    Job that demonstrates true streaming BAM processing.

    This job can be triggered by sensors and processes chunks as they're yielded.
    """
    # Stream chunks and process each one
    chunk_results = stream_bam_chunks_op().map(process_bam_chunk_op)

    # Collect final results
    collect_chunk_results_op(chunk_results.collect())


# Sensor that triggers the streaming job
@sensor(job=streaming_bam_job)
def bam_file_sensor(context):
    """
    Sensor that detects new BAM files and triggers streaming jobs.

    In a real implementation, this would:
    1. Check for new BAM files in a directory/S3 bucket
    2. Trigger the streaming job for each new file
    """
    # For demo purposes, we'll simulate finding a BAM file
    bam_files = ["https://example.com/sample.bam"]  # Would be real file detection

    for bam_url in bam_files:
        # Check if we've already processed this file
        file_id = bam_url.split("/")[-1].replace(".bam", "")

        if not context.has_cursor:
            context.update_cursor("processed_files", set())

        processed_files = context.cursor.get("processed_files", set())

        if file_id not in processed_files:
            context.log.info(f"🎯 Found new BAM file: {bam_url}")

            # Yield a run request for the streaming job
            yield dagster.RunRequest(
                run_key=file_id,
                run_config={
                    "ops": {
                        "stream_bam_chunks_op": {
                            "inputs": {
                                "bam_url": bam_url,
                                "chunk_size": 500,  # Smaller chunks for demo
                            }
                        }
                    }
                },
                tags={"file_id": file_id, "bam_url": bam_url},
            )

            # Mark as processed
            processed_files.add(file_id)
            context.update_cursor("processed_files", processed_files)


# Sensor that monitors job completion
@run_status_sensor(
    run_status=dagster.DagsterRunStatus.SUCCESS,
    monitored_jobs=[streaming_bam_job],
)
def streaming_job_success_sensor(context):
    """
    Sensor that reacts to successful streaming job completions.
    """
    run = context.dagster_run
    context.log.info(f"🎉 Streaming job completed successfully: {run.run_id}")

    # Could trigger downstream processing, notifications, etc.
    # For example, send results to a dashboard or trigger ML training


# For comparison: Asset-based approach (what we have now)
@asset
def demo_asset_buffering():
    """
    Demo asset that shows the buffering limitation.

    This would buffer ALL chunks before downstream assets can start.
    """
    return ["chunk_1", "chunk_2", "chunk_3"]  # All at once


@asset
def demo_asset_processor(buffering_demo: list):
    """
    Downstream asset that can't start until upstream is completely done.
    """
    return f"Processed {len(buffering_demo)} chunks"
