"""
Dagster components for streaming BAM file processing.

This module provides:
- A sensor that detects BAM file availability
- A loading operation that streams BAM data in chunks
- A processing operation that handles each chunk
"""

import time
from dataclasses import dataclass
from typing import Iterator, List

import pysam
from dagster import (
    DynamicOut,
    DynamicOutput,
    OpExecutionContext,
    RunRequest,
    SensorEvaluationContext,
    job,
    op,
    sensor,
)

from stream_bam import BamStats, calculate_total_chunks, format_progress


def serialize_reads(reads: List[pysam.AlignedSegment]) -> List[dict]:
    """
    Convert pysam AlignedSegment objects to serializable dictionaries.

    Only extracts the essential read information needed for processing.
    """
    serialized = []
    for read in reads:
        serialized.append(
            {
                "query_name": read.query_name,
                "flag": read.flag,
                "reference_id": read.reference_id,
                "reference_start": read.reference_start,
                "mapping_quality": read.mapping_quality,
                "cigarstring": read.cigarstring,
                "next_reference_id": read.next_reference_id,
                "next_reference_start": read.next_reference_start,
                "template_length": read.template_length,
                "query_sequence": read.query_sequence,
                "query_qualities": read.query_qualities,
            }
        )
    return serialized


BAM_URL = "https://s3.amazonaws.com/1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam"
CHUNK_SIZE = 1000


@dataclass
class BamChunk:
    """Represents a chunk of BAM reads with metadata."""

    chunk_id: int
    total_chunks: int
    reads: List[dict]  # Changed from List[pysam.AlignedSegment] to List[dict]
    bam_url: str


@sensor(
    job_name="bam_processing_job",
    minimum_interval_seconds=60,  # Check every minute
)
def bam_file_sensor(context: SensorEvaluationContext) -> Iterator[RunRequest]:
    """
    Sensor that detects when the BAM file is available.

    Can be triggered manually via Dagster UI or CLI.
    In a real scenario, this could check multiple URLs or a database
    for available BAM files. For this demo, it just checks the hardcoded URL.
    """
    try:
        # Quick check if the BAM file is accessible by getting stats
        stats = BamStats.from_url(BAM_URL)

        context.log.info(f"BAM file detected: {BAM_URL}")
        context.log.info(f"Total reads: {stats.total_reads:,}")
        context.log.info(f"References: {stats.num_references}")

        # Yield a run request with the BAM URL as config
        yield RunRequest(
            run_config={
                "ops": {
                    "load_bam_chunks": {
                        "config": {
                            "bam_url": BAM_URL,
                            "chunk_size": CHUNK_SIZE,
                        }
                    }
                }
            },
            tags={"bam_url": BAM_URL},
        )

    except Exception as e:
        context.log.debug(f"BAM file not available: {e}")
        return


@op(
    config_schema={
        "bam_url": str,
        "chunk_size": int,
    },
    out=DynamicOut(),
)
def load_bam_chunks(context: OpExecutionContext) -> Iterator[DynamicOutput[BamChunk]]:
    """
    Load operation that streams BAM file in chunks without loading into memory.

    Yields dynamic outputs for each chunk, allowing downstream processing
    to happen incrementally without storing all data in memory.
    """
    bam_url = context.op_config["bam_url"]
    chunk_size = context.op_config["chunk_size"]

    context.log.info(f"Starting BAM streaming from: {bam_url}")

    # Get statistics efficiently
    stats = BamStats.from_url(bam_url)
    total_chunks = calculate_total_chunks(stats.total_reads, chunk_size)

    context.log.info(
        f"BAM file opened successfully. References: {stats.num_references}"
    )
    context.log.info(
        f"Total reads: {stats.total_reads:,} | Total chunks: {total_chunks:,}"
    )

    # Open file for streaming
    samfile = pysam.AlignmentFile(bam_url, "rb")
    start_time = time.time()

    chunk = []
    chunk_count = 0
    reads_processed = 0

    try:
        for read in samfile:
            chunk.append(read)
            reads_processed += 1

            if len(chunk) >= chunk_size:
                chunk_count += 1
                elapsed_time = time.time() - start_time
                rate = reads_processed / elapsed_time if elapsed_time > 0 else 0

                progress_msg = format_progress(
                    chunk_count, total_chunks, reads_processed, rate
                )
                context.log.info(progress_msg)

                # Yield the chunk as a dynamic output
                bam_chunk = BamChunk(
                    chunk_id=chunk_count,
                    total_chunks=total_chunks,
                    reads=serialize_reads(chunk),  # Convert to serializable format
                    bam_url=bam_url,
                )

                yield DynamicOutput(
                    bam_chunk,
                    f"chunk_{chunk_count}",
                    metadata={
                        "chunk_id": chunk_count,
                        "total_chunks": total_chunks,
                        "reads_in_chunk": len(chunk),
                        "reads_processed": reads_processed,
                        "progress_rate": f"{rate:.0f} reads/sec",
                    },
                )

                chunk = []

        # Handle final partial chunk
        if chunk:
            chunk_count += 1
            elapsed_time = time.time() - start_time
            rate = reads_processed / elapsed_time if elapsed_time > 0 else 0

            progress_msg = (
                format_progress(chunk_count, total_chunks, reads_processed, rate)
                + " (FINAL)"
            )
            context.log.info(progress_msg)

            bam_chunk = BamChunk(
                chunk_id=chunk_count,
                total_chunks=total_chunks,
                reads=serialize_reads(chunk),  # Convert to serializable format
                bam_url=bam_url,
            )

            yield DynamicOutput(
                bam_chunk,
                f"chunk_{chunk_count}",
                metadata={
                    "chunk_id": chunk_count,
                    "total_chunks": total_chunks,
                    "reads_in_chunk": len(chunk),
                    "reads_processed": reads_processed,
                    "progress_rate": f"{rate:.0f} reads/sec",
                    "is_final": True,
                },
            )

    finally:
        samfile.close()

        # Final summary
        total_time = time.time() - start_time
        avg_rate = reads_processed / total_time if total_time > 0 else 0

        context.log.info("=" * 70)
        context.log.info("Streaming complete!")
        context.log.info(f"Total chunks processed: {chunk_count}")
        context.log.info(f"Total reads processed: {reads_processed}")
        context.log.info(f"Total time: {total_time:.2f} seconds")
        context.log.info(f"Average rate: {avg_rate:.0f} reads/second")


@op
def process_bam_chunk(context: OpExecutionContext, bam_chunk: BamChunk) -> dict:
    """
    Process operation that handles individual BAM chunks.

    In a real application, this could perform analysis, filtering,
    quality control, or any other processing on the reads.
    """
    context.log.info(f"Processing chunk {bam_chunk.chunk_id}/{bam_chunk.total_chunks}")
    context.log.info(f"Chunk contains {len(bam_chunk.reads)} reads")

    # Example processing: count mapped vs unmapped reads
    # For BAM format, unmapped reads have the 0x4 flag set
    mapped_count = sum(1 for read in bam_chunk.reads if not (read["flag"] & 4))
    unmapped_count = len(bam_chunk.reads) - mapped_count

    # Calculate some basic statistics
    total_bases = sum(
        len(read["query_sequence"])
        for read in bam_chunk.reads
        if read.get("query_sequence")
    )
    avg_read_length = total_bases / len(bam_chunk.reads) if bam_chunk.reads else 0

    # In a real application, you might:
    # - Filter reads based on quality
    # - Extract specific genomic regions
    # - Perform variant calling
    # - Calculate coverage statistics
    # - Store results in a database

    result = {
        "chunk_id": bam_chunk.chunk_id,
        "total_chunks": bam_chunk.total_chunks,
        "reads_in_chunk": len(bam_chunk.reads),
        "mapped_reads": mapped_count,
        "unmapped_reads": unmapped_count,
        "avg_read_length": round(avg_read_length, 1),
        "bam_url": bam_chunk.bam_url,
    }

    context.log.info(
        f"Chunk {bam_chunk.chunk_id} processed: {mapped_count} mapped, {unmapped_count} unmapped reads"
    )
    context.log.info(f"Average read length: {avg_read_length:.1f} bases")

    return result


@job
def bam_processing_job():
    """
    Job that orchestrates the streaming BAM processing pipeline.

    The job uses dynamic outputs to process chunks in parallel
    while maintaining memory efficiency.
    """
    # Load chunks dynamically
    chunks = load_bam_chunks()

    # Process each chunk (can run in parallel)
    results = chunks.map(process_bam_chunk)
