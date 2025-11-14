"""
BAM Chunk Streamer Component

A reusable asset component that streams BAM files in chunks.
"""

import time
from typing import List

import dagster
import pysam
from dagster import AssetExecutionContext, asset

from .stream_bam import (
    BamStats,
    calculate_total_chunks,
    format_progress,
)

from .types import BamChunk


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


class BamChunkStreamer(dagster.Model, dagster.Resolvable):
    """
    Op component for streaming BAM files in memory-efficient chunks.

    This component streams BAM data without loading the entire file into memory,
    yielding dynamic outputs for parallel processing.
    """

    name: str  # Unique identifier for this streamer instance
    bam_url: str
    chunk_size: int = 1000

    def build_defs(self, context):
        @asset(
            name=f"{self.name}_chunks",  # Unique asset name based on instance
            compute_kind="bam_streaming",
            description=f"Streams BAM file in memory-efficient chunks for {self.name}",
        )
        def load_bam_chunks(context: AssetExecutionContext) -> List[BamChunk]:
            """
            Asset that streams BAM file in chunks and returns all chunks as a list.

            While not as memory-efficient as dynamic outputs, this allows for
            asset-based composition where other components can consume the chunks.
            """
            bam_url = self.bam_url
            chunk_size = self.chunk_size

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

            chunks = []
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

                        # Create chunk and add to list
                        bam_chunk = BamChunk(
                            chunk_id=chunk_count,
                            total_chunks=total_chunks,
                            reads=serialize_reads(
                                chunk
                            ),  # Convert to serializable format
                            bam_url=bam_url,
                        )
                        chunks.append(bam_chunk)
                        chunk = []

                # Handle final partial chunk
                if chunk:
                    chunk_count += 1
                    elapsed_time = time.time() - start_time
                    rate = reads_processed / elapsed_time if elapsed_time > 0 else 0

                    progress_msg = (
                        format_progress(
                            chunk_count, total_chunks, reads_processed, rate
                        )
                        + " (FINAL)"
                    )
                    context.log.info(progress_msg)

                    bam_chunk = BamChunk(
                        chunk_id=chunk_count,
                        total_chunks=total_chunks,
                        reads=serialize_reads(chunk),  # Convert to serializable format
                        bam_url=bam_url,
                    )
                    chunks.append(bam_chunk)

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

            return chunks

        return load_bam_chunks
