"""
Memory-efficient BAM file streaming processor.

This module provides functionality to stream BAM files from URLs in chunks,
with efficient upfront calculation of total chunks using BAM index statistics.
"""

import time
from dataclasses import dataclass
from typing import Generator, List

import pysam

BAM_URL = "https://s3.amazonaws.com/1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam"


@dataclass
class BamStats:
    """BAM file statistics container."""

    total_reads: int
    num_references: int
    url: str

    @classmethod
    def from_url(cls, bam_url: str) -> "BamStats":
        """
        Create BamStats from BAM file URL using index statistics.

        This is much faster than counting all reads as it uses the BAM index.
        """
        with pysam.AlignmentFile(bam_url, "rb") as samfile:
            stats = samfile.get_index_statistics()
            total_reads = sum(stat.mapped + stat.unmapped for stat in stats)
            num_references = len(samfile.references)
        return cls(total_reads=total_reads, num_references=num_references, url=bam_url)


def calculate_total_chunks(total_reads: int, chunk_size: int) -> int:
    """Calculate total number of chunks needed for given read count and chunk size."""
    return (total_reads + chunk_size - 1) // chunk_size  # Ceiling division


def format_progress(
    chunk_num: int, total_chunks: int, reads_processed: int, rate: float = None
) -> str:
    """Format progress message for consistent logging."""
    base = f"Chunk {chunk_num}:{total_chunks} | Reads: {reads_processed:8d}"
    if rate is not None:
        base += f" | Rate: {rate:6.0f} reads/sec"
    return base


def stream_bam_chunks(
    bam_url: str, chunk_size: int = 1000
) -> Generator[List[pysam.AlignedSegment], None, None]:
    print(f"Opening BAM file: {bam_url}")

    # Get statistics efficiently
    stats = BamStats.from_url(bam_url)
    total_chunks = calculate_total_chunks(stats.total_reads, chunk_size)

    print(f"BAM file opened successfully. References: {stats.num_references}")
    print(f"Total reads: {stats.total_reads:,} | Total chunks: {total_chunks:,}")
    print(f"Starting streaming with chunk_size={chunk_size} reads per chunk")
    print("=" * 70)

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

                print(format_progress(chunk_count, total_chunks, reads_processed, rate))
                yield chunk
                chunk = []

        # Handle final partial chunk
        if chunk:
            chunk_count += 1
            elapsed_time = time.time() - start_time
            rate = reads_processed / elapsed_time if elapsed_time > 0 else 0

            print(
                format_progress(chunk_count, total_chunks, reads_processed, rate)
                + " (FINAL)"
            )
            yield chunk

    finally:
        samfile.close()

        # Final summary
        total_time = time.time() - start_time
        avg_rate = reads_processed / total_time if total_time > 0 else 0

        print("=" * 70)
        print("Streaming complete!")
        print(f"Total chunks processed: {chunk_count}")
        print(f"Total reads processed: {reads_processed}")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Average rate: {avg_rate:.0f} reads/second")
