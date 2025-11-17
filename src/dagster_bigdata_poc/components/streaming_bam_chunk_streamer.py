"""
Streaming BAM Chunk Streamer Component

An ops-based component that streams BAM chunks without buffering in memory.
"""

import dagster
from dagster import DynamicOut, DynamicOutput, op, Out
from typing import Iterator, List
import pysam
from .stream_bam import stream_bam_chunks, BamStats, calculate_total_chunks
from .types import BamChunk


def serialize_reads(reads: List[pysam.AlignedSegment]) -> List[dict]:
    """
    Convert pysam AlignedSegment objects to serializable dictionaries.

    Only extracts the essential read information needed for processing.
    """
    import numpy as np

    def to_python_types(obj):
        """Recursively convert numpy types to Python types."""
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, dict):
            return {k: to_python_types(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [to_python_types(item) for item in obj]
        else:
            return obj

    serialized = []
    for read in reads:
        read_dict = {
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
        # Apply recursive conversion to handle all numpy types
        serialized.append(to_python_types(read_dict))
    return serialized


class StreamingBamChunkStreamer(dagster.Model, dagster.Resolvable):
    """
    Component for streaming BAM chunks dynamically.

    This component creates an op that streams BAM files in chunks without
    buffering the entire file in memory, enabling processing of terabyte files.
    """

    name: str = "streaming_bam_chunk_streamer"
    chunk_size: int = 1000  # Number of reads per chunk
    max_chunks: int = None  # Maximum number of chunks to process (None = all)

    def build_defs(self, context):
        @op(
            name=self.name,
            out=Out(List[dict]),  # Returns list of processing results
            description="Streams and processes BAM chunks sequentially without buffering",
        )
        def stream_and_process_op(context, bam_url: str) -> List[dict]:
            """
            Op that streams chunks and processes them immediately in a loop.

            This op:
            1. Streams chunks one at a time using a generator
            2. Processes each chunk immediately (no buffering)
            3. Collects results from all chunks
            4. Enables processing of terabyte files without memory limits
            """
            import urllib.parse
            from pathlib import Path

            # Parse the BAM URL
            parsed_url = urllib.parse.urlparse(bam_url)
            if parsed_url.scheme in ("http", "https"):
                bam_file_path = bam_url
            else:
                bam_file_path = parsed_url.path

            # Get statistics
            stats = BamStats.from_url(bam_file_path)
            total_chunks = calculate_total_chunks(stats.total_reads, self.chunk_size)

            context.log.info(
                f"🎯 Streaming and processing {total_chunks} chunks from {bam_url}"
            )

            results = []
            chunks_processed = 0
            total_reads_processed = 0

            try:
                # Stream and process chunks one at a time
                for chunk_reads in stream_bam_chunks(
                    bam_file_path, chunk_size=self.chunk_size
                ):
                    chunks_processed += 1

                    # Check max_chunks limit
                    if self.max_chunks and chunks_processed > self.max_chunks:
                        context.log.info(
                            f"🛑 Stopping after {self.max_chunks} chunks (limit reached)"
                        )
                        break

                    # Process this chunk immediately
                    chunk_size = len(chunk_reads)
                    total_reads_processed += chunk_size

                    context.log.info(
                        f"🔄 Processing chunk {chunks_processed}/{total_chunks}: {chunk_size} reads"
                    )

                    # Serialize reads for processing
                    serialized_reads = serialize_reads(chunk_reads)

                    # Create chunk data
                    chunk_data = {
                        "chunk_id": chunks_processed,
                        "total_chunks": total_chunks,
                        "reads": serialized_reads,
                        "bam_url": bam_url,
                    }

                    # Process the chunk (simulate processing - in real implementation, call processor logic)
                    processed_result = {
                        "chunk_id": chunks_processed,
                        "reads_processed": chunk_size,
                        "status": "completed",
                        "output_path": f"chunk_{chunks_processed:06d}.json",
                    }

                    # Save processed result (in real implementation, this would be the processor's job)
                    output_dir = Path("output")
                    output_dir.mkdir(exist_ok=True)
                    output_file = output_dir / f"chunk_{chunks_processed:06d}.json"

                    import json

                    with open(output_file, "w") as f:
                        json.dump(processed_result, f, indent=2)

                    results.append(processed_result)

                    context.log.info(
                        f"✅ Completed chunk {chunks_processed}: {chunk_size} reads → {output_file}"
                    )

                context.log.info(
                    f"🎉 Streaming complete: {chunks_processed} chunks, {total_reads_processed} total reads"
                )

            except Exception as e:
                context.log.error(f"❌ Error during streaming: {e}")
                raise

            return results

        return stream_and_process_op
