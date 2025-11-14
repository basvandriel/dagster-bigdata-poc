"""
Streaming BAM Chunk Streamer Component

An ops-based component that streams BAM chunks without buffering in memory.
"""

import dagster
from dagster import DynamicOut, DynamicOutput, op
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
    Ops-based component for streaming BAM chunks without memory buffering.

    This component uses Dagster's dynamic outputs to yield chunks incrementally
    as they are processed, enabling true streaming for terabyte-scale BAM files.
    """

    name: str = "streaming_bam_chunk_streamer"
    chunk_size: int = 1000  # Number of reads per chunk

    def build_defs(self, context):
        @op(
            name=self.name,
            out=DynamicOut(BamChunk),
            description="Streams BAM chunks dynamically without buffering in memory",
        )
        def stream_bam_chunks_op(
            context, bam_url: str
        ) -> Iterator[DynamicOutput[BamChunk]]:
            """
            Op that streams BAM chunks using dynamic outputs.

            This op:
            1. Opens the BAM file from the provided URL
            2. Streams chunks incrementally using dynamic outputs
            3. Each chunk is yielded immediately without buffering
            4. Enables processing of terabyte files without memory limits
            """
            import urllib.parse

            # Parse the BAM URL (supports file:// and other schemes)
            parsed_url = urllib.parse.urlparse(bam_url)

            # For HTTP/HTTPS URLs, use the full URL. For file:// URLs, use the path
            if parsed_url.scheme in ("http", "https"):
                bam_file_path = bam_url  # Use full URL for HTTP
            else:
                bam_file_path = parsed_url.path  # Use path for file://

            context.log.info(f"🎬 Starting streaming for BAM file: {bam_file_path}")
            context.log.info(f"📏 Using chunk size: {self.chunk_size}")

            # Get statistics to calculate total chunks
            stats = BamStats.from_url(bam_file_path)
            total_chunks = calculate_total_chunks(stats.total_reads, self.chunk_size)
            
            context.log.info(f"📊 BAM stats: {stats.total_reads:,} reads, {stats.num_references} references")
            context.log.info(f"📦 Total chunks: {total_chunks}")

            chunk_count = 0
            total_reads = 0

            try:
                # Stream chunks from the BAM file
                for chunk_reads in stream_bam_chunks(
                    bam_file_path, chunk_size=self.chunk_size
                ):
                    chunk_count += 1
                    total_reads += len(chunk_reads)

                    # Serialize reads for the BamChunk
                    serialized_reads = serialize_reads(chunk_reads)

                    # Create BamChunk object
                    bam_chunk = BamChunk(
                        chunk_id=chunk_count,
                        total_chunks=total_chunks,
                        reads=serialized_reads,
                        bam_url=bam_url,
                    )

                    # Create dynamic output for this chunk
                    chunk_id_str = f"chunk_{chunk_count:06d}"

                    context.log.info(
                        f"📤 Yielding {chunk_id_str}: {len(serialized_reads)} reads "
                        f"(total: {total_reads} reads, {chunk_count}/{total_chunks} chunks)"
                    )

                    yield DynamicOutput(
                        value=bam_chunk,
                        mapping_key=chunk_id_str,
                        metadata={
                            "chunk_id": chunk_count,
                            "read_count": len(serialized_reads),
                            "total_reads": total_reads,
                            "chunk_number": chunk_count,
                            "bam_file": bam_file_path,
                        },
                    )

                context.log.info(
                    f"✅ Streaming complete: {chunk_count} chunks, {total_reads} total reads"
                )

            except Exception as e:
                context.log.error(f"❌ Error during streaming: {e}")
                raise

        return stream_bam_chunks_op
