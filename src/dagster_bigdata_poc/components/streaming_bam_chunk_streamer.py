"""
Streaming BAM Chunk Streamer Component

An ops-based component that streams BAM chunks without buffering in memory.
"""

import dagster
from dagster import op, Out, job
from typing import List
from .stream_bam import stream_bam_chunks, BamStats, calculate_total_chunks


class StreamingBamChunkStreamer(dagster.Model, dagster.Resolvable):
    """
    Component for streaming BAM chunks dynamically.

    This component creates an op that streams BAM files in chunks without
    buffering the entire file in memory, enabling processing of terabyte files.
    """

    name: str = "streaming_bam_chunk_streamer"
    chunk_size: int = 10_000  # Number of reads per chunk
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

        # Create and return the job that uses the op
        @job(name=f"{self.name}_job")
        def streaming_bam_job(bam_url: str):
            """Job that streams and processes all BAM chunks sequentially in one op."""
            stream_and_process_op(bam_url)

        return streaming_bam_job
