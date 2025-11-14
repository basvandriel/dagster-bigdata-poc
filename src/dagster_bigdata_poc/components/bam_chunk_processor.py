"""
BAM Chunk Processor Component

A simple component that processes BAM chunks and saves results to JSON files.
"""

import array
import json
from pathlib import Path
from typing import List

import dagster
import numpy as np
from dagster import AssetExecutionContext, AssetIn, asset

from .types import BamChunk


class BamChunkProcessor(dagster.Model, dagster.Resolvable):
    """
    Simple processor that saves BAM chunks to JSON files.

    Processes chunks from the streamer and saves them as JSON files
    with basic statistics.
    """

    name: str  # Unique identifier for this processor instance

    def build_defs(self, context):
        @asset(
            name=f"{self.name}_processed_chunks",
            ins={"chunks": AssetIn(f"{self.name}_chunks")},
            compute_kind="bam_processing",
            description=f"Saves BAM chunks to JSON files for {self.name}",
        )
        def process_bam_chunks(
            context: AssetExecutionContext, chunks: List[BamChunk]
        ) -> List[str]:
            """
            Process BAM chunks and save to JSON files.

            Returns list of saved file paths.
            """
            # Create output directory
            output_dir = Path(f"output/{self.name}")
            output_dir.mkdir(parents=True, exist_ok=True)

            saved_files = []

            for bam_chunk in chunks:
                context.log.info(
                    f"Processing chunk {bam_chunk.chunk_id}/{bam_chunk.total_chunks}"
                )

                # Calculate basic statistics
                mapped_count = sum(
                    1 for read in bam_chunk.reads if not (read["flag"] & 4)
                )
                unmapped_count = len(bam_chunk.reads) - mapped_count

                total_bases = sum(
                    len(read["query_sequence"])
                    for read in bam_chunk.reads
                    if read.get("query_sequence")
                )
                avg_read_length = (
                    total_bases / len(bam_chunk.reads) if bam_chunk.reads else 0
                )

                # Create chunk data structure
                # Reads are already serialized by the streamer, just use them directly
                chunk_data = {
                    "chunk_id": bam_chunk.chunk_id,
                    "metadata": {
                        "total_reads": len(bam_chunk.reads),
                        "mapped_count": mapped_count,
                        "unmapped_count": unmapped_count,
                        "avg_read_length": round(avg_read_length, 1),
                    },
                    "reads": bam_chunk.reads,  # Reads are already serialized dictionaries
                }

                # Save to JSON file
                filename = f"chunk_{bam_chunk.chunk_id:04d}.json"
                filepath = output_dir / filename

                def numpy_encoder(obj):
                    """Custom JSON encoder for numpy types and array.array."""
                    if isinstance(obj, np.ndarray):
                        return obj.tolist()
                    elif isinstance(
                        obj, (np.integer, np.int64, np.int32, np.int16, np.int8)
                    ):
                        return int(obj)
                    elif isinstance(
                        obj, (np.floating, np.float64, np.float32, np.float16)
                    ):
                        return float(obj)
                    elif isinstance(obj, np.bool_):
                        return bool(obj)
                    elif isinstance(obj, np.str_):
                        return str(obj)
                    elif isinstance(obj, array.array):
                        return list(obj)
                    else:
                        raise TypeError(
                            f"Object of type {obj.__class__.__name__} is not JSON serializable"
                        )

                with open(filepath, "w") as f:
                    json.dump(chunk_data, f, indent=2, default=numpy_encoder)

                context.log.info(f"✓ Saved chunk {bam_chunk.chunk_id} to {filepath}")
                context.log.info(
                    f"  {mapped_count} mapped, {unmapped_count} unmapped reads"
                )
                context.log.info(f"  Average read length: {avg_read_length:.1f} bases")

                saved_files.append(str(filepath))

            context.log.info(
                f"✅ Processed {len(chunks)} chunks, saved {len(saved_files)} JSON files"
            )
            return saved_files

        return process_bam_chunks
