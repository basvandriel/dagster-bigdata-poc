"""
Streaming BAM Chunk Processor Component

An ops-based component that processes streaming BAM chunks dynamically.
"""

import dagster
from dagster import op
from typing import Dict, Any
from .types import BamChunk


class StreamingBamChunkProcessor(dagster.Model, dagster.Resolvable):
    """
    Ops-based component for processing streaming BAM chunks.

    This component processes chunks as they arrive from the streamer,
    enabling incremental processing without memory buffering.
    """

    name: str = "streaming_bam_chunk_processor"
    output_directory: str = "processed"  # Directory for processed outputs

    def build_defs(self, context):
        @op(
            name=self.name,
            description="Processes streaming BAM chunks incrementally",
        )
        def process_bam_chunk_op(context, chunk: BamChunk) -> Dict[str, Any]:
            """
            Op that processes individual BAM chunks from the streaming pipeline.

            This op:
            1. Receives chunks dynamically as they are streamed
            2. Processes each chunk independently
            3. Can perform analysis, filtering, or transformation
            4. Outputs results incrementally without waiting for all chunks
            """
            import json
            from pathlib import Path

            chunk_id = chunk.chunk_id
            read_count = len(chunk.reads)

            # Example processing: analyze read quality, filter reads, etc.
            context.log.info(f"🔄 Processing chunk {chunk_id} with {read_count} reads")
            processed_data = self._process_chunk(chunk)

            # Save processed chunk to output directory
            output_dir = Path(self.output_directory)
            output_dir.mkdir(parents=True, exist_ok=True)

            output_file = output_dir / f"processed_{chunk_id}.json"

            # Convert numpy arrays to Python types for JSON serialization
            def serialize_for_json(obj):
                if isinstance(obj, dict):
                    return {k: serialize_for_json(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [serialize_for_json(item) for item in obj]
                elif hasattr(obj, "tolist"):  # numpy array
                    return obj.tolist()
                elif hasattr(obj, "item"):  # numpy scalar
                    return obj.item()
                else:
                    return obj

            serializable_data = serialize_for_json(processed_data)

            with open(output_file, "w") as f:
                json.dump(serializable_data, f, indent=2)

            return {
                "chunk_id": chunk_id,
                "read_count": read_count,
                "processed_file": str(output_file),
                "processing_stats": processed_data.get("stats", {}),
            }

        return process_bam_chunk_op

    def _process_chunk(self, chunk: BamChunk) -> Dict[str, Any]:
        """
        Process a single BAM chunk.

        This is where you would implement your specific processing logic:
        - Quality filtering
        - Variant calling
        - Statistical analysis
        - Data transformation
        """
        reads = chunk.reads

        # Example: Calculate basic statistics
        total_reads = len(reads)
        mapped_reads = sum(1 for read in reads if not read.get("is_unmapped", True))
        avg_quality = (
            sum(read.get("mapping_quality", 0) for read in reads) / total_reads
            if total_reads > 0
            else 0
        )

        # Example: Extract read positions (simplified)
        positions = [read.get("pos", 0) for read in reads if read.get("pos", 0) > 0]

        stats = {
            "total_reads": total_reads,
            "mapped_reads": mapped_reads,
            "unmapped_reads": total_reads - mapped_reads,
            "avg_mapping_quality": round(avg_quality, 2),
            "position_range": {
                "min": min(positions) if positions else 0,
                "max": max(positions) if positions else 0,
            },
            "chunk_info": {
                "chunk_id": chunk.chunk_id,
                "total_chunks": chunk.total_chunks,
                "bam_url": chunk.bam_url,
            },
        }

        return {
            "stats": stats,
            "reads": reads,  # Include processed reads if needed
        }
