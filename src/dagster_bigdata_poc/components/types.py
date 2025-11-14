"""
Shared types for BAM processing components.
"""

from dataclasses import dataclass
from typing import List


@dataclass
class BamChunk:
    """Represents a chunk of BAM reads with metadata."""

    chunk_id: int
    total_chunks: int
    reads: List[dict]  # Serialized BAM reads
    bam_url: str
