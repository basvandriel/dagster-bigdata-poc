"""
BAM Processing Pipeline Component

A composite component that combines streaming and processing components into a complete pipeline.
"""

import dagster
from dagster import job, Model, Resolvable

from .bam_chunk_processor import BamChunkProcessor
from .bam_chunk_streamer import BamChunkStreamer
from .bam_file_sensor import BamFileSensor


class BamProcessingPipeline(Model, Resolvable):
    """
    Complete BAM processing pipeline component.

    This composite component brings together the streamer and processor components
    to create a fully functional streaming BAM analysis pipeline with configurable persistence.

    Uses dependency injection for loose coupling - components are injected rather than instantiated.
    """

    # Component dependencies - loosely coupled through dependency injection
    streamer: BamChunkStreamer
    processor: BamChunkProcessor
    sensor: BamFileSensor

    def build_defs(self, context):
        # Get definitions from the injected components
        load_bam_chunks_op = self.streamer.build_defs(context)
        process_bam_chunk_op = self.processor.build_defs(context)
        bam_file_sensor = self.sensor.build_defs(context)

        # Define the job that orchestrates the components
        @job
        def bam_processing_job():
            """
            Job that orchestrates the streaming BAM processing pipeline.

            Uses the injected streamer and processor components to handle
            the data flow from streaming to persistence.
            """
            chunks = load_bam_chunks_op()
            chunks.map(process_bam_chunk_op)

        return dagster.Definitions(
            assets=[],
            jobs=[bam_processing_job],
            sensors=[bam_file_sensor],
        )
