from dagster import definitions, Definitions, job
from dagster.components.core.component_tree import ComponentTree

from .components.streaming_bam_chunk_streamer import StreamingBamChunkStreamer
from .components.streaming_bam_chunk_processor import StreamingBamChunkProcessor
from .components.streaming_bam_file_sensor import (
    BamFileSensor as StreamingBamFileSensor,
)

# TODO allow for better typing
# The streaming bam chunk processor should only work with streaming bam chunks


@definitions
def defs():
    context = ComponentTree.for_test().load_context

    # Create streaming components that depend on each other
    streaming_streamer = StreamingBamChunkStreamer()
    streaming_processor = StreamingBamChunkProcessor()

    streaming_sensor = StreamingBamFileSensor(
        name="streaming_bam_sensor",
        bam_urls=[
            "https://s3.amazonaws.com/1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam"
        ],
        job_name="streaming_bam_job",
    )

    # Get the ops from the components
    stream_op = streaming_streamer.build_defs(context)
    process_op = streaming_processor.build_defs(context)
    sensor_def = streaming_sensor.build_defs(context)

    # Create the streaming job by composing the ops directly
    @job(name="streaming_bam_job")
    def streaming_bam_job(bam_url: str):
        """Job that streams and processes BAM chunks without orchestration layer."""
        # Connect streamer output to processor input using map
        stream_op(bam_url).map(process_op)

    return Definitions(sensors=[sensor_def], jobs=[streaming_bam_job])
