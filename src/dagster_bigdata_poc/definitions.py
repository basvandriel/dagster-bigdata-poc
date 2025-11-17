import logging

from dagster import definitions, Definitions, job
from dagster.components.core.component_tree import ComponentTree

from .components.streaming_bam_chunk_streamer import StreamingBamChunkStreamer
from .components.streaming_bam_file_sensor import (
    BamFileSensor as StreamingBamFileSensor,
)

# Configure logging to reduce verbosity - set at the very beginning
logging.basicConfig(level=logging.ERROR, force=True)
logging.getLogger().setLevel(logging.ERROR)
logging.getLogger("dagster").setLevel(logging.ERROR)
logging.getLogger("dagster._core").setLevel(logging.ERROR)
logging.getLogger("dagster._core.executor").setLevel(logging.ERROR)
logging.getLogger("dagster._core.execution").setLevel(logging.ERROR)

# TODO allow for better typing
# The streaming bam chunk processor should only work with streaming bam chunks


@definitions
def defs():
    context = ComponentTree.for_test().load_context

    # Create streaming components
    streaming_streamer = StreamingBamChunkStreamer(max_chunks=20)

    streaming_sensor = StreamingBamFileSensor(
        name="streaming_bam_sensor",
        bam_urls=[
            "https://s3.amazonaws.com/1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam"
        ],
        job_name="streaming_bam_job",
    )

    # Get the ops from the components
    stream_and_process_op = streaming_streamer.build_defs(context)
    sensor_def = streaming_sensor.build_defs(context)

    # Create the streaming job using the combined streaming and processing op
    @job(name="streaming_bam_job")
    def streaming_bam_job(bam_url: str):
        """Job that streams and processes all BAM chunks sequentially in one op."""
        # Single op handles streaming and processing
        stream_and_process_op(bam_url)

    return Definitions(sensors=[sensor_def], jobs=[streaming_bam_job])
