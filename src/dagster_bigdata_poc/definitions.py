from dagster import definitions, Definitions
from dagster.components.core.component_tree import ComponentTree

from .components.streaming_bam_chunk_streamer import StreamingBamChunkStreamer
from .components.streaming_bam_file_sensor import (
    BamFileSensor as StreamingBamFileSensor,
)


@definitions
def defs():
    context = ComponentTree.for_test().load_context

    # Create streaming components
    streaming_streamer = StreamingBamChunkStreamer()

    streaming_sensor = StreamingBamFileSensor(
        name="streaming_bam_sensor",
        bam_urls=[
            "https://s3.amazonaws.com/1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam"
        ],
        job_name="streaming_bam_chunk_streamer_job",  # Updated to match the component's job name
    )

    # Get the job from the component (it now returns the complete job)
    streaming_job = streaming_streamer.build_defs(context)
    sensor_def = streaming_sensor.build_defs(context)

    return Definitions(sensors=[sensor_def], jobs=[streaming_job])
