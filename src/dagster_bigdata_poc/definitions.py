from dagster import definitions, Definitions
from dagster.components.core.component_tree import ComponentTree

from .components.bam_chunk_processor import BamChunkProcessor
from .components.bam_chunk_streamer import BamChunkStreamer
from .components.bam_file_sensor import BamFileSensor


# This is great, but we can allow much better typing if we have one orchestrator I believe.


@definitions
def defs():
    # Create multiple BAM ingestion pipelines with unique names
    pipelines = []

    hg00096_streamer = BamChunkStreamer(
        name="hg00096",
        bam_url="https://s3.amazonaws.com/1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam",
        chunk_size=1000,
    )
    hg00096_processor = BamChunkProcessor(
        name="hg00096",
    )
    pipelines.extend([hg00096_streamer, hg00096_processor])

    # Create sensors for each pipeline
    sensors = []
    hg00096_sensor = BamFileSensor(
        name="hg00096",  # Must match the streamer name
        bam_url="https://s3.amazonaws.com/1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam",
        minimum_interval_seconds=60,
    )
    sensors.append(hg00096_sensor)

    # Build definitions from all components
    context = ComponentTree.for_test().load_context
    return Definitions(
        assets=[component.build_defs(context) for component in pipelines],
        sensors=[sensor.build_defs(context) for sensor in sensors],
    )
