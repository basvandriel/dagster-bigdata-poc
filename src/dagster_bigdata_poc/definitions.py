from dagster import definitions
from dagster.components.core.component_tree import ComponentTree

from .components.bam_processing_pipeline import BamProcessingPipeline
from .components.bam_chunk_processor import BamChunkProcessor
from .components.bam_chunk_streamer import BamChunkStreamer
from .components.bam_file_sensor import BamFileSensor


@definitions
def defs():
    # Create individual components with their configurations
    streamer = BamChunkStreamer(
        bam_url="https://s3.amazonaws.com/1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam",
        chunk_size=1000,
    )

    processor = BamChunkProcessor(
        persistence_backend="logging",  # Can be changed to "neo4j" or "mysql"
    )

    sensor = BamFileSensor(
        bam_url="https://s3.amazonaws.com/1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam",
        minimum_interval_seconds=60,
    )

    # Inject components into the pipeline - loose coupling
    pipeline_component = BamProcessingPipeline(
        streamer=streamer, processor=processor, sensor=sensor
    )

    # Build definitions from the component
    context = ComponentTree.for_test().load_context
    return pipeline_component.build_defs(context)
