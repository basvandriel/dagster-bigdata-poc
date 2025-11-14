"""
BAM File Sensor Component

A reusable sensor component that detects available BAM files.
"""

from typing import Iterator

import dagster
from dagster import RunRequest, SensorEvaluationContext, sensor

from dagster_bigdata_poc.components.stream_bam import BamStats


class BamFileSensor(dagster.Model, dagster.Resolvable):
    """
    Sensor component for detecting BAM file availability.

    This component can be configured to monitor specific BAM URLs
    or check multiple sources for available files.
    """

    name: str  # Unique identifier matching the streamer name
    bam_url: str
    minimum_interval_seconds: int = 60

    def build_defs(self, context):
        @sensor(
            name=f"{self.name}_bam_file_sensor",  # Unique sensor name per pipeline
            asset_selection=[f"{self.name}_chunks"],  # Target the specific chunks asset
            minimum_interval_seconds=self.minimum_interval_seconds,
        )
        def bam_file_sensor(context: SensorEvaluationContext) -> Iterator[RunRequest]:
            """
            Sensor that detects when the BAM file is available and triggers asset materialization.

            Triggers materialization of the specific chunks asset when a BAM file becomes available.
            In a real scenario, this could check multiple URLs or a database for available BAM files.
            """
            try:
                # Quick check if the BAM file is accessible by getting stats
                stats = BamStats.from_url(self.bam_url)

                context.log.info(f"BAM file detected: {self.bam_url}")
                context.log.info(f"Total reads: {stats.total_reads:,}")
                context.log.info(f"References: {stats.num_references}")

                # Yield a run request to materialize the specific chunks asset
                yield RunRequest(
                    asset_selection=[
                        f"{self.name}_chunks"
                    ],  # Target the specific chunks asset
                    tags={"bam_url": self.bam_url, "pipeline": self.name},
                )

            except Exception as e:
                context.log.debug(f"BAM file not available: {e}")
                return

        return bam_file_sensor
