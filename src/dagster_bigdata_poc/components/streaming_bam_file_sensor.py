"""
Streaming BAM File Sensor Component

A sensor component that detects new BAM files and triggers streaming jobs.
"""

import time
import dagster
from dagster import RunRequest, sensor
from .stream_bam import BamStats


class BamFileSensor(dagster.Model, dagster.Resolvable):
    """
    Sensor component for triggering streaming BAM jobs.

    This component monitors for BAM files to process and triggers streaming jobs
    with the appropriate BAM URLs, similar to the original asset-based approach.
    """

    name: str = "bam_file_sensor"
    bam_urls: list[str] = []
    job_name: str = "streaming_bam_job"
    minimum_interval_seconds: int = 30

    def build_defs(self, context):
        @sensor(
            name=self.name,
            job_name=self.job_name,  # Use configurable job name
            minimum_interval_seconds=30,  # Check every 30 seconds
        )
        def bam_file_sensor_fn(context):
            """
            Sensor that triggers streaming jobs for configured BAM URLs.

            This sensor:
            1. Checks configured BAM URLs
            2. Identifies URLs that haven't been processed yet
            3. Calculates total chunks for each URL
            4. Triggers individual jobs for each chunk
            5. Tracks processed URLs to avoid duplicates
            """
            if not self.bam_urls:
                context.log.info("No BAM URLs configured for sensor")
                return

            context.log.info(f"Checking {len(self.bam_urls)} BAM URLs for processing")

            # Get cursor (tracks processed URLs)
            if context.cursor is None:
                context.update_cursor("[]")  # Empty JSON array as string

            import json

            processed_urls = (
                set(json.loads(context.cursor)) if context.cursor else set()
            )

            new_urls = []
            for bam_url in self.bam_urls:
                url_id = bam_url  # Use full URL as unique ID

                if url_id not in processed_urls:
                    new_urls.append(bam_url)

            if not new_urls:
                context.log.info("No new BAM URLs to process")
                return

            context.log.info(f"Found {len(new_urls)} new BAM URLs to process")

            # Yield run requests for each new URL - one per chunk
            for bam_url in new_urls:
                url_id = bam_url

                # Calculate total chunks for this BAM file
                try:
                    stats = BamStats.from_url(bam_url)
                    context.log.info(
                        f"📊 BAM file {bam_url}: {stats.total_reads:,} reads"
                    )
                except Exception as e:
                    context.log.error(f"❌ Failed to analyze BAM file {bam_url}: {e}")
                    continue

                # Launch one job per BAM file (streaming handles all chunks internally)
                # Use timestamp to make run_key unique
                run_key = f"{url_id}_{int(time.time())}"

                context.log.info(
                    f"🎯 Triggering streaming job for: {bam_url} ({stats.total_reads:,} reads)"
                )

                yield RunRequest(
                    run_key=run_key,
                    run_config={
                        "inputs": {
                            "bam_url": bam_url,
                        }
                    },
                    tags={
                        "url_id": url_id,
                        "bam_url": bam_url,
                        "job_type": "streaming_bam_file",
                    },
                )

                # Mark as processed (disabled for testing)
                # processed_urls.add(url_id)
                # context.update_cursor(json.dumps(list(processed_urls)))

        return bam_file_sensor_fn
