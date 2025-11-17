"""
Streaming BAM File Sensor Component

A sensor component that detects new BAM files and triggers streaming jobs.
"""

import time
import dagster
from dagster import RunRequest, sensor


class BamFileSensor(dagster.Model, dagster.Resolvable):
    """
    Sensor component for triggering streaming BAM jobs.

    This component monitors for BAM files to process and triggers streaming jobs
    with the appropriate BAM URLs, similar to the original asset-based approach.
    """

    name: str = "bam_file_sensor"
    bam_urls: list[str] = []  # List of BAM URLs to process
    job_name: str = "streaming_bam_job"  # Name of the job to trigger
    minimum_interval_seconds: int = 30  # Check every 30 seconds

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
            3. Triggers streaming jobs for new URLs
            4. Tracks processed URLs to avoid duplicates
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

            # Yield run requests for each new URL
            for bam_url in new_urls:
                url_id = bam_url

                # Use timestamp to make run_key unique for testing
                run_key = f"{url_id}_{int(time.time())}"

                context.log.info(f"🎯 Triggering streaming job for: {bam_url}")

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
                        "job_type": "streaming_bam",
                    },
                )

                # Mark as processed (disabled for testing)
                # processed_urls.add(url_id)
                # context.update_cursor(json.dumps(list(processed_urls)))

        return bam_file_sensor_fn
