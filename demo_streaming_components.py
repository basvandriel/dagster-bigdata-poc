#!/usr/bin/env python3
"""
Demo script for the new streaming BAM components.

This script demonstrates the ops-based streaming components that can handle
terabyte-scale BAM files without memory buffering.
"""

import sys
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dagster_bigdata_poc.definitions import defs


def main():
    """Demonstrate the streaming components."""
    print("🎬 Streaming BAM Components Demo")
    print("=" * 50)

    # Load the definitions
    d = defs()

    print("✓ Component definitions loaded successfully!")
    print(f"📊 Assets: {len(d.assets)}")
    print(f"👁️  Sensors: {len(d.sensors)}")
    print(f"⚙️  Jobs: {len(d.jobs)}")

    print("\n📦 Assets:")
    for asset in d.assets:
        print(f"  • {asset.key}")

    print("\n👁️  Sensors:")
    for sensor in d.sensors:
        print(f"  • {sensor.name}")

    print("\n⚙️  Jobs:")
    for job in d.jobs:
        print(f"  • {job.name}")

    print("\n🎯 Streaming Components Overview:")
    print("  • StreamingBamFileSensor: Monitors configured BAM URLs and triggers jobs")
    print(
        "  • StreamingBamChunkStreamer: Streams BAM chunks from URLs without buffering"
    )
    print("  • StreamingBamChunkProcessor: Processes chunks incrementally")
    print(
        "  • Direct Dependencies: Components depend on each other directly (no orchestrator)"
    )

    print("\n🚀 Key Features:")
    print("  • True streaming: No memory buffering for large files")
    print("  • Dynamic outputs: Incremental processing with ops")
    print("  • Sensor-triggered: Automatic job execution on new files")
    print("  • Scalable: Handles terabyte BAM files efficiently")

    print("\n💡 Usage:")
    print("  1. Configure sensor with BAM URLs to process")
    print("  2. Start Dagster: dagster dev")
    print("  3. Sensor will automatically trigger streaming jobs for configured URLs")
    print("  4. Watch as streaming jobs process BAM files from URLs without buffering!")

    print("\n✅ Demo completed successfully!")


if __name__ == "__main__":
    main()
