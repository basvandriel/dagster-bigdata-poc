#!/usr/bin/env python3
"""
Demo script for streaming ops approach.

This shows how sensors can trigger streaming jobs that process BAM files
without buffering everything in memory.
"""

from src.dagster_bigdata_poc.streaming_ops_demo import (
    streaming_bam_job,
    bam_file_sensor,
)


def demo_streaming_concept():
    """Demo the streaming concept and show how sensors work."""
    print("🚀 Streaming Ops Demo")
    print("=" * 50)

    print("\n📋 Available Components:")
    print(f"   Job: {streaming_bam_job.name}")
    print(f"   Sensor: {bam_file_sensor.name}")

    print("\n🔄 How Streaming Works:")
    print("   1. Sensor detects new BAM files")
    print("   2. Sensor triggers streaming job")
    print("   3. Job streams chunks without buffering")
    print("   4. Each chunk processed as it's yielded")

    print("\n💡 Key Difference from Assets:")
    print("   Assets: Buffer ALL chunks → High memory usage")
    print("   Ops: Yield chunks incrementally → Low memory usage")

    print("\n📊 Memory Comparison:")
    print("   Asset approach: O(total_chunks) memory")
    print("   Ops approach: O(1) memory (constant)")

    print("\n✅ For production terabyte files: Use ops with dynamic outputs")
    print("✅ For development/small files: Asset approach works fine")

    print("\n🎯 To run streaming jobs:")
    print("   1. Start sensor daemon: dagster sensor daemon start")
    print("   2. Sensors automatically trigger jobs for new BAM files")
    print("   3. Monitor progress in Dagster UI")


if __name__ == "__main__":
    demo_streaming_concept()
