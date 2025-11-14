# Streaming Ops Demo

This module demonstrates **true streaming** BAM processing using Dagster ops with dynamic outputs, compared to the asset-based approach that buffers everything in memory.

## The Problem with Assets

Assets require complete materialization - they can't yield incrementally. For large BAM files, this means buffering millions of chunks in memory:

```python
@asset
def stream_chunks() -> List[Chunk]:
    chunks = []  # Grows to millions of items
    for chunk in generate_chunks():
        chunks.append(chunk)  # Memory usage keeps growing
    return chunks  # Only then can downstream assets start
```

## The Solution: Ops with Dynamic Outputs

Ops can yield results incrementally using `DynamicOut`, enabling true streaming:

```python
@op(out=DynamicOut())
def stream_chunks_op():
    for chunk in generate_chunks():
        yield DynamicOutput(chunk, mapping_key=f"chunk_{i}")
        # Memory freed immediately after each yield!
```

## How Sensors Trigger Streaming Jobs

Sensors detect new BAM files and trigger streaming jobs:

```python
@sensor(job=streaming_bam_job)
def bam_file_sensor(context):
    # Detect new BAM files...
    for bam_url in new_files:
        yield dagster.RunRequest(
            run_key=file_id,
            run_config={
                "ops": {
                    "stream_bam_chunks_op": {
                        "inputs": {"bam_url": bam_url, "chunk_size": 500}
                    }
                }
            }
        )
```

## Memory Comparison

| Approach | Memory Usage | Processing |
|----------|-------------|-----------|
| **Assets** | O(total_chunks) | Batch processing |
| **Ops + Dynamic Outputs** | O(1) | True streaming |

## Usage

```bash
# Start Dagster daemon
dagster sensor daemon start

# The sensor will automatically detect new BAM files and trigger streaming jobs
# Each job processes chunks as they're yielded, with minimal memory usage
```

## For Production Scale

For terabyte BAM files, use the ops-based approach. The asset approach only works for moderate-sized files where the chunk list fits in memory.