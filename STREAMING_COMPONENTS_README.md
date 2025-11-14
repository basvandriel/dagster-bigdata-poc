# Streaming BAM Components

This directory contains the new ops-based streaming components for processing large BAM files without memory buffering.

## Components

### 1. StreamingBamFileSensor
**File:** `streaming_bam_file_sensor.py`

A sensor component that monitors configured BAM URLs and triggers streaming jobs.

**Features:**
- Configurable list of BAM URLs to process
- Cursor-based tracking of processed URLs
- Automatic job triggering with run configurations
- Similar to original asset-based approach but for ops

**Configuration:**
```python
sensor = StreamingBamFileSensor(
    name="my_bam_sensor",
    bam_urls=[
        "https://s3.amazonaws.com/1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam"
    ],
    job_name="streaming_bam_job"
)
```

### 2. StreamingBamChunkStreamer
**File:** `streaming_bam_chunk_streamer.py`

An ops-based component that streams BAM chunks using Dagster's dynamic outputs, enabling true streaming without memory buffering.

**Features:**
- Dynamic output yielding for incremental processing
- Configurable chunk sizes
- Memory-efficient streaming for terabyte files
- Rich metadata for each chunk

**Configuration:**
```python
streamer = StreamingBamChunkStreamer(
    name="bam_streamer",
    chunk_size=1000
)
```

### 3. StreamingBamChunkProcessor
**File:** `streaming_bam_chunk_processor.py`

An ops-based component that processes streaming BAM chunks incrementally as they arrive.

**Features:**
- Incremental processing without waiting for all chunks
- Configurable output directory
- Statistical analysis and quality metrics
- JSON output for processed chunks

**Configuration:**
```python
processor = StreamingBamChunkProcessor(
    name="bam_processor",
    output_directory="/data/processed"
)
```

### 4. Direct Job Composition
**File:** `definitions.py`

Instead of a separate orchestrator component, the streaming components depend on each other directly. The job is created by composing the ops from the individual components in the definitions file.

**Features:**
- No orchestrator layer - components reference each other directly
- Cleaner architecture with direct dependencies
- Job composition happens in definitions.py

## Usage

### Basic Setup

```python
from dagster_bigdata_poc.components.streaming_bam_file_sensor import BamFileSensor
from dagster_bigdata_poc.components.streaming_bam_chunk_streamer import StreamingBamChunkStreamer
from dagster_bigdata_poc.components.streaming_bam_chunk_processor import StreamingBamChunkProcessor
from dagster import job

# Create the streaming components
streaming_sensor = BamFileSensor(
    name="my_bam_sensor",
    bam_urls=[
        "https://s3.amazonaws.com/1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam"
    ],
    job_name="streaming_bam_job"
)
streaming_streamer = StreamingBamChunkStreamer(chunk_size=1000)
streaming_processor = StreamingBamChunkProcessor(output_directory="/data/processed")

# Get the ops from components
stream_op = streaming_streamer.build_defs(context)
process_op = streaming_processor.build_defs(context)

# Create job by composing ops directly
@job(name="streaming_bam_job")
def streaming_bam_job():
    stream_op().map(process_op)

# Build sensor definition
sensor_def = streaming_sensor.build_defs(context)
```

### Running the Pipeline

1. **Configure BAM URLs** in the sensor component
2. **Start Dagster**: `dagster dev`
3. **Sensor automatically triggers** streaming jobs for configured URLs
4. **Watch streaming jobs** process BAM files from URLs without buffering

### Manual Job Execution

You can also run the streaming job manually:

```bash
dagster job execute -f streaming_bam_job --config '{
  "ops": {
    "stream_bam_chunks_op": {
      "inputs": {
        "bam_url": "file:///path/to/your/file.bam",
        "chunk_size": 1000
      }
    }
  }
}'
```

## Key Differences from Asset-Based Components

| Feature | Asset-Based | Ops-Based (Streaming) |
|---------|-------------|----------------------|
| Memory Usage | Buffers all chunks | True streaming |
| Scalability | Limited by RAM | Handles terabyte files |
| Processing | Batch (all chunks at once) | Incremental (chunk by chunk) |
| Dynamic Outputs | No | Yes |
| Sensor Integration | Limited | Full automation |

## Architecture

```
BAM File Directory
        ↓
StreamingBamFileSensor (detects new files, references job by name)
        ↓
Job Composition (in definitions.py)
        ↓
StreamingBamChunkStreamer (yields chunks dynamically)
        ↓
StreamingBamChunkProcessor (processes each chunk)
        ↓
Processed Output Files
```

**Direct Dependencies:**
- Sensor references job by configurable name
- Streamer and processor ops are composed directly in job definition
- No orchestrator component - components depend on each other through job composition

## Benefits

- **Memory Efficient:** No buffering means unlimited file size support
- **Fast Processing:** Incremental processing starts immediately
- **Automated:** Sensors trigger jobs automatically on new files
- **Scalable:** Dynamic outputs enable parallel processing
- **Observable:** Rich logging and metadata for monitoring

## Configuration Options

### Sensor Configuration
- `bam_directory`: Directory to monitor for BAM files
- `file_pattern`: Glob pattern for matching files (default: `*.bam`)
- `name`: Unique sensor name

### Streamer Configuration
- `chunk_size`: Number of reads per chunk (default: 1000)
- `name`: Unique streamer name

### Processor Configuration
- `output_directory`: Where to save processed chunks
- `name`: Unique processor name

## Example Output

Each processed chunk generates a JSON file with statistics:

```json
{
  "stats": {
    "total_reads": 1000,
    "mapped_reads": 950,
    "unmapped_reads": 50,
    "avg_mapping_quality": 45.2,
    "position_range": {
      "min": 1000000,
      "max": 2000000
    }
  },
  "chunk_info": {
    "chunk_id": 1,
    "total_chunks": 50000,
    "bam_url": "file:///data/sample.bam"
  }
}
```

## Dependencies

- `dagster`: For ops, jobs, and sensors
- `pysam`: For BAM file processing
- `pathlib`: For file path handling
- `json`: For output serialization