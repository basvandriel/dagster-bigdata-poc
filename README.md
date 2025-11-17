# Streaming Big Data Files in Dagster: How I stopped loading Gigabytes into RAM

I was building a data pipeline with [Dagster](https://dagster.io/) for bioinformatical information. In this case, I worked with BAM files: compressed DNA sequencing data. These can easily go up terabytes in size - millions of DNA reads, each representing a tiny part of someone's genome.

The goal of the pipeline was to ingest these into any file-based database. However, with limited resources (512 MB RAM, 1 CPU) this obviously was a problem. You can't just load the entire file into memory, that doesn't scale. 

TLDR: We're streaming it.


## Test case

I'm using a file with **2.9 million DNA reads** coming in around `~300 Mb` - just a tiny slice of what real genomic datasets look like. The requirements:
- Handle big files without overloading the RAM
- Keep processing speed reasonable
- Use [Dagster's modern component system](https://docs.dagster.io/guides/build/components)
- Set it up so new files get processed automatically

---

## Attempt 1: using [`DynamicOut`](https://docs.dagster.io/api/dagster/dynamic)

I started with Dagster's dynamic outputs, which seemed perfect for streaming. The idea was to yield chunks one at a time and process them downstream.

```python
@op(out=DynamicOut())
def stream_bam_chunks_op(context, bam_url: str):
    for chunk in stream_bam_chunks(bam_url):
        yield DynamicOutput(chunk, mapping_key=f"chunk_{i}")

@op
def process_chunk_op(context, chunk: BamChunk):
    return process(chunk)

@op
def streaming_job(bam_url: str):
    chunks = stream_bam_chunks_op(bam_url)
    process_chunk_op.map(chunks)
```

**What actually happened:** Dagster collected all the dynamic outputs before even starting the downstream processing. For our test file with 2,932 chunks, this meant the entire file got loaded into memory anyway. Back to the drawing board 😅


## Attempt 2: One job run per chunk

What if there were separate jobs for each chunk? That way, each job only handles one chunk.

```python
# Sensor launches 2932 jobs (one per chunk)
yield RunRequest(
    run_key=f"{bam_url}_chunk_{chunk_index}",
    run_config={"inputs": {"bam_url": bam_url, "chunk_index": chunk_index}}
)

@job
def streaming_job(bam_url: str, chunk_index: int):
    chunk = stream_single_chunk_op(bam_url, chunk_index)
    process_chunk_op(chunk)
```

**The good:** Each job only processes one chunk, so memory usage stays low.

**The bad:** For our test file, this created 2,932 separate jobs. Quite a bit of overhead. Plus, how would we launch one ingestion run?

## The solution: Single-Op Streaming

Instead of using multiple jobs runs or multiple operations, just use one `op`. In here, we can use the iteration result from `stream_bam_chunks` and for every chunk, save it to our file system.

```python
@op
def stream_and_process_op(context, bam_url: str):
    for chunk_reads in stream_bam_chunks(bam_url):  # Generator yields chunks
        # Process chunk immediately - no buffering!
        serialize_reads(chunk_reads)
        save_to_file(processed_result)
        results.append(metadata)  # Only metadata, not chunk data

    return results
```

Python generators are lazy. They yield one chunk at a time. Process it immediately, then let the generator forget about it. No buffering required.

```python
def stream_bam_chunks(bam_url: str, chunk_size: int = 1000):
    samfile = pysam.AlignmentFile(bam_url, "rb")
    chunk = []

    try:
        for read in samfile:
            chunk.append(read)
            if len(chunk) >= chunk_size:
                yield chunk  # Hand chunk to consumer
                chunk = []   # Start fresh - memory freed

        if chunk:  # Don't forget the last few reads
            yield chunk
    finally:
        samfile.close()
```

This means the following.

- **Only one chunk exists at a time** - When we yield a chunk, the generator pauses. The consumer processes it, then the generator resumes and creates a new empty chunk.
- **Memory gets freed immediately** - No holding onto old chunks
- **File handles are managed properly** - The `finally` block ensures cleanup
---

## Performance

I tested this with our 2.9 million read BAM file. Here's what happened:

- **Memory usage:** Rock solid at ~5MB regardless of file size
- **Processing speed:** ~34,000 reads per second (sequential)
- **Scalability:** Would work identically for a 100GB file

**Memory scaling - the dream scenario:**
- 1GB file: ~5MB RAM
- 10GB file: ~5MB RAM
- 100GB file: ~5MB RAM

---

## Lessons learned 

1. **Dagster's dynamic outputs aren't truly streaming** - They buffer everything first.
2. **Python generators are your friend** - They enable true streaming with minimal memory overhead.
3. **Keep it simple** - One op handling streaming + processing beat complex multi-op architectures.
4. **Test with real data** - Our 2.9M read file caught issues that toy examples wouldn't have.

## What's next

- **Advanced monitoring:** Add memory metrics and measure time
- **Database integration:** Direct streaming into analytical databases

---

## The code

Everything I built is open source at:
[github.com/basvandriel/dagster-bigdata-poc](https://github.com/basvandriel/dagster-bigdata-poc)

Key files to check out:
- `src/dagster_bigdata_poc/components/streaming_bam_chunk_streamer.py` - Streaming component implementation
- `src/dagster_bigdata_poc/components/stream_bam.py` - Core streaming logic and BAM utilities
- `src/dagster_bigdata_poc/definitions.py` - Dagster job and sensor definitions


*Published: November 2025*
*Author: Bas van Driel*
*Tags: dagster, big-data, streaming, bioinformatics, python*