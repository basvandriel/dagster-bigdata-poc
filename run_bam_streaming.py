#!/usr/bin/env python3

import time
from dagster_bigdata_poc.components.stream_bam import stream_bam_chunks


def main():
    """Run BAM file streaming."""

    # BAM URL (HG00096 from 1000 Genomes)
    bam_url = "https://s3.amazonaws.com/1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam"
    chunk_size = 1000

    try:
        start_time = time.time()

        chunks_processed = 0
        total_reads_processed = 0

        for chunk in stream_bam_chunks(bam_url, chunk_size=chunk_size):
            chunks_processed += 1
            chunk_reads = len(chunk)
            total_reads_processed += chunk_reads

            print(f"   Processed chunk {chunks_processed} with {chunk_reads} reads")

            # For demo purposes, stop after a few chunks
            if chunks_processed >= 5:
                print("   ... (stopping demo after 5 chunks)")
                break

        elapsed = time.time() - start_time
        print(f"✅ Streaming demo complete in {elapsed:.2f} seconds!")

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    main()
