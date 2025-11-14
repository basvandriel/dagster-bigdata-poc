import pysam

# Open the BAM file
bamfile = pysam.AlignmentFile(
    "HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam", "rb"
)


# Define a simple domain model for a read
class Read:
    def __init__(
        self, qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
    ):
        self.qname = qname  # Query name
        self.flag = flag  # Flag
        self.rname = rname  # Reference name
        self.pos = pos  # Position
        self.mapq = mapq  # Mapping quality
        self.cigar = cigar  # CIGAR string
        self.rnext = rnext  # Next reference
        self.pnext = pnext  # Next position
        self.tlen = tlen  # Template length
        self.seq = seq  # Sequence
        self.qual = qual  # Quality


def chunked_reads(bamfile, chunk_size=1000):
    """Generator that yields chunks of reads."""
    chunk = []
    for read in bamfile:
        read_obj = Read(
            qname=read.qname,
            flag=read.flag,
            rname=bamfile.get_reference_name(read.reference_id)
            if read.reference_id is not None
            else None,
            pos=read.pos,
            mapq=read.mapq,
            cigar=read.cigarstring,
            rnext=bamfile.get_reference_name(read.next_reference_id)
            if read.next_reference_id is not None
            else None,
            pnext=read.pnext,
            tlen=read.template_length,
            seq=read.seq,
            qual=read.qual,
        )
        chunk.append(read_obj)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


# Process in chunks
for i, chunk in enumerate(chunked_reads(bamfile, chunk_size=10)):
    print(f"Chunk {i + 1}: {len(chunk)} reads")
    for read in chunk[:3]:  # Print first 3 in each chunk
        print(
            f"  Read: {read.qname}, Position: {read.pos}, Sequence: {read.seq[:20]}..."
        )
    if i >= 4:  # Limit to 5 chunks for demo
        break

bamfile.close()
