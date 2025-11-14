import sys


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


def parse_sam_line(line):
    """Parse a SAM line into a Read object."""
    fields = line.strip().split("\t")
    if len(fields) < 11:
        return None
    qname = fields[0]
    flag = int(fields[1])
    rname = fields[2]
    pos = int(fields[3]) - 1  # SAM is 1-based, convert to 0-based
    mapq = int(fields[4])
    cigar = fields[5]
    rnext = fields[6]
    pnext = int(fields[7]) if fields[7] != "*" else 0
    tlen = int(fields[8])
    seq = fields[9]
    qual = fields[10]
    return Read(qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual)


def chunked_reads_from_sam(chunk_size=1000, max_chunks=5):
    """Generator that yields chunks of reads from SAM stdin."""
    chunk = []
    for line in sys.stdin:
        if line.startswith("@"):  # Skip header lines
            continue
        read_obj = parse_sam_line(line)
        if read_obj:
            chunk.append(read_obj)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
                max_chunks -= 1
                if max_chunks <= 0:
                    break
    if chunk:
        yield chunk


# Process in chunks from stdin
for i, chunk in enumerate(chunked_reads_from_sam(chunk_size=10, max_chunks=5)):
    print(f"Chunk {i + 1}: {len(chunk)} reads")
    for read in chunk[:3]:  # Print first 3 in each chunk
        print(
            f"  Read: {read.qname}, Position: {read.pos}, Sequence: {read.seq[:20]}..."
        )
    print()  # Blank line between chunks
