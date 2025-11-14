"""
BAM Chunk Processor Component

A configurable component that processes BAM chunks and persists results to various backends.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import dagster
from dagster import AssetExecutionContext, AssetIn, asset

from .types import BamChunk


class PersistenceBackend(ABC):
    """Abstract base class for persistence backends."""

    @abstractmethod
    def persist_chunk_stats(self, stats: Dict[str, Any]) -> None:
        """Persist chunk statistics to the backend."""
        pass

    @abstractmethod
    def persist_read_data(self, chunk: BamChunk) -> None:
        """Persist individual read data to the backend."""
        pass


class LoggingBackend(PersistenceBackend):
    """Logs results to the console/logger."""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def persist_chunk_stats(self, stats: Dict[str, Any]) -> None:
        self.logger.info(f"Chunk stats: {stats}")

    def persist_read_data(self, chunk: BamChunk) -> None:
        self.logger.info(
            f"Processed chunk {chunk.chunk_id} with {len(chunk.reads)} reads"
        )


class Neo4jBackend(PersistenceBackend):
    """Persists to Neo4j database."""

    def __init__(self, uri: str, user: str, password: str):
        try:
            from neo4j import GraphDatabase

            self.driver = GraphDatabase.driver(uri, auth=(user, password))
        except ImportError:
            raise ImportError("neo4j package required for Neo4j backend")

    def persist_chunk_stats(self, stats: Dict[str, Any]) -> None:
        with self.driver.session() as session:
            session.run(
                """
                CREATE (c:ChunkStats {
                    chunk_id: $chunk_id,
                    total_chunks: $total_chunks,
                    reads_in_chunk: $reads_in_chunk,
                    mapped_reads: $mapped_reads,
                    unmapped_reads: $unmapped_reads,
                    avg_read_length: $avg_read_length,
                    bam_url: $bam_url
                })
                """,
                **stats,
            )

    def persist_read_data(self, chunk: BamChunk) -> None:
        with self.driver.session() as session:
            for read in chunk.reads:
                session.run(
                    """
                    CREATE (r:Read {
                        query_name: $query_name,
                        flag: $flag,
                        reference_id: $reference_id,
                        reference_start: $reference_start,
                        mapping_quality: $mapping_quality,
                        cigarstring: $cigarstring,
                        query_sequence: $query_sequence,
                        chunk_id: $chunk_id
                    })
                    """,
                    **read,
                    chunk_id=chunk.chunk_id,
                )


class MySQLBackend(PersistenceBackend):
    """Persists to MySQL database."""

    def __init__(self, host: str, user: str, password: str, database: str):
        try:
            import mysql.connector

            self.connection = mysql.connector.connect(
                host=host, user=user, password=password, database=database
            )
            self._create_tables()
        except ImportError:
            raise ImportError(
                "mysql-connector-python package required for MySQL backend"
            )

    def _create_tables(self):
        cursor = self.connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chunk_stats (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chunk_id INT,
                total_chunks INT,
                reads_in_chunk INT,
                mapped_reads INT,
                unmapped_reads INT,
                avg_read_length FLOAT,
                bam_url VARCHAR(500)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reads (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chunk_id INT,
                query_name VARCHAR(255),
                flag INT,
                reference_id INT,
                reference_start INT,
                mapping_quality INT,
                cigarstring VARCHAR(500),
                query_sequence TEXT
            )
        """)
        self.connection.commit()
        cursor.close()

    def persist_chunk_stats(self, stats: Dict[str, Any]) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO chunk_stats
            (chunk_id, total_chunks, reads_in_chunk, mapped_reads, unmapped_reads, avg_read_length, bam_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
            (
                stats["chunk_id"],
                stats["total_chunks"],
                stats["reads_in_chunk"],
                stats["mapped_reads"],
                stats["unmapped_reads"],
                stats["avg_read_length"],
                stats["bam_url"],
            ),
        )
        self.connection.commit()
        cursor.close()

    def persist_read_data(self, chunk: BamChunk) -> None:
        cursor = self.connection.cursor()
        for read in chunk.reads:
            cursor.execute(
                """
                INSERT INTO reads
                (chunk_id, query_name, flag, reference_id, reference_start, mapping_quality, cigarstring, query_sequence)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    chunk.chunk_id,
                    read["query_name"],
                    read["flag"],
                    read["reference_id"],
                    read["reference_start"],
                    read["mapping_quality"],
                    read["cigarstring"],
                    read["query_sequence"],
                ),
            )
        self.connection.commit()
        cursor.close()


class BamChunkProcessor(dagster.Model, dagster.Resolvable):
    """
    Configurable component for processing BAM chunks with pluggable persistence backends.

    Supports multiple persistence strategies: logging, Neo4j, MySQL, etc.
    """

    name: str  # Unique identifier for this processor instance
    persistence_backend: str = "logging"  # Options: logging, neo4j, mysql
    neo4j_uri: Optional[str] = None
    neo4j_user: Optional[str] = None
    neo4j_password: Optional[str] = None
    mysql_host: Optional[str] = None
    mysql_user: Optional[str] = None
    mysql_password: Optional[str] = None
    mysql_database: Optional[str] = None

    def _create_backend(self) -> PersistenceBackend:
        """Create the appropriate persistence backend based on configuration."""
        if self.persistence_backend == "logging":
            return LoggingBackend()
        elif self.persistence_backend == "neo4j":
            if not all([self.neo4j_uri, self.neo4j_user, self.neo4j_password]):
                raise ValueError("Neo4j configuration incomplete")
            return Neo4jBackend(self.neo4j_uri, self.neo4j_user, self.neo4j_password)
        elif self.persistence_backend == "mysql":
            if not all(
                [
                    self.mysql_host,
                    self.mysql_user,
                    self.mysql_password,
                    self.mysql_database,
                ]
            ):
                raise ValueError("MySQL configuration incomplete")
            return MySQLBackend(
                self.mysql_host,
                self.mysql_user,
                self.mysql_password,
                self.mysql_database,
            )
        else:
            raise ValueError(f"Unknown persistence backend: {self.persistence_backend}")

    def build_defs(self, context):
        backend = self._create_backend()

        @asset(
            name=f"{self.name}_processed_chunks",  # Unique asset name based on instance
            ins={
                "chunks": AssetIn(f"{self.name}_chunks")
            },  # Reference the corresponding chunks asset
            compute_kind="bam_processing",
            description=f"Processes BAM chunks and persists to {self.persistence_backend} for {self.name}",
        )
        def process_bam_chunk(
            context: AssetExecutionContext, chunks: List[BamChunk]
        ) -> List[dict]:
            """
            Asset that processes individual BAM chunks and persists results.

            Consumes chunks from the streamer asset and produces processed statistics.
            The processing logic is configurable based on the persistence backend.
            """
            results = []

            for bam_chunk in chunks:
                context.log.info(
                    f"Processing chunk {bam_chunk.chunk_id}/{bam_chunk.total_chunks} with {self.persistence_backend} backend"
                )
                context.log.info(f"Chunk contains {len(bam_chunk.reads)} reads")

                # Calculate statistics
                mapped_count = sum(
                    1 for read in bam_chunk.reads if not (read["flag"] & 4)
                )
                unmapped_count = len(bam_chunk.reads) - mapped_count

                total_bases = sum(
                    len(read["query_sequence"])
                    for read in bam_chunk.reads
                    if read.get("query_sequence")
                )
                avg_read_length = (
                    total_bases / len(bam_chunk.reads) if bam_chunk.reads else 0
                )

                stats = {
                    "chunk_id": bam_chunk.chunk_id,
                    "total_chunks": bam_chunk.total_chunks,
                    "reads_in_chunk": len(bam_chunk.reads),
                    "mapped_reads": mapped_count,
                    "unmapped_reads": unmapped_count,
                    "avg_read_length": round(avg_read_length, 1),
                    "bam_url": bam_chunk.bam_url,
                }

                # Persist statistics
                try:
                    backend.persist_chunk_stats(stats)
                    context.log.info(
                        f"✓ Chunk {bam_chunk.chunk_id} statistics persisted to {self.persistence_backend}"
                    )
                except Exception as e:
                    context.log.error(f"✗ Failed to persist chunk statistics: {e}")
                    raise

                # Optionally persist individual read data (can be expensive for large datasets)
                if (
                    self.persistence_backend != "logging"
                ):  # Skip for logging to avoid spam
                    try:
                        backend.persist_read_data(bam_chunk)
                        context.log.info(
                            f"✓ Chunk {bam_chunk.chunk_id} read data persisted to {self.persistence_backend}"
                        )
                    except Exception as e:
                        context.log.error(f"✗ Failed to persist read data: {e}")
                        raise

                context.log.info(
                    f"Chunk {bam_chunk.chunk_id} processed: {mapped_count} mapped, {unmapped_count} unmapped reads"
                )
                context.log.info(f"Average read length: {avg_read_length:.1f} bases")

                results.append(stats)

            return results

        return process_bam_chunk
