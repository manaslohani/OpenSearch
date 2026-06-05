/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Resolves the Parquet file that backs a Lucene segment's Parquet-resident doc values.
 *
 * <p>The composite indexing engine writes Parquet files into a {@code parquet/} data
 * directory using the deterministic name {@code _parquet_file_generation_<hexGeneration>.parquet}
 * (see {@code ParquetIndexingEngine.buildParquetFileName}). A Lucene segment is bound to its
 * Parquet file through one of two mechanisms, tried in order:
 *
 * <ol>
 *   <li><b>Segment attribute</b> — the composite codec (segment-level {@code DocValuesFormat})
 *       stamps {@link #PARQUET_FILE_ATTRIBUTE} on the {@code SegmentInfo} with the resolved
 *       file path. This is the authoritative binding once composite routing (task 10) is wired.</li>
 *   <li><b>Directory scan</b> — when no attribute is present (e.g. read-time reader wrapping
 *       against an already-written index, or unit tests that write a single Parquet file
 *       alongside the segment), the segment directory, a {@code parquet/} subdirectory, and a
 *       sibling {@code parquet/} directory (the composite-engine deployment layout) are scanned
 *       for a {@code *.parquet} file.</li>
 * </ol>
 *
 * <p>Returns {@code null} when no Parquet file can be resolved; the producer turns that into a
 * descriptive {@link IOException} (Req 9.3).
 */
public final class ParquetSegmentLayout {

    /** {@code SegmentInfo} attribute key holding the absolute Parquet file path for the segment. */
    public static final String PARQUET_FILE_ATTRIBUTE = "parquet.docvalues.file";

    /** Parquet file extension, matching the indexing engine. */
    static final String PARQUET_EXT = ".parquet";

    private ParquetSegmentLayout() {}

    /**
     * Resolves the Parquet file path for {@code state}'s segment, or {@code null} if none exists.
     *
     * @throws IOException if directory access fails
     */
    public static Path resolve(SegmentReadState state) throws IOException {
        // 1. Authoritative: explicit path stamped on the segment by the composite codec.
        String attr = state.segmentInfo.getAttribute(PARQUET_FILE_ATTRIBUTE);
        if (attr != null && attr.isEmpty() == false) {
            Path p = Path.of(attr);
            return Files.exists(p) ? p : null;
        }

        // 2. Fallback: scan for a .parquet file across the locations the composite/parquet engines
        //    use. The composite engine's ParquetIndexingEngine writes files to the shard's
        //    "parquet/" data directory, which is a *sibling* of the Lucene segment directory
        //    (shardPath/parquet vs shardPath/index), so we must look both inside the segment
        //    directory (single-file unit-test layout) and in the sibling parquet/ dir
        //    (real deployment layout).
        Path dir = directoryPath(state.directory);
        if (dir == null) {
            return null;
        }
        for (Path candidate : candidateParquetDirs(dir)) {
            Path found = firstParquetFile(candidate);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /**
     * Candidate directories to scan for the segment's Parquet file, in priority order:
     * <ol>
     *   <li>the Lucene segment directory itself (unit tests that drop a single {@code .parquet}
     *       alongside the segment);</li>
     *   <li>a {@code parquet/} subdirectory of the segment directory;</li>
     *   <li>a {@code parquet/} directory that is a sibling of the segment directory — the real
     *       composite-engine layout, where the Lucene index lives in {@code shardPath/index} and
     *       Parquet files in {@code shardPath/parquet}.</li>
     * </ol>
     */
    private static List<Path> candidateParquetDirs(Path luceneDir) {
        List<Path> dirs = new ArrayList<>(3);
        dirs.add(luceneDir);
        dirs.add(luceneDir.resolve("parquet"));
        Path parent = luceneDir.getParent();
        if (parent != null) {
            dirs.add(parent.resolve("parquet"));
        }
        return dirs;
    }

    /** Returns the first {@code *.parquet} file in {@code dir} (lexicographically), or null. */
    private static Path firstParquetFile(Path dir) throws IOException {
        if (Files.isDirectory(dir) == false) {
            return null;
        }
        List<Path> matches = new ArrayList<>();
        try (var stream = Files.newDirectoryStream(dir, "*" + PARQUET_EXT)) {
            for (Path p : stream) {
                matches.add(p);
            }
        }
        if (matches.isEmpty()) {
            return null;
        }
        matches.sort(Path::compareTo);
        return matches.get(0);
    }

    /**
     * Extracts the filesystem path backing a Lucene {@link Directory}, unwrapping
     * {@link FilterDirectory} layers. Returns {@code null} for non-filesystem directories
     * (e.g. in-memory test directories), in which case attribute-based resolution is required.
     */
    private static Path directoryPath(Directory directory) {
        Directory unwrapped = FilterDirectory.unwrap(directory);
        if (unwrapped instanceof FSDirectory fs) {
            return fs.getDirectory();
        }
        return null;
    }
}
