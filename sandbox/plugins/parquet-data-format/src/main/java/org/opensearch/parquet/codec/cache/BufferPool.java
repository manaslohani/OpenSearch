/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.HashMap;
import java.util.Map;

/**
 * Layer 5 — hot-buffer pooling. Owns a confined {@link Arena} and hands out reusable,
 * grow-on-demand native scratch segments for the FFM out-buffers that the column reader
 * passes to the native page-decode / value-read functions.
 *
 * <h2>Named slots</h2>
 * Buffers are keyed by a caller-chosen <em>slot</em> name (for example {@code "value"},
 * {@code "presence"}, {@code "offsets"}). Each slot retains a single backing segment that
 * grows monotonically to the largest size ever requested for that slot — so steady-state
 * page decoding reuses the same native memory with no per-call allocation. A request for a
 * size that still fits the slot's current backing segment returns that same segment (sliced
 * to the requested length); a larger request replaces the backing segment.
 *
 * <p>A single pool instance is confined to one thread: doc IDs arrive ascending and each pool
 * serves one thread's column readers, so slot access needs no synchronization. Under intra-segment
 * concurrent search each partition thread gets its OWN pool (see {@code ParquetDocValuesProducer}),
 * so pools are never shared across threads. {@link #close()} frees every slot at once (the "release"
 * step); it may run on a different thread than the one that allocated the segments (the producer
 * closes all threads' pools centrally), which is why the backing {@link Arena} is a
 * {@linkplain Arena#ofShared() shared} arena — a confined arena would reject cross-thread close.
 * The arena is only touched on the cold page-decode / slow-read paths (the warm per-doc scan reads
 * heap {@code long[]}), so the shared arena's marginally costlier access is off the hot path.
 *
 * <p>Callers must re-fetch (not cache across calls) a slot's segment, since a growth event
 * replaces it. Returned segments are owned by the pool and must not be freed by callers.
 */
public final class BufferPool implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(BufferPool.class);

    /** A pooled slot: its backing segment and the byte capacity that segment was sized to. */
    private static final class Slot {
        MemorySegment segment;
        long capacity;
    }

    // Shared (not confined) so the producer can close pools allocated on other partition threads.
    private final Arena arena = Arena.ofShared();
    private final Map<String, Slot> slots = new HashMap<>();
    private boolean closed;

    // Layer 5 diagnostics: how often a slot request was served from the existing backing
    // segment (reuse) vs forced a (re)allocation (grow). High reuse = the pool is doing its job.
    private long reuseCount;
    private long growCount;

    /**
     * Returns the named slot's segment, grown if necessary to hold at least {@code byteSize}
     * bytes (minimum 1). The returned segment may be larger than requested; callers slice it
     * to the exact length they need.
     */
    public MemorySegment bytes(String slot, long byteSize) {
        ensureOpen();
        long needed = Math.max(byteSize, 1);
        Slot s = slots.computeIfAbsent(slot, k -> new Slot());
        if (s.segment == null || s.capacity < needed) {
            // Grow geometrically to avoid repeated resizes as pages get larger.
            long newCap = s.capacity == 0 ? needed : Math.max(needed, s.capacity * 2);
            s.segment = arena.allocate(newCap);
            s.capacity = newCap;
            growCount++;
        } else {
            reuseCount++;
        }
        return s.segment;
    }

    /** Returns the named slot's segment sized for at least {@code count} {@code long} slots. */
    public MemorySegment longs(String slot, long count) {
        return bytes(slot, Math.max(count, 1) * ValueLayout.JAVA_LONG.byteSize());
    }

    /** Returns the named slot's segment sized for at least {@code count} {@code int} slots. */
    public MemorySegment ints(String slot, long count) {
        return bytes(slot, Math.max(count, 1) * ValueLayout.JAVA_INT.byteSize());
    }

    /**
     * Returns the named slot's segment sized for exactly one {@code long} out-pointer. Each
     * distinct name gets its own stable single-long scratch cell, reused across calls.
     */
    public MemorySegment longOut(String slot) {
        return bytes(slot, ValueLayout.JAVA_LONG.byteSize());
    }

    /** Number of distinct slots currently backed by a segment (for tests / diagnostics). */
    public int slotCount() {
        return slots.size();
    }

    /** Layer 5 diagnostics: requests served from an existing backing segment (no allocation). */
    public long reuseCount() {
        return reuseCount;
    }

    /** Layer 5 diagnostics: requests that forced a (re)allocation of a slot's backing segment. */
    public long growCount() {
        return growCount;
    }

    /** Layer 5 reuse rate in [0,1]; 0 when there were no requests. */
    public double reuseRate() {
        long total = reuseCount + growCount;
        return total == 0 ? 0.0 : (double) reuseCount / total;
    }

    /** Single-line Layer 5 summary suitable for an INFO log. */
    public String summary() {
        return String.format(
            java.util.Locale.ROOT,
            "L5 buffer-pool: slots=%d reuse=%d grow=%d (reuseRate=%.2f%%)",
            slots.size(),
            reuseCount,
            growCount,
            reuseRate() * 100.0
        );
    }

    /** Alias for {@link #close()}, matching the design's "release" vocabulary. */
    public void release() {
        close();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("BufferPool already closed");
        }
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            if (reuseCount + growCount > 0 && logger.isDebugEnabled()) {
                // Benchmarking: demoted from info to debug so per-query buffer-pool stats
                // are not emitted during raw-performance runs.
                logger.debug("[PARQUET_DV_CACHE_STATS] {}", summary());
            }
            slots.clear();
            arena.close();
        }
    }
}
