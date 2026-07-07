/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Per-phase timing for the codec's page-decode hot path.
//!
//! The Java/profiler view collapses the whole FFM `parquet_decode_page_at_row` call into one
//! "uber-level" frame, so it can't tell how much of a query's native time is row-group setup vs
//! the actual Parquet decode vs the Arrow round-trip in the liquid cache. This module records
//! wall-clock nanoseconds (and a call count) per phase into process-global atomics, and dumps a
//! per-query summary via `dump_and_reset` (wired to the FFM entry point called at producer close).
//!
//! All instrumented phases are on the **per-page** path (~one call per ~20k-row page, i.e. a few
//! thousand times for a 100M-row scan), so the `Instant::now()` overhead is negligible against the
//! work measured. Do NOT time anything on the per-doc path (100M calls) — there the timer cost
//! rivals the measured work and the numbers become meaningless.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// One timed phase: cumulative nanoseconds and call count. Cheap relaxed atomics.
pub struct Phase {
    nanos: AtomicU64,
    count: AtomicU64,
    label: &'static str,
}

impl Phase {
    const fn new(label: &'static str) -> Self {
        Phase { nanos: AtomicU64::new(0), count: AtomicU64::new(0), label }
    }

    #[inline]
    fn add(&self, nanos: u64) {
        self.nanos.fetch_add(nanos, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Add the time elapsed since `start` as one call. For call sites where a borrow makes the
    /// `timed()` closure form impossible (e.g. a value borrowed from another in the timed block).
    #[inline]
    pub fn add_elapsed(&self, start: Instant) {
        self.add(start.elapsed().as_nanos() as u64);
    }

    fn take(&self) -> (u64, u64) {
        // Read then reset so each dump reports the delta since the previous dump.
        let n = self.nanos.swap(0, Ordering::Relaxed);
        let c = self.count.swap(0, Ordering::Relaxed);
        (n, c)
    }
}

// ── Decode-path phases (ffm.rs) ──
/// `get_row_group` + `get_column_reader` — open the row group and column reader (miss path only).
pub static SETUP: Phase = Phase::new("setup(get_row_group+get_column_reader)");
/// `skip_records` — advance the column reader to the page's first row.
pub static SKIP: Phase = Phase::new("skip_records");
/// `read_records` — the real Parquet decode (decompress + RLE/bit-unpack/dict/def-levels).
pub static READ_RECORDS: Phase = Phase::new("read_records(parquet decode)");
/// Building the per-row presence vector from definition levels.
pub static PRESENCE: Phase = Phase::new("presence_build");
/// `expand_primitive` — densify decoded values into one raw-bit `long` per row.
pub static EXPAND: Phase = Phase::new("expand_primitive");
/// `write_primitive_page` — copy raw longs + presence bitset into the caller's FFM out-buffers.
pub static WRITE_OUT: Phase = Phase::new("write_primitive_page");

// ── Liquid-cache phases (liquid_page_cache.rs) ──
/// `get_page`: the `block_on(cache.get().read())` — liquid index lookup + async read.
pub static LIQUID_GET_READ: Phase = Phase::new("liquid_get:read(index+async)");
/// `get_page`: the Arrow→(Vec<i64>,Vec<bool>) rebuild loop.
pub static LIQUID_GET_REBUILD: Phase = Phase::new("liquid_get:arrow->vec rebuild");
/// `put_page`: building the Arrow `Int64Array` from raw longs + presence.
pub static LIQUID_PUT_BUILD: Phase = Phase::new("liquid_put:build Int64Array");
/// `put_page`: the `block_on(cache.insert())` — liquid index insert + async.
pub static LIQUID_PUT_INSERT: Phase = Phase::new("liquid_put:insert(index+async)");

/// All phases, in report order.
fn all() -> [&'static Phase; 10] {
    [
        &SETUP,
        &SKIP,
        &READ_RECORDS,
        &PRESENCE,
        &EXPAND,
        &WRITE_OUT,
        &LIQUID_GET_READ,
        &LIQUID_GET_REBUILD,
        &LIQUID_PUT_BUILD,
        &LIQUID_PUT_INSERT,
    ]
}

/// Run `f`, adding its elapsed time to `phase`. The timer is only taken on the per-page path, so
/// the `Instant::now()` cost is negligible.
#[inline]
pub fn timed<T>(phase: &Phase, f: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let result = f();
    phase.add(start.elapsed().as_nanos() as u64);
    result
}

/// Log a `[PARQUET_DV_DECODE_PROFILE]` line per phase (total ms + call count + avg µs) and reset
/// all counters. Intended to be called once per query (producer close), never on the hot path.
/// A phase with zero calls is skipped so the log stays readable.
pub fn dump_and_reset() {
    for p in all() {
        let (nanos, count) = p.take();
        if count == 0 {
            continue;
        }
        let total_ms = nanos as f64 / 1_000_000.0;
        let avg_us = (nanos as f64 / count as f64) / 1_000.0;
        crate::log_info!(
            "[PARQUET_DV_DECODE_PROFILE] {}: total={:.2}ms calls={} avg={:.3}us",
            p.label,
            total_ms,
            count,
            avg_us,
        );
    }
}
