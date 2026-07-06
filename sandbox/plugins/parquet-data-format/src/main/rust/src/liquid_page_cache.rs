/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Codec-owned, cross-query decoded-page cache backed by liquid-cache's core API.
//!
//! The Parquet DocValues codec's `PageCache` is per-query scratch: every query re-decodes the
//! same Parquet pages. This module gives the codec a **node-level** (process-lifetime) cache of
//! decoded primitive pages so a later query reuses a page an earlier query already decoded —
//! the cross-query tier the codec otherwise lacks.
//!
//! Design (v1):
//! - A single process-global `Arc<LiquidCache>` (liquid core), built lazily. This is a
//!   **codec-owned** instance, independent of the DataFusion/PPL liquid cache — same technology,
//!   separate instance and keyspace. Sharing the DataFusion instance is a later optimization.
//! - Entries are keyed by `(file_id, column_id, page_idx)` packed into liquid's `EntryID` (a
//!   `usize`). `file_id` comes from a codec-local path→id registry, so the key carries file
//!   identity (and Parquet's immutable-file/generation model means changed data = new path =
//!   new key = automatic miss — no invalidation logic needed).
//! - Values are cached as an Arrow `Int64Array` (with a null buffer derived from the page's
//!   presence bits). On a hit we convert back to the raw `Vec<i64>` + `Vec<bool>` the codec's
//!   `write_primitive_page` already consumes, so the Java/PageCache/per-doc path is byte-identical
//!   whether the page was decoded or served from cache.
//! - liquid's `insert`/`get` are async; we drive them on a dedicated single-threaded tokio runtime
//!   via `block_on`, mirroring `merge::io_task`'s `OnceLock<Runtime>` pattern (the codec's FFM
//!   entry points are synchronous `extern "C"`).
//!
//! Primitives only (INT32/INT64/date → i64 words). BYTE_ARRAY/keyword pages are not cached here.
//! Gated by `set_enabled(true)` from Java; when disabled every entry point is a cheap no-op and the
//! codec's decode path is unchanged.

use std::collections::HashMap;
use std::future::IntoFuture;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::{Array, ArrayRef, Int64Array};
use liquid_cache::cache::{EntryID, LiquidCache, LiquidCacheBuilder};
use tokio::runtime::Runtime;

/// The process-global codec-owned decoded-page cache. Built on first use (or via `init`).
static CACHE: OnceLock<Arc<LiquidCache>> = OnceLock::new();

/// Dedicated runtime for driving liquid's async `insert`/`get` from the synchronous FFM path.
static RT: OnceLock<Runtime> = OnceLock::new();

/// Master on/off switch, set by Java at init. Off by default → the codec decode path is untouched.
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Configured max memory budget for the cache (bytes). Applied when the cache is first built.
static MAX_MEMORY_BYTES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Codec-local file path → small integer id registry, so entries carry file identity without
/// depending on DataFusion's file numbering.
static FILE_IDS: OnceLock<Mutex<HashMap<String, u32>>> = OnceLock::new();

/// Cumulative process-wide hit/miss counters for the cache. Cheap O(1) relaxed atomics bumped on
/// the per-page path. These are the observability signal — liquid's own `cache_hit`/`cache_miss`
/// stats are only incremented by its DataFusion reader wrapper, not the core `get`/`insert` path
/// the codec uses, so they always read 0 on this instance. A snapshot is logged per query via the
/// `parquet_liquid_cache_stats` FFM entry point (see `ffm.rs`), which is where the O(entries)
/// `LiquidCache::stats()` call lives — never on the hot path.
static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);

/// Enable/disable the cache and set the memory budget. Called by Java at plugin init when the
/// `parquet_liquid_cache` feature flag is on. A `max_memory_bytes` of 0 leaves the liquid default.
pub fn set_enabled(enabled: bool, max_memory_bytes: usize) {
    MAX_MEMORY_BYTES.store(max_memory_bytes, Ordering::Relaxed);
    ENABLED.store(enabled, Ordering::Relaxed);
}

/// True when the cache should be consulted. Cheap relaxed load on the hot path.
#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

fn runtime() -> &'static Runtime {
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("liquid_page_cache: failed to build tokio runtime")
    })
}

fn cache() -> &'static Arc<LiquidCache> {
    CACHE.get_or_init(|| {
        let mut builder = LiquidCacheBuilder::new();
        let budget = MAX_MEMORY_BYTES.load(Ordering::Relaxed);
        if budget > 0 {
            builder = builder.with_max_memory_bytes(budget);
        }
        runtime().block_on(builder.build())
    })
}

/// Resolve (or assign) a stable small id for a Parquet file path. Codec-local; independent of any
/// DataFusion file numbering.
pub fn file_id(path: &str) -> u32 {
    let map = FILE_IDS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().expect("liquid_page_cache: file id registry poisoned");
    let next = guard.len() as u32;
    *guard.entry(path.to_string()).or_insert(next)
}

/// Pack `(file_id, column_id, page_idx)` into a liquid `EntryID`. u16 column id + u32 page fit
/// alongside the file id in a usize on 64-bit targets.
#[inline]
pub fn entry_id(file_id: u32, column_id: u32, page_idx: u32) -> EntryID {
    let v = ((file_id as usize) << 48) | ((column_id as usize) << 32) | (page_idx as usize);
    EntryID::from(v)
}

/// Look up a cached decoded page. Returns `(longs, presence)` in the exact form the decode arms
/// produce (`longs[i]` valid iff `presence[i]`), or `None` on a miss. Bumps the cumulative
/// hit/miss counters so a `None` here (miss → caller decodes + backfills) and a `Some` (hit →
/// caller skips decode) are both accounted for in one place.
pub fn get_page(eid: EntryID) -> Option<(Vec<i64>, Vec<bool>)> {
    let array: ArrayRef = match runtime().block_on(cache().get(&eid).read()) {
        Some(a) => a,
        None => {
            MISSES.fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };
    // A downcast failure is treated as a miss: the caller will re-decode this page.
    let int_array = match array.as_any().downcast_ref::<Int64Array>() {
        Some(a) => a,
        None => {
            MISSES.fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };
    HITS.fetch_add(1, Ordering::Relaxed);
    let len = int_array.len();
    let mut longs = Vec::with_capacity(len);
    let mut presence = Vec::with_capacity(len);
    for i in 0..len {
        if int_array.is_null(i) {
            longs.push(0);
            presence.push(false);
        } else {
            longs.push(int_array.value(i));
            presence.push(true);
        }
    }
    Some((longs, presence))
}

/// Cache a decoded primitive page. `longs[i]` is meaningful only where `presence[i]` is true;
/// null rows are stored as Arrow nulls so a later `get_page` reconstructs presence exactly.
pub fn put_page(eid: EntryID, longs: &[i64], presence: &[bool]) {
    debug_assert_eq!(longs.len(), presence.len());
    let array: Int64Array = longs
        .iter()
        .zip(presence.iter())
        .map(|(&v, &present)| if present { Some(v) } else { None })
        .collect();
    let array_ref: ArrayRef = Arc::new(array);
    // Best-effort: a CacheFull error just means this page is not cached this time.
    // `insert`/`get` return builder types that implement `IntoFuture`, so convert before block_on.
    let _ = runtime().block_on(cache().insert(eid, array_ref).into_future());
}

/// Log a snapshot of the codec liquid cache: the cumulative O(1) hit/miss counters plus liquid's
/// own resident-entry stats. Intended to be called at most once per query (segment producer close),
/// NOT on the per-page path — `LiquidCache::stats()` iterates every resident entry (O(entries)),
/// so calling it per page would turn a page lookup into a full-cache scan. A no-op when the cache
/// was never enabled/built (avoids forcing the lazy `cache()` build just to log zeros).
pub fn log_stats() {
    let hits = HITS.load(Ordering::Relaxed);
    let misses = MISSES.load(Ordering::Relaxed);
    let lookups = hits + misses;
    let hit_rate = if lookups == 0 { 0.0 } else { hits as f64 / lookups as f64 * 100.0 };

    // Only touch LiquidCache::stats() (O(entries)) if the cache was actually built; if it was never
    // consulted there is nothing resident and no reason to force the OnceLock init.
    if let Some(cache) = CACHE.get() {
        let s = cache.stats();
        crate::log_info!(
            "[PARQUET_DV_LIQUID_STATS] hits={} misses={} (hitRate={:.2}%) | entries={} mem={}/{} bytes",
            hits,
            misses,
            hit_rate,
            s.total_entries,
            s.memory_usage_bytes,
            s.max_memory_bytes,
        );
    } else {
        crate::log_info!(
            "[PARQUET_DV_LIQUID_STATS] hits={} misses={} (hitRate={:.2}%) | cache not built",
            hits,
            misses,
            hit_rate,
        );
    }
}
