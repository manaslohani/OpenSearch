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
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::{Array, ArrayRef, Int64Array};
use liquid_cache::cache::{EntryID, LiquidCache, LiquidCacheBuilder};
use tokio::runtime::Runtime;

/// The process-global codec-owned decoded-page cache, built on first use. `None` when the cache is
/// disabled or its one-time build failed (e.g. the store directory could not be mounted) — in that
/// case the codec silently falls back to decoding every page, rather than failing the query.
static CACHE: OnceLock<Option<Arc<LiquidCache>>> = OnceLock::new();

/// Dedicated runtime for driving liquid's async `insert`/`get` from the synchronous FFM path.
static RT: OnceLock<Runtime> = OnceLock::new();

/// Master on/off switch, set by Java at init. Off by default → the codec decode path is untouched.
/// May be flipped back off internally if the cache fails to build (see `cache`).
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Configured max memory budget for the cache (bytes). Applied when the cache is first built.
static MAX_MEMORY_BYTES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Observability counters, so a benchmark can confirm the liquid path is actually exercised
/// (a "liquid-on" run that is all misses tells you nothing about the hit path, and a "cold" run
/// must show hits == 0). `hits` counts pages served from the cache, `misses` counts lookups that
/// fell through to decode, `backfills` counts pages inserted after a miss.
static HITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static MISSES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static BACKFILLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Snapshot of the cache counters: `(hits, misses, backfills)`.
pub fn stats() -> (u64, u64, u64) {
    (
        HITS.load(Ordering::Relaxed),
        MISSES.load(Ordering::Relaxed),
        BACKFILLS.load(Ordering::Relaxed),
    )
}

/// Directory under which the liquid `t4` store is mounted, supplied by Java at init (a writable
/// path derived from the node's data directory — never tmpfs). Empty until `set_enabled` runs.
static CACHE_DIR: OnceLock<Mutex<String>> = OnceLock::new();

/// Codec-local file path → small integer id registry, so entries carry file identity without
/// depending on DataFusion's file numbering.
static FILE_IDS: OnceLock<Mutex<HashMap<String, u32>>> = OnceLock::new();

/// Enable/disable the cache and set the memory budget + store directory. Called by Java at plugin
/// init when the `parquet_liquid_cache` feature flag is on. `cache_dir` must be a writable directory
/// on real disk (the caller passes a path under the node's data dir); the `t4` store is mounted
/// inside it. A `max_memory_bytes` of 0 leaves the liquid default.
pub fn set_enabled(enabled: bool, max_memory_bytes: usize, cache_dir: &str) {
    MAX_MEMORY_BYTES.store(max_memory_bytes, Ordering::Relaxed);
    let slot = CACHE_DIR.get_or_init(|| Mutex::new(String::new()));
    if let Ok(mut guard) = slot.lock() {
        *guard = cache_dir.to_string();
    }
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

/// Build the `Arc<LiquidCache>` once, mounting a `t4` store at `<cache_dir>/parquet_liquid_cache.t4`.
/// Returns `None` on any failure (missing/unwritable dir, mount error) after logging — the caller
/// then disables the cache so the decode path continues unaffected. Never panics: a cache problem
/// must not poison the column-reader mutex or fail doc-values reads.
fn build_cache() -> Option<Arc<LiquidCache>> {
    let dir = CACHE_DIR
        .get()
        .and_then(|m| m.lock().ok().map(|g| g.clone()))
        .unwrap_or_default();
    if dir.is_empty() {
        crate::log_error!("liquid_page_cache: no cache_dir configured; disabling codec liquid cache");
        return None;
    }
    let base = PathBuf::from(&dir).join(format!("parquet_liquid_cache_{}", std::process::id()));
    if let Err(e) = std::fs::create_dir_all(&base) {
        crate::log_error!(
            "liquid_page_cache: failed to create cache dir {:?}: {}; disabling codec liquid cache",
            base, e
        );
        return None;
    }
    let store_path = base.join("store.t4");
    let store = match runtime().block_on(t4::mount(&store_path)) {
        Ok(s) => s,
        Err(e) => {
            crate::log_error!(
                "liquid_page_cache: failed to mount t4 store at {:?}: {}; disabling codec liquid cache",
                store_path, e
            );
            return None;
        }
    };
    let mut builder = LiquidCacheBuilder::new().with_store(store);
    let budget = MAX_MEMORY_BYTES.load(Ordering::Relaxed);
    if budget > 0 {
        builder = builder.with_max_memory_bytes(budget);
    }
    crate::log_info!("liquid_page_cache: codec liquid cache initialized at {:?}", base);
    Some(runtime().block_on(builder.build()))
}

/// Access the process-global cache, building it once. On build failure this returns `None` and
/// flips `ENABLED` off so subsequent `get_page`/`put_page` calls short-circuit without retrying.
fn cache() -> Option<&'static Arc<LiquidCache>> {
    let slot = CACHE.get_or_init(build_cache);
    if slot.is_none() {
        ENABLED.store(false, Ordering::Relaxed);
    }
    slot.as_ref()
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
/// produce (`longs[i]` valid iff `presence[i]`), or `None` on a miss.
///
/// Retained alongside the faster [`get_page_into_outbuf`] so the two hit paths can be A/B profiled
/// (this one materializes a `Vec<i64>` + `Vec<bool>`; the other writes straight to the FFM buffers).
/// Not on the live hot path — `parquet_decode_page_at_row` calls `get_page_into_outbuf`.
#[allow(dead_code)]
pub fn get_page(eid: EntryID) -> Option<(Vec<i64>, Vec<bool>)> {
    let cache = cache()?;
    let array: ArrayRef = runtime().block_on(cache.get(&eid).read())?;
    let int_array = array.as_any().downcast_ref::<Int64Array>()?;
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

/// Look up a cached decoded page and write it **straight into the caller's FFM out-buffers**,
/// skipping the intermediate `Vec<i64>` + `Vec<bool>` rebuild that [`get_page`] does. This is the
/// hot-path variant: on a warm aggregation the cached `Int64Array` already stores the data in the
/// exact layout Java reads, so a hit is two `memcpy`s (values + validity) instead of an
/// element-by-element loop that is then recopied by `write_primitive_page`.
///
/// Buffer contract mirrors `write_primitive_page` exactly:
/// - writes `out_value_actual_len = len * 8` up front,
/// - returns `Some(RC_OVERFLOW)` if either out-buffer is too small (caller sizes buffers and
///   retries — the actual length is already populated),
/// - returns `Some(RC_OK)` after copying, or
/// - returns `None` on a cache miss (caller then decodes normally).
///
/// Layout equivalences that make this a raw copy (little-endian target, which the codec value
/// buffer already assumes):
/// - `Int64Array::values()` derefs to `&[i64]` in native-endian order — identical to the FFM value
///   buffer Java reads as a `long[]`.
/// - Arrow's validity bitmap is LSB-first packed bytes with `1 == valid == present`; the codec
///   presence bitset is a little-endian `long[]` with bit `i` == row `i` present. On little-endian
///   the byte layouts coincide, so a byte copy of the validity buffer reproduces
///   `write_presence_bitset`. Null value slots hold 0 and are never read by Java (every read is
///   gated on `isPresent`), so copying them verbatim is behavior-preserving.
///
/// # Safety
/// The out pointers must be valid for writes of their stated capacities (`out_value_buf_cap` bytes;
/// `out_presence_bits_cap` `i64` words), matching the `parquet_decode_page_at_row` FFM contract.
pub unsafe fn get_page_into_outbuf(
    eid: EntryID,
    out_value_buf: *mut u8,
    out_value_buf_cap: i64,
    out_value_actual_len: *mut i64,
    out_presence_bitset: *mut i64,
    out_presence_bits_cap: i64,
) -> Option<i64> {
    let cache = cache()?;
    let array: ArrayRef = match runtime().block_on(cache.get(&eid).read()) {
        Some(a) => a,
        None => {
            MISSES.fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };
    // Only the Int64Array layout is cached (see put_page); a wrong type falls back to decode.
    let int_array = match array.as_any().downcast_ref::<Int64Array>() {
        Some(a) => a,
        None => {
            MISSES.fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };
    HITS.fetch_add(1, Ordering::Relaxed);
    let len = int_array.len();

    let value_bytes = (len * 8) as i64;
    if !out_value_actual_len.is_null() {
        *out_value_actual_len = value_bytes;
    }
    let presence_words = ((len + 63) / 64) as i64;
    if value_bytes > out_value_buf_cap
        || out_value_buf.is_null()
        || presence_words > out_presence_bits_cap
        || out_presence_bitset.is_null()
    {
        return Some(crate::ffm::RC_OVERFLOW);
    }
    if len == 0 {
        return Some(crate::ffm::RC_OK);
    }

    // Zero the whole presence word region first so any trailing bits past `len` are clean; only the
    // low `len` bits are ever read by Java, but this keeps the buffer well-defined.
    let presence_bytes = (presence_words as usize) * 8;
    std::ptr::write_bytes(out_presence_bitset as *mut u8, 0, presence_bytes);
    let presence_dst = out_presence_bitset as *mut u8;

    // Cached arrays are always freshly built (offset 0) in `put_page`, so the fast raw-copy path is
    // the norm. Guard on the array/validity offsets anyway and fall back to element-wise if a sliced
    // array ever reaches here, so correctness never depends on the layout assumption.
    let arr_offset = int_array.offset();
    let nulls = int_array.nulls();
    let nulls_unaligned = nulls.map(|n| n.inner().offset() != 0).unwrap_or(false);

    if arr_offset == 0 && !nulls_unaligned {
        // Values: one memcpy of the native-endian i64 words.
        std::ptr::copy_nonoverlapping(int_array.values().as_ptr() as *const u8, out_value_buf, len * 8);
        // Presence: byte copy of the validity bitmap, or all-ones when the column has no nulls.
        match nulls {
            None => {
                let full = len / 8;
                std::ptr::write_bytes(presence_dst, 0xFF, full);
                let rem = len % 8;
                if rem > 0 {
                    *presence_dst.add(full) = ((1u16 << rem) - 1) as u8;
                }
            }
            Some(nb) => {
                let validity: &[u8] = nb.inner().values();
                let n = validity.len().min(presence_bytes);
                std::ptr::copy_nonoverlapping(validity.as_ptr(), presence_dst, n);
            }
        }
    } else {
        // Rare fallback: sliced array. Copy element by element into the out-buffers. Write values
        // as raw bytes (out_value_buf has no guaranteed 8-byte alignment, so an aligned i64 store
        // would be UB — mirrors write_primitive_page).
        for i in 0..len {
            let word: i64 = if int_array.is_null(i) {
                0
            } else {
                *presence_dst.add(i / 8) |= 1u8 << (i % 8);
                int_array.value(i)
            };
            std::ptr::copy_nonoverlapping(&word as *const i64 as *const u8, out_value_buf.add(i * 8), 8);
        }
    }
    Some(crate::ffm::RC_OK)
}

/// Cache a decoded primitive page. `longs[i]` is meaningful only where `presence[i]` is true;
/// null rows are stored as Arrow nulls so a later `get_page` reconstructs presence exactly.
pub fn put_page(eid: EntryID, longs: &[i64], presence: &[bool]) {
    debug_assert_eq!(longs.len(), presence.len());
    let cache = match cache() {
        Some(c) => c,
        None => return,
    };
    let array: Int64Array = longs
        .iter()
        .zip(presence.iter())
        .map(|(&v, &present)| if present { Some(v) } else { None })
        .collect();
    let array_ref: ArrayRef = Arc::new(array);
    // Best-effort: a CacheFull error just means this page is not cached this time.
    // `insert`/`get` return builder types that implement `IntoFuture`, so convert before block_on.
    if runtime()
        .block_on(cache.insert(eid, array_ref).into_future())
        .is_ok()
    {
        BACKFILLS.fetch_add(1, Ordering::Relaxed);
    }
}

/// Cache a decoded page as its **native** Arrow array (Int32Array, Float64Array, ...),
/// mimicking how DataFusion's liquid instance stores columns (native type, not normalized
/// to i64). Paired with [`get_page_array`], which returns the array as-is; the FFM decode
/// path then widens it to i64 on read via `write_primitive_page_from_arrow`.
///
/// This exists to measure the "read DF-format entries" cost: vs [`put_page`] +
/// [`get_page_into_outbuf`] (store Int64Array, read via one memcpy), this stores the native
/// type and pays a per-row widen on every read. Comparing the two branches isolates that cost.
pub fn put_page_native(eid: EntryID, array: ArrayRef) {
    let cache = match cache() {
        Some(c) => c,
        None => return,
    };
    if runtime()
        .block_on(cache.insert(eid, array).into_future())
        .is_ok()
    {
        BACKFILLS.fetch_add(1, Ordering::Relaxed);
    }
}

/// Look up a cached page as its raw native `ArrayRef` (whatever type `put_page_native` stored),
/// or `None` on a miss. The caller widens it to i64 into the FFM out-buffers via
/// `write_primitive_page_from_arrow`. Counterpart to [`get_page_into_outbuf`]'s memcpy fast
/// path, used only on the DF-format (native-storage) branch.
pub fn get_page_array(eid: EntryID) -> Option<ArrayRef> {
    let cache = cache()?;
    match runtime().block_on(cache.get(&eid).read()) {
        Some(array) => {
            HITS.fetch_add(1, Ordering::Relaxed);
            Some(array)
        }
        None => {
            MISSES.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
}
