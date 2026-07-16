# Plan: measure the "read DF-format entries" latency cost (point 2)

Branch: `codec-liquid-df-format` (off `codec-arpit-full`).
A/B baseline: `codec-arpit-full` (stores normalized Int64Array, reads via one memcpy).

## Goal

Isolate and measure the per-read cost of point 2 from the cache-sharing discussion:

> DF stores the **native** Arrow array (Int32Array, Float64Array, ...). Reading a
> DF-written entry means widening arbitrary Arrow types to i64 per row, instead of
> the codec's single memcpy of an already-normalized Int64Array.

We do NOT need the full shared-instance plumbing (shared budget, init ordering, key
grid, file-id map) to measure this. We only need the codec's own cache to *mimic DF's
storage format*: store the native primitive array and widen on read. Then compare warm
latency of this branch vs `codec-arpit-full`.

## What changes (Rust only, primitive numeric path)

Today (baseline, `codec-arpit-full`):
- liquid-on MISS: `arrow_primitive_to_page(array)` -> `(Vec<i64>, Vec<bool>)` -> `put_page` stores an **Int64Array** -> `write_primitive_page` copies out.
- liquid-on HIT: `get_page_into_outbuf` downcasts to **Int64Array** and does **one memcpy**.

This branch (DF-format mimic):
- liquid-on MISS: store the **native** decoded `ArrayRef` directly (`put_page_native`),
  and write out via `write_primitive_page_from_arrow` (widen to i64).
- liquid-on HIT: `get_page_array` returns the native `ArrayRef`; write out via
  `write_primitive_page_from_arrow` (widen to i64 per row) instead of a memcpy.

`write_primitive_page_from_arrow` already exists (ported from a9448b9) and already handles
every Arrow primitive -> i64 with the exact bit conventions (sign-extend ints, `to_bits`
for floats, unsigned reinterpret), plus presence from the validity bitmap. So the read
path is a straight reuse; the delta vs baseline is exactly memcpy -> per-row widen.

## Out of scope (explicitly)

- **Nested fields (LIST/STRUCT)**: leaf-vs-root key divergence. Ignored for now; the codec
  only caches flat primitive leaves. Left as a comment at the key site. Not touched here.
- **Keyword / DictionaryArray (BYTE_ARRAY path)**: DF stores these as dictionaries; the codec
  uses a separate byteBuf/offsets path. Out of scope for this numeric-latency experiment;
  the BYTE_ARRAY arm is left unchanged.
- **Shared instance / key grid / file-id mapping**: not needed to measure point 2. Separate
  future work.

## A/B protocol

- Baseline branch `codec-arpit-full`, this branch `codec-liquid-df-format`.
- Same box/data/queries, liquid ON, warm median of N. The only difference is the codec's
  cache value format + read (memcpy vs widen). Any warm-latency delta is the point-2 cost.
- Correctness: agg results must be byte-identical between the two branches (widen preserves
  values). The existing `direct_write_matches_reference` test already proves the widen path
  matches the reference i64 conversion.
