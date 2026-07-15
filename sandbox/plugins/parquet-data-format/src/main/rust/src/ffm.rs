/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM bridge for the Parquet writer.
//!
//! Return convention: `>= 0` success, `< 0` error pointer (negate to get ptr,
//! call `native_error_message`/`native_error_free`).

use std::collections::HashMap;
use std::slice;
use std::str;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use lazy_static::lazy_static;
use native_bridge_common::{ffm_safe, log_debug};

use crate::native_settings::NativeSettings;
use crate::field_config::FieldConfig;
use crate::merge;
use crate::writer::{NativeParquetWriter, SETTINGS_STORE};

unsafe fn str_from_raw<'a>(ptr: *const u8, len: i64) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err("null string pointer".to_string());
    }
    if len < 0 {
        return Err(format!("negative string length: {}", len));
    }
    let bytes = slice::from_raw_parts(ptr, len as usize);
    str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {}", e))
}

/// Decode a parallel (pointers, lengths, count) triple into `Vec<String>`.
unsafe fn str_array_from_raw(
    ptrs: *const *const u8,
    lens: *const i64,
    count: i64,
) -> Result<Vec<String>, String> {
    if count == 0 {
        return Ok(vec![]);
    }
    if ptrs.is_null() || lens.is_null() {
        return Err("null string array pointer".to_string());
    }
    let n = count as usize;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let p = *ptrs.add(i);
        let l = *lens.add(i);
        out.push(str_from_raw(p, l)?.to_string());
    }
    Ok(out)
}

/// Decode a parallel (pointers, count) array of i64 values interpreted as booleans (0 = false).
unsafe fn bool_array_from_raw(
    vals: *const i64,
    count: i64,
) -> Vec<bool> {
    if count == 0 || vals.is_null() {
        return vec![];
    }
    let n = count as usize;
    (0..n).map(|i| *vals.add(i) != 0).collect()
}

// ---------------------------------------------------------------------------
// Writer lifecycle
// ---------------------------------------------------------------------------

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_create_writer(
    file_ptr: *const u8,
    file_len: i64,
    index_name_ptr: *const u8,
    index_name_len: i64,
    schema_address: i64,
    sort_ptrs: *const *const u8,
    sort_lens: *const i64,
    sort_count: i64,
    reverse_vals: *const i64,
    reverse_count: i64,
    nulls_first_vals: *const i64,
    nulls_first_count: i64,
    writer_generation: i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_create_writer file: {}", e))?.to_string();
    let index_name = str_from_raw(index_name_ptr, index_name_len)
        .map_err(|e| format!("parquet_create_writer index_name: {}", e))?.to_string();
    let sort_columns = str_array_from_raw(sort_ptrs, sort_lens, sort_count)
        .map_err(|e| format!("parquet_create_writer sort_columns: {}", e))?;
    let reverse_sorts = bool_array_from_raw(reverse_vals, reverse_count);
    let nulls_first = bool_array_from_raw(nulls_first_vals, nulls_first_count);

    NativeParquetWriter::create_writer(filename, index_name, schema_address, sort_columns, reverse_sorts, nulls_first, writer_generation)
        .map(|_| 0)
        .map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_write(
    file_ptr: *const u8,
    file_len: i64,
    array_address: i64,
    schema_address: i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_write: {}", e))?.to_string();
    NativeParquetWriter::write_data(filename, array_address, schema_address)
        .map(|_| 0)
        .map_err(|e| e.to_string())
}

/// Returns 0 with metadata in out-pointers, 1 if no writer found.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_finalize_writer(
    file_ptr: *const u8,
    file_len: i64,
    version_out: *mut i32,
    num_rows_out: *mut i64,
    created_by_buf: *mut u8,
    created_by_buf_len: i64,
    created_by_len_out: *mut i64,
    crc32_out: *mut i64,
    num_row_groups_out: *mut i64,
    sort_perm_ptr_out: *mut i64,
    sort_perm_len_out: *mut i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_finalize_writer: {}", e))?.to_string();
    match NativeParquetWriter::finalize_writer(filename) {
        Ok(Some(result)) => {
            let fm = result.metadata.file_metadata();
            if !version_out.is_null() { *version_out = fm.version(); }
            if !num_rows_out.is_null() { *num_rows_out = fm.num_rows(); }
            if let Some(cb) = fm.created_by() {
                if !created_by_buf.is_null() && created_by_buf_len > 0 {
                    let bytes = cb.as_bytes();
                    let n = bytes.len().min(created_by_buf_len as usize);
                    std::ptr::copy_nonoverlapping(bytes.as_ptr(), created_by_buf, n);
                    if !created_by_len_out.is_null() { *created_by_len_out = n as i64; }
                }
            } else if !created_by_len_out.is_null() {
                *created_by_len_out = -1;
            }
            if !crc32_out.is_null() { *crc32_out = result.crc32 as i64; }
            if !num_row_groups_out.is_null() { *num_row_groups_out = result.metadata.num_row_groups() as i64; }

            // Return sort permutation if present
            if !sort_perm_ptr_out.is_null() && !sort_perm_len_out.is_null() {
                if let Some(perm) = result.row_id_mapping {
                    let len = perm.len();
                    let mapping_bytes = len * std::mem::size_of::<i64>();
                    // Track mapping handoff to Java — Java holds until parquet_free_row_id_mapping
                    crate::memory::write_pool().grow(mapping_bytes);
                    let boxed = perm.into_boxed_slice();
                    *sort_perm_len_out = len as i64;
                    *sort_perm_ptr_out = Box::into_raw(boxed) as *mut i64 as i64;
                } else {
                    *sort_perm_len_out = 0;
                    *sort_perm_ptr_out = 0;
                }
            }
            Ok(0)
        }
        Ok(None) => Ok(1),
        Err(e) => Err(e.to_string()),
    }
}


#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_file_metadata(
    file_ptr: *const u8,
    file_len: i64,
    version_out: *mut i32,
    num_rows_out: *mut i64,
    created_by_buf: *mut u8,
    created_by_buf_len: i64,
    created_by_len_out: *mut i64,
    num_row_groups_out: *mut i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_get_file_metadata: {}", e))?.to_string();
    let metadata = NativeParquetWriter::get_file_metadata(filename).map_err(|e| e.to_string())?;
    let fm = metadata.file_metadata();
    if !version_out.is_null() { *version_out = fm.version(); }
    if !num_rows_out.is_null() { *num_rows_out = fm.num_rows(); }
    if !num_row_groups_out.is_null() { *num_row_groups_out = metadata.num_row_groups() as i64; }
    if let Some(cb) = fm.created_by() {
        if !created_by_buf.is_null() && created_by_buf_len > 0 {
            let bytes = cb.as_bytes();
            let n = bytes.len().min(created_by_buf_len as usize);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), created_by_buf, n);
            if !created_by_len_out.is_null() { *created_by_len_out = n as i64; }
        }
    } else if !created_by_len_out.is_null() {
        *created_by_len_out = -1;
    }
    Ok(0)
}

/// Returns a JSON string with per-column encoding and compression metadata.
/// Format: {"column_name": {"encodings": ["PLAIN", "RLE_DICTIONARY"], "compression": "LZ4_RAW"}, ...}
/// Reads from the first row group.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_column_metadata(
    file_ptr: *const u8,
    file_len: i64,
    out_buf: *mut u8,
    out_buf_len: i64,
    out_len: *mut i64,
) -> i64 {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File;

    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_get_column_metadata: {}", e))?.to_string();
    let file = File::open(&filename).map_err(|e| format!("Failed to open file: {}", e))?;
    let reader = SerializedFileReader::new(file).map_err(|e| format!("Failed to read parquet: {}", e))?;
    let metadata = reader.metadata();

    if metadata.num_row_groups() == 0 {
        let json = "{}".to_string();
        let bytes = json.as_bytes();
        let n = bytes.len().min(out_buf_len as usize);
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, n);
        if !out_len.is_null() { *out_len = n as i64; }
        return Ok(0);
    }

    let rg = metadata.row_group(0);
    let mut json = String::from("{");
    for i in 0..rg.num_columns() {
        let col = rg.column(i);
        let col_name = col.column_path().string();
        let encodings: Vec<String> = col.encodings().map(|e| format!("{:?}", e)).collect();
        let compression = format!("{:?}", col.compression());
        let has_bloom_filter = col.bloom_filter_offset().is_some();
        if i > 0 { json.push(','); }
        json.push_str(&format!(
            "\"{}\":{{\"encodings\":[{}],\"compression\":\"{}\",\"bloom_filter\":{}}}",
            col_name,
            encodings.iter().map(|e| format!("\"{}\"" , e)).collect::<Vec<_>>().join(","),
            compression,
            has_bloom_filter
        ));
    }
    json.push('}');

    let bytes = json.as_bytes();
    let n = bytes.len().min(out_buf_len as usize);
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, n);
    if !out_len.is_null() { *out_len = n as i64; }
    Ok(0)
}

#[no_mangle]
pub unsafe extern "C" fn parquet_get_filtered_native_bytes_used(
    prefix_ptr: *const u8,
    prefix_len: i64,
) -> i64 {
    let prefix = str_from_raw(prefix_ptr, prefix_len).unwrap_or("").to_string();
    NativeParquetWriter::get_filtered_writer_memory_usage(prefix).unwrap_or(0) as i64
}

// ---------------------------------------------------------------------------
// Settings management
// ---------------------------------------------------------------------------

/// Update native settings for an index. Nullable fields use sentinel -1 for "not set".
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_on_settings_update(
    index_name_ptr: *const u8,
    index_name_len: i64,
    compression_type_ptr: *const u8,
    compression_type_len: i64,
    compression_level: i64,
    page_size_bytes: i64,
    page_row_limit: i64,
    dict_size_bytes: i64,
    bloom_filter_enabled: i64,
    bloom_filter_fpp: f64,
    bloom_filter_ndv: i64,
    sort_in_memory_threshold_bytes: i64,
    row_group_max_rows: i64,
    row_group_max_bytes: i64,
    merge_batch_size: i64,
    merge_rayon_threads: i64,
    merge_io_threads: i64,
    field_name_ptrs: *const *const u8,
    field_name_lens: *const i64,
    field_encoding_ptrs: *const *const u8,
    field_encoding_lens: *const i64,
    field_count: i64,
    field_compression_name_ptrs: *const *const u8,
    field_compression_name_lens: *const i64,
    field_compression_value_ptrs: *const *const u8,
    field_compression_value_lens: *const i64,
    field_compression_count: i64,
    type_encoding_name_ptrs: *const *const u8,
    type_encoding_name_lens: *const i64,
    type_encoding_value_ptrs: *const *const u8,
    type_encoding_value_lens: *const i64,
    type_encoding_count: i64,
    type_compression_name_ptrs: *const *const u8,
    type_compression_name_lens: *const i64,
    type_compression_value_ptrs: *const *const u8,
    type_compression_value_lens: *const i64,
    type_compression_count: i64,
    bf_enabled_name_ptrs: *const *const u8,
    bf_enabled_name_lens: *const i64,
    bf_enabled_vals: *const i64,
    bf_enabled_count: i64,
    type_bf_enabled_name_ptrs: *const *const u8,
    type_bf_enabled_name_lens: *const i64,
    type_bf_enabled_vals: *const i64,
    type_bf_enabled_count: i64,
    type_bf_fpp_name_ptrs: *const *const u8,
    type_bf_fpp_name_lens: *const i64,
    type_bf_fpp_vals: *const f64,
    type_bf_fpp_count: i64,
    type_bf_ndv_name_ptrs: *const *const u8,
    type_bf_ndv_name_lens: *const i64,
    type_bf_ndv_vals: *const i64,
    type_bf_ndv_count: i64,
) -> i64 {
    let index_name = str_from_raw(index_name_ptr, index_name_len)
        .map_err(|e| format!("parquet_on_settings_update index_name: {}", e))?.to_string();

    let compression_type = if compression_type_ptr.is_null() || compression_type_len < 0 {
        None
    } else {
        Some(str_from_raw(compression_type_ptr, compression_type_len)
            .map_err(|e| format!("parquet_on_settings_update compression_type: {}", e))?.to_string())
    };

    fn opt_i32(v: i64) -> Option<i32> { if v < 0 { None } else { Some(v as i32) } }
    fn opt_usize(v: i64) -> Option<usize> { if v < 0 { None } else { Some(v as usize) } }
    fn opt_bool(v: i64) -> Option<bool> { if v < 0 { None } else { Some(v != 0) } }
    fn opt_f64(v: f64) -> Option<f64> { if v < 0.0 { None } else { Some(v) } }
    fn opt_u64(v: i64) -> Option<u64> { if v < 0 { None } else { Some(v as u64) } }

    let field_names = str_array_from_raw(field_name_ptrs, field_name_lens, field_count)
        .map_err(|e| format!("parquet_on_settings_update field_names: {}", e))?;
    let field_encodings = str_array_from_raw(field_encoding_ptrs, field_encoding_lens, field_count)
        .map_err(|e| format!("parquet_on_settings_update field_encodings: {}", e))?;
    let field_compression_names = str_array_from_raw(field_compression_name_ptrs, field_compression_name_lens, field_compression_count)
        .map_err(|e| format!("parquet_on_settings_update field_compression_names: {}", e))?;
    let field_compressions = str_array_from_raw(field_compression_value_ptrs, field_compression_value_lens, field_compression_count)
        .map_err(|e| format!("parquet_on_settings_update field_compressions: {}", e))?;

    let type_encoding_names = str_array_from_raw(type_encoding_name_ptrs, type_encoding_name_lens, type_encoding_count)
        .map_err(|e| format!("parquet_on_settings_update type_encoding_names: {}", e))?;
    let type_encodings = str_array_from_raw(type_encoding_value_ptrs, type_encoding_value_lens, type_encoding_count)
        .map_err(|e| format!("parquet_on_settings_update type_encodings: {}", e))?;
    let type_compression_names = str_array_from_raw(type_compression_name_ptrs, type_compression_name_lens, type_compression_count)
        .map_err(|e| format!("parquet_on_settings_update type_compression_names: {}", e))?;
    let type_compressions = str_array_from_raw(type_compression_value_ptrs, type_compression_value_lens, type_compression_count)
        .map_err(|e| format!("parquet_on_settings_update type_compressions: {}", e))?;

    // Parse per-field bloom filter arrays
    let bf_enabled_names = str_array_from_raw(bf_enabled_name_ptrs, bf_enabled_name_lens, bf_enabled_count)
        .map_err(|e| format!("parquet_on_settings_update bf_enabled_names: {}", e))?;

    let field_configs = {
        let mut map = std::collections::HashMap::new();
        for (name, encoding) in field_names.into_iter().zip(field_encodings.into_iter()) {
            map.insert(name, FieldConfig { encoding_type: Some(encoding), ..Default::default() });
        }
        for (name, compression) in field_compression_names.into_iter().zip(field_compressions.into_iter()) {
            map.entry(name)
               .and_modify(|fc| fc.compression_type = Some(compression.clone()))
               .or_insert(FieldConfig { compression_type: Some(compression), ..Default::default() });
        }
        for (i, name) in bf_enabled_names.into_iter().enumerate() {
            let val = *bf_enabled_vals.add(i) != 0;
            map.entry(name)
               .and_modify(|fc| fc.bloom_filter_enabled = Some(val))
               .or_insert(FieldConfig { bloom_filter_enabled: Some(val), ..Default::default() });
        }
        if map.is_empty() { None } else { Some(map) }
    };

    let type_encoding_configs: Option<std::collections::HashMap<String, String>> = {
        let map: std::collections::HashMap<_, _> = type_encoding_names.into_iter().zip(type_encodings.into_iter()).collect();
        if map.is_empty() { None } else { Some(map) }
    };
    let type_compression_configs: Option<std::collections::HashMap<String, String>> = {
        let map: std::collections::HashMap<_, _> = type_compression_names.into_iter().zip(type_compressions.into_iter()).collect();
        if map.is_empty() { None } else { Some(map) }
    };

    // Parse type-level bloom filter arrays
    let type_bf_enabled_names = str_array_from_raw(type_bf_enabled_name_ptrs, type_bf_enabled_name_lens, type_bf_enabled_count)
        .map_err(|e| format!("parquet_on_settings_update type_bf_enabled_names: {}", e))?;
    let type_bf_fpp_names = str_array_from_raw(type_bf_fpp_name_ptrs, type_bf_fpp_name_lens, type_bf_fpp_count)
        .map_err(|e| format!("parquet_on_settings_update type_bf_fpp_names: {}", e))?;
    let type_bf_ndv_names = str_array_from_raw(type_bf_ndv_name_ptrs, type_bf_ndv_name_lens, type_bf_ndv_count)
        .map_err(|e| format!("parquet_on_settings_update type_bf_ndv_names: {}", e))?;

    let type_bloom_filter_enabled: Option<std::collections::HashMap<String, bool>> = {
        let map: std::collections::HashMap<_, _> = type_bf_enabled_names.into_iter().enumerate()
            .map(|(i, name)| (name, *type_bf_enabled_vals.add(i) != 0)).collect();
        if map.is_empty() { None } else { Some(map) }
    };
    let type_bloom_filter_fpp: Option<std::collections::HashMap<String, f64>> = {
        let map: std::collections::HashMap<_, _> = type_bf_fpp_names.into_iter().enumerate()
            .map(|(i, name)| (name, *type_bf_fpp_vals.add(i))).collect();
        if map.is_empty() { None } else { Some(map) }
    };
    let type_bloom_filter_ndv: Option<std::collections::HashMap<String, u64>> = {
        let map: std::collections::HashMap<_, _> = type_bf_ndv_names.into_iter().enumerate()
            .map(|(i, name)| (name, *type_bf_ndv_vals.add(i) as u64)).collect();
        if map.is_empty() { None } else { Some(map) }
    };

    let config = NativeSettings {
        index_name: Some(index_name.clone()),
        compression_type,
        compression_level: opt_i32(compression_level),
        page_size_bytes: opt_usize(page_size_bytes),
        page_row_limit: opt_usize(page_row_limit),
        dict_size_bytes: opt_usize(dict_size_bytes),
        bloom_filter_enabled: opt_bool(bloom_filter_enabled),
        bloom_filter_fpp: opt_f64(bloom_filter_fpp),
        bloom_filter_ndv: opt_u64(bloom_filter_ndv),
        sort_in_memory_threshold_bytes: opt_u64(sort_in_memory_threshold_bytes),
        row_group_max_rows: opt_usize(row_group_max_rows),
        row_group_max_bytes: opt_usize(row_group_max_bytes),
        merge_batch_size: opt_usize(merge_batch_size),
        merge_rayon_threads: opt_usize(merge_rayon_threads),
        merge_io_threads: opt_usize(merge_io_threads),
        field_configs,
        type_encoding_configs,
        type_compression_configs,
        type_bloom_filter_enabled,
        type_bloom_filter_fpp,
        type_bloom_filter_ndv,
        ..Default::default()
    };

    SETTINGS_STORE.insert(index_name, config);
    Ok(0)
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_remove_settings(
    index_name_ptr: *const u8,
    index_name_len: i64,
) -> i64 {
    let index_name = str_from_raw(index_name_ptr, index_name_len)
        .map_err(|e| format!("parquet_remove_settings: {}", e))?.to_string();
    SETTINGS_STORE.remove(&index_name);
    Ok(0)
}

// ---------------------------------------------------------------------------
// Merge
// ---------------------------------------------------------------------------

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_merge_files(
    input_ptrs: *const *const u8,
    input_lens: *const i64,
    input_count: i64,
    output_ptr: *const u8,
    output_len: i64,
    index_name_ptr: *const u8,
    index_name_len: i64,
    output_writer_generation: i64,
    version_out: *mut i32,
    num_rows_out: *mut i64,
    created_by_buf: *mut u8,
    created_by_buf_len: i64,
    created_by_len_out: *mut i64,
    crc32_out: *mut i64,
    out_mapping_ptr: *mut i64,
    out_mapping_len: *mut i64,
    out_gen_keys_ptr: *mut i64,
    out_gen_offsets_ptr: *mut i64,
    out_gen_sizes_ptr: *mut i64,
    out_gen_count: *mut i64,
    // Per-merge stats forwarded to the per-shard ParquetShardStatsTracker on the Java side.
    out_flush_and_sort_chunk_count: *mut i64,
    out_flush_and_sort_chunk_time_millis: *mut i64,
    out_row_id_mapping_max: *mut i64,
) -> i64 {
    let input_files = str_array_from_raw(input_ptrs, input_lens, input_count)
        .map_err(|e| format!("parquet_merge_files inputs: {}", e))?;
    let output_path = str_from_raw(output_ptr, output_len)
        .map_err(|e| format!("parquet_merge_files output: {}", e))?;
    let index_name = str_from_raw(index_name_ptr, index_name_len)
        .map_err(|e| format!("parquet_merge_files index_name: {}", e))?;

    let (sort_cols, reverse_flags, nulls_first_flags) = match SETTINGS_STORE.get(index_name) {
        Some(s) => {
            let sc = s.sort_columns.clone();
            let rf = s.reverse_sorts.clone();
            let nf = s.nulls_first.clone();
            if !sc.is_empty() && rf.is_empty() {
                crate::log_info!("parquet_merge_files: sort columns present but reverse_sorts is empty for index '{}', defaulting to ascending", index_name);
            }
            if !sc.is_empty() && nf.is_empty() {
                crate::log_info!("parquet_merge_files: sort columns present but nulls_first is empty for index '{}', defaulting to nulls last", index_name);
            }
            (sc, rf, nf)
        }
        None => {
            crate::log_info!("parquet_merge_files: no settings found for index '{}', proceeding with unsorted merge", index_name);
            (vec![], vec![], vec![])
        }
    };

    let result = if sort_cols.is_empty() {
        merge::merge_unsorted(&input_files, output_path, index_name, output_writer_generation)
    } else {
        merge::merge_sorted(
            &input_files,
            output_path,
            index_name,
            &sort_cols,
            &reverse_flags,
            &nulls_first_flags,
            output_writer_generation,
        )
    }
    .map_err(|e| format!("{}", e))?;

    // Write Parquet file metadata to out-pointers.
    let fm = result.metadata.file_metadata();
    if !version_out.is_null() { *version_out = fm.version(); }
    if !num_rows_out.is_null() { *num_rows_out = fm.num_rows(); }
    if let Some(cb) = fm.created_by() {
        if !created_by_buf.is_null() && created_by_buf_len > 0 {
            let bytes = cb.as_bytes();
            let n = bytes.len().min(created_by_buf_len as usize);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), created_by_buf, n);
            if !created_by_len_out.is_null() { *created_by_len_out = n as i64; }
        }
    } else if !created_by_len_out.is_null() {
        *created_by_len_out = -1;
    }
    if !crc32_out.is_null() { *crc32_out = result.crc32 as i64; }

    // Write row-ID mapping into out-pointers as heap-allocated arrays.
    // Java reads them and then calls parquet_free_merge_result to deallocate.
    let mapping = result.mapping.into_boxed_slice();
    let mapping_bytes = mapping.len() * std::mem::size_of::<i64>();
    // Track merge mapping handoff to Java — Java holds until parquet_free_merge_result
    crate::memory::merge_pool().grow(mapping_bytes);
    *out_mapping_len = mapping.len() as i64;
    *out_mapping_ptr = Box::into_raw(mapping) as *mut i64 as i64;

    let count = result.gen_keys.len();
    let keys = result.gen_keys.into_boxed_slice();
    let offsets = result.gen_offsets.into_boxed_slice();
    let sizes = result.gen_sizes.into_boxed_slice();
    *out_gen_count = count as i64;
    *out_gen_keys_ptr = Box::into_raw(keys) as *mut i64 as i64;
    *out_gen_offsets_ptr = Box::into_raw(offsets) as *mut i32 as i64;
    *out_gen_sizes_ptr = Box::into_raw(sizes) as *mut i32 as i64;

    // Per-merge stats out-pointers — callers always pass valid pointers (matches existing convention).
    *out_flush_and_sort_chunk_count = result.flush_and_sort_chunk_count;
    *out_flush_and_sort_chunk_time_millis = result.flush_and_sort_chunk_time_millis;
    *out_row_id_mapping_max = result.row_id_mapping_max;

    Ok(0)
}

/// Frees the heap-allocated arrays returned by `parquet_merge_files`.
#[no_mangle]
pub unsafe extern "C" fn parquet_free_merge_result(
    mapping_ptr: i64,
    mapping_len: i64,
    gen_keys_ptr: i64,
    gen_offsets_ptr: i64,
    gen_sizes_ptr: i64,
    gen_count: i64,
) {
    if mapping_ptr != 0 && mapping_len > 0 {
        let mapping_bytes = mapping_len as usize * std::mem::size_of::<i64>();
        // Java released merge mapping — free from pool
        crate::memory::merge_pool().shrink(mapping_bytes);
        let _ = Box::from_raw(slice::from_raw_parts_mut(mapping_ptr as *mut i64, mapping_len as usize));
    }
    let n = gen_count as usize;
    if gen_keys_ptr != 0 && n > 0 {
        let _ = Box::from_raw(slice::from_raw_parts_mut(gen_keys_ptr as *mut i64, n));
    }
    if gen_offsets_ptr != 0 && n > 0 {
        let _ = Box::from_raw(slice::from_raw_parts_mut(gen_offsets_ptr as *mut i32, n));
    }
    if gen_sizes_ptr != 0 && n > 0 {
        let _ = Box::from_raw(slice::from_raw_parts_mut(gen_sizes_ptr as *mut i32, n));
    }
}

// ---------------------------------------------------------------------------
// Parquet reader (for test verification)
// ---------------------------------------------------------------------------

/// Reads a parquet file and returns its contents as a JSON string.
/// Each row is a JSON object. The result is a JSON array of objects.
/// The JSON bytes are written into `out_buf`, actual length into `out_len`.
/// Returns 0 on success.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_read_as_json(
    file_ptr: *const u8,
    file_len: i64,
    out_buf: *mut u8,
    buf_capacity: i64,
    out_len: *mut i64,
) -> i64 {
    use arrow::array::Array;

    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_read_as_json: {}", e))?.to_string();

    let file = std::fs::File::open(&filename)
        .map_err(|e| format!("Failed to open {}: {}", filename, e))?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("Failed to read parquet: {}", e))?;
    let reader = builder.with_batch_size(8192).build()
        .map_err(|e| format!("Failed to build reader: {}", e))?;

    let mut rows: Vec<serde_json::Value> = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("Read error: {}", e))?;
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut obj = serde_json::Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let val = if col.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    match col.data_type() {
                        arrow::datatypes::DataType::Int32 => {
                            let arr = col.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
                            serde_json::Value::Number(arr.value(row_idx).into())
                        }
                        arrow::datatypes::DataType::Int64 => {
                            let arr = col.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
                            serde_json::Value::Number(arr.value(row_idx).into())
                        }
                        arrow::datatypes::DataType::Utf8 => {
                            let arr = col.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                            serde_json::Value::String(arr.value(row_idx).to_string())
                        }
                        arrow::datatypes::DataType::Boolean => {
                            let arr = col.as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
                            serde_json::Value::Bool(arr.value(row_idx))
                        }
                        arrow::datatypes::DataType::Float64 => {
                            let arr = col.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
                            serde_json::json!(arr.value(row_idx))
                        }
                        _ => serde_json::Value::String(format!("<unsupported:{}>", col.data_type())),
                    }
                };
                obj.insert(field.name().clone(), val);
            }
            rows.push(serde_json::Value::Object(obj));
        }
    }

    let json_str = serde_json::to_string(&rows)
        .map_err(|e| format!("JSON serialization failed: {}", e))?;
    let bytes = json_str.as_bytes();
    if bytes.len() > buf_capacity as usize {
        return Err(format!("JSON output ({} bytes) exceeds buffer capacity ({})", bytes.len(), buf_capacity));
    }
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, bytes.len());
    *out_len = bytes.len() as i64;
    Ok(0)
}

// ---------------------------------------------------------------------------
// Sort permutation memory management
// ---------------------------------------------------------------------------

/// Frees the heap-allocated row ID mapping array returned as part of `parquet_finalize_writer`.
#[no_mangle]
pub unsafe extern "C" fn parquet_free_row_id_mapping(
    mapping_ptr: i64,
    mapping_len: i64,
) {
    if mapping_ptr != 0 && mapping_len > 0 {
        let mapping_bytes = mapping_len as usize * std::mem::size_of::<i64>();
        // Java released write mapping — free from pool
        crate::memory::write_pool().shrink(mapping_bytes);
        let _ = Box::from_raw(slice::from_raw_parts_mut(mapping_ptr as *mut i64, mapping_len as usize));
    }
}

// ---------------------------------------------------------------------------
// Native runtime metrics
// ---------------------------------------------------------------------------

/// Collect a snapshot of native runtime stats for the parquet merge path.
///
/// The caller passes a buffer of 11 i64s. On success, writes the 11-stat snapshot in the order
/// documented in `ParquetNativeRuntimeStats.fromArray`. Returns 0 on success, or a negative
/// error pointer on failure (per FFM convention).
/// Returns 0 on success, negative error pointer on failure (per FFM convention).
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_collect_runtime_metrics(
    out_buf: *mut i64,
    out_len: i64,
) -> i64 {
    if out_buf.is_null() {
        return Err("parquet_collect_runtime_metrics: null out_buf".to_string());
    }
    if out_len < 17 {
        return Err(format!(
            "parquet_collect_runtime_metrics: out_len {} < 17",
            out_len
        ));
    }
    let s = crate::merge::metrics::collect();
    let arr: [i64; 17] = [
        s.rayon_configured_threads,
        s.rayon_merge_tasks_submitted,
        s.rayon_merge_tasks_started,
        s.rayon_merge_tasks_completed,
        s.rayon_merge_tasks_failed,
        s.rayon_merge_tasks_panicked,
        s.rayon_merge_wall_millis,
        s.tokio_num_workers,
        s.tokio_num_blocking_threads,
        s.tokio_active_tasks,
        s.tokio_global_queue_depth,
        s.tokio_blocking_queue_depth,
        s.tokio_local_queue_depth_total,
        s.tokio_polls_count_total,
        s.tokio_overflow_count_total,
        s.tokio_spawned_tasks_total,
        s.tokio_workers_busy_millis_total,
    ];
    std::ptr::copy_nonoverlapping(arr.as_ptr(), out_buf, 17);
    Ok(0)
}

// ---------------------------------------------------------------------------
// Memory pool management (Phase 1 stubs)
// ---------------------------------------------------------------------------

/// Initialize write and merge memory pool counters.
#[no_mangle]
pub extern "C" fn parquet_init_memory_pools(write_limit: i64, merge_limit: i64) {
    crate::memory::init_pools(write_limit as usize, merge_limit as usize);
}

/// Set write pool limit. Called by Java rebalancer via FFM.
#[no_mangle]
pub extern "C" fn parquet_set_write_pool_limit(new_limit: i64) {
    crate::memory::set_write_limit(new_limit as usize);
}

/// Set merge pool limit. Called by Java rebalancer via FFM.
#[no_mangle]
pub extern "C" fn parquet_set_merge_pool_limit(new_limit: i64) {
    crate::memory::set_merge_limit(new_limit as usize);
}

/// Get pool stats: writes 6 i64s to out_buf.
/// Layout: [write_limit, write_used, write_peak, merge_limit, merge_used, merge_peak]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_pool_stats(out_buf: *mut i64) {
    let stats = crate::memory::get_stats();
    for (i, val) in stats.iter().enumerate() {
        *out_buf.add(i) = *val as i64;
    }
}
// Parquet column reader (DocValues codec — Strategy 1 FFM bridge)
// ---------------------------------------------------------------------------
//
// Read-only per-column random-access reader used by the Lucene
// `ParquetDocValuesProducer`. The reader exposes the column's physical values
// by row position so the Java side can materialise per-document doc values.
//
// Return convention (consistent with the rest of this file):
//   - `>= 0` success. For reads, `0` (`RC_OK`) means "value(s) written"; the
//     positive sentinel `RC_OVERFLOW` (1) means "caller buffer too small —
//     required sizes were written to the out-parameters, retry once".
//   - `< 0` error pointer (negate, then `native_error_message`/`native_error_free`).
//
// NOTE on the overflow sentinel: the design sketch refers to this as
// `-E_OVERFLOW`, but this file's established FFM contract reserves *all*
// negative returns for heap error pointers (negated `Box`/`CString` address).
// Returning a small negative constant would be indistinguishable from — and
// dereferenced as — an error pointer by `native_error_message`, corrupting
// memory. We therefore signal overflow with a positive status code, mirroring
// the existing `parquet_finalize_writer` precedent (`Ok(1)` == "no writer").
// The Java wrapper checks `ret == RC_OVERFLOW` before the `ret < 0` error path.

use std::fs::File;
use std::sync::MutexGuard;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType as ArrowDataType;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelector,
};
use parquet::arrow::ProjectionMask;
use parquet::basic::Type as PhysicalType;
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::data_type::DataType as ParquetDataType;
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;

/// Read succeeded; value(s) written to the caller buffers.
pub(crate) const RC_OK: i64 = 0;
/// A caller buffer was too small. Required sizes were written to the
/// out-parameters; the caller should grow its buffers and retry once.
pub(crate) const RC_OVERFLOW: i64 = 1;

/// `expected_type` discriminants exchanged with Java
/// (matches `ParquetPhysicalType` on the Java side).
const TYPE_INT32: i32 = 0;
const TYPE_INT64: i32 = 1;
const TYPE_FLOAT: i32 = 2;
const TYPE_DOUBLE: i32 = 3;
const TYPE_BOOL: i32 = 4;
const TYPE_BYTE_ARRAY: i32 = 5;

/// One open per-column reader. Owns the file handle (via `SerializedFileReader`)
/// and the row-group layout needed to translate a global row position into a
/// `(row_group, local_offset)` pair. Random access reopens the relevant row
/// group per call — this is the documented *slow path*; the hot path goes
/// through `parquet_decode_page_at_row` (page-resident caching on the Java side),
/// which decodes via the arrow-rs record-batch reader built from the
/// once-parsed `arrow_metadata` (no footer re-read per decode).
struct ColumnReaderState {
    /// Low-level reader kept for the slow paths (`parquet_read_value_at_row`,
    /// `parquet_read_repeated_at_row`); the page-decode hot path uses
    /// `arrow_metadata` + `file` instead.
    reader: SerializedFileReader<File>,
    /// Parsed footer + arrow schema, constructed ONCE at `open()` (including the
    /// page index) and reused for every `parquet_decode_page_at_row` via
    /// `ParquetRecordBatchReaderBuilder::new_with_metadata` — cloning it is
    /// cheap (`Arc`s all the way down).
    arrow_metadata: ArrowReaderMetadata,
    /// File handle for the arrow decode path; `try_clone()`d per decode (cheaper
    /// than reopening by path).
    file: File,
    /// Leaf column index within the Parquet schema descriptor.
    leaf_idx: usize,
    /// Physical type of the column (validated against the caller's expectation).
    physical_type: PhysicalType,
    /// True when the column has a repetition level > 0 (multi-valued).
    repeated: bool,
    /// Total number of rows (records) in the file.
    row_count: i64,
    /// Global row index of the first row in each row group.
    rg_first_row: Vec<i64>,
    /// Number of rows in each row group.
    rg_num_rows: Vec<i64>,
    /// Per-page layout (Layer 3 jump table + Layer 4 page stats), ascending by
    /// `global_first_row`. Built once at `open()` from the Parquet OffsetIndex +
    /// ColumnIndex when present, else one entry per row group as a fallback.
    pages: Vec<PageEntry>,
    /// Codec-local file id (path→id) for the cross-query decoded-page cache key. See
    /// `crate::liquid_page_cache`.
    liquid_file_id: u32,
}

/// One row-aligned page in the column (or one row group, in the no-page-index
/// fallback). All row indices are global (file-relative).
struct PageEntry {
    /// Global index of the first row in the page.
    global_first_row: i64,
    /// Number of rows in the page.
    num_rows: i64,
    /// Byte offset of the page in the file (Layer 3). 0 when unknown.
    file_offset: i64,
    /// Compressed page size in bytes (Layer 3). 0 when unknown.
    compressed_size: i32,
    /// Number of nulls in the page (Layer 4). -1 when unknown.
    null_count: i64,
    /// Min value raw bits (Layer 4); meaningful only for numeric columns with a
    /// page index, else 0.
    min_long: i64,
    /// Max value raw bits (Layer 4); meaningful only for numeric columns with a
    /// page index, else 0.
    max_long: i64,
    /// Row group containing the page.
    rg_idx: usize,
    /// Index of the page's first row within its row group.
    local_first_row: i64,
}

impl ColumnReaderState {
    fn open(filename: &str, column: &str, expected_type: i32) -> Result<ColumnReaderState, String> {
        let file = File::open(filename).map_err(|e| format!("Failed to open '{}': {}", filename, e))?;
        // Keep a second handle to the same file for the arrow decode path
        // (the first is consumed by `SerializedFileReader`).
        let arrow_file = file
            .try_clone()
            .map_err(|e| format!("Failed to clone file handle for '{}': {}", filename, e))?;
        // Load the Parquet page index (OffsetIndex + ColumnIndex) so we can build
        // the Layer 3/4 page layout. Falls back gracefully to row-group
        // granularity if the file was written without a page index.
        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(file, options)
            .map_err(|e| format!("Failed to read parquet '{}': {}", filename, e))?;

        // Extract everything we need from the (borrowed) metadata inside a block so
        // the borrow ends before we move `reader` into the returned struct.
        let (leaf_idx, physical_type, repeated, row_count, rg_first_row, rg_num_rows, pages, arrow_metadata) = {
            let metadata = reader.metadata();
            let schema = metadata.file_metadata().schema_descr();

            let mut found: Option<(usize, PhysicalType, i16)> = None;
            for i in 0..schema.num_columns() {
                let descr = schema.column(i);
                if descr.name() == column || descr.path().string() == column {
                    found = Some((i, descr.physical_type(), descr.max_rep_level()));
                    break;
                }
            }
            let (leaf_idx, phys, max_rep) = found.ok_or_else(|| {
                format!("Column '{}' not found in parquet file '{}'", column, filename)
            })?;

            let actual = physical_type_code(phys);
            if actual != expected_type {
                return Err(format!(
                    "Column '{}' physical type mismatch in '{}': expected type code {}, found {:?} (code {})",
                    column, filename, expected_type, phys, actual
                ));
            }

            let n_rg = metadata.num_row_groups();
            let mut rg_first_row = Vec::with_capacity(n_rg);
            let mut rg_num_rows = Vec::with_capacity(n_rg);
            let mut acc = 0i64;
            for i in 0..n_rg {
                let rn = metadata.row_group(i).num_rows();
                rg_first_row.push(acc);
                rg_num_rows.push(rn);
                acc += rn;
            }
            let row_count = metadata.file_metadata().num_rows();

            let pages = build_page_layout(metadata, leaf_idx, phys, &rg_first_row, &rg_num_rows);

            // Parse the footer into arrow-reader form ONCE. `try_new` reuses the
            // already-parsed `ParquetMetaData` (page index included, loaded above),
            // so per-decode builders never re-read the footer from disk.
            let arrow_metadata = ArrowReaderMetadata::try_new(
                std::sync::Arc::new(metadata.clone()),
                ArrowReaderOptions::new(),
            )
            .map_err(|e| {
                format!("Failed to build arrow reader metadata for '{}': {}", filename, e)
            })?;

            (leaf_idx, phys, max_rep > 0, row_count, rg_first_row, rg_num_rows, pages, arrow_metadata)
        };

        let liquid_file_id = crate::liquid_page_cache::file_id(filename);

        Ok(ColumnReaderState {
            reader,
            arrow_metadata,
            file: arrow_file,
            leaf_idx,
            physical_type,
            repeated,
            row_count,
            rg_first_row,
            rg_num_rows,
            pages,
            liquid_file_id,
        })
    }

    /// Translate a global row position into `(row_group_index, local_offset)`.
    fn locate(&self, row: i64) -> Result<(usize, i64), String> {
        for i in 0..self.rg_first_row.len() {
            let start = self.rg_first_row[i];
            let end = start + self.rg_num_rows[i];
            if row >= start && row < end {
                return Ok((i, row - start));
            }
        }
        Err(format!("Row {} not found in any row group (row count {})", row, self.row_count))
    }

    /// Find the index of the page containing global row `row` (binary search over
    /// the ascending page layout).
    fn page_for_row(&self, row: i64) -> Result<usize, String> {
        // partition_point finds the first page whose global_first_row > row; the
        // page we want is the one immediately before it.
        let p = self.pages.partition_point(|e| e.global_first_row <= row);
        if p == 0 {
            return Err(format!("Row {} precedes the first page (row count {})", row, self.row_count));
        }
        let idx = p - 1;
        let entry = &self.pages[idx];
        if row >= entry.global_first_row && row < entry.global_first_row + entry.num_rows {
            Ok(idx)
        } else {
            Err(format!("Row {} not found in any page (row count {})", row, self.row_count))
        }
    }
}

/// Builds the per-page layout for a column. Prefers the Parquet OffsetIndex +
/// ColumnIndex (true page granularity); falls back to one entry per row group
/// when the file has no page index.
fn build_page_layout(
    metadata: &parquet::file::metadata::ParquetMetaData,
    leaf_idx: usize,
    phys: PhysicalType,
    rg_first_row: &[i64],
    rg_num_rows: &[i64],
) -> Vec<PageEntry> {
    let n_rg = metadata.num_row_groups();
    let offset_index = metadata.offset_index();
    let column_index = metadata.column_index();

    let mut pages: Vec<PageEntry> = Vec::new();

    for rg in 0..n_rg {
        let oi_pages = offset_index
            .and_then(|oi| oi.get(rg))
            .and_then(|cols| cols.get(leaf_idx));
        let ci = column_index
            .and_then(|ci| ci.get(rg))
            .and_then(|cols| cols.get(leaf_idx));

        match oi_pages {
            Some(oi) => {
                let locations = oi.page_locations();
                let rg_rows = rg_num_rows[rg];
                for (p, loc) in locations.iter().enumerate() {
                    let local_first = loc.first_row_index;
                    let next_local = if p + 1 < locations.len() {
                        locations[p + 1].first_row_index
                    } else {
                        rg_rows
                    };
                    let num_rows = next_local - local_first;
                    let null_count = ci.and_then(|c| c.null_count(p)).unwrap_or(-1);
                    let (min_long, max_long) = ci
                        .map(|c| page_min_max(c, p, phys))
                        .unwrap_or((0, 0));
                    pages.push(PageEntry {
                        global_first_row: rg_first_row[rg] + local_first,
                        num_rows,
                        file_offset: loc.offset,
                        compressed_size: loc.compressed_page_size,
                        null_count,
                        min_long,
                        max_long,
                        rg_idx: rg,
                        local_first_row: local_first,
                    });
                }
            }
            None => {
                // Fallback: treat the whole row group as a single "page".
                let cc = metadata.row_group(rg).column(leaf_idx);
                let null_count = cc
                    .statistics()
                    .and_then(|s| s.null_count_opt())
                    .map(|n| n as i64)
                    .unwrap_or(-1);
                let compressed = cc.compressed_size().min(i32::MAX as i64) as i32;
                pages.push(PageEntry {
                    global_first_row: rg_first_row[rg],
                    num_rows: rg_num_rows[rg],
                    file_offset: cc.data_page_offset(),
                    compressed_size: compressed,
                    null_count,
                    min_long: 0,
                    max_long: 0,
                    rg_idx: rg,
                    local_first_row: 0,
                });
            }
        }
    }

    pages
}

/// Extracts the per-page min/max as raw i64 bits from a typed ColumnIndex.
/// Returns `(0, 0)` for byte-array/unsupported columns (binary min/max is not
/// exchanged as i64; the Java side treats it as "unused").
fn page_min_max(ci: &ColumnIndexMetaData, idx: usize, _phys: PhysicalType) -> (i64, i64) {
    match ci {
        ColumnIndexMetaData::INT32(p) => (
            p.min_value(idx).map(|v| *v as i64).unwrap_or(0),
            p.max_value(idx).map(|v| *v as i64).unwrap_or(0),
        ),
        ColumnIndexMetaData::INT64(p) => (
            p.min_value(idx).copied().unwrap_or(0),
            p.max_value(idx).copied().unwrap_or(0),
        ),
        ColumnIndexMetaData::FLOAT(p) => (
            p.min_value(idx).map(|v| v.to_bits() as i64).unwrap_or(0),
            p.max_value(idx).map(|v| v.to_bits() as i64).unwrap_or(0),
        ),
        ColumnIndexMetaData::DOUBLE(p) => (
            p.min_value(idx).map(|v| v.to_bits() as i64).unwrap_or(0),
            p.max_value(idx).map(|v| v.to_bits() as i64).unwrap_or(0),
        ),
        ColumnIndexMetaData::BOOLEAN(p) => (
            p.min_value(idx).map(|v| if *v { 1 } else { 0 }).unwrap_or(0),
            p.max_value(idx).map(|v| if *v { 1 } else { 0 }).unwrap_or(0),
        ),
        _ => (0, 0),
    }
}

/// Maps a Parquet physical type to the Java-facing `expected_type` discriminant.
/// Returns `-1` for unsupported physical types (e.g. INT96), which can never
/// match a valid expectation and therefore surfaces as a clear mismatch error.
fn physical_type_code(t: PhysicalType) -> i32 {
    match t {
        PhysicalType::BOOLEAN => TYPE_BOOL,
        PhysicalType::INT32 => TYPE_INT32,
        PhysicalType::INT64 => TYPE_INT64,
        PhysicalType::FLOAT => TYPE_FLOAT,
        PhysicalType::DOUBLE => TYPE_DOUBLE,
        PhysicalType::BYTE_ARRAY => TYPE_BYTE_ARRAY,
        PhysicalType::FIXED_LEN_BYTE_ARRAY => TYPE_BYTE_ARRAY,
        PhysicalType::INT96 => -1,
    }
}

/// Reads exactly one record (after skipping `skip` records) from a typed column
/// reader, returning that record's non-null values. For a single-valued column
/// the result holds 0 (null) or 1 value; for a repeated column it holds all the
/// values of the record.
fn read_record_values<T: ParquetDataType>(
    r: &mut ColumnReaderImpl<T>,
    skip: usize,
) -> Result<Vec<T::T>, String> {
    if skip > 0 {
        let skipped = r.skip_records(skip).map_err(|e| e.to_string())?;
        if skipped < skip {
            return Err(format!("requested skip of {} records but only {} available", skip, skipped));
        }
    }
    let mut def_levels: Vec<i16> = Vec::new();
    let mut rep_levels: Vec<i16> = Vec::new();
    let mut values: Vec<T::T> = Vec::new();
    r.read_records(1, Some(&mut def_levels), Some(&mut rep_levels), &mut values)
        .map_err(|e| e.to_string())?;
    Ok(values)
}

lazy_static! {
    /// Per-handle registry of open column readers, keyed by an opaque i64 handle.
    /// The outer map is behind a mutex held ONLY for handle lookup/insert/remove;
    /// each reader's state is behind its own `Arc<Mutex<...>>` so a decode (or a
    /// background prefetch) on one handle never serialises decodes on another.
    static ref COLUMN_READERS: Mutex<ColumnReaderRegistry> = Mutex::new(HashMap::new());
}

/// Registry type: outer map lock for handle bookkeeping, per-handle inner lock
/// for the reader state itself.
type ColumnReaderRegistry = HashMap<i64, Arc<Mutex<ColumnReaderState>>>;

/// Monotonic handle allocator. Always `>= 0`, so a returned handle is never
/// confused with the `< 0` error-pointer convention.
static NEXT_COLUMN_READER_HANDLE: AtomicI64 = AtomicI64::new(0);

/// Locks the column-reader registry (the outer map lock — hold only for handle
/// lookup/insert/remove, never across IO or decode), converting a poisoned
/// mutex into a normal FFM error instead of propagating the panic.
fn lock_registry<'a>() -> Result<MutexGuard<'a, ColumnReaderRegistry>, String> {
    COLUMN_READERS
        .lock()
        .map_err(|_| "column reader registry mutex poisoned".to_string())
}

/// Resolves `handle` to its state cell, holding the registry lock only for the
/// `Arc` clone. `ctx` names the calling entry point for the error message.
fn state_for_handle(handle: i64, ctx: &str) -> Result<Arc<Mutex<ColumnReaderState>>, String> {
    lock_registry()?
        .get(&handle)
        .cloned()
        .ok_or_else(|| format!("{}: unknown handle {}", ctx, handle))
}

/// Locks one column reader's state for the duration of a call, converting a
/// poisoned mutex into a normal FFM error.
fn lock_state(cell: &Arc<Mutex<ColumnReaderState>>) -> Result<MutexGuard<'_, ColumnReaderState>, String> {
    cell.lock()
        .map_err(|_| "column reader state mutex poisoned".to_string())
}

/// Opens a per-column reader over `file` for `col`, validating that the column
/// exists and its physical type matches `expected_type`
/// (0=INT32,1=INT64,2=FLOAT,3=DOUBLE,4=BOOL,5=BYTE_ARRAY).
///
/// Returns `>= 0` handle id on success, `< 0` negated error pointer on failure.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_open_column_reader(
    file_ptr: *const u8,
    file_len: i64,
    col_ptr: *const u8,
    col_len: i64,
    expected_type: i32,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_open_column_reader file: {}", e))?
        .to_string();
    let column = str_from_raw(col_ptr, col_len)
        .map_err(|e| format!("parquet_open_column_reader column: {}", e))?
        .to_string();

    let state = ColumnReaderState::open(&filename, &column, expected_type)?;

    let handle = NEXT_COLUMN_READER_HANDLE.fetch_add(1, Ordering::SeqCst);
    lock_registry()?.insert(handle, Arc::new(Mutex::new(state)));
    log_debug!(
        "parquet_open_column_reader: file={}, column={}, handle={}",
        filename, column, handle
    );
    Ok(handle)
}

/// Closes a column reader handle and releases its file handle and buffers.
/// Returns `0` on success, a `< 0` error pointer if the handle is unknown.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_close_column_reader(handle: i64) -> i64 {
    match lock_registry()?.remove(&handle) {
        Some(_) => {
            log_debug!("parquet_close_column_reader: handle={}", handle);
            Ok(RC_OK)
        }
        None => Err(format!("parquet_close_column_reader: unknown handle {}", handle)),
    }
}

/// Debug-only symbol: returns the number of currently open column-reader
/// handles. Used by Property 7 (native handle non-leakage). Never errors;
/// recovers from a poisoned mutex rather than panicking.
#[no_mangle]
pub unsafe extern "C" fn parquet_open_column_reader_count() -> i64 {
    match COLUMN_READERS.lock() {
        Ok(guard) => guard.len() as i64,
        Err(poisoned) => poisoned.into_inner().len() as i64,
    }
}

/// Enable/disable the cross-query decoded-page cache and set its memory budget (bytes) + store
/// directory. Called by Java at plugin init when the `parquet_liquid_cache` feature flag is on. When
/// disabled (the default), `parquet_decode_page_at_row` never consults the cache and the decode
/// path is unchanged. A `max_memory_bytes` of 0 leaves the liquid-cache default budget. `cache_dir`
/// must point to a writable directory on real disk (Java passes a path under the node data dir); the
/// `t4` store is mounted inside it. If the store can't be built the cache silently disables itself.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_liquid_cache_set_enabled(
    enabled: i32,
    max_memory_bytes: i64,
    cache_dir_ptr: *const u8,
    cache_dir_len: i64,
) -> i64 {
    let bytes = if max_memory_bytes > 0 { max_memory_bytes as usize } else { 0 };
    let cache_dir = str_from_raw(cache_dir_ptr, cache_dir_len)
        .map_err(|e| format!("parquet_liquid_cache_set_enabled cache_dir: {}", e))?
        .to_string();
    crate::liquid_page_cache::set_enabled(enabled != 0, bytes, &cache_dir);
    Ok(0)
}

/// Slow-path single-value read at `row`.
///
/// On success writes:
///   - `out_present` = 1 if the row has a value, 0 if null/absent
///   - `out_long`    = the value's raw bits for primitive columns:
///                       INT32 sign-extended to i64; INT64 verbatim;
///                       FLOAT  = `f32::to_bits` (zero-extended);
///                       DOUBLE = `f64::to_bits`;
///                       BOOL   = 0 or 1
///   - for BYTE_ARRAY columns: the value bytes are copied into `out_buf` and
///     `out_len` is set to the byte length (or -1 when the value is null).
///
/// Returns a `< 0` error pointer naming the row when `row >= row_count`, when
/// the handle is unknown, or when `out_buf` is too small for a BYTE_ARRAY value
/// (in which case `out_len` is set to the required length first).
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_read_value_at_row(
    handle: i64,
    row: i64,
    out_present: *mut i64,
    out_long: *mut i64,
    out_buf: *mut u8,
    out_buf_cap: i64,
    out_len: *mut i64,
) -> i64 {
    let cell = state_for_handle(handle, "parquet_read_value_at_row")?;
    let state = lock_state(&cell)?;

    if row < 0 {
        return Err(format!("parquet_read_value_at_row: negative row {}", row));
    }
    if row >= state.row_count {
        return Err(format!(
            "parquet_read_value_at_row: row {} out of range (row count {})",
            row, state.row_count
        ));
    }

    // Default outputs: absent value.
    if !out_present.is_null() {
        *out_present = 0;
    }
    if !out_long.is_null() {
        *out_long = 0;
    }
    if !out_len.is_null() {
        *out_len = -1;
    }

    let (rg_idx, local) = state.locate(row)?;
    let rg = state.reader.get_row_group(rg_idx).map_err(|e| e.to_string())?;
    let col = rg.get_column_reader(state.leaf_idx).map_err(|e| e.to_string())?;
    let local = local as usize;

    match col {
        ColumnReader::Int32ColumnReader(mut r) => {
            if let Some(v) = read_record_values(&mut r, local)?.first() {
                set_present(out_present, out_long, *v as i64);
            }
        }
        ColumnReader::Int64ColumnReader(mut r) => {
            if let Some(v) = read_record_values(&mut r, local)?.first() {
                set_present(out_present, out_long, *v);
            }
        }
        ColumnReader::FloatColumnReader(mut r) => {
            if let Some(v) = read_record_values(&mut r, local)?.first() {
                set_present(out_present, out_long, v.to_bits() as i64);
            }
        }
        ColumnReader::DoubleColumnReader(mut r) => {
            if let Some(v) = read_record_values(&mut r, local)?.first() {
                set_present(out_present, out_long, v.to_bits() as i64);
            }
        }
        ColumnReader::BoolColumnReader(mut r) => {
            if let Some(v) = read_record_values(&mut r, local)?.first() {
                set_present(out_present, out_long, if *v { 1 } else { 0 });
            }
        }
        ColumnReader::ByteArrayColumnReader(mut r) => {
            if let Some(v) = read_record_values(&mut r, local)?.first() {
                return write_bytes_value(v.data(), out_present, out_buf, out_buf_cap, out_len);
            }
        }
        ColumnReader::FixedLenByteArrayColumnReader(mut r) => {
            if let Some(v) = read_record_values(&mut r, local)?.first() {
                return write_bytes_value(v.data(), out_present, out_buf, out_buf_cap, out_len);
            }
        }
        ColumnReader::Int96ColumnReader(_) => {
            return Err("parquet_read_value_at_row: INT96 columns are not supported".to_string());
        }
    }

    Ok(RC_OK)
}

/// Marks a primitive value present and stores its raw bits.
unsafe fn set_present(out_present: *mut i64, out_long: *mut i64, bits: i64) {
    if !out_present.is_null() {
        *out_present = 1;
    }
    if !out_long.is_null() {
        *out_long = bits;
    }
}

/// Copies a single BYTE_ARRAY value into the caller buffer. Sets `out_present=1`
/// and `out_len` to the byte length. Returns `RC_OVERFLOW` (after recording the
/// required length in `out_len`) when the value does not fit in `out_buf_cap`,
/// so the caller can grow its buffer and retry once.
unsafe fn write_bytes_value(
    bytes: &[u8],
    out_present: *mut i64,
    out_buf: *mut u8,
    out_buf_cap: i64,
    out_len: *mut i64,
) -> Result<i64, String> {
    if !out_present.is_null() {
        *out_present = 1;
    }
    let n = bytes.len();
    if !out_len.is_null() {
        *out_len = n as i64;
    }
    if (n as i64) > out_buf_cap || (n > 0 && out_buf.is_null()) {
        return Ok(RC_OVERFLOW);
    }
    if n > 0 {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, n);
    }
    Ok(RC_OK)
}

/// Slow-path repeated read at `row` for a repeated (multi-valued) column.
///
/// Capacity contract: `out_long_cap` is the maximum element count for *both*
/// primitive and BYTE_ARRAY columns; `out_byte_offsets` (BYTE_ARRAY only) must
/// have capacity `out_long_cap + 1`.
///
/// On success (`RC_OK`):
///   - `out_count` = number of values at the row
///   - primitive columns: raw bits (see `parquet_read_value_at_row`) written to
///     `out_longs`
///   - BYTE_ARRAY columns: concatenated bytes in `out_byte_buf`, CSR offsets
///     (length `count + 1`) in `out_byte_offsets`
///
/// On `RC_OVERFLOW`: `out_count` holds the required element count. When the
/// element count fits but only the byte buffer is too small, the full CSR
/// offsets are still written so `out_byte_offsets[count]` reports the required
/// total byte size, enabling a single retry.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_read_repeated_at_row(
    handle: i64,
    row: i64,
    out_count: *mut i64,
    out_longs: *mut i64,
    out_long_cap: i64,
    out_byte_buf: *mut u8,
    out_byte_offsets: *mut i64,
    out_byte_buf_cap: i64,
) -> i64 {
    let cell = state_for_handle(handle, "parquet_read_repeated_at_row")?;
    let state = lock_state(&cell)?;

    if row < 0 {
        return Err(format!("parquet_read_repeated_at_row: negative row {}", row));
    }
    if row >= state.row_count {
        return Err(format!(
            "parquet_read_repeated_at_row: row {} out of range (row count {})",
            row, state.row_count
        ));
    }

    if !out_count.is_null() {
        *out_count = 0;
    }

    let (rg_idx, local) = state.locate(row)?;
    let rg = state.reader.get_row_group(rg_idx).map_err(|e| e.to_string())?;
    let col = rg.get_column_reader(state.leaf_idx).map_err(|e| e.to_string())?;
    let local = local as usize;

    match col {
        ColumnReader::Int32ColumnReader(mut r) => {
            let vals = read_record_values(&mut r, local)?;
            write_primitive_repeated(vals.iter().map(|v| *v as i64), vals.len(), out_count, out_longs, out_long_cap)
        }
        ColumnReader::Int64ColumnReader(mut r) => {
            let vals = read_record_values(&mut r, local)?;
            write_primitive_repeated(vals.iter().copied(), vals.len(), out_count, out_longs, out_long_cap)
        }
        ColumnReader::FloatColumnReader(mut r) => {
            let vals = read_record_values(&mut r, local)?;
            write_primitive_repeated(vals.iter().map(|v| v.to_bits() as i64), vals.len(), out_count, out_longs, out_long_cap)
        }
        ColumnReader::DoubleColumnReader(mut r) => {
            let vals = read_record_values(&mut r, local)?;
            write_primitive_repeated(vals.iter().map(|v| v.to_bits() as i64), vals.len(), out_count, out_longs, out_long_cap)
        }
        ColumnReader::BoolColumnReader(mut r) => {
            let vals = read_record_values(&mut r, local)?;
            write_primitive_repeated(vals.iter().map(|v| if *v { 1i64 } else { 0i64 }), vals.len(), out_count, out_longs, out_long_cap)
        }
        ColumnReader::ByteArrayColumnReader(mut r) => {
            let vals = read_record_values(&mut r, local)?;
            let slices: Vec<&[u8]> = vals.iter().map(|v| v.data()).collect();
            write_bytes_repeated(&slices, out_count, out_long_cap, out_byte_buf, out_byte_offsets, out_byte_buf_cap)
        }
        ColumnReader::FixedLenByteArrayColumnReader(mut r) => {
            let vals = read_record_values(&mut r, local)?;
            let slices: Vec<&[u8]> = vals.iter().map(|v| v.data()).collect();
            write_bytes_repeated(&slices, out_count, out_long_cap, out_byte_buf, out_byte_offsets, out_byte_buf_cap)
        }
        ColumnReader::Int96ColumnReader(_) => {
            Err("parquet_read_repeated_at_row: INT96 columns are not supported".to_string())
        }
    }
}

/// Writes repeated primitive values to `out_longs`, or reports overflow.
unsafe fn write_primitive_repeated(
    values: impl Iterator<Item = i64>,
    count: usize,
    out_count: *mut i64,
    out_longs: *mut i64,
    out_long_cap: i64,
) -> Result<i64, String> {
    if !out_count.is_null() {
        *out_count = count as i64;
    }
    if (count as i64) > out_long_cap || out_longs.is_null() {
        return Ok(RC_OVERFLOW);
    }
    for (i, v) in values.enumerate() {
        *out_longs.add(i) = v;
    }
    Ok(RC_OK)
}

/// Writes repeated BYTE_ARRAY values (CSR layout) to the caller buffers, or
/// reports overflow with required sizes.
unsafe fn write_bytes_repeated(
    slices: &[&[u8]],
    out_count: *mut i64,
    out_long_cap: i64,
    out_byte_buf: *mut u8,
    out_byte_offsets: *mut i64,
    out_byte_buf_cap: i64,
) -> Result<i64, String> {
    let count = slices.len();
    let total_bytes: usize = slices.iter().map(|s| s.len()).sum();
    if !out_count.is_null() {
        *out_count = count as i64;
    }

    // Element-count overflow: cannot safely write offsets (capacity is count+1).
    if (count as i64) > out_long_cap {
        return Ok(RC_OVERFLOW);
    }

    // Element count fits: write the full CSR offsets so that, even on a byte
    // overflow, out_byte_offsets[count] == total_bytes reports the required size.
    if !out_byte_offsets.is_null() {
        let mut acc = 0i64;
        for (i, s) in slices.iter().enumerate() {
            *out_byte_offsets.add(i) = acc;
            acc += s.len() as i64;
        }
        *out_byte_offsets.add(count) = acc;
    }

    if (total_bytes as i64) > out_byte_buf_cap || (total_bytes > 0 && out_byte_buf.is_null()) {
        return Ok(RC_OVERFLOW);
    }

    let mut acc = 0usize;
    for s in slices {
        if !s.is_empty() {
            std::ptr::copy_nonoverlapping(s.as_ptr(), out_byte_buf.add(acc), s.len());
        }
        acc += s.len();
    }
    Ok(RC_OK)
}

// ---------------------------------------------------------------------------
// Page-index loader + page decoder (DocValues codec — cache Layers 1-4)
// ---------------------------------------------------------------------------
//
// These are the hot-path functions used by the Java `ParquetColumnReader`:
//   - `parquet_get_column_num_pages`  — page count, so Java can pre-size buffers
//   - `parquet_get_column_page_index` — Layer 3 jump table + Layer 4 page stats
//   - `parquet_decode_page_at_row`    — Layer 1 values + Layer 2 presence bitset
//
// All row indices exchanged here are global (file-relative), consistent with
// the Row ID = Doc ID invariant.

/// Returns the number of pages in the column (`>= 0`), or a `< 0` error pointer
/// for an unknown handle. Java reads this first to size the parallel arrays
/// passed to `parquet_get_column_page_index`.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_column_num_pages(handle: i64) -> i64 {
    let cell = state_for_handle(handle, "parquet_get_column_num_pages")?;
    let state = lock_state(&cell)?;
    Ok(state.pages.len() as i64)
}

/// Layer 3 + 4: writes the column's per-page jump table and page statistics into
/// caller-provided parallel arrays, each of capacity `out_buf_capacity`
/// (= the page count from `parquet_get_column_num_pages`).
///
/// Arrays (length = page count):
///   - `out_first_row`       global index of the page's first row
///   - `out_file_offset`     byte offset of the page in the file (0 if unknown)
///   - `out_compressed_size` compressed page size in bytes (0 if unknown)
///   - `out_null_count`      nulls in the page, or -1 when unknown
///   - `out_min_long`        per-page min raw bits (numeric only; 0 otherwise)
///   - `out_max_long`        per-page max raw bits (numeric only; 0 otherwise)
///
/// `out_actual_pages` always receives the true page count. Returns `RC_OVERFLOW`
/// (a positive sentinel) without writing the arrays when `out_buf_capacity` is
/// smaller than the page count, so the caller can grow and retry. Returns a
/// `< 0` error pointer for an unknown handle.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_column_page_index(
    handle: i64,
    out_first_row: *mut i64,
    out_file_offset: *mut i64,
    out_compressed_size: *mut i32,
    out_null_count: *mut i64,
    out_min_long: *mut i64,
    out_max_long: *mut i64,
    out_buf_capacity: i64,
    out_actual_pages: *mut i64,
) -> i64 {
    let cell = state_for_handle(handle, "parquet_get_column_page_index")?;
    let state = lock_state(&cell)?;

    let n = state.pages.len();
    if !out_actual_pages.is_null() {
        *out_actual_pages = n as i64;
    }
    if (n as i64) > out_buf_capacity {
        return Ok(RC_OVERFLOW);
    }

    for (i, e) in state.pages.iter().enumerate() {
        if !out_first_row.is_null() {
            *out_first_row.add(i) = e.global_first_row;
        }
        if !out_file_offset.is_null() {
            *out_file_offset.add(i) = e.file_offset;
        }
        if !out_compressed_size.is_null() {
            *out_compressed_size.add(i) = e.compressed_size;
        }
        if !out_null_count.is_null() {
            *out_null_count.add(i) = e.null_count;
        }
        if !out_min_long.is_null() {
            *out_min_long.add(i) = e.min_long;
        }
        if !out_max_long.is_null() {
            *out_max_long.add(i) = e.max_long;
        }
    }
    Ok(RC_OK)
}

/// Packs a per-row presence slice into a little-endian `long[]` bitset (bit i set
/// when row i is present), writing into `out` (capacity `out_words`). Returns the
/// number of words required; on capacity shortfall writes nothing and the caller
/// treats the positive required count as an overflow signal.
unsafe fn write_presence_bitset(presence: &[bool], out: *mut i64, out_words: i64) -> i64 {
    let words_needed = ((presence.len() + 63) / 64) as i64;
    if words_needed > out_words || out.is_null() {
        return words_needed;
    }
    for w in 0..words_needed as usize {
        let mut bits: u64 = 0;
        let base = w * 64;
        for b in 0..64 {
            let idx = base + b;
            if idx >= presence.len() {
                break;
            }
            if presence[idx] {
                bits |= 1u64 << b;
            }
        }
        *out.add(w) = bits as i64;
    }
    words_needed
}

/// Layer 1 + 2: decode the page containing global row `row` into caller buffers.
///
/// On success (`RC_OK`):
///   - `out_first_row` / `out_last_row` = inclusive global row range of the page
///   - primitive columns: per-row raw bits written to `out_value_buf`
///     (interpreted as `long[]`, one slot per row; null rows hold 0);
///     `out_value_actual_len` = `rows * 8`
///   - BYTE_ARRAY columns: concatenated value bytes in `out_value_buf`, per-row
///     CSR offsets (length `rows + 1`) in `out_byte_offsets`;
///     `out_value_actual_len` = total bytes used
///   - `out_presence_bitset` = packed `long[]`, one bit per row
///
/// On `RC_OVERFLOW` (a positive sentinel): `out_first_row`, `out_last_row` and
/// `out_value_actual_len` are populated so the caller can size every buffer
/// (values = `out_value_actual_len` bytes; offsets = `rows + 1`; presence =
/// `ceil(rows / 64)` words) and retry once. Returns a `< 0` error pointer for an
/// unknown handle, an out-of-range row, or a repeated (multi-valued) column.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_decode_page_at_row(
    handle: i64,
    row: i64,
    out_first_row: *mut i64,
    out_last_row: *mut i64,
    out_value_buf: *mut u8,
    out_value_buf_cap: i64,
    out_value_actual_len: *mut i64,
    out_byte_offsets: *mut i32,
    out_byte_offsets_cap: i64,
    out_presence_bitset: *mut i64,
    out_presence_bits_cap: i64,
) -> i64 {
    let cell = state_for_handle(handle, "parquet_decode_page_at_row")?;
    let state = lock_state(&cell)?;

    if row < 0 || row >= state.row_count {
        return Err(format!(
            "parquet_decode_page_at_row: row {} out of range (row count {})",
            row, state.row_count
        ));
    }
    if state.repeated {
        return Err(format!(
            "parquet_decode_page_at_row: column is repeated (multi-valued); use parquet_read_repeated_at_row (handle {})",
            handle
        ));
    }

    let page_idx = state.page_for_row(row)?;
    let (rg_idx, local_first, num_rows, first_global) = {
        let e = &state.pages[page_idx];
        (e.rg_idx, e.local_first_row, e.num_rows, e.global_first_row)
    };
    let num_rows_usize = num_rows as usize;

    // Always report the page row range so the caller can bound its cache and
    // size buffers even on the overflow path.
    if !out_first_row.is_null() {
        *out_first_row = first_global;
    }
    if !out_last_row.is_null() {
        *out_last_row = first_global + num_rows - 1;
    }

    // Cross-query decoded-page cache (codec-owned liquid instance). When enabled, a hit serves the
    // decoded page from the node-level cache and skips the Parquet decode below entirely; a miss
    // decodes as usual and backfills the cache (see the primitive arms). Keyed by
    // (file, column, page); primitives only. No-op when the feature flag is off.
    let lc_eid = if crate::liquid_page_cache::enabled() {
        Some(crate::liquid_page_cache::entry_id(
            state.liquid_file_id,
            state.leaf_idx as u32,
            page_idx as u32,
        ))
    } else {
        None
    };
    if let Some(eid) = lc_eid {
        // Serve a cache hit straight into the caller's out-buffers (two memcpys), bypassing the
        // Vec<i64>+Vec<bool> rebuild that get_page does and its recopy through write_primitive_page.
        if let Some(rc) = crate::liquid_page_cache::get_page_into_outbuf(
            eid, out_value_buf, out_value_buf_cap, out_value_actual_len,
            out_presence_bitset, out_presence_bits_cap,
        ) {
            return Ok(rc);
        }
    }

    // Liquid miss: decode the whole page via the arrow record-batch reader.
    let array = decode_page_arrow(&state, rg_idx, local_first, num_rows_usize)?;

    match state.physical_type {
        PhysicalType::BOOLEAN
        | PhysicalType::INT32
        | PhysicalType::INT64
        | PhysicalType::FLOAT
        | PhysicalType::DOUBLE => {
            match lc_eid {
                // Liquid on: build owned vecs so we can backfill the cache, then write.
                Some(eid) => {
                    let (longs, presence) = arrow_primitive_to_page(&array)?;
                    crate::liquid_page_cache::put_page(eid, &longs, &presence);
                    write_primitive_page(
                        &longs, &presence, out_value_buf, out_value_buf_cap, out_value_actual_len,
                        out_presence_bitset, out_presence_bits_cap,
                    )
                }
                // Liquid off (codec default): write straight from the arrow array into
                // the FFM buffers, skipping the intermediate Vec<i64> + Vec<bool>.
                None => write_primitive_page_from_arrow(
                    &array, out_value_buf, out_value_buf_cap, out_value_actual_len,
                    out_presence_bitset, out_presence_bits_cap,
                ),
            }
        }
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
            let (slices, presence) = arrow_bytes_to_page(&array)?;
            write_bytes_page(
                &slices, &presence, out_value_buf, out_value_buf_cap, out_value_actual_len,
                out_byte_offsets, out_byte_offsets_cap, out_presence_bitset, out_presence_bits_cap,
            )
        }
        PhysicalType::INT96 => {
            Err("parquet_decode_page_at_row: INT96 columns are not supported".to_string())
        }
    }
}

/// Decodes one whole page (`num_rows` records starting `local_first` into row
/// group `rg_idx`) of the state's leaf column into a single arrow array.
///
/// Selection shape: `[skip(local_first), select(num_rows), skip(rest of RG)]`
/// over just `rg_idx`; batch size = page rows so the reader usually yields one
/// batch (multiple batches are concatenated). The builder reuses the state's
/// once-parsed `ArrowReaderMetadata` and a `try_clone()` of its file handle —
/// no footer re-read, no path re-open.
///
/// Dictionary-encoded columns: the writer does not embed a `Dictionary` arrow
/// type hint, so parquet-rs hydrates dictionary pages to plain arrays. As a
/// belt-and-braces guard the result is cast to the dictionary's value type if
/// a `DictionaryArray` ever surfaces, keeping the conversion helpers simple.
fn decode_page_arrow(
    state: &ColumnReaderState,
    rg_idx: usize,
    local_first: i64,
    num_rows: usize,
) -> Result<ArrayRef, String> {
    let rg_rows = state.rg_num_rows[rg_idx];
    let trailing = rg_rows - local_first - num_rows as i64;
    let mut selectors = Vec::with_capacity(3);
    if local_first > 0 {
        selectors.push(RowSelector::skip(local_first as usize));
    }
    selectors.push(RowSelector::select(num_rows));
    if trailing > 0 {
        selectors.push(RowSelector::skip(trailing as usize));
    }

    let file = state
        .file
        .try_clone()
        .map_err(|e| format!("page decode: failed to clone file handle: {}", e))?;
    let mask = ProjectionMask::leaves(state.arrow_metadata.parquet_schema(), [state.leaf_idx]);
    let reader =
        ParquetRecordBatchReaderBuilder::new_with_metadata(file, state.arrow_metadata.clone())
            .with_row_groups(vec![rg_idx])
            .with_projection(mask)
            .with_batch_size(num_rows)
            .with_row_selection(RowSelection::from(selectors))
            .build()
            .map_err(|e| format!("page decode: failed to build arrow reader: {}", e))?;

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(1);
    for batch in reader {
        let batch = batch.map_err(|e| format!("page decode: batch read failed: {}", e))?;
        if batch.num_columns() != 1 {
            return Err(format!(
                "page decode: expected 1 projected column, got {}",
                batch.num_columns()
            ));
        }
        arrays.push(batch.column(0).clone());
    }
    let array = match arrays.len() {
        0 => return Err("page decode: reader returned no batches".to_string()),
        1 => arrays.pop().expect("len checked"),
        _ => {
            let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
            arrow::compute::concat(&refs)
                .map_err(|e| format!("page decode: failed to concat batches: {}", e))?
        }
    };
    if array.len() != num_rows {
        return Err(format!(
            "page decode: expected {} rows but decoded {}",
            num_rows,
            array.len()
        ));
    }
    // Hydrate a DictionaryArray to its value type (see doc comment).
    if let ArrowDataType::Dictionary(_, value_type) = array.data_type() {
        let value_type = value_type.as_ref().clone();
        return arrow::compute::cast(&array, &value_type)
            .map_err(|e| format!("page decode: failed to hydrate dictionary column: {}", e));
    }
    Ok(array)
}

/// Converts a decoded primitive arrow array into the codec's wire form: per-row
/// raw `i64` bits (null slots hold 0) + per-row presence flags from the array's
/// null buffer. Bit conventions match the old def-level decode path exactly:
/// INT32-backed values sign-extend, FLOAT is `f32::to_bits` zero-extended,
/// DOUBLE is `f64::to_bits`, BOOL is 0/1, and date/time/timestamp logical
/// types pass through as their raw INT32/INT64 physical values.
fn arrow_primitive_to_page(array: &ArrayRef) -> Result<(Vec<i64>, Vec<bool>), String> {
    use arrow::array::AsArray;
    use arrow::datatypes as adt;
    use arrow::datatypes::ArrowPrimitiveType;

    fn conv<T: ArrowPrimitiveType>(
        array: &ArrayRef,
        to_bits: impl Fn(T::Native) -> i64,
    ) -> (Vec<i64>, Vec<bool>) {
        let a = array.as_primitive::<T>();
        let mut longs = Vec::with_capacity(a.len());
        let mut presence = Vec::with_capacity(a.len());
        for i in 0..a.len() {
            if a.is_valid(i) {
                longs.push(to_bits(a.value(i)));
                presence.push(true);
            } else {
                longs.push(0);
                presence.push(false);
            }
        }
        (longs, presence)
    }

    let out = match array.data_type() {
        ArrowDataType::Boolean => {
            let a = array.as_boolean();
            let mut longs = Vec::with_capacity(a.len());
            let mut presence = Vec::with_capacity(a.len());
            for i in 0..a.len() {
                if a.is_valid(i) {
                    longs.push(if a.value(i) { 1 } else { 0 });
                    presence.push(true);
                } else {
                    longs.push(0);
                    presence.push(false);
                }
            }
            (longs, presence)
        }
        ArrowDataType::Int8 => conv::<adt::Int8Type>(array, |v| v as i64),
        ArrowDataType::Int16 => conv::<adt::Int16Type>(array, |v| v as i64),
        ArrowDataType::Int32 => conv::<adt::Int32Type>(array, |v| v as i64),
        ArrowDataType::Int64 => conv::<adt::Int64Type>(array, |v| v),
        // Unsigned types are stored as their signed physical INT32/INT64
        // counterparts; reinterpret through the signed type so the raw bits
        // match what the old physical-value path produced.
        ArrowDataType::UInt8 => conv::<adt::UInt8Type>(array, |v| v as i64),
        ArrowDataType::UInt16 => conv::<adt::UInt16Type>(array, |v| v as i64),
        ArrowDataType::UInt32 => conv::<adt::UInt32Type>(array, |v| (v as i32) as i64),
        ArrowDataType::UInt64 => conv::<adt::UInt64Type>(array, |v| v as i64),
        ArrowDataType::Float32 => conv::<adt::Float32Type>(array, |v| v.to_bits() as i64),
        ArrowDataType::Float64 => conv::<adt::Float64Type>(array, |v| v.to_bits() as i64),
        // Date/time/timestamp logical types: raw physical INT32/INT64 passthrough.
        ArrowDataType::Date32 => conv::<adt::Date32Type>(array, |v| v as i64),
        ArrowDataType::Date64 => conv::<adt::Date64Type>(array, |v| v),
        ArrowDataType::Time32(adt::TimeUnit::Second) => {
            conv::<adt::Time32SecondType>(array, |v| v as i64)
        }
        ArrowDataType::Time32(adt::TimeUnit::Millisecond) => {
            conv::<adt::Time32MillisecondType>(array, |v| v as i64)
        }
        ArrowDataType::Time64(adt::TimeUnit::Microsecond) => {
            conv::<adt::Time64MicrosecondType>(array, |v| v)
        }
        ArrowDataType::Time64(adt::TimeUnit::Nanosecond) => {
            conv::<adt::Time64NanosecondType>(array, |v| v)
        }
        ArrowDataType::Timestamp(adt::TimeUnit::Second, _) => {
            conv::<adt::TimestampSecondType>(array, |v| v)
        }
        ArrowDataType::Timestamp(adt::TimeUnit::Millisecond, _) => {
            conv::<adt::TimestampMillisecondType>(array, |v| v)
        }
        ArrowDataType::Timestamp(adt::TimeUnit::Microsecond, _) => {
            conv::<adt::TimestampMicrosecondType>(array, |v| v)
        }
        ArrowDataType::Timestamp(adt::TimeUnit::Nanosecond, _) => {
            conv::<adt::TimestampNanosecondType>(array, |v| v)
        }
        ArrowDataType::Duration(adt::TimeUnit::Second) => {
            conv::<adt::DurationSecondType>(array, |v| v)
        }
        ArrowDataType::Duration(adt::TimeUnit::Millisecond) => {
            conv::<adt::DurationMillisecondType>(array, |v| v)
        }
        ArrowDataType::Duration(adt::TimeUnit::Microsecond) => {
            conv::<adt::DurationMicrosecondType>(array, |v| v)
        }
        ArrowDataType::Duration(adt::TimeUnit::Nanosecond) => {
            conv::<adt::DurationNanosecondType>(array, |v| v)
        }
        other => {
            return Err(format!(
                "page decode: unsupported arrow type {:?} for primitive column",
                other
            ))
        }
    };
    Ok(out)
}

/// Converts a decoded BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY arrow array into per-row
/// byte slices (null rows map to an empty slice, matching the old expand path)
/// + per-row presence flags from the array's null buffer.
fn arrow_bytes_to_page(array: &ArrayRef) -> Result<(Vec<&[u8]>, Vec<bool>), String> {
    use arrow::array::AsArray;

    fn conv<'a>(
        len: usize,
        is_valid: impl Fn(usize) -> bool,
        value: impl Fn(usize) -> &'a [u8],
    ) -> (Vec<&'a [u8]>, Vec<bool>) {
        let mut slices: Vec<&[u8]> = Vec::with_capacity(len);
        let mut presence = Vec::with_capacity(len);
        for i in 0..len {
            if is_valid(i) {
                slices.push(value(i));
                presence.push(true);
            } else {
                slices.push(&[]);
                presence.push(false);
            }
        }
        (slices, presence)
    }

    let out = match array.data_type() {
        ArrowDataType::Utf8 => {
            let a = array.as_string::<i32>();
            conv(a.len(), |i| a.is_valid(i), |i| a.value(i).as_bytes())
        }
        ArrowDataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            conv(a.len(), |i| a.is_valid(i), |i| a.value(i).as_bytes())
        }
        ArrowDataType::Utf8View => {
            let a = array.as_string_view();
            conv(a.len(), |i| a.is_valid(i), |i| a.value(i).as_bytes())
        }
        ArrowDataType::Binary => {
            let a = array.as_binary::<i32>();
            conv(a.len(), |i| a.is_valid(i), |i| a.value(i))
        }
        ArrowDataType::LargeBinary => {
            let a = array.as_binary::<i64>();
            conv(a.len(), |i| a.is_valid(i), |i| a.value(i))
        }
        ArrowDataType::BinaryView => {
            let a = array.as_binary_view();
            conv(a.len(), |i| a.is_valid(i), |i| a.value(i))
        }
        ArrowDataType::FixedSizeBinary(_) => {
            let a = array.as_fixed_size_binary();
            conv(a.len(), |i| a.is_valid(i), |i| a.value(i))
        }
        other => {
            return Err(format!(
                "page decode: unsupported arrow type {:?} for byte-array column",
                other
            ))
        }
    };
    Ok(out)
}

/// Fills the presence bitset directly from an arrow array's validity, no
/// intermediate `Vec<bool>`. No-null arrays fill whole words with all-ones
/// (last word masked to the row count); otherwise bits are set per valid row.
/// Caller must have verified `out` holds at least `words` longs.
unsafe fn write_presence_bits_from_array(array: &ArrayRef, out: *mut i64, words: usize) {
    let n = array.len();
    if array.null_count() == 0 {
        // All present: set every bit, mask the tail of the last word.
        for w in 0..words {
            let base = w * 64;
            let word = if base + 64 <= n {
                u64::MAX
            } else {
                let rem = n - base; // 1..=63
                (1u64 << rem) - 1
            };
            *out.add(w) = word as i64;
        }
        return;
    }
    for w in 0..words {
        let mut bits: u64 = 0;
        let base = w * 64;
        for b in 0..64 {
            let idx = base + b;
            if idx >= n {
                break;
            }
            if array.is_valid(idx) {
                bits |= 1u64 << b;
            }
        }
        *out.add(w) = bits as i64;
    }
}

/// Direct-write primitive page: converts an arrow primitive array into the
/// codec's wire form (per-row raw `i64` bits + presence bitset) writing STRAIGHT
/// into the FFM out-buffers, skipping the intermediate `Vec<i64>` + `Vec<bool>`
/// that `arrow_primitive_to_page` + `write_primitive_page` allocate. Used on the
/// liquid-off decode path (the codec default); the liquid-on path still builds
/// the vecs because `put_page` needs owned buffers.
///
/// Fast sub-case: when the array is an 8-byte-native primitive
/// (Int64/Float64/Date64/Time64/Timestamp/Duration) with no nulls, its value
/// buffer is already `[i64; n]` in native layout (f64 bits are bit-identical to
/// the `to_bits` path), so the values are `memcpy`'d wholesale. All other types
/// are written one slot at a time (null rows → 0), matching the bit conventions
/// of `arrow_primitive_to_page` exactly.
unsafe fn write_primitive_page_from_arrow(
    array: &ArrayRef,
    out_value_buf: *mut u8,
    out_value_buf_cap: i64,
    out_value_actual_len: *mut i64,
    out_presence_bitset: *mut i64,
    out_presence_bits_cap: i64,
) -> Result<i64, String> {
    use arrow::array::AsArray;
    use arrow::datatypes as adt;
    use arrow::datatypes::ArrowPrimitiveType;

    let n = array.len();
    let value_bytes = (n * 8) as i64;
    if !out_value_actual_len.is_null() {
        *out_value_actual_len = value_bytes;
    }
    let presence_words = ((n + 63) / 64) as i64;
    if value_bytes > out_value_buf_cap
        || (n > 0 && out_value_buf.is_null())
        || presence_words > out_presence_bits_cap
        || out_presence_bitset.is_null()
    {
        return Ok(RC_OVERFLOW);
    }

    write_presence_bits_from_array(array, out_presence_bitset, presence_words as usize);

    // --- memcpy fast path: 8-byte-native, no nulls ---
    let dt = array.data_type();
    let src8: Option<*const u8> = if array.null_count() == 0 {
        match dt {
            ArrowDataType::Int64 => {
                Some(array.as_primitive::<adt::Int64Type>().values().as_ptr() as *const u8)
            }
            ArrowDataType::Float64 => {
                Some(array.as_primitive::<adt::Float64Type>().values().as_ptr() as *const u8)
            }
            ArrowDataType::Date64 => {
                Some(array.as_primitive::<adt::Date64Type>().values().as_ptr() as *const u8)
            }
            ArrowDataType::Time64(adt::TimeUnit::Microsecond) => Some(
                array
                    .as_primitive::<adt::Time64MicrosecondType>()
                    .values()
                    .as_ptr() as *const u8,
            ),
            ArrowDataType::Time64(adt::TimeUnit::Nanosecond) => Some(
                array
                    .as_primitive::<adt::Time64NanosecondType>()
                    .values()
                    .as_ptr() as *const u8,
            ),
            ArrowDataType::Timestamp(adt::TimeUnit::Second, _) => Some(
                array
                    .as_primitive::<adt::TimestampSecondType>()
                    .values()
                    .as_ptr() as *const u8,
            ),
            ArrowDataType::Timestamp(adt::TimeUnit::Millisecond, _) => Some(
                array
                    .as_primitive::<adt::TimestampMillisecondType>()
                    .values()
                    .as_ptr() as *const u8,
            ),
            ArrowDataType::Timestamp(adt::TimeUnit::Microsecond, _) => Some(
                array
                    .as_primitive::<adt::TimestampMicrosecondType>()
                    .values()
                    .as_ptr() as *const u8,
            ),
            ArrowDataType::Timestamp(adt::TimeUnit::Nanosecond, _) => Some(
                array
                    .as_primitive::<adt::TimestampNanosecondType>()
                    .values()
                    .as_ptr() as *const u8,
            ),
            ArrowDataType::Duration(adt::TimeUnit::Second) => Some(
                array
                    .as_primitive::<adt::DurationSecondType>()
                    .values()
                    .as_ptr() as *const u8,
            ),
            ArrowDataType::Duration(adt::TimeUnit::Millisecond) => Some(
                array
                    .as_primitive::<adt::DurationMillisecondType>()
                    .values()
                    .as_ptr() as *const u8,
            ),
            ArrowDataType::Duration(adt::TimeUnit::Microsecond) => Some(
                array
                    .as_primitive::<adt::DurationMicrosecondType>()
                    .values()
                    .as_ptr() as *const u8,
            ),
            ArrowDataType::Duration(adt::TimeUnit::Nanosecond) => Some(
                array
                    .as_primitive::<adt::DurationNanosecondType>()
                    .values()
                    .as_ptr() as *const u8,
            ),
            _ => None,
        }
    } else {
        None
    };
    if let Some(src) = src8 {
        if n > 0 {
            std::ptr::copy_nonoverlapping(src, out_value_buf, n * 8);
        }
        return Ok(RC_OK);
    }

    // --- one-pass write for widening / bit-packed / null-bearing types ---
    // Writes every slot (null rows → 0) as native-endian i64 bytes; out_value_buf
    // has no alignment guarantee so we copy bytes rather than store through *mut i64.
    unsafe fn put_slot(out: *mut u8, i: usize, v: i64) {
        std::ptr::copy_nonoverlapping(v.to_ne_bytes().as_ptr(), out.add(i * 8), 8);
    }
    fn one_pass<T: ArrowPrimitiveType>(
        array: &ArrayRef,
        out: *mut u8,
        to_bits: impl Fn(T::Native) -> i64,
    ) {
        use arrow::array::AsArray;
        let a = array.as_primitive::<T>();
        for i in 0..a.len() {
            let v = if a.is_valid(i) {
                to_bits(a.value(i))
            } else {
                0
            };
            unsafe { put_slot(out, i, v) };
        }
    }

    match dt {
        ArrowDataType::Boolean => {
            let a = array.as_boolean();
            for i in 0..a.len() {
                let v = if a.is_valid(i) && a.value(i) { 1 } else { 0 };
                put_slot(out_value_buf, i, v);
            }
        }
        ArrowDataType::Int8 => one_pass::<adt::Int8Type>(array, out_value_buf, |v| v as i64),
        ArrowDataType::Int16 => one_pass::<adt::Int16Type>(array, out_value_buf, |v| v as i64),
        ArrowDataType::Int32 => one_pass::<adt::Int32Type>(array, out_value_buf, |v| v as i64),
        ArrowDataType::Int64 => one_pass::<adt::Int64Type>(array, out_value_buf, |v| v),
        ArrowDataType::UInt8 => one_pass::<adt::UInt8Type>(array, out_value_buf, |v| v as i64),
        ArrowDataType::UInt16 => one_pass::<adt::UInt16Type>(array, out_value_buf, |v| v as i64),
        ArrowDataType::UInt32 => {
            one_pass::<adt::UInt32Type>(array, out_value_buf, |v| (v as i32) as i64)
        }
        ArrowDataType::UInt64 => one_pass::<adt::UInt64Type>(array, out_value_buf, |v| v as i64),
        ArrowDataType::Float32 => {
            one_pass::<adt::Float32Type>(array, out_value_buf, |v| v.to_bits() as i64)
        }
        ArrowDataType::Float64 => {
            one_pass::<adt::Float64Type>(array, out_value_buf, |v| v.to_bits() as i64)
        }
        ArrowDataType::Date32 => one_pass::<adt::Date32Type>(array, out_value_buf, |v| v as i64),
        ArrowDataType::Date64 => one_pass::<adt::Date64Type>(array, out_value_buf, |v| v),
        ArrowDataType::Time32(adt::TimeUnit::Second) => {
            one_pass::<adt::Time32SecondType>(array, out_value_buf, |v| v as i64)
        }
        ArrowDataType::Time32(adt::TimeUnit::Millisecond) => {
            one_pass::<adt::Time32MillisecondType>(array, out_value_buf, |v| v as i64)
        }
        ArrowDataType::Time64(adt::TimeUnit::Microsecond) => {
            one_pass::<adt::Time64MicrosecondType>(array, out_value_buf, |v| v)
        }
        ArrowDataType::Time64(adt::TimeUnit::Nanosecond) => {
            one_pass::<adt::Time64NanosecondType>(array, out_value_buf, |v| v)
        }
        ArrowDataType::Timestamp(adt::TimeUnit::Second, _) => {
            one_pass::<adt::TimestampSecondType>(array, out_value_buf, |v| v)
        }
        ArrowDataType::Timestamp(adt::TimeUnit::Millisecond, _) => {
            one_pass::<adt::TimestampMillisecondType>(array, out_value_buf, |v| v)
        }
        ArrowDataType::Timestamp(adt::TimeUnit::Microsecond, _) => {
            one_pass::<adt::TimestampMicrosecondType>(array, out_value_buf, |v| v)
        }
        ArrowDataType::Timestamp(adt::TimeUnit::Nanosecond, _) => {
            one_pass::<adt::TimestampNanosecondType>(array, out_value_buf, |v| v)
        }
        ArrowDataType::Duration(adt::TimeUnit::Second) => {
            one_pass::<adt::DurationSecondType>(array, out_value_buf, |v| v)
        }
        ArrowDataType::Duration(adt::TimeUnit::Millisecond) => {
            one_pass::<adt::DurationMillisecondType>(array, out_value_buf, |v| v)
        }
        ArrowDataType::Duration(adt::TimeUnit::Microsecond) => {
            one_pass::<adt::DurationMicrosecondType>(array, out_value_buf, |v| v)
        }
        ArrowDataType::Duration(adt::TimeUnit::Nanosecond) => {
            one_pass::<adt::DurationNanosecondType>(array, out_value_buf, |v| v)
        }
        other => {
            return Err(format!(
                "page decode: unsupported arrow type {:?} for primitive column",
                other
            ))
        }
    }
    Ok(RC_OK)
}

/// Writes a decoded primitive page (per-row raw bits + presence bitset) to the
/// caller buffers, or returns `RC_OVERFLOW` after recording the required value
/// byte length.
unsafe fn write_primitive_page(
    longs: &[i64],
    presence: &[bool],
    out_value_buf: *mut u8,
    out_value_buf_cap: i64,
    out_value_actual_len: *mut i64,
    out_presence_bitset: *mut i64,
    out_presence_bits_cap: i64,
) -> Result<i64, String> {
    let value_bytes = (longs.len() * 8) as i64;
    if !out_value_actual_len.is_null() {
        *out_value_actual_len = value_bytes;
    }

    let presence_words = ((presence.len() + 63) / 64) as i64;
    if value_bytes > out_value_buf_cap
        || out_value_buf.is_null()
        || presence_words > out_presence_bits_cap
        || out_presence_bitset.is_null()
    {
        return Ok(RC_OVERFLOW);
    }

    // Write values as native-endian i64 words (Java reads them as a long[] via
    // a MemorySegment using native byte order). Copy raw bytes rather than
    // storing through a *mut i64 — out_value_buf is a u8 buffer with no
    // guaranteed 8-byte alignment, so an aligned i64 store would be UB.
    if !longs.is_empty() {
        std::ptr::copy_nonoverlapping(
            longs.as_ptr() as *const u8,
            out_value_buf,
            longs.len() * 8,
        );
    }
    write_presence_bitset(presence, out_presence_bitset, out_presence_bits_cap);
    Ok(RC_OK)
}

/// Writes a decoded BYTE_ARRAY page (concatenated bytes + CSR offsets + presence
/// bitset) to the caller buffers, or returns `RC_OVERFLOW` after recording the
/// required total byte length.
unsafe fn write_bytes_page(
    slices: &[&[u8]],
    presence: &[bool],
    out_value_buf: *mut u8,
    out_value_buf_cap: i64,
    out_value_actual_len: *mut i64,
    out_byte_offsets: *mut i32,
    out_byte_offsets_cap: i64,
    out_presence_bitset: *mut i64,
    out_presence_bits_cap: i64,
) -> Result<i64, String> {
    let total_bytes: usize = slices.iter().map(|s| s.len()).sum();
    if !out_value_actual_len.is_null() {
        *out_value_actual_len = total_bytes as i64;
    }

    let offsets_needed = (slices.len() + 1) as i64;
    let presence_words = ((presence.len() + 63) / 64) as i64;
    if (total_bytes as i64) > out_value_buf_cap
        || (total_bytes > 0 && out_value_buf.is_null())
        || offsets_needed > out_byte_offsets_cap
        || out_byte_offsets.is_null()
        || presence_words > out_presence_bits_cap
        || out_presence_bitset.is_null()
    {
        return Ok(RC_OVERFLOW);
    }

    let mut acc: i32 = 0;
    for (i, s) in slices.iter().enumerate() {
        *out_byte_offsets.add(i) = acc;
        if !s.is_empty() {
            std::ptr::copy_nonoverlapping(s.as_ptr(), out_value_buf.add(acc as usize), s.len());
        }
        acc += s.len() as i32;
    }
    *out_byte_offsets.add(slices.len()) = acc;

    write_presence_bitset(presence, out_presence_bitset, out_presence_bits_cap);
    Ok(RC_OK)
}

#[cfg(test)]
mod direct_write_tests {
    use super::*;
    use arrow::array::{BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array};

    /// The direct-write path (`write_primitive_page_from_arrow`, used when liquid is off)
    /// must produce byte-identical value + presence buffers to the reference
    /// `arrow_primitive_to_page` + `write_primitive_page` path (used when liquid is on).
    #[test]
    fn direct_write_matches_reference() {
        fn reference(array: &ArrayRef) -> (Vec<u8>, Vec<i64>) {
            let (longs, presence) = arrow_primitive_to_page(array).unwrap();
            let vbytes = longs.len() * 8;
            let pwords = (presence.len() + 63) / 64;
            let mut vbuf = vec![0u8; vbytes.max(1)];
            let mut pbuf = vec![0i64; pwords.max(1)];
            let mut len = 0i64;
            let rc = unsafe {
                write_primitive_page(
                    &longs,
                    &presence,
                    vbuf.as_mut_ptr(),
                    vbytes as i64,
                    &mut len,
                    pbuf.as_mut_ptr(),
                    pwords as i64,
                )
            }
            .unwrap();
            assert_eq!(rc, RC_OK);
            (vbuf, pbuf)
        }
        fn direct(array: &ArrayRef) -> (Vec<u8>, Vec<i64>) {
            let vbytes = array.len() * 8;
            let pwords = (array.len() + 63) / 64;
            let mut vbuf = vec![0u8; vbytes.max(1)];
            let mut pbuf = vec![0i64; pwords.max(1)];
            let mut len = 0i64;
            let rc = unsafe {
                write_primitive_page_from_arrow(
                    array,
                    vbuf.as_mut_ptr(),
                    vbytes as i64,
                    &mut len,
                    pbuf.as_mut_ptr(),
                    pwords as i64,
                )
            }
            .unwrap();
            assert_eq!(rc, RC_OK);
            (vbuf, pbuf)
        }
        let cases: Vec<ArrayRef> = vec![
            // memcpy fast path: Int64/Float64 no nulls
            Arc::new(Int64Array::from(vec![Some(1), Some(-2), Some(i64::MAX), Some(0)])),
            Arc::new(Float64Array::from(vec![Some(2.25), Some(-1.5), Some(0.0)])),
            // nulls force the per-slot path
            Arc::new(Int64Array::from(vec![Some(7), None, Some(8), None, Some(9)])),
            // widening + sign-extend
            Arc::new(Int32Array::from(vec![Some(-1), Some(42), None])),
            // float bits with a null
            Arc::new(Float32Array::from(vec![Some(-1.5f32), None])),
            // bool 0/1
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            // >64 rows to exercise multi-word presence + tail masking
            Arc::new(Int64Array::from(
                (0..100)
                    .map(|i| if i % 7 == 0 { None } else { Some(i as i64) })
                    .collect::<Vec<_>>(),
            )),
            // all-null
            Arc::new(Int64Array::from(vec![None, None, None])),
        ];
        for (i, c) in cases.iter().enumerate() {
            assert_eq!(reference(c), direct(c), "mismatch on case {}", i);
        }
    }
}
