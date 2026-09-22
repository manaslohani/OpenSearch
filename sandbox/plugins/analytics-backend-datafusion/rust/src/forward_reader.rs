/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Forward-only, page-lazy Parquet batch reader for the doc-values read path.
//!
//! Reads a single projected column forward-only over one retained Arrow reader:
//! complete pages between the current position and the requested row are skipped
//! without fetching or decoding them, and pages the ColumnIndex marks entirely
//! null are satisfied without decoding at all.
//!
//! The reader works over either a local file (fast path) or a DataFusion
//! object-store [`AsyncFileReader`] (remote/tiered storage), selected by
//! [`ParquetForwardBatchReaderFactory`]. Single-level repeated (multi-valued)
//! columns are served one List per row; nested (multi-level) repetition is not
//! supported yet.
//!
//! The factory holds everything a reader needs to be rebuilt without new IO: the
//! `ParquetMetaData` (footer plus the page index for the projected column), the
//! `ProjectionMask` naming that column, the decode batch size, and the byte
//! source. [`ParquetForwardBatchReaderFactory::open`] is therefore also the reset
//! path. Page bounds come from the `OffsetIndex`; all-null pages are recognised
//! from the `ColumnIndex`.

use bytes::Bytes;
use datafusion::arrow::array::new_null_array;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchReader};
use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder,
};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::errors::{ParquetError as ArrowParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::file::page_index::column_index::ColumnIndexMetaData;
use datafusion::parquet::file::reader::{ChunkReader, Length};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::PartitionedFile;
use parking_lot::Mutex;
use std::fmt::Debug;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Absent per-page statistics widen the range rather than narrow it, so the page intersects every query range and is never wrongly skipped.
const MINMAX_UNKNOWN: (i64, i64) = (i64::MIN, i64::MAX);

/// OffsetIndex/ColumnIndex information for one projected data page.
#[derive(Debug, Clone)]
pub struct ParquetForwardPage {
    /// First logical row of the page within the file.
    pub first_row: usize,
    /// Number of rows in the page.
    pub row_count: usize,
    /// Whether every value in the page is null, so it can be satisfied without
    /// decoding.
    pub all_null: bool,
    /// Null count from the ColumnIndex, retained so the page-index export can surface it.
    pub null_count: Option<i64>,
    /// Per-page minimum from the ColumnIndex; MINMAX_UNKNOWN when absent.
    pub min: i64,
    /// Per-page maximum from the ColumnIndex; MINMAX_UNKNOWN when absent.
    pub max: i64,
}

/// A forward-only, page-lazy Parquet batch reader over a single projected leaf
/// column, backed by either a local file or a DataFusion [`AsyncFileReader`].
///
/// [`Self::read_batch_at`] only moves forward: complete pages between the
/// current position and the requested row are skipped without fetching or
/// decoding them. The projected column must have an OffsetIndex on every
/// non-empty row group so Arrow can request whole page ranges lazily.
pub struct ParquetForwardBatchReader {
    reader: ParquetRecordBatchReader,
    physical_position: usize, // deviates from position for null rows
    position: usize,
    row_count: usize,
    pages: Vec<ParquetForwardPage>,
    // Physical on-disk shape, read from the file schema at open. See `is_repeated`.
    repeated: bool,
}

/// Reusable factory for independent forward readers over the same file,
/// metadata, and projection.
///
/// [`Self::open`] uses a synchronous retained file descriptor when the file
/// exists locally, and otherwise builds a DataFusion object-store reader for
/// remote/tiered storage.
pub struct ParquetForwardBatchReaderFactory {
    reader_factory: Arc<dyn ParquetFileReaderFactory>,
    file: PartitionedFile,
    metadata: Arc<ParquetMetaData>, // Arc as it is shared by readers
    projection: ProjectionMask,
    batch_size: usize,
    runtime: Arc<Runtime>,
    local_file: Option<PathBuf>,
}

impl ParquetForwardBatchReaderFactory {
    /// Creates a factory whose readers use the supplied DataFusion file reader
    /// factory and cached Parquet metadata.
    pub fn new(
        reader_factory: Arc<dyn ParquetFileReaderFactory>,
        file: PartitionedFile,
        metadata: Arc<ParquetMetaData>,
        projection: ProjectionMask,
        batch_size: usize,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            reader_factory,
            file,
            metadata,
            projection,
            batch_size,
            runtime,
            local_file: None,
        }
    }

    /// Uses a synchronous retained file descriptor for `path`, which the caller resolves from the
    /// store that owns the file. `None` continues through DataFusion's reader factory.
    ///
    /// Resolving the path is the store's job, not this module's: only a store backed by the local
    /// filesystem can name a descriptor, and only it knows how an object path maps onto one.
    pub fn with_local_file(mut self, path: Option<PathBuf>) -> Self {
        self.local_file = path;
        self
    }

    /// Opens a new retained forward reader (local descriptor if available, else
    /// a DataFusion object-store reader).
    pub fn open(&self) -> ParquetResult<ParquetForwardBatchReader> {
        if let Some(path) = self.local_file.as_ref() {
            let file = std::fs::File::open(path)
                .map_err(|error| ArrowParquetError::External(Box::new(error)))?;
            return ParquetForwardBatchReader::try_new_with_chunk_reader(
                file,
                Arc::clone(&self.metadata),
                self.projection.clone(),
                self.batch_size,
            );
        }
        // create_reader requires a metrics sink; we do not surface these read
        // metrics, so pass a throwaway set.
        let metrics = ExecutionPlanMetricsSet::new();
        let async_reader = self
            .reader_factory
            .create_reader(0, self.file.clone(), None, &metrics)
            .map_err(|error| ArrowParquetError::External(Box::new(error)))?;
        ParquetForwardBatchReader::try_new(
            async_reader,
            self.file.object_meta.size,
            Arc::clone(&self.metadata),
            self.projection.clone(),
            self.batch_size,
            Arc::clone(&self.runtime),
        )
    }
}

impl Debug for ParquetForwardBatchReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetForwardBatchReader")
            .field("position", &self.position)
            .field("physical_position", &self.physical_position)
            .field("row_count", &self.row_count)
            .field("page_count", &self.pages.len())
            .finish_non_exhaustive()
    }
}

impl ParquetForwardBatchReader {
    /// Creates a retained Arrow reader over a DataFusion object-store async
    /// reader (remote/tiered storage).
    pub fn try_new(
        async_reader: Box<dyn AsyncFileReader + Send>,
        file_len: u64,
        metadata: Arc<ParquetMetaData>,
        projection: ProjectionMask,
        batch_size: usize,
        runtime: Arc<Runtime>,
    ) -> ParquetResult<Self> {
        let chunk_reader = AsyncFileChunkReader {
            reader: Mutex::new(async_reader),
            file_len,
            runtime,
        };
        Self::try_new_with_chunk_reader(chunk_reader, metadata, projection, batch_size)
    }

    /// Creates a retained Arrow reader over an existing synchronous chunk reader
    /// (a local file), reusing Parquet metadata already loaded by DataFusion.
    pub fn try_new_with_chunk_reader<T>(
        chunk_reader: T,
        metadata: Arc<ParquetMetaData>,
        projection: ProjectionMask,
        batch_size: usize,
    ) -> ParquetResult<Self>
    where
        T: ChunkReader + 'static,
    {
        let pages = projected_pages(&metadata, &projection)?;

        let repeated = projected_is_repeated(&metadata, &projection)?;

        let row_count = usize::try_from(metadata.file_metadata().num_rows()).map_err(|_| {
            ArrowParquetError::General("Parquet row count does not fit in usize".to_string())
        })?;
        let arrow_metadata =
            ArrowReaderMetadata::try_new(Arc::clone(&metadata), ArrowReaderOptions::new())?;
        let reader =
            ParquetRecordBatchReaderBuilder::new_with_metadata(chunk_reader, arrow_metadata)
                .with_projection(projection)
                .with_batch_size(batch_size.max(1))
                .build()?;

        Ok(Self {
            reader,
            physical_position: 0,
            position: 0,
            row_count,
            pages,
            repeated,
        })
    }

    /// Skips to `target_row` and decodes the next Arrow batch, clamped to the end
    /// of `target_row`'s page.
    ///
    /// Returns `None` when `target_row == self.row_count()`. Backward seeks and
    /// rows beyond the end of the file return an error.
    pub fn read_batch_at(
        &mut self,
        target_row: usize,
        max_rows: usize,
    ) -> ParquetResult<Option<RecordBatch>> {
        if target_row > self.row_count {
            return Err(ArrowParquetError::General(format!(
                "row {target_row} is beyond Parquet row count {}",
                self.row_count
            )));
        }
        if target_row < self.position {
            return Err(ArrowParquetError::General(format!(
                "backward seek from {} to {target_row} is not supported",
                self.position
            )));
        }
        if target_row == self.row_count {
            return Ok(None);
        }
        if max_rows == 0 {
            return Err(ArrowParquetError::General(
                "forward batch size must be greater than zero".to_string(),
            ));
        }

        let page_containing_row = self.page_at(target_row)?.clone();
        let rows_to_read = max_rows
            .min(page_containing_row.first_row + page_containing_row.row_count - target_row);
        if page_containing_row.all_null {
            let page_end = page_containing_row.first_row + page_containing_row.row_count;
            if self.physical_position < page_end {
                let to_skip = page_end - self.physical_position;
                let skipped = self.reader.skip_rows(to_skip)?;
                if skipped != to_skip {
                    return Err(ArrowParquetError::General(format!(
                        "requested all-null page skip of {to_skip} rows but skipped {skipped}"
                    )));
                }
                self.physical_position = page_end;
            }
            let schema = self.reader.schema();
            let columns = schema
                .fields()
                .iter()
                .map(|field| new_null_array(field.data_type(), rows_to_read))
                .collect();
            let batch = RecordBatch::try_new(schema, columns)?;
            self.position = target_row + rows_to_read;
            return Ok(Some(batch));
        }

        if target_row < self.physical_position {
            return Err(ArrowParquetError::General(format!(
                "physical reader is at {} before non-null row {target_row}",
                self.physical_position
            )));
        }
        let to_skip = target_row - self.physical_position;
        let skipped = self.reader.skip_rows(to_skip)?;
        if skipped != to_skip {
            return Err(ArrowParquetError::General(format!(
                "requested skip of {to_skip} rows but skipped {skipped}"
            )));
        }

        let batch = self.reader.read_next_batch(rows_to_read)?.ok_or_else(|| {
            ArrowParquetError::General(format!("Parquet reader exhausted before row {target_row}"))
        })?;
        self.physical_position = target_row + batch.num_rows();
        self.position = self.physical_position;
        Ok(Some(batch))
    }

    /// Current physical row position of the retained Arrow reader.
    pub fn position(&self) -> usize {
        self.position
    }

    /// Total rows in the Parquet file.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Per-page table, exposed so the cursor's page-index export can read page bounds.
    pub fn pages(&self) -> &[ParquetForwardPage] {
        &self.pages
    }

    /// Whether the projected column is physically repeated (a Parquet LIST), read from the schema at open.
    pub fn is_repeated(&self) -> bool {
        self.repeated
    }

    /// Number of physical rows in the page containing `target_row`.
    pub fn page_row_count(&self, target_row: usize) -> ParquetResult<usize> {
        Ok(self.page_at(target_row)?.row_count)
    }

    /// Number of physical rows from `target_row` through the end of its page.
    pub fn rows_remaining_in_page(&self, target_row: usize) -> ParquetResult<usize> {
        let page = self.page_at(target_row)?;
        Ok(page.first_row + page.row_count - target_row)
    }

    fn page_at(&self, target_row: usize) -> ParquetResult<&ParquetForwardPage> {
        let index = self
            .pages
            .partition_point(|page| page.first_row + page.row_count <= target_row);
        self.pages
            .get(index)
            .filter(|page| {
                target_row >= page.first_row && target_row < page.first_row + page.row_count
            })
            .ok_or_else(|| {
                ArrowParquetError::General(format!("OffsetIndex does not contain row {target_row}"))
            })
    }
}

/// Whether the single projected leaf column is physically repeated (`max_rep_level >= 1`) on disk.
/// Enforces the same single-projected-leaf-column contract as [`projected_pages`].
fn projected_is_repeated(
    metadata: &ParquetMetaData,
    projection: &ProjectionMask,
) -> ParquetResult<bool> {
    let schema = metadata.file_metadata().schema_descr();
    let projected_columns = (0..schema.num_columns())
        .filter(|&column_idx| projection.leaf_included(column_idx))
        .collect::<Vec<_>>();
    let [column_idx] = projected_columns.as_slice() else {
        return Err(ArrowParquetError::General(format!(
            "ParquetForwardBatchReader requires exactly one projected leaf column, got {}",
            projected_columns.len()
        )));
    };
    Ok(schema.column(*column_idx).max_rep_level() >= 1)
}

/// Builds the per-page table for the single projected leaf column.
///
/// Requires exactly one projected leaf column with an OffsetIndex on every
/// non-empty row group. Single-level repetition is accepted (page bounds stay
/// row-indexed); nested (multi-level) repeated columns are rejected until the
/// nested read path is implemented.
fn projected_pages(
    metadata: &ParquetMetaData,
    projection: &ProjectionMask,
) -> ParquetResult<Vec<ParquetForwardPage>> {
    let schema = metadata.file_metadata().schema_descr();
    let projected_columns = (0..schema.num_columns())
        .filter(|&column_idx| projection.leaf_included(column_idx))
        .collect::<Vec<_>>();
    let [column_idx] = projected_columns.as_slice() else {
        return Err(ArrowParquetError::General(format!(
            "ParquetForwardBatchReader requires exactly one projected leaf column, got {}",
            projected_columns.len()
        )));
    };
    let column_idx = *column_idx;
    // Single-level only: page bounds are row-indexed, so one List per row keeps the forward skip correct.
    //
    // TODO: nested repeated columns -- rows can span repetition boundaries the row-indexed page bounds do not align to.
    let max_rep_level = schema.column(column_idx).max_rep_level();
    if max_rep_level > 1 {
        return Err(ArrowParquetError::General(
            "ParquetForwardBatchReader does not support nested repeated columns yet".to_string(),
        ));
    }

    let offset_index = metadata.offset_index().ok_or_else(|| {
        ArrowParquetError::General("ParquetForwardBatchReader requires an OffsetIndex".to_string())
    })?;
    let column_index = metadata.column_index();
    // Accumulated across the loop: `pages` is the list of page entries returned to
    // the caller; `row_group_start` tracks the file-absolute first row of each row group.
    let mut row_group_start = 0usize;
    let mut pages = vec![];

    // Walk every row group, appending one page entry per data page to `pages`.
    for (row_group_idx, row_group) in metadata.row_groups().iter().enumerate() {
        let row_group_rows = usize::try_from(row_group.num_rows()).map_err(|_| {
            ArrowParquetError::General(format!("negative row count for row group {row_group_idx}"))
        })?;
        if row_group_rows == 0 {
            continue;
        }
        let locations = &offset_index
            .get(row_group_idx)
            .and_then(|row_group| row_group.get(column_idx))
            .filter(|index| !index.page_locations.is_empty())
            .ok_or_else(|| {
                ArrowParquetError::General(format!(
                    "OffsetIndex missing for row group {row_group_idx}, column {column_idx}"
                ))
            })?
            .page_locations;

        let page_statistics = column_index
            .and_then(|index| index.get(row_group_idx))
            .and_then(|row_group| row_group.get(column_idx))
            .filter(|index| !matches!(index, ColumnIndexMetaData::NONE));

        // Append one page entry per data page in this row group.
        for (page_idx, location) in locations.iter().enumerate() {
            let start = usize::try_from(location.first_row_index).map_err(|_| {
                ArrowParquetError::General(format!(
                    "negative first row for row group {row_group_idx}, page {page_idx}"
                ))
            })?;
            let end = match locations.get(page_idx + 1) {
                Some(next) => usize::try_from(next.first_row_index).map_err(|_| {
                    ArrowParquetError::General(format!(
                        "negative first row for row group {row_group_idx}, page {}",
                        page_idx + 1
                    ))
                })?,
                None => row_group_rows,
            };
            if start >= end || end > row_group_rows {
                return Err(ArrowParquetError::General(format!(
                    "invalid OffsetIndex row range {start}..{end} for row group {row_group_idx}"
                )));
            }
            let null_count = page_statistics.and_then(|index| {
                (page_idx < index.num_pages() as usize)
                    .then(|| index.null_count(page_idx))
                    .flatten()
            });
            // Gated to max_rep_level == 0: the null-page flag compares null leaf slots against row count, so it can be set on a repeated page that still holds values.
            let all_null = max_rep_level == 0
                && (page_statistics.is_some_and(|index| {
                    page_idx < index.num_pages() as usize && index.is_null_page(page_idx)
                }) || null_count == Some((end - start) as i64));
            // Same ColumnIndex the null count came from, guarded on page index the way null_count is.
            let bounds_index =
                page_statistics.filter(|index| page_idx < index.num_pages() as usize);
            let (min, max) = page_min_max(bounds_index, page_idx);
            pages.push(ParquetForwardPage {
                first_row: row_group_start + start,
                row_count: end - start,
                all_null,
                null_count,
                min,
                max,
            });
        }
        row_group_start += row_group_rows;
    }
    Ok(pages)
}

/// Maps a page's ColumnIndex bounds to (min, max) as i64; only INT32/INT64/BOOLEAN yield real bounds, everything else (FLOAT/DOUBLE, byte arrays) widens to MINMAX_UNKNOWN.
fn page_min_max(index: Option<&ColumnIndexMetaData>, page_index: usize) -> (i64, i64) {
    match index {
        Some(ColumnIndexMetaData::INT32(index)) => {
            match (index.min_value(page_index), index.max_value(page_index)) {
                (Some(min), Some(max)) => (*min as i64, *max as i64),
                _ => MINMAX_UNKNOWN,
            }
        }
        Some(ColumnIndexMetaData::INT64(index)) => {
            match (index.min_value(page_index), index.max_value(page_index)) {
                (Some(min), Some(max)) => (*min, *max),
                _ => MINMAX_UNKNOWN,
            }
        }
        Some(ColumnIndexMetaData::BOOLEAN(index)) => {
            match (index.min_value(page_index), index.max_value(page_index)) {
                (Some(min), Some(max)) => (i64::from(*min), i64::from(*max)),
                _ => MINMAX_UNKNOWN,
            }
        }
        _ => MINMAX_UNKNOWN,
    }
}

/// Adapts DataFusion's async object-store reader to Arrow's synchronous lazy
/// page reader. Calls are serialized because `AsyncFileReader` takes `&mut
/// self`, matching the single-threaded cursor contract.
struct AsyncFileChunkReader {
    reader: Mutex<Box<dyn AsyncFileReader + Send>>,
    file_len: u64,
    runtime: Arc<Runtime>,
}

impl Length for AsyncFileChunkReader {
    fn len(&self) -> u64 {
        self.file_len
    }
}

impl ChunkReader for AsyncFileChunkReader {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        Err(ArrowParquetError::General(format!(
            "page-header scanning at byte {start} is disabled; an OffsetIndex is required"
        )))
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let end = start
            .checked_add(length as u64)
            .ok_or_else(|| ArrowParquetError::General("page range overflow".to_string()))?;
        if end > self.file_len {
            return Err(ArrowParquetError::General(format!(
                "page range {start}..{end} exceeds file length {}",
                self.file_len
            )));
        }
        self.runtime
            .block_on(self.reader.lock().get_bytes(start..end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Array, ArrayRef, Int32Array, Int32Builder, ListArray, ListBuilder,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::metadata::{
        FileMetaData, PageIndexPolicy, ParquetMetaDataBuilder, ParquetMetaDataReader,
    };
    use datafusion::parquet::file::properties::WriterProperties;
    use std::fs::File;

    /// Writes `values` to a temp Parquet file (row groups of 8 rows, pages of 3
    /// rows, OffsetIndex + ColumnIndex enabled) and returns it with its metadata.
    fn write_fixture(values: Vec<Option<i32>>) -> (File, Arc<ParquetMetaData>) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let column: ArrayRef = Arc::new(Int32Array::from(values));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap();
        let file = tempfile::tempfile().unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(8))
            .set_data_page_row_count_limit(3)
            // Page-row limits are only checked at write-batch boundaries, so make
            // the batch small enough to actually split each row group into pages.
            .set_write_batch_size(3)
            .set_offset_index_disabled(false)
            .build();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&file)
            .unwrap();
        (File::from(file), Arc::new(metadata))
    }

    /// Writes a single-column fixture over an arbitrary Arrow column (list or scalar) with the same
    /// settings as [`write_fixture`], returning the metadata [`projected_pages`] reads.
    fn metadata_for_column(column: ArrayRef) -> Arc<ParquetMetaData> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            column.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap();
        let file = tempfile::tempfile().unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(8))
            .set_data_page_row_count_limit(3)
            .set_write_batch_size(3)
            .set_offset_index_disabled(false)
            .build();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&file)
            .map(Arc::new)
            .unwrap()
    }

    /// Opens a forward reader over the single column of a fixture file.
    fn reader_for(values: Vec<Option<i32>>) -> ParquetForwardBatchReader {
        let (file, metadata) = write_fixture(values);
        let projection = ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [0]);
        ParquetForwardBatchReader::try_new_with_chunk_reader(file, metadata, projection, 4096)
            .unwrap()
    }

    fn dense_reader() -> ParquetForwardBatchReader {
        reader_for((0..20).map(Some).collect())
    }

    /// Opens a forward reader over a single-level `list<int32>` column, for the repeated-shape
    /// probe: a physically repeated column that `is_repeated` must report as repeated.
    fn list_reader() -> ParquetForwardBatchReader {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
            None,
            Some(vec![Some(4), Some(5), Some(6)]),
            Some(vec![]),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            list.data_type().clone(),
            true,
        )]));
        let column: ArrayRef = Arc::new(list);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap();
        let file = tempfile::tempfile().unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(8))
            .set_data_page_row_count_limit(3)
            .set_write_batch_size(3)
            .set_offset_index_disabled(false)
            .build();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&file)
            .map(Arc::new)
            .unwrap();
        let projection = ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [0]);
        ParquetForwardBatchReader::try_new_with_chunk_reader(
            File::from(file),
            metadata,
            projection,
            4096,
        )
        .unwrap()
    }

    fn ints(batch: &RecordBatch) -> Vec<Option<i32>> {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..column.len())
            .map(|i| column.is_valid(i).then(|| column.value(i)))
            .collect()
    }

    #[test]
    fn read_batch_at_clamps_to_page_end() {
        // Pages are 3 rows wide; asking for 10 rows from row 2 yields just row 2.
        let batch = dense_reader().read_batch_at(2, 10).unwrap().unwrap();
        assert_eq!(ints(&batch), vec![Some(2)]);
    }

    #[test]
    fn forward_jump_skips_intervening_pages() {
        let mut reader = dense_reader();
        // Jump straight to row 6, skipping pages [0,3) and [3,6) without decoding.
        let batch = reader.read_batch_at(6, 2).unwrap().unwrap();
        assert_eq!(ints(&batch), vec![Some(6), Some(7)]);
        assert_eq!(reader.position(), 8);
    }

    #[test]
    fn backward_seek_is_rejected() {
        let mut reader = dense_reader();
        reader.read_batch_at(6, 2).unwrap();
        assert!(reader.read_batch_at(1, 1).is_err());
    }

    #[test]
    fn target_row_at_row_count_returns_none() {
        let mut reader = dense_reader();
        assert!(reader.read_batch_at(20, 1).unwrap().is_none());
    }

    #[test]
    fn a_short_skip_is_rejected_rather_than_decoding_misaligned_rows() {
        // A footer that claims more rows than the pages hold: 8 rows are written, then the row group
        // and file counts are inflated to 12, stretching the last page to [6, 12). Skipping to row 9
        // then runs out of data. The skip must fail rather than leave `physical_position` out of step
        // with the reader, which would decode every later row from the wrong offset.
        let (file, metadata) = write_fixture((0..8).map(Some).collect());
        let inflated_rows = 12i64;
        let file_meta = metadata.file_metadata();
        let inflated_file_meta = FileMetaData::new(
            file_meta.version(),
            inflated_rows,
            file_meta.created_by().map(str::to_string),
            file_meta.key_value_metadata().cloned(),
            file_meta.schema_descr_ptr(),
            file_meta.column_orders().cloned(),
        );
        let inflated_row_group = metadata
            .row_group(0)
            .clone()
            .into_builder()
            .set_num_rows(inflated_rows)
            .build()
            .unwrap();
        let inflated = Arc::new(
            ParquetMetaDataBuilder::new(inflated_file_meta)
                .set_row_groups(vec![inflated_row_group])
                .set_column_index(metadata.column_index().cloned())
                .set_offset_index(metadata.offset_index().cloned())
                .build(),
        );

        let projection = ProjectionMask::leaves(inflated.file_metadata().schema_descr(), [0]);
        let mut reader =
            ParquetForwardBatchReader::try_new_with_chunk_reader(file, inflated, projection, 4096)
                .unwrap();

        let error = reader.read_batch_at(9, 1).unwrap_err().to_string();
        assert!(
            error.contains("requested skip of 9 rows but skipped 8"),
            "{error}"
        );
    }

    #[test]
    fn an_all_null_final_page_skips_to_end_of_file_without_a_short_skip() {
        // Row groups of 8 and pages of 3 leave the last row in its own page, [19, 20). Nulling it
        // makes that final page all-null, so serving it skips the physical reader to exactly the end
        // of the file - the boundary where a short skip would be reported if one were possible.
        let mut values: Vec<Option<i32>> = (0..20).map(Some).collect();
        values[19] = None;
        let mut reader = reader_for(values);
        let batch = reader.read_batch_at(19, 1).unwrap().unwrap();
        assert_eq!(ints(&batch), vec![None]);
        assert!(
            reader.read_batch_at(20, 1).unwrap().is_none(),
            "the file must be exhausted after its final page"
        );
    }

    #[test]
    fn all_null_page_is_served_without_decoding() {
        // Rows 3,4,5 form an entirely-null page; reading it returns nulls and the
        // physical reader skips past it.
        let mut values: Vec<Option<i32>> = (0..20).map(Some).collect();
        for slot in values.iter_mut().take(6).skip(3) {
            *slot = None;
        }
        let mut reader = reader_for(values);
        let batch = reader.read_batch_at(3, 3).unwrap().unwrap();
        assert_eq!(ints(&batch), vec![None, None, None]);
        // Next page decodes normally.
        let next = reader.read_batch_at(6, 2).unwrap().unwrap();
        assert_eq!(ints(&next), vec![Some(6), Some(7)]);
    }

    /// A physically repeated column (a single-level `list<int32>`) must report repeated, so the read
    /// path routes it to the list reader regardless of what the mapping later declares.
    #[test]
    fn is_repeated_true_for_single_level_list_column() {
        assert!(list_reader().is_repeated());
    }

    /// A physically scalar column must report not-repeated, so an older scalar segment under a
    /// promoted (multi-valued) mapping falls through to the scalar path.
    #[test]
    fn is_repeated_false_for_scalar_column() {
        assert!(!dense_reader().is_repeated());
    }

    /// A single-level repeated column (`max_rep_level == 1`) is served on the forward path, so
    /// `projected_pages` must accept it and yield decodable pages.
    #[test]
    fn single_level_repeated_column_is_accepted() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
            None,
            Some(vec![Some(4), Some(5), Some(6)]),
            Some(vec![]),
        ]);
        let metadata = metadata_for_column(Arc::new(list));
        let schema = metadata.file_metadata().schema_descr();
        assert_eq!(
            schema.column(0).max_rep_level(),
            1,
            "fixture must be single-level repeated for this test to prove anything"
        );
        let projection = ProjectionMask::leaves(schema, [0]);
        let pages = projected_pages(&metadata, &projection)
            .expect("a single-level repeated column must be accepted");
        assert!(
            !pages.is_empty(),
            "the accepted list column must still yield decodable pages"
        );
    }

    /// A nested repeated column (`max_rep_level > 1`) has no forward read path yet, so
    /// `projected_pages` must reject it rather than mis-slice rows.
    #[test]
    fn nested_repeated_column_is_still_rejected() {
        // list<list<int32>>: each row is a list of inner lists, so the leaf's max_rep_level is 2.
        let mut builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        // row 0: [[1, 2], [3]]
        builder.values().values().append_value(1);
        builder.values().values().append_value(2);
        builder.values().append(true);
        builder.values().values().append_value(3);
        builder.values().append(true);
        builder.append(true);
        // row 1: [[4]]
        builder.values().values().append_value(4);
        builder.values().append(true);
        builder.append(true);
        let nested = builder.finish();

        let metadata = metadata_for_column(Arc::new(nested));
        let schema = metadata.file_metadata().schema_descr();
        assert_eq!(
            schema.column(0).max_rep_level(),
            2,
            "fixture must be nested-repeated for this test to prove anything"
        );
        let projection = ProjectionMask::leaves(schema, [0]);
        let error = projected_pages(&metadata, &projection)
            .expect_err("a nested repeated column must be rejected")
            .to_string();
        assert!(
            error.contains("does not support nested repeated columns yet"),
            "{error}"
        );
    }

    /// A `list<int32>` page whose null leaf-slot count equals its row count must still be decoded,
    /// not shortcut as all-null. Fixture `[null], [null, null], [10]`: 3 rows, 4 leaf slots (3 null).
    #[test]
    fn a_list_page_with_null_slots_matching_the_row_count_is_still_decoded() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![None]),
            Some(vec![None, None]),
            Some(vec![Some(10)]),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            list.data_type().clone(),
            true,
        )]));
        let column: ArrayRef = Arc::new(list);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap();
        let file = tempfile::tempfile().unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(8))
            .set_data_page_row_count_limit(3)
            .set_write_batch_size(3)
            .set_offset_index_disabled(false)
            .build();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&file)
            .map(Arc::new)
            .unwrap();
        // All three rows must sit in one page, or the page-level null signal would not cover value 10.
        let schema_descr = metadata.file_metadata().schema_descr();
        assert_eq!(
            schema_descr.column(0).max_rep_level(),
            1,
            "fixture must be single-level repeated for this test to prove anything"
        );
        let projection = ProjectionMask::leaves(schema_descr, [0]);
        let mut reader = ParquetForwardBatchReader::try_new_with_chunk_reader(
            File::from(file),
            metadata,
            projection,
            4096,
        )
        .unwrap();

        // If the all-null shortcut fired, row 2 would come back as a fabricated null, not `[10]`.
        let batch = reader.read_batch_at(0, 3).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        let lists = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("the list page must decode to a ListArray, not a fabricated null batch");
        assert!(lists.is_valid(2), "row 2's list must be present, not null");
        let last = lists.value(2);
        let last = last
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("the list's leaf values are int32");
        assert_eq!(
            (0..last.len())
                .map(|i| last.is_valid(i).then(|| last.value(i)))
                .collect::<Vec<_>>(),
            vec![Some(10)],
            "the real value 10 must survive - an all-null skip would have dropped it"
        );
    }
}
