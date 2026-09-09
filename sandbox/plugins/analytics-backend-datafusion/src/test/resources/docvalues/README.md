# Doc-values reader fixtures

Parquet files read by the doc-values bridge and codec tests. Committed
rather than generated so this plugin's tests do not depend on the
parquet-data-format plugin (the same convention as test.parquet and
hits1/2.parquet one directory up).

All three were written once by parquet-data-format's NativeParquetWriter,
so they carry the opensearch.format_version footer stamp the doc-values
producer gates on. When that version bumps, the producer tests will start
rejecting these files: regenerate them with the recipe below and commit
the new bytes.

## Contents

long_dense.parquet
  One Int64 column "value", 500 rows, not nullable.
  value at row r = r * 7 + 1

long_sparse.parquet
  Same as long_dense, but nullable: rows where r % 5 == 0 are null.

numeric_kinds.parquet
  Eight non-nullable columns, 8 rows each:
    d_double (Float64): -100.5, -0.5, 0.0, 3.25, -2.75, 42.0, -1.0e300, 1.0e300
    d_float  (Float32): -100.5, -0.5, 0.0, 3.25, -2.75, 42.0, -3.4e38, 3.4e38
    d_int    (Int32):   -1, -2147483648, -987654, 0, 42, 2147483647, 7, -7
    d_uint   (UInt32):  0, 42, 3000000000, 4294967295, 7, 1, 2147483648, 65536
    d_short  (Int16):   -1, -32768, -1234, 0, 42, 32767, 7, -7
    d_ushort (UInt16):  0, 42, 50000, 65535, 7, 1, 32768, 256
    d_byte   (Int8):    -1, -128, -50, 0, 42, 127, 7, -7
    d_ubyte  (UInt8):   0, 42, 200, 255, 7, 1, 128, 16
  Signed columns carry negatives and unsigned columns carry values above
  the signed maximum, so a sign- vs zero-extension swap in the reader
  cannot pass.

## Regenerating

Temporarily add a testImplementation dependency on
:sandbox:plugins:parquet-data-format (and :sandbox:libs:plugin-stats-spi),
then write each file with NativeParquetWriter:

  NativeParquetWriter writer = new NativeParquetWriter(file.toString());
  // export an Arrow schema with the columns above, then:
  writer.initialize("test-index", schemaAddress, ParquetSortConfig.empty(), 0L);
  writer.write(arrayAddress, schemaAddress);   // one VectorSchemaRoot with the rows above
  writer.flush();

Values must match the tables above exactly; the tests assert them
verbatim. Write from inside a test (createTempDir), since the test
security manager denies writes elsewhere, run with
-Dtests.leaveTemporary=true, and copy the files out of
build/testrun/test/temp/.
