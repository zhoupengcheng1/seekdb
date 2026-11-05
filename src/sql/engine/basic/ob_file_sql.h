#pragma once

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{
class ObRawExpr;
class ObSQLSessionInfo;

struct FileSqlSpan
{
  const char *data_;
  size_t len_;
  static FileSqlSpan from(const common::ObString &str) { return {str.ptr(), static_cast<size_t>(str.length())}; }
  common::ObString str() const { return common::ObString(static_cast<int32_t>(len_), data_); }
};

enum class FileSqlKind : uint8_t { NULL_VALUE, BIGINT, DOUBLE, BOOLEAN, VARCHAR, DATE, DATETIME };

struct FileSqlBound;
struct FileSqlScan;
struct FileSqlCopy;
extern "C" {
FileSqlBound *seekdb_file_bind(FileSqlSpan name, FileSqlSpan path, FileSqlSpan root,
                             const FileSqlSpan *options, size_t count, char *error, size_t error_len);
FileSqlBound *seekdb_file_decode(FileSqlSpan descriptor);
void seekdb_file_bound_close(FileSqlBound *bound);
FileSqlSpan seekdb_file_descriptor(const FileSqlBound *bound);
size_t seekdb_file_column_count(const FileSqlBound *bound);
FileSqlSpan seekdb_file_column(const FileSqlBound *bound, size_t index, FileSqlKind &kind,
                             bool &nullable, uint8_t &scale);
FileSqlScan *seekdb_file_open(FileSqlSpan descriptor, FileSqlSpan root, const uint32_t *columns,
                            size_t count, char *error, size_t error_len);
void seekdb_file_close(FileSqlScan *scan);
int seekdb_file_next(FileSqlScan *scan, char *error, size_t error_len);
FileSqlKind seekdb_file_cell(const FileSqlScan *scan, size_t index, int64_t &integer,
                            double &real, FileSqlSpan &text);
FileSqlCopy *seekdb_file_copy_open(FileSqlSpan path, FileSqlSpan root, uint8_t delimiter,
                                 uint8_t quote, uint8_t escape, FileSqlSpan null_text,
                                 char *error, size_t error_len);
bool seekdb_file_copy_cell(FileSqlCopy *writer, FileSqlSpan text, bool is_null, char *error, size_t error_len);
bool seekdb_file_copy_end_row(FileSqlCopy *writer, char *error, size_t error_len);
bool seekdb_file_copy_publish(FileSqlCopy *writer, char *error, size_t error_len);
void seekdb_file_copy_close(FileSqlCopy *writer);
}

inline constexpr char FILE_SQL_DESCRIPTOR_PREFIX[] = "\x01seekdb:file:";
common::ObString file_sql_descriptor(const ObRawExpr *expr);
int get_file_sql_root(const ObSQLSessionInfo *session, common::ObString &root);
int file_sql_error(const char *message);

} // namespace sql
} // namespace oceanbase
