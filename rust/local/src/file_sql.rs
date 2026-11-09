//! Local file table readers. The SQL engine owns query execution.
use chrono::{NaiveDate, NaiveDateTime};
use parquet::basic::{ConvertedType, Repetition, Type as PhysicalType};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::record::reader::{ReaderIter, TreeBuilder};
use parquet::record::Field;
use parquet::schema::types::{SchemaDescriptor, Type};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const MAX_RECORD: usize = 1024 * 1024;
const MAX_COLUMNS: usize = 1024;
const MAX_MATCHES: usize = 10_000;
const MAX_DIR_ENTRIES: usize = 100_000;
const MAX_PARQUET_FOOTER: usize = 16 * 1024 * 1024;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Span {
    data: *const u8,
    len: usize,
}

impl Span {
    fn from_str(value: &str) -> Self {
        Self { data: value.as_ptr(), len: value.len() }
    }

    unsafe fn text<'a>(self) -> Result<&'a str> {
        if self.len == 0 {
            Ok("")
        } else {
            Ok(std::str::from_utf8(std::slice::from_raw_parts(self.data, self.len))?)
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
enum Kind {
    Null, Bigint, Double, Boolean, Varchar, Date, Datetime,
}

#[derive(Serialize, Deserialize)]
struct Column {
    name: String,
    kind: Kind,
    nullable: bool,
    scale: u8,
}

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
struct Stamp {
    device: u64,
    inode: u64,
    size: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
}

#[cfg(unix)]
fn stamp(file: &File) -> Result<Stamp> {
    use std::os::unix::fs::MetadataExt;
    let m = file.metadata()?;
    Ok(Stamp {
        device: m.dev(), inode: m.ino(), size: m.len(),
        mtime: (m.mtime(), m.mtime_nsec()), ctime: (m.ctime(), m.ctime_nsec()),
    })
}

#[cfg(not(unix))]
fn stamp(_: &File) -> Result<Stamp> {
    Err("file SQL is unavailable on this platform".into())
}

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
enum Format { Csv, Json, Parquet, Glob }

#[derive(Serialize, Deserialize)]
struct Options {
    header: bool,
    delimiter: u8,
    quote: u8,
    escape: u8,
    null: String,
}

impl Default for Options {
    fn default() -> Self {
        Self { header: true, delimiter: b',', quote: b'"', escape: b'"', null: String::new() }
    }
}

struct Sandbox {
    path: PathBuf,
    directory: File,
}

fn clean(path: &Path) -> Result<PathBuf> {
    let mut normalized = PathBuf::new();
    if path.as_os_str().len() >= 4096 {
        return Err("file path is too long".into());
    }
    for part in path.components() {
        match part {
            Component::ParentDir => return Err("parent path is forbidden".into()),
            Component::CurDir => (),
            _ => normalized.push(part),
        }
    }
    Ok(normalized)
}

#[cfg(target_os = "linux")]
fn open_relative_legacy(base: Option<&File>, path: &Path, directory: bool) -> Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    let path = clean(path)?;
    if base.is_some() && path.is_absolute() {
        return Err("absolute path is forbidden beneath a directory descriptor".into());
    }
    let mut parent = if let Some(base) = base {
        base.try_clone()?
    } else {
        File::open(if path.is_absolute() { "/" } else { "." })?
    };
    let parts: Vec<_> = path.components().filter_map(|part| match part {
        Component::Normal(name) => Some(name),
        _ => None,
    }).collect();
    // Resolve every component relative to an already pinned directory. Never
    // follow a symlink, including the final component, and never traverse "..".
    let count = parts.len().max(1);
    for index in 0..count {
        let last = index + 1 == count;
        let name = std::ffi::CString::new(
            parts.get(index).map_or(b".".as_slice(), |name| name.as_bytes())
        )?;
        let flags = libc::O_CLOEXEC | libc::O_NOFOLLOW | if last {
            if directory { libc::O_RDONLY | libc::O_DIRECTORY } else { libc::O_PATH }
        } else {
            libc::O_PATH | libc::O_DIRECTORY
        };
        let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        parent = unsafe { File::from_raw_fd(fd) };
    }
    if !directory && !parent.metadata()?.is_file() {
        return Err("expected a regular file".into());
    }
    Ok(parent)
}

#[cfg(target_os = "linux")]
fn open_relative(base: Option<&File>, path: &Path, directory: bool) -> Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    #[repr(C)]
    struct OpenHow { flags: u64, mode: u64, resolve: u64 }
    const RESOLVE_NO_SYMLINKS: u64 = 0x04;
    const RESOLVE_BENEATH: u64 = 0x08;
    let flags = libc::O_CLOEXEC | if directory {
        libc::O_RDONLY | libc::O_DIRECTORY
    } else {
        libc::O_PATH
    };
    let how = OpenHow {
        flags: flags as u64, mode: 0,
        resolve: RESOLVE_NO_SYMLINKS | if base.is_some() { RESOLVE_BENEATH } else { 0 },
    };
    let encoded_path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    let fd = unsafe {
        libc::syscall(libc::SYS_openat2, base.map_or(libc::AT_FDCWD, File::as_raw_fd),
                      encoded_path.as_ptr(), &how, std::mem::size_of::<OpenHow>())
    };
    if fd < 0 {
        let error = std::io::Error::last_os_error();
        // Older kernels and some container profiles do not expose openat2.
        // Preserve other errors instead of retrying a denied operation.
        if error.raw_os_error() == Some(libc::ENOSYS) {
            return open_relative_legacy(base, path, directory);
        }
        return Err(error.into());
    }
    let file = unsafe { File::from_raw_fd(fd as i32) };
    if !directory && !file.metadata()?.is_file() {
        return Err("expected a regular file".into());
    }
    Ok(file)
}

#[cfg(not(target_os = "linux"))]
fn open_relative(_: Option<&File>, _: &Path, _: bool) -> Result<File> {
    Err("file SQL requires Linux openat2".into())
}

impl Sandbox {
    fn new(root: &str) -> Result<Self> {
        if root.is_empty() || root.eq_ignore_ascii_case("null") {
            return Err("secure_file_priv must name an allowed directory".into());
        }
        let path = std::fs::canonicalize(root)?;
        let directory = open_relative(None, &path, true)?;
        Ok(Self { path, directory })
    }

    fn relative(&self, path: &str) -> Result<PathBuf> {
        let path = clean(Path::new(path))?;
        let relative = if path.is_absolute() { path.strip_prefix(&self.path)? } else { &path };
        if relative.as_os_str().is_empty() {
            return Err("missing file name".into());
        }
        Ok(relative.to_owned())
    }

    fn open(&self, path: &str) -> Result<File> {
        let descriptor = self.probe(path)?;
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            // O_PATH inspects the object before opening it for reading. The
            // descriptor keeps the same inode even if its name is replaced.
            Ok(File::open(format!("/proc/self/fd/{}", descriptor.as_raw_fd()))?)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(descriptor)
        }
    }

    fn probe(&self, path: &str) -> Result<File> {
        open_relative(Some(&self.directory), &self.relative(path)?, false)
    }

    #[cfg(target_os = "linux")]
    fn discover(&self, path: &str) -> Result<Vec<String>> {
        use std::os::fd::AsRawFd;
        let path = self.relative(path)?;
        let parts: Vec<_> = path.iter().collect();
        if parts.len() > 64 {
            return Err("glob path is too deep".into());
        }
        let mut prefixes = vec![PathBuf::new()];
        let mut visited = 0;
        for (i, part) in parts.iter().enumerate() {
            let pattern = glob::Pattern::new(part.to_str().ok_or("glob is not UTF-8")?)?;
            let last = i + 1 == parts.len();
            let mut matches = Vec::new();
            for prefix in prefixes {
                let dir = if prefix.as_os_str().is_empty() {
                    self.directory.try_clone()?
                } else {
                    open_relative(Some(&self.directory), &prefix, true)?
                };
                for entry in std::fs::read_dir(format!("/proc/self/fd/{}", dir.as_raw_fd()))? {
                    visited += 1;
                    if visited > MAX_DIR_ENTRIES || matches.len() >= MAX_MATCHES {
                        return Err("glob discovery limit exceeded".into());
                    }
                    let entry = entry?;
                    if let Some(name) = entry.file_name().to_str() {
                        // Wildcards do not discover hidden names unless this
                        // path component explicitly starts with a dot.
                        if pattern.matches_with(name, glob::MatchOptions {
                            require_literal_leading_dot: true,
                            ..glob::MatchOptions::new()
                        }) {
                            let ty = entry.file_type()?;
                            if (last && ty.is_file()) || (!last && ty.is_dir()) {
                                let relative = prefix.join(name);
                                open_relative(Some(&self.directory), &relative, !last)?;
                                matches.push(relative);
                            }
                        }
                    }
                }
            }
            prefixes = matches;
        }
        let mut paths: Vec<_> = prefixes.into_iter()
            .map(|p| self.path.join(p).to_string_lossy().into_owned()).collect();
        paths.sort_unstable();
        Ok(paths)
    }

    #[cfg(not(target_os = "linux"))]
    fn discover(&self, _: &str) -> Result<Vec<String>> {
        Err("glob requires Linux openat2".into())
    }
}

struct TextReader {
    input: BufReader<File>,
    record: u64,
}

impl TextReader {
    fn new(file: File) -> Self {
        Self { input: BufReader::new(file), record: 0 }
    }

    fn byte(&mut self) -> Result<Option<u8>> {
        let byte = self.input.fill_buf()?.first().copied();
        if byte.is_some() {
            self.input.consume(1);
        }
        Ok(byte)
    }

    fn csv(&mut self, options: &Options) -> Result<Option<Vec<(String, bool)>>> {
        #[derive(PartialEq)]
        enum State { Start, Plain, Quoted, Closed }
        let mut state = State::Start;
        let mut fields = Vec::new();
        let mut field = Vec::new();
        let mut bytes = 0;
        let mut quoted = false;
        loop {
            let byte = self.byte()?;
            bytes += 1;
            if bytes > MAX_RECORD {
                return Err("CSV record exceeds 1 MiB".into());
            }
            if state == State::Quoted {
                match byte {
                    None => return Err("unterminated CSV quote".into()),
                    Some(b) if b == options.quote => state = State::Closed,
                    Some(b) if b == options.escape && options.escape != 0 => {
                        let next = self.byte()?.ok_or("unterminated CSV escape")?;
                        bytes += 1;
                        if next != options.quote && next != options.escape {
                            return Err("invalid CSV escape".into());
                        }
                        field.push(next);
                    }
                    Some(b) => field.push(b),
                }
            } else if state == State::Closed && byte == Some(options.quote)
                      && options.quote == options.escape {
                field.push(options.quote);
                state = State::Quoted;
            } else if byte.is_none() || matches!(byte, Some(b'\n' | b'\r'))
                      || byte == Some(options.delimiter) {
                if bytes == 1 && byte.is_none() {
                    return Ok(None);
                }
                if fields.len() >= MAX_COLUMNS {
                    return Err("CSV column limit exceeded".into());
                }
                fields.push((String::from_utf8(std::mem::take(&mut field))?, quoted));
                if byte != Some(options.delimiter) {
                    if byte == Some(b'\r') && self.input.fill_buf()?.first() == Some(&b'\n') {
                        self.input.consume(1);
                    }
                    self.record += 1;
                    return Ok(Some(fields));
                }
                state = State::Start;
                quoted = false;
            } else if state == State::Closed {
                return Err("unexpected byte after CSV quote".into());
            } else if let Some(b) = byte {
                if options.quote != 0 && b == options.quote {
                    if state != State::Start {
                        return Err("unexpected quote in CSV field".into());
                    }
                    state = State::Quoted;
                    quoted = true;
                } else {
                    field.push(b);
                    state = State::Plain;
                }
            }
        }
    }

    fn json(&mut self) -> Result<Option<serde_json::Map<String, Value>>> {
        loop {
            let mut line = Vec::new();
            let read = (&mut self.input).take(MAX_RECORD as u64 + 1).read_until(b'\n', &mut line)?;
            if read == 0 {
                return Ok(None);
            }
            if read > MAX_RECORD {
                return Err("JSONL record exceeds 1 MiB".into());
            }
            self.record += 1;
            if self.record == 1 && line.starts_with(&[0xef, 0xbb, 0xbf]) {
                line.drain(..3);
            }
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let values: serde_json::Map<String, Value> = serde_json::from_slice(&line)?;
            if values.len() > MAX_COLUMNS || values.values().any(|v| v.is_array() || v.is_object()) {
                return Err("expected a flat JSON object within the column limit".into());
            }
            return Ok(Some(values));
        }
    }
}

fn date(s: &str) -> Option<NaiveDate> {
    (s.len() == 10 && s.as_bytes()[0..4] != *b"0000")
        .then(|| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()).flatten()
        .filter(|d| d.format("%Y-%m-%d").to_string() == s)
}

fn datetime(s: &str) -> Option<NaiveDateTime> {
    let prefix = s.get(..10)?;
    date(prefix)?;
    let separator = *s.as_bytes().get(10)?;
    if !matches!(separator, b' ' | b'T') || s.len() < 19 || s.len() > 26 {
        return None;
    }
    let pattern = if separator == b'T' { "%Y-%m-%dT%H:%M:%S%.f" } else { "%Y-%m-%d %H:%M:%S%.f" };
    NaiveDateTime::parse_from_str(s, pattern).ok().filter(|d| {
        let base = if separator == b'T' { "%Y-%m-%dT%H:%M:%S" } else { "%Y-%m-%d %H:%M:%S" };
        d.format(base).to_string() == s[..19] && d.and_utc().timestamp_subsec_nanos() < 1_000_000_000
    })
}

fn numeric(s: &str) -> Kind {
    if s.parse::<i64>().is_ok() {
        Kind::Bigint
    } else if (s.contains('.') || s.contains('e') || s.contains('E'))
              && s.parse::<f64>().is_ok_and(f64::is_finite) {
        Kind::Double
    } else {
        Kind::Varchar
    }
}

fn text_kind(s: &str) -> Kind {
    if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false") {
        Kind::Boolean
    } else if date(s).is_some() {
        Kind::Date
    } else if datetime(s).is_some() {
        Kind::Datetime
    } else {
        numeric(s)
    }
}

fn json_kind(v: &Value) -> Kind {
    match v {
        Value::Null => Kind::Null,
        Value::Bool(_) => Kind::Boolean,
        Value::Number(n) => numeric(&n.to_string()),
        Value::String(s) if date(s).is_some() => Kind::Date,
        Value::String(s) if datetime(s).is_some() => Kind::Datetime,
        _ => Kind::Varchar,
    }
}

fn widen(old: Kind, new: Kind) -> Kind {
    use Kind::*;
    match (old, new) {
        (Null, k) | (k, Null) => k,
        (a, b) if a == b => a,
        (Bigint, Double) | (Double, Bigint) => Double,
        (Date, Datetime) | (Datetime, Date) => Datetime,
        _ => Varchar,
    }
}

fn update(column: &mut Column, kind: Kind) {
    column.nullable |= kind == Kind::Null;
    column.kind = widen(column.kind, kind);
}

fn named_columns(names: impl IntoIterator<Item = String>) -> Result<Vec<Column>> {
    let mut used = HashSet::new();
    let mut columns = Vec::new();
    for (index, raw) in names.into_iter().enumerate() {
        let base = if raw.is_empty() { format!("column{}", index + 1) } else { raw };
        let mut name = base.clone();
        let mut suffix = 2;
        while !used.insert(name.to_lowercase()) {
            name = format!("{base}__{suffix}");
            suffix += 1;
        }
        if name.len() > 256 || name.contains('\0') {
            return Err("invalid file column name".into());
        }
        columns.push(Column { name, kind: Kind::Null, nullable: false, scale: 0 });
    }
    Ok(columns)
}

fn csv_null(text: &str, quoted: bool, options: &Options) -> bool {
    !quoted && (text == options.null || (options.null.is_empty() && text == "NULL"))
}

fn infer_csv(reader: &mut TextReader, options: &Options) -> Result<Vec<Column>> {
    if reader.input.fill_buf()?.starts_with(&[0xef, 0xbb, 0xbf]) {
        reader.input.consume(3);
    }
    let first = reader.csv(options)?.ok_or("empty CSV file")?;
    let mut columns = if options.header {
        named_columns(first.into_iter().map(|(name, _)| name))?
    } else {
        let mut columns = named_columns(first.iter().map(|_| String::new()))?;
        update_csv(&mut columns, &first, options)?;
        columns
    };
    while let Some(row) = reader.csv(options)? {
        update_csv(&mut columns, &row, options)?;
    }
    Ok(columns)
}

fn update_csv(columns: &mut [Column], row: &[(String, bool)], options: &Options) -> Result<()> {
    if columns.len() != row.len() {
        return Err(format!("expected {} CSV fields, got {}", columns.len(), row.len()).into());
    }
    for (column, (text, quoted)) in columns.iter_mut().zip(row) {
        let kind = if csv_null(text, *quoted, options) { Kind::Null } else { text_kind(text) };
        update(column, kind);
        if kind == Kind::Datetime {
            column.scale = column.scale.max(text.len().saturating_sub(20) as u8);
        }
    }
    Ok(())
}

fn infer_json(reader: &mut TextReader) -> Result<(Vec<Column>, Vec<String>)> {
    let mut schema: HashMap<String, (Kind, u64, u8)> = HashMap::new();
    let mut names = Vec::new();
    let mut rows = 0;
    while let Some(row) = reader.json()? {
        rows += 1;
        for (key, value) in row {
            let entry = schema.entry(key.clone()).or_insert_with(|| {
                names.push(key);
                (Kind::Null, 0, 0)
            });
            let kind = json_kind(&value);
            entry.0 = widen(entry.0, kind);
            entry.1 += u64::from(kind != Kind::Null);
            if kind == Kind::Datetime {
                entry.2 = entry.2.max(value.as_str().map_or(0, |s| s.len().saturating_sub(20) as u8));
            }
            if names.len() > MAX_COLUMNS {
                return Err("JSONL schema exceeds the column limit".into());
            }
        }
    }
    let mut columns = named_columns(names.iter().cloned())?;
    for (column, key) in columns.iter_mut().zip(&names) {
        let (kind, non_null, scale) = schema[key];
        column.kind = kind;
        column.nullable = non_null != rows;
        column.scale = scale;
    }
    Ok((columns, names))
}

fn parquet(file: File) -> Result<SerializedFileReader<File>> {
    use std::io::{Seek, SeekFrom};
    let mut file = file;
    let len = file.metadata()?.len();
    if len < 12 {
        return Err("truncated Parquet file".into());
    }
    file.seek(SeekFrom::End(-8))?;
    let mut tail = [0; 8];
    file.read_exact(&mut tail)?;
    let footer = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]) as usize;
    if tail[4..] != *b"PAR1" || footer > MAX_PARQUET_FOOTER || footer as u64 > len - 12 {
        return Err("invalid or oversized Parquet footer".into());
    }
    let reader = SerializedFileReader::new(file)?;
    let meta = reader.metadata();
    if meta.file_metadata().schema_descr().num_columns() > MAX_COLUMNS {
        return Err("Parquet column limit exceeded".into());
    }
    for group in meta.row_groups() {
        if group.num_rows() < 0 || group.total_byte_size() < 0
           || group.total_byte_size() > 256 * 1024 * 1024 {
            return Err("Parquet row group exceeds resource limit".into());
        }
        for column in group.columns() {
            let start = column.dictionary_page_offset().unwrap_or(column.data_page_offset());
            let size = column.compressed_size();
            if column.file_path().is_some() || start < 4 || size < 0 || size > 64 * 1024 * 1024
               || (start as u64).saturating_add(size as u64) > len - 8 - footer as u64
               || column.uncompressed_size() < 0 || column.uncompressed_size() > 64 * 1024 * 1024 {
                return Err("invalid or oversized Parquet column chunk".into());
            }
        }
    }
    Ok(reader)
}

fn parquet_columns(reader: &SerializedFileReader<File>) -> Result<Vec<Column>> {
    let root = reader.metadata().file_metadata().schema_descr().root_schema();
    let mut columns = named_columns(root.get_fields().iter().map(|f| f.name().to_owned()))?;
    for (column, field) in columns.iter_mut().zip(root.get_fields()) {
        let info = field.get_basic_info();
        if !field.is_primitive() || !info.has_repetition() || info.repetition() == Repetition::REPEATED {
            return Err("nested Parquet columns are unsupported".into());
        }
        column.nullable = info.repetition() == Repetition::OPTIONAL;
        column.kind = match (field.get_physical_type(), info.converted_type()) {
            (PhysicalType::INT32, ConvertedType::DATE) => Kind::Date,
            (PhysicalType::INT64, ConvertedType::TIMESTAMP_MILLIS | ConvertedType::TIMESTAMP_MICROS)
                | (PhysicalType::INT96, ConvertedType::NONE) => Kind::Datetime,
            (PhysicalType::INT32 | PhysicalType::INT64, ConvertedType::NONE | ConvertedType::INT_8
                | ConvertedType::INT_16 | ConvertedType::INT_32 | ConvertedType::INT_64
                | ConvertedType::UINT_8 | ConvertedType::UINT_16 | ConvertedType::UINT_32) => Kind::Bigint,
            (PhysicalType::BOOLEAN, ConvertedType::NONE) => Kind::Boolean,
            (PhysicalType::FLOAT | PhysicalType::DOUBLE, ConvertedType::NONE) => Kind::Double,
            (PhysicalType::BYTE_ARRAY, ConvertedType::UTF8 | ConvertedType::NONE) => Kind::Varchar,
            _ => return Err("unsupported Parquet column type".into()),
        };
        if column.kind == Kind::Datetime {
            column.scale = if info.converted_type() == ConvertedType::TIMESTAMP_MILLIS { 3 } else { 6 };
        }
    }
    Ok(columns)
}

#[derive(Serialize, Deserialize)]
struct Descriptor {
    path: String,
    format: Format,
    options: Options,
    stamp: Option<Stamp>,
    columns: Vec<Column>,
    keys: Vec<String>,
}

fn bind(root: &str, path: &str, format: Format, options: Options) -> Result<Descriptor> {
    let sandbox = Sandbox::new(root)?;
    let path = sandbox.path.join(sandbox.relative(path)?).into_os_string()
        .into_string().map_err(|_| "path is not UTF-8")?;
    let mut descriptor = Descriptor { path, format, options, stamp: None, columns: Vec::new(), keys: Vec::new() };
    if format == Format::Glob {
        descriptor.columns.push(Column { name: "file".into(), kind: Kind::Varchar, nullable: false, scale: 0 });
    } else {
        let file = sandbox.open(&descriptor.path)?;
        let original = stamp(&file)?;
        if format == Format::Parquet {
            descriptor.columns = parquet_columns(&parquet(file.try_clone()?)?)?;
        } else {
            let mut reader = TextReader::new(file.try_clone()?);
            let schema = match format {
                Format::Csv => infer_csv(&mut reader, &descriptor.options).map(|columns| (columns, Vec::new())),
                Format::Json => infer_json(&mut reader),
                _ => unreachable!(),
            };
            (descriptor.columns, descriptor.keys) = schema.map_err(|e| {
                format!("{} at record {}: {e}", descriptor.path, reader.record.max(1))
            })?;
        }
        if stamp(&file)? != original || stamp(&sandbox.probe(&descriptor.path)?)? != original {
            return Err("file changed during schema inference".into());
        }
        descriptor.stamp = Some(original);
        if descriptor.columns.is_empty() {
            return Err("could not infer any columns".into());
        }
    }
    Ok(descriptor)
}

enum Cell { Null, Int(i64), Double(f64), Text(String) }

fn convert(text: String, kind: Kind) -> Result<Cell> {
    Ok(match kind {
        Kind::Null => return Err("unexpected value in NULL column".into()),
        Kind::Bigint => Cell::Int(text.parse()?),
        Kind::Double => Cell::Double(text.parse()?),
        Kind::Boolean => Cell::Int(i64::from(text.eq_ignore_ascii_case("true"))),
        Kind::Date => Cell::Int(date(&text).ok_or("invalid date")?.and_hms_opt(0, 0, 0)
            .ok_or("invalid date")?.and_utc().timestamp().div_euclid(86400)),
        Kind::Datetime => Cell::Int(datetime(&text)
            .or_else(|| date(&text).and_then(|d| d.and_hms_opt(0, 0, 0)))
            .ok_or("invalid datetime")?.and_utc().timestamp_micros()),
        Kind::Varchar => Cell::Text(text),
    })
}

struct ParquetScan {
    reader: SerializedFileReader<File>,
    projection: Arc<SchemaDescriptor>,
    group: usize,
    rows: Option<ReaderIter>,
}

impl ParquetScan {
    fn next(&mut self) -> Result<Option<Vec<Cell>>> {
        loop {
            if let Some(row) = self.rows.as_mut().and_then(Iterator::next) {
                let row = row?;
                let mut cells = Vec::new();
                for (_, field) in row.into_columns() {
                    let cell = match field {
                        Field::Null => Cell::Null,
                        Field::Bool(v) => Cell::Int(i64::from(v)),
                        Field::Byte(v) => Cell::Int(v.into()),
                        Field::Short(v) => Cell::Int(v.into()),
                        Field::Int(v) => Cell::Int(v.into()),
                        Field::Long(v) => Cell::Int(v),
                        Field::UByte(v) => Cell::Int(v.into()),
                        Field::UShort(v) => Cell::Int(v.into()),
                        Field::UInt(v) => Cell::Int(v.into()),
                        Field::Float(v) if v.is_finite() => Cell::Double(v.into()),
                        Field::Double(v) if v.is_finite() => Cell::Double(v),
                        Field::Str(v) if v.len() <= MAX_RECORD => Cell::Text(v),
                        Field::Bytes(v) if v.len() <= MAX_RECORD => Cell::Text(std::str::from_utf8(v.data())?.into()),
                        Field::Date(v) => Cell::Int(v.into()),
                        Field::TimestampMillis(v) => Cell::Int(v.checked_mul(1000).ok_or("timestamp overflow")?),
                        Field::TimestampMicros(v) => Cell::Int(v),
                        _ => return Err("invalid or unsupported Parquet value".into()),
                    };
                    cells.push(cell);
                }
                return Ok(Some(cells));
            }
            if self.group >= self.reader.num_row_groups() {
                return Ok(None);
            }
            let group = self.reader.get_row_group(self.group)?;
            self.rows = Some(TreeBuilder::new().with_batch_size(128)
                .as_iter(self.projection.clone(), group.as_ref())?);
            self.group += 1;
        }
    }
}

enum Input {
    Csv(TextReader), Json(TextReader), Parquet(ParquetScan), Count(i64), Glob(std::vec::IntoIter<String>),
}

pub struct Scan {
    descriptor: Descriptor,
    sandbox: Sandbox,
    file: Option<File>,
    input: Input,
    projection: Vec<usize>,
    row: Vec<Cell>,
}

impl Scan {
    fn open(descriptor: Descriptor, root: &str, projection: &[u32]) -> Result<Self> {
        let sandbox = Sandbox::new(root)?;
        let projection: Vec<_> = projection.iter().map(|&i| i as usize).collect();
        if projection.iter().any(|&i| i >= descriptor.columns.len()) {
            return Err("file projection is out of bounds".into());
        }
        let mut file = None;
        let input = if descriptor.format == Format::Glob {
            Input::Glob(sandbox.discover(&descriptor.path)?.into_iter())
        } else {
            let opened = sandbox.open(&descriptor.path)?;
            if Some(stamp(&opened)?) != descriptor.stamp {
                return Err("file changed after schema inference".into());
            }
            let reader_file = opened.try_clone()?;
            file = Some(opened);
            match descriptor.format {
                Format::Csv => {
                    let mut reader = TextReader::new(reader_file);
                    if reader.input.fill_buf()?.starts_with(&[0xef, 0xbb, 0xbf]) {
                        reader.input.consume(3);
                    }
                    if descriptor.options.header {
                        reader.csv(&descriptor.options)?;
                    }
                    Input::Csv(reader)
                }
                Format::Json => Input::Json(TextReader::new(reader_file)),
                Format::Parquet => {
                    let reader = parquet(reader_file)?;
                    if projection.is_empty() {
                        Input::Count(reader.metadata().file_metadata().num_rows())
                    } else {
                        let root = reader.metadata().file_metadata().schema_descr().root_schema();
                        let fields = projection.iter().map(|&i| root.get_fields()[i].clone()).collect();
                        let projected = Type::group_type_builder(root.name()).with_fields(fields).build()?;
                        Input::Parquet(ParquetScan { reader, group: 0, rows: None,
                            projection: Arc::new(SchemaDescriptor::new(Arc::new(projected))) })
                    }
                }
                Format::Glob => unreachable!(),
            }
        };
        Ok(Self { descriptor, sandbox, file, input, projection, row: Vec::new() })
    }

    fn check(&self) -> Result<()> {
        if let Some(file) = &self.file {
            let expected = self.descriptor.stamp;
            if Some(stamp(file)?) != expected
               || Some(stamp(&self.sandbox.probe(&self.descriptor.path)?)?) != expected {
                return Err("file changed during scan".into());
            }
        }
        Ok(())
    }

    fn next(&mut self) -> Result<bool> {
        let descriptor = &self.descriptor;
        let result = match &mut self.input {
            Input::Csv(reader) => {
                let Some(row) = reader.csv(&descriptor.options)? else { return Ok(false) };
                if row.len() != descriptor.columns.len() {
                    return Err(format!("wrong number of CSV fields at record {}", reader.record).into());
                }
                self.projection.iter().map(|&i| {
                    let (text, quoted) = &row[i];
                    if csv_null(text, *quoted, &descriptor.options) { Ok(Cell::Null) }
                    else { convert(text.clone(), descriptor.columns[i].kind) }
                }).collect::<Result<Vec<_>>>()?
            }
            Input::Json(reader) => {
                let Some(mut row) = reader.json()? else { return Ok(false) };
                self.projection.iter().map(|&i| {
                    match row.remove(&descriptor.keys[i]) {
                        None | Some(Value::Null) => Ok(Cell::Null),
                        Some(Value::String(s)) => convert(s, descriptor.columns[i].kind),
                        Some(value) => convert(value.to_string(), descriptor.columns[i].kind),
                    }
                }).collect::<Result<Vec<_>>>()?
            }
            Input::Parquet(reader) => {
                let Some(row) = reader.next()? else { return Ok(false) };
                row
            }
            Input::Count(remaining) => {
                if *remaining <= 0 { return Ok(false) }
                *remaining -= 1;
                Vec::new()
            }
            Input::Glob(paths) => {
                let Some(path) = paths.next() else { return Ok(false) };
                if self.projection.is_empty() { Vec::new() } else { vec![Cell::Text(path)] }
            }
        };
        self.row = result;
        Ok(true)
    }
}

pub struct Bound { descriptor: Descriptor, encoded: String }

fn parse_options(args: &[Span], format: Format) -> Result<Options> {
    let mut options = Options::default();
    let mut seen = HashSet::new();
    for pair in args.chunks_exact(2) {
        let key = unsafe { pair[0].text()? }.to_ascii_lowercase();
        let key = match key.as_str() {
            "delim" | "sep" => "delimiter",
            "nullstr" => "null",
            other => other,
        };
        let value = unsafe { pair[1].text()? };
        if !seen.insert(key.to_owned()) {
            return Err(format!("duplicate option {key}").into());
        }
        let character = || -> Result<u8> {
            if value.len() != 1 || !value.is_ascii() || matches!(value.as_bytes()[0], 0 | b'\n' | b'\r') {
                Err(format!("{key} must be one ASCII character").into())
            } else {
                Ok(value.as_bytes()[0])
            }
        };
        match key {
            "sample_size" if matches!(format, Format::Csv | Format::Json) => {
                if value != "-1" { return Err("sample_size supports only -1".into()) }
            }
            "format" if format == Format::Json => {
                if !value.eq_ignore_ascii_case("newline_delimited") && !value.eq_ignore_ascii_case("ndjson") {
                    return Err("read_json supports only newline_delimited format".into());
                }
            }
            "header" if format == Format::Csv => {
                options.header = match value.to_ascii_lowercase().as_str() {
                    "true" | "1" => true, "false" | "0" => false,
                    _ => return Err("header requires a Boolean".into()),
                };
            }
            "delimiter" if format == Format::Csv => options.delimiter = character()?,
            "quote" if format == Format::Csv => options.quote = if value.is_empty() { 0 } else { character()? },
            "escape" if format == Format::Csv => options.escape = if value.is_empty() { 0 } else { character()? },
            "null" if format == Format::Csv => options.null = value.to_owned(),
            _ => return Err(format!("unsupported file option {key}").into()),
        }
    }
    if seen.contains("quote") && !seen.contains("escape") {
        options.escape = options.quote;
    }
    if options.delimiter == options.quote {
        return Err("delimiter and quote must differ".into());
    }
    Ok(options)
}

unsafe fn write_error(out: *mut u8, len: usize, error: &dyn std::fmt::Display) {
    if len > 0 {
        let msg = error.to_string();
        let size = msg.len().min(len - 1);
        std::ptr::copy_nonoverlapping(msg.as_ptr(), out, size);
        *out.add(size) = 0;
    }
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_bind(name: Span, path: Span, root: Span, args: *const Span,
                                         count: usize, err: *mut u8, len: usize) -> *mut Bound {
    let result = (|| -> Result<_> {
        let format = match name.text()?.to_ascii_lowercase().as_str() {
            "read_csv" => Format::Csv, "read_json" | "read_ndjson" => Format::Json,
            "read_parquet" => Format::Parquet, "glob" => Format::Glob,
            _ => return Err("unknown file table function".into()),
        };
        let args = if count == 0 { &[] } else { std::slice::from_raw_parts(args, count) };
        let descriptor = bind(root.text()?, path.text()?, format, parse_options(args, format)?)?;
        let encoded = serde_json::to_string(&descriptor)?;
        Ok(Box::into_raw(Box::new(Bound { descriptor, encoded })))
    })();
    match result {
        Ok(bound) => bound,
        Err(error) => { write_error(err, len, &error); std::ptr::null_mut() }
    }
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_bound_close(bound: *mut Bound) {
    if !bound.is_null() { drop(Box::from_raw(bound)); }
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_decode(encoded: Span) -> *mut Bound {
    let decoded = encoded.text().ok().and_then(|s| serde_json::from_str(s).ok());
    match decoded {
        Some(descriptor) => Box::into_raw(Box::new(Bound { descriptor, encoded: String::new() })),
        None => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_descriptor(bound: &Bound) -> Span {
    Span::from_str(&bound.encoded)
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_column_count(bound: &Bound) -> usize {
    bound.descriptor.columns.len()
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_column(bound: &Bound, index: usize, kind: &mut u8,
                                           nullable: &mut bool, scale: &mut u8) -> Span {
    let column = &bound.descriptor.columns[index];
    *kind = column.kind as u8;
    *nullable = column.nullable;
    *scale = column.scale;
    Span::from_str(&column.name)
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_open(encoded: Span, root: Span, projection: *const u32,
                                         count: usize, err: *mut u8, len: usize) -> *mut Scan {
    let result = (|| -> Result<_> {
        let descriptor = serde_json::from_str(encoded.text()?)?;
        let projection = if count == 0 { &[] } else { std::slice::from_raw_parts(projection, count) };
        Ok(Box::into_raw(Box::new(Scan::open(descriptor, root.text()?, projection)?)))
    })();
    match result {
        Ok(scan) => scan,
        Err(error) => { write_error(err, len, &error); std::ptr::null_mut() }
    }
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_close(scan: *mut Scan) {
    if !scan.is_null() { drop(Box::from_raw(scan)); }
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_next(scan: &mut Scan, err: *mut u8, len: usize) -> i32 {
    match scan.check().and_then(|_| scan.next()).and_then(|found| scan.check().map(|()| found)) {
        Ok(found) => i32::from(found),
        Err(error) => { write_error(err, len, &error); -1 }
    }
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_cell(scan: &Scan, index: usize, integer: &mut i64,
                                         real: &mut f64, text: &mut Span) -> u8 {
    match &scan.row[index] {
        Cell::Null => Kind::Null as u8,
        Cell::Int(v) => { *integer = *v; Kind::Bigint as u8 }
        Cell::Double(v) => { *real = *v; Kind::Double as u8 }
        Cell::Text(v) => { *text = Span::from_str(v); Kind::Varchar as u8 }
    }
}

pub struct CopyFile {
    sandbox: Sandbox,
    parent: PathBuf,
    directory: File,
    name: std::ffi::CString,
    writer: std::io::BufWriter<File>,
    options: Options,
    row: Vec<u8>,
    columns: usize,
}

impl CopyFile {
    #[cfg(target_os = "linux")]
    fn open(root: &str, path: &str, options: Options) -> Result<Self> {
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::ffi::OsStrExt;
        if options.delimiter == 0 || options.delimiter == options.quote
           || options.null.bytes().any(|b| matches!(b, 0 | b'\n' | b'\r')
                                           || b == options.delimiter || b == options.quote) {
            return Err("invalid COPY CSV delimiter, quote, or NULL marker".into());
        }
        let sandbox = Sandbox::new(root)?;
        let target = sandbox.relative(path)?;
        let name = std::ffi::CString::new(target.file_name().ok_or("missing output file name")?.as_bytes())?;
        let parent = target.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new(".")).to_owned();
        let directory = open_relative(Some(&sandbox.directory), &parent, true)?;
        let mut existing = std::mem::MaybeUninit::<libc::stat>::uninit();
        let rc = unsafe { libc::fstatat(directory.as_raw_fd(), name.as_ptr(), existing.as_mut_ptr(),
                                       libc::AT_SYMLINK_NOFOLLOW) };
        if rc == 0 {
            return Err("COPY target already exists".into());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(error.into());
        }
        // Unnamed files disappear automatically on every failure path.
        let fd = unsafe { libc::openat(directory.as_raw_fd(), c".".as_ptr(),
                                       libc::O_TMPFILE | libc::O_WRONLY | libc::O_CLOEXEC, 0o600) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let file = unsafe { File::from_raw_fd(fd) };
        Ok(Self { sandbox, parent, directory, name, writer: std::io::BufWriter::new(file),
            options, row: Vec::new(), columns: 0 })
    }

    #[cfg(not(target_os = "linux"))]
    fn open(_: &str, _: &str, _: Options) -> Result<Self> {
        Err("COPY requires Linux O_TMPFILE".into())
    }

    fn cell(&mut self, value: Option<&str>) -> Result<()> {
        let options = &self.options;
        if self.columns > 0 {
            self.row.push(options.delimiter);
        }
        self.columns += 1;
        if let Some(text) = value {
            if text.len() > MAX_RECORD {
                return Err("COPY CSV field exceeds 1 MiB".into());
            }
            let quote = csv_null(text, false, options)
                || text.bytes().any(|b| matches!(b, b'\n' | b'\r') || b == options.delimiter || b == options.quote);
            if quote {
                if options.quote == 0 {
                    return Err("COPY field requires CSV quoting".into());
                }
                self.row.push(options.quote);
                for b in text.bytes() {
                    if b == options.quote || b == options.escape {
                        if options.escape == 0 {
                            return Err("COPY quote requires an escape character".into());
                        }
                        self.row.push(options.escape);
                    }
                    self.row.push(b);
                }
                self.row.push(options.quote);
            } else {
                self.row.extend_from_slice(text.as_bytes());
            }
        } else {
            self.row.extend_from_slice(options.null.as_bytes());
        }
        if self.row.len() >= MAX_RECORD {
            return Err("COPY CSV record exceeds 1 MiB".into());
        }
        Ok(())
    }

    fn end_row(&mut self) -> Result<()> {
        use std::io::Write;
        self.row.push(b'\n');
        self.writer.write_all(&self.row)?;
        self.row.clear();
        self.columns = 0;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn publish(&mut self) -> Result<()> {
        use std::io::Write;
        use std::os::fd::AsRawFd;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        let current = open_relative(Some(&self.sandbox.directory), &self.parent, true)?;
        let original = stamp(&self.directory)?;
        let now = stamp(&current)?;
        if original.device != now.device || original.inode != now.inode {
            return Err("COPY output directory changed".into());
        }
        let source = std::ffi::CString::new(format!("/proc/self/fd/{}", self.writer.get_ref().as_raw_fd()))?;
        let rc = unsafe {
            libc::linkat(libc::AT_FDCWD, source.as_ptr(), current.as_raw_fd(), self.name.as_ptr(),
                         libc::AT_SYMLINK_FOLLOW)
        };
        if rc != 0 { return Err(std::io::Error::last_os_error().into()) }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn publish(&mut self) -> Result<()> { Err("COPY requires Linux O_TMPFILE".into()) }
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_copy_open(path: Span, root: Span, delimiter: u8, quote: u8,
    escape: u8, null: Span, err: *mut u8, len: usize) -> *mut CopyFile {
    let result = (|| -> Result<_> {
        let options = Options { header: false, delimiter, quote, escape, null: null.text()?.to_owned() };
        Ok(Box::into_raw(Box::new(CopyFile::open(root.text()?, path.text()?, options)?)))
    })();
    match result {
        Ok(writer) => writer,
        Err(error) => { write_error(err, len, &error); std::ptr::null_mut() }
    }
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_copy_cell(writer: &mut CopyFile, value: Span, is_null: bool,
    err: *mut u8, len: usize) -> bool {
    let result = if is_null { writer.cell(None) } else { value.text().and_then(|s| writer.cell(Some(s))) };
    if let Err(error) = result { write_error(err, len, &error); false } else { true }
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_copy_end_row(writer: &mut CopyFile, err: *mut u8, len: usize) -> bool {
    if let Err(error) = writer.end_row() { write_error(err, len, &error); false } else { true }
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_copy_publish(writer: &mut CopyFile, err: *mut u8, len: usize) -> bool {
    if let Err(error) = writer.publish() { write_error(err, len, &error); false } else { true }
}

#[no_mangle]
pub unsafe extern "C" fn seekdb_file_copy_close(writer: *mut CopyFile) {
    if !writer.is_null() { drop(Box::from_raw(writer)); }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "seekdb-file-sql-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn root(&self) -> &str {
            self.0.to_str().unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn scan_rejects_replacement_and_mutation() {
        let dir = Fixture::new();
        let path = dir.0.join("rows.csv");
        std::fs::write(&path, "id\n1\n2\n").unwrap();
        let desc = bind(dir.root(), "rows.csv", Format::Csv, Options::default()).unwrap();
        let mut scan = Scan::open(desc, dir.root(), &[0]).unwrap();
        assert!(scan.next().unwrap());
        std::fs::write(&path, "id\n10\n2\n").unwrap();
        assert!(scan.check().is_err());
        let desc = bind(dir.root(), "rows.csv", Format::Csv, Options::default()).unwrap();
        std::fs::rename(&path, dir.0.join("old.csv")).unwrap();
        std::fs::write(&path, "id\n10\n2\n").unwrap();
        assert!(Scan::open(desc, dir.root(), &[0]).is_err());
    }

    #[test]
    fn secure_path_rejects_symlinks_and_sibling_prefixes() {
        let dir = Fixture::new();
        let outside = Fixture::new();
        let path = outside.0.join("secret.csv");
        std::fs::write(&path, "secret\n1\n").unwrap();
        symlink(&outside.0, dir.0.join("outside")).unwrap();
        let sandbox = Sandbox::new(dir.root()).unwrap();
        assert!(sandbox.open("outside/secret.csv").is_err());
        assert!(sandbox.open(path.to_str().unwrap()).is_err());
        assert!(sandbox.discover("outside/*").unwrap().is_empty());
    }

    #[test]
    fn glob_requires_explicit_leading_dot_in_each_component() {
        let dir = Fixture::new();
        for sub in ["visible", ".hidden"] {
            std::fs::create_dir(dir.0.join(sub)).unwrap();
            std::fs::write(dir.0.join(sub).join("row.csv"), "id\n1\n").unwrap();
        }
        for name in ["rows.csv", "._rows.csv", ".settings"] {
            std::fs::write(dir.0.join(name), "id\n1\n").unwrap();
        }
        let sandbox = Sandbox::new(dir.root()).unwrap();
        let paths = |pattern| sandbox.discover(pattern).unwrap().into_iter()
            .map(|p| p.strip_prefix(&format!("{}/", dir.root())).unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(paths("*"), vec!["rows.csv"]);
        assert_eq!(paths("?*"), vec!["rows.csv"]);
        assert_eq!(paths("[r.]*"), vec!["rows.csv"]);
        assert_eq!(paths(".*"), vec!["._rows.csv", ".settings"]);
        assert_eq!(paths("._*"), vec!["._rows.csv"]);
        assert_eq!(paths("._rows.csv"), vec!["._rows.csv"]);
        assert_eq!(paths("*/*.csv"), vec!["visible/row.csv"]);
        assert_eq!(paths(".*/*.csv"), vec![".hidden/row.csv"]);
    }

    #[test]
    fn invalid_parquet_is_rejected_before_scan() {
        let dir = Fixture::new();
        std::fs::write(dir.0.join("broken.parquet"), b"PAR1bad\xff\xff\xff\xffPAR1").unwrap();
        assert!(bind(dir.root(), "broken.parquet", Format::Parquet, Options::default()).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn legacy_open_preserves_directory_confinement() {
        use std::os::unix::fs::MetadataExt;
        let dir = Fixture::new();
        let outside = Fixture::new();
        std::fs::create_dir(dir.0.join("nested")).unwrap();
        std::fs::write(dir.0.join("nested/rows.csv"), "id\n1\n").unwrap();
        std::fs::write(outside.0.join("secret.csv"), "secret\n2\n").unwrap();
        let root = open_relative_legacy(None, &dir.0, true).unwrap();
        let file = open_relative_legacy(Some(&root), Path::new("nested/rows.csv"), false).unwrap();
        assert_eq!(file.metadata().unwrap().ino(),
                   std::fs::metadata(dir.0.join("nested/rows.csv")).unwrap().ino());
        assert!(open_relative_legacy(Some(&root), Path::new("."), true).is_ok());
        symlink(&outside.0, dir.0.join("link")).unwrap();
        symlink(outside.0.join("secret.csv"), dir.0.join("file_link")).unwrap();
        assert!(open_relative_legacy(Some(&root), Path::new("link/secret.csv"), false).is_err());
        assert!(open_relative_legacy(Some(&root), Path::new("file_link"), false).is_err());
        assert!(open_relative_legacy(Some(&root), Path::new("../secret.csv"), false).is_err());
        assert!(open_relative_legacy(Some(&root), &outside.0.join("secret.csv"), false).is_err());
        assert!(open_relative_legacy(Some(&root), Path::new("nested"), false).is_err());
        // Replacing the pathname with a symlink must not redirect a pinned root.
        std::fs::rename(&dir.0, dir.0.with_extension("moved")).unwrap();
        symlink(&outside.0, &dir.0).unwrap();
        assert!(open_relative_legacy(Some(&root), Path::new("nested/rows.csv"), false).is_ok());
        assert!(open_relative_legacy(Some(&root), Path::new("secret.csv"), false).is_err());
        std::fs::remove_file(&dir.0).unwrap();
        std::fs::rename(dir.0.with_extension("moved"), &dir.0).unwrap();
    }

    #[test]
    fn scans_projected_nullable_snappy_column_across_groups() {
        use parquet::basic::Compression;
        use parquet::data_type::Int32Type;
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::parser::parse_message_type;

        let dir = Fixture::new();
        let file = File::create(dir.0.join("groups.parquet")).unwrap();
        let schema = Arc::new(parse_message_type(
            "message schema { REQUIRED INT32 id; OPTIONAL INT32 score; }"
        ).unwrap());
        let properties = Arc::new(WriterProperties::builder()
            .set_compression(Compression::SNAPPY).build());
        let mut writer = SerializedFileWriter::new(file, schema, properties).unwrap();
        for first in [1, 3] {
            let mut group = writer.next_row_group().unwrap();
            let mut id = group.next_column().unwrap().unwrap();
            id.typed::<Int32Type>().write_batch(&[first, first + 1], None, None).unwrap();
            id.close().unwrap();
            let mut score = group.next_column().unwrap().unwrap();
            score.typed::<Int32Type>().write_batch(&[10 * first], Some(&[1, 0]), None).unwrap();
            score.close().unwrap();
            group.close().unwrap();
        }
        writer.close().unwrap();
        let desc = bind(dir.root(), "groups.parquet", Format::Parquet, Options::default()).unwrap();
        assert!(desc.columns[1].nullable);
        let mut scan = Scan::open(desc, dir.root(), &[1]).unwrap();
        for value in [Some(10), None, Some(30), None] {
            assert!(scan.next().unwrap());
            match (&scan.row[..], value) {
                ([Cell::Int(actual)], Some(expected)) => assert_eq!(*actual, expected),
                ([Cell::Null], None) => (),
                _ => panic!("unexpected projected column value"),
            }
        }
        assert!(!scan.next().unwrap());
    }
}
