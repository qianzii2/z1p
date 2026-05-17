use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::RecordBatchReader;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl, ListingOptions};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rustyline::completion::Pair;
use rustyline::config::Config;
use rustyline::error::ReadlineError;
use rustyline::{validate::Validator, Editor};

mod commands;
mod output;

use commands::{Command, ExportOptions, OpenOptions, UseOptions};
use output::TablePrinter;

// ─── Tab 补全器 ───
struct SqlCompleter {
    session: Arc<Mutex<Option<Session>>>,
}

static SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
    "DELETE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "ON",
    "ORDER BY", "GROUP BY", "HAVING", "LIMIT", "OFFSET", "AS", "DISTINCT",
    "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN", "IS", "NULL", "TRUE", "FALSE",
    "UNION", "ALL", "EXCEPT", "INTERSECT", "CASE", "WHEN", "THEN", "ELSE", "END",
    "WITH", "OVER", "PARTITION BY", "WINDOW",
    "OPEN", "USE", "CLOSE", "CLOSE USE", "LIST", "SCHEMA", "EXPORT", "EXIT", "QUIT",
    "WITH (",
];

impl SqlCompleter {
    fn word_start(line: &str, pos: usize) -> usize {
        line[..pos]
            .rfind(|c: char| !c.is_alphanumeric() && c != '_' && c != '.')
            .map(|i| i + 1)
            .unwrap_or(0)
    }

    fn complete_word(&self, line: &str, pos: usize) -> (usize, Vec<Pair>) {
        let session = match self.session.lock() {
            Ok(s) => s,
            Err(_) => return (pos, vec![]),
        };
        let session = match session.as_ref() {
            Some(s) => s,
            None => return (pos, vec![]),
        };

        let start = Self::word_start(line, pos);
        let prefix = &line[start..pos];

        let mut candidates = vec![];

        for kw in SQL_KEYWORDS {
            if kw.to_uppercase().starts_with(&prefix.to_uppercase()) {
                candidates.push(Pair {
                    display: kw.to_string(),
                    replacement: kw.to_string(),
                });
            }
        }

        for name in session.tables.keys() {
            if name.starts_with(prefix) {
                candidates.push(Pair {
                    display: name.clone(),
                    replacement: name.clone(),
                });
            }
        }

        if let Some(use_sess) = &session.use_table {
            if let Some(t) = session.tables.get(&use_sess.table_name) {
                for field in t.schema.fields() {
                    let col_ref = format!("{}.{}", use_sess.table_name, field.name());
                    if col_ref.starts_with(prefix) {
                        candidates.push(Pair {
                            display: col_ref.clone(),
                            replacement: col_ref,
                        });
                    }
                }
            }
        }

        (start, candidates)
    }
}

impl rustyline::completion::Completer for SqlCompleter {
    type Candidate = Pair;

    fn complete(&self, line: &str, pos: usize, _ctx: &rustyline::Context<'_>) -> Result<(usize, Vec<Pair>), ReadlineError> {
        Ok(self.complete_word(line, pos))
    }
}

impl rustyline::highlight::Highlighter for SqlCompleter {}

impl rustyline::hint::Hinter for SqlCompleter {
    type Hint = String;
}

impl Validator for SqlCompleter {}

impl rustyline::Helper for SqlCompleter {}

// ─── Session ───

struct Session {
    ctx: SessionContext,
    tables: HashMap<String, RegisteredTable>,
    last_result: Option<Vec<RecordBatch>>,
    use_table: Option<UseSession>,
}

struct UseSession {
    table_name: String,
    path: String,
}

struct RegisteredTable {
    #[allow(dead_code)]
    path: String,
    schema: Arc<Schema>,
    estimated_rows: Option<usize>,
}

impl Session {
    fn new() -> Self {
        let ctx = SessionContext::new();
        Self {
            ctx,
            tables: HashMap::new(),
            last_result: None,
            use_table: None,
        }
    }

    async fn run(self) -> Result<()> {
        let session_arc: Arc<Mutex<Option<Session>>> = Arc::new(Mutex::new(Some(self)));

        let _completer = SqlCompleter {
            session: session_arc.clone(),
        };

        let mut rl: Editor<SqlCompleter, _> = Editor::with_config(Config::default())?;

        let mut input_buf = String::new();

        loop {
            let prompt = if input_buf.is_empty() { "> " } else { "  " };
            let line = match rl.readline(prompt) {
                Ok(l) => l,
                Err(rustyline::error::ReadlineError::Eof) => break,
                Err(rustyline::error::ReadlineError::Interrupted) => {
                    input_buf.clear();
                    println!();
                    continue;
                }
                Err(e) => {
                    eprintln!("readline error: {}", e);
                    break;
                }
            };

            input_buf.push_str(&line);

            if !input_buf.trim_end().ends_with(';') {
                continue;
            }

            let full_input = input_buf.trim().trim_end_matches(';').to_string();
            input_buf.clear();

            if full_input.trim().is_empty() {
                continue;
            }

            let _ = rl.add_history_entry(full_input.trim());

            let mut guard = session_arc.lock().unwrap();
            let session = match guard.as_mut() {
                Some(s) => s,
                None => break,
            };

            match Command::parse(&full_input) {
                Ok(Command::Open(opts)) => {
                    if let Err(e) = session.do_open(opts).await {
                        eprintln!("error: {}", e);
                    }
                }
                Ok(Command::Use(opts)) => {
                    if let Err(e) = session.do_use(opts).await {
                        eprintln!("error: {}", e);
                    }
                }
                Ok(Command::CloseUse) => {
                    if let Err(e) = session.close_use_if_needed().await {
                        eprintln!("error: {}", e);
                    }
                }
                Ok(Command::Close(name)) => {
                    session.do_close(&name);
                }
                Ok(Command::List) => {
                    session.do_list();
                }
                Ok(Command::Schema(name)) => {
                    session.do_schema(&name);
                }
                Ok(Command::Sql(sql)) => {
                    if let Err(e) = session.execute_sql(&sql).await {
                        eprintln!("error: {}", e);
                    }
                }
                Ok(Command::Exit) => {
                    if let Err(e) = session.close_use_if_needed().await {
                        eprintln!("warning: failed to save use table: {}", e);
                    }
                    println!("Goodbye!");
                    break;
                }
                Ok(Command::Export(opts)) => {
                    if let Err(e) = session.do_export(&opts) {
                        eprintln!("error: {}", e);
                    }
                }
                Err(msg) => {
                    eprintln!("error: {}", msg);
                }
            }
        }

        Ok(())
    }

    fn estimate_rows_from_parquet(path: &str) -> Option<usize> {
        use parquet::file::reader::FileReader;
        let file = std::fs::File::open(path).ok()?;
        let reader = parquet::file::reader::SerializedFileReader::new(file).ok()?;
        let total_rows: usize = (0..reader.metadata().num_row_groups())
            .map(|i| reader.metadata().row_group(i).num_rows() as usize)
            .sum();
        Some(total_rows)
    }

    async fn do_open(&mut self, opts: OpenOptions) -> Result<()> {
        let path = opts.path();
        let table_name = opts.table_name();

        if self.tables.contains_key(table_name) {
            anyhow::bail!("table '{table_name}' is already open");
        }

        let path_obj = Path::new(path);
        if !path_obj.exists() {
            anyhow::bail!("file not found: '{path}'");
        }

        let url = ListingTableUrl::parse(path)?;
        let state = self.ctx.state();

        let listing_options = ListingOptions::new(Arc::new(
            datafusion::datasource::file_format::parquet::ParquetFormat::default(),
        ));

        let full_schema = listing_options
            .infer_schema(&state, &url)
            .await
            .context("failed to infer schema from parquet file")?;

        let config = ListingTableConfig::new(url)
            .with_listing_options(listing_options)
            .with_schema(full_schema.clone());
        let listing = ListingTable::try_new(config)?;

        let schema = listing.schema();
        self.ctx
            .register_table(datafusion::sql::TableReference::bare(table_name.as_ref()), Arc::new(listing))?;

        let estimated_rows = Self::estimate_rows_from_parquet(path);

        self.tables.insert(
            table_name.to_string(),
            RegisteredTable {
                path: path.to_string(),
                schema,
                estimated_rows,
            },
        );

        println!("ok: opened '{table_name}' from '{path}'");
        Ok(())
    }

    fn do_close(&mut self, name: &str) {
        match self.tables.remove(name) {
            Some(_) => {
                let state = self.ctx.state();
                if let Some(catalog) = state.catalog_list().catalog("datafusion") {
                    if let Some(schema) = catalog.schema("public") {
                        let _ = schema.deregister_table(name.into());
                    }
                }
                println!("ok: closed '{name}'");
            }
            None => {
                eprintln!("error: table '{name}' is not open");
            }
        }
    }

    async fn do_use(&mut self, opts: UseOptions) -> Result<()> {
        let path = opts.path().to_string();
        let table_name = opts.table_name().to_string();

        if self.use_table.is_some() {
            self.close_use_if_needed().await?;
        }

        let path_obj = Path::new(&path);
        if !path_obj.exists() {
            anyhow::bail!("file not found: '{path}'");
        }

        let parquet_file = std::fs::File::open(&path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(parquet_file)?.build()?;
        let schema_ref = reader.schema();
        let schema = Schema::clone(&schema_ref);
        let schema_for_insert = schema.clone();
        let batches: Vec<RecordBatch> = reader
            .collect::<Result<Vec<RecordBatch>, datafusion::arrow::error::ArrowError>>()
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        let provider = datafusion::datasource::memory::MemTable::try_new(
            schema_for_insert.into(),
            vec![batches],
        )?;
        self.ctx
            .register_table(
                datafusion::sql::TableReference::bare(table_name.as_str()),
                Arc::new(provider),
            )
            .context("failed to register table")?;

        let estimated_rows = Self::estimate_rows_from_parquet(&path);
        self.tables.insert(
            table_name.clone(),
            RegisteredTable {
                path: path.clone(),
                schema: schema.into(),
                estimated_rows,
            },
        );

        self.use_table = Some(UseSession {
            table_name: table_name.clone(),
            path,
        });

        println!("ok: using '{table_name}' (read-write)");
        Ok(())
    }

    async fn close_use_if_needed(&mut self) -> Result<()> {
        let Some(use_session) = self.use_table.take() else {
            return Ok(());
        };

        let batches = self
            .ctx
            .sql(&format!("SELECT * FROM {}", use_session.table_name))
            .await?
            .collect()
            .await?;

        if batches.is_empty() {
            let _ = self.ctx.deregister_table(use_session.table_name.as_str());
            return Ok(());
        }

        let file = std::fs::File::create(&use_session.path)?;
        let schema = batches[0].schema();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.close()?;

        let _ = self.ctx.deregister_table(use_session.table_name.as_str());
        println!(
            "ok: saved '{}' ({} rows) to '{}'",
            use_session.table_name,
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            use_session.path
        );
        Ok(())
    }

    fn do_list(&self) {
        if self.tables.is_empty() {
            println!("(no tables open)");
            return;
        }

        let headers = ["name", "rows", "columns"];
        let rows: Vec<Vec<String>> = self
            .tables
            .iter()
            .map(|(name, t)| {
                let rows_str = t.estimated_rows.map_or("?".to_string(), |n| {
                    if n >= 1_000_000 {
                        format!("{:.1}M", n as f64 / 1_000_000.0)
                    } else if n >= 1_000 {
                        format!("{:.1}K", n as f64 / 1_000.0)
                    } else {
                        n.to_string()
                    }
                });

                let cols_str = t
                    .schema
                    .fields()
                    .iter()
                    .map(|f| format!("{}:{}", f.name(), f.data_type()))
                    .collect::<Vec<_>>()
                    .join(", ");

                vec![name.clone(), rows_str, cols_str]
            })
            .collect();

        let mut printer = TablePrinter::new();
        printer.print_table(&headers, &rows);
    }

    fn do_schema(&self, name: &str) {
        match self.tables.get(name) {
            Some(t) => {
                for field in t.schema.fields() {
                    println!("# {}: {}", field.name(), field.data_type());
                }
            }
            None => {
                eprintln!("error: table '{name}' is not open");
            }
        }
    }

    fn do_export(&self, opts: &ExportOptions) -> Result<()> {
        let results = self
            .last_result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no result to export"))?;

        if results.is_empty() {
            anyhow::bail!("no result to export");
        }

        let mut path = opts.path.clone();
        if !path.ends_with(".parquet") {
            path.push_str(".parquet");
        }

        let compression = match opts.compression.as_str() {
            "gzip" | "gz" => parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::default()),
            "zstd" | "zst" => parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default()),
            "lz4" => parquet::basic::Compression::LZ4,
            "none" | "uncompressed" => parquet::basic::Compression::UNCOMPRESSED,
            _ => parquet::basic::Compression::SNAPPY,
        };

        let file = std::fs::File::create(&path)?;
        let schema = results[0].schema();
        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

        for batch in results {
            writer.write(batch)?;
        }

        writer.close()?;

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        println!("ok: exported {} rows to '{}'", total_rows, path);
        Ok(())
    }

    async fn execute_sql(&mut self, sql: &str) -> Result<()> {
        let df = self.ctx.sql(sql).await?;
        let results = df.collect().await?.to_vec();

        let mut printer = TablePrinter::new();
        printer.print_batches(&results);

        self.last_result = Some(results);

        Ok(())
    }
}

impl Command {
    fn parse(s: &str) -> Result<Self, String> {
        let trimmed = s.trim();
        let upper = trimmed.to_uppercase();

        if upper == "LIST" {
            return Ok(Command::List);
        }
        if upper == "EXIT" || upper == "QUIT" {
            return Ok(Command::Exit);
        }
        if upper.starts_with("CLOSE ") {
            let name = trimmed[6..].trim().to_string();
            return Ok(Command::Close(name));
        }
        if upper.starts_with("SCHEMA ") {
            let name = trimmed[7..].trim().to_string();
            return Ok(Command::Schema(name));
        }
        if upper.starts_with("OPEN ") {
            return Self::parse_open(trimmed);
        }
        if upper.starts_with("USE ") {
            return Self::parse_use(trimmed);
        }
        if upper == "CLOSE USE" {
            return Ok(Command::CloseUse);
        }
        if upper.starts_with("EXPORT ") {
            return Self::parse_export(trimmed);
        }
        Ok(Command::Sql(s.to_string()))
    }

    fn parse_open(s: &str) -> Result<Self, String> {
        let body = &s[5..];
        let parts: Vec<&str> = body.split(" AS ").collect();
        if parts.len() != 2 {
            return Err("OPEN syntax: OPEN 'file.parquet' AS t".to_string());
        }

        let path = parts[0].trim().trim_matches(|c| c == '\'' || c == '"').to_string();
        let table_name = parts[1].trim().to_string();

        Ok(Command::Open(OpenOptions {
            path,
            table_name,
        }))
    }

    fn parse_use(s: &str) -> Result<Self, String> {
        let body = &s[4..];
        let parts: Vec<&str> = body.split(" AS ").collect();
        if parts.len() == 1 {
            let path = parts[0].trim().trim_matches(|c| c == '\'' || c == '"').to_string();
            let table_name = std::path::Path::new(&path)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("t")
                .to_string();
            Ok(Command::Use(UseOptions { path, table_name }))
        } else if parts.len() == 2 {
            let path = parts[0].trim().trim_matches(|c| c == '\'' || c == '"').to_string();
            let table_name = parts[1].trim().to_string();
            Ok(Command::Use(UseOptions { path, table_name }))
        } else {
            Err("USE syntax: USE 'file.parquet' [AS t]".to_string())
        }
    }

    fn parse_export(s: &str) -> Result<Self, String> {
        let body = &s[7..];
        let with_idx = body.to_uppercase().find(" WITH (");

        let (path, options_str) = match with_idx {
            Some(idx) => (&body[..idx], Some(&body[idx + 1..])),
            None => (body, None),
        };

        let path = path.trim().trim_matches(|c| c == '\'' || c == '"').to_string();
        let mut compression = "snappy".to_string();

        if let Some(opts) = options_str {
            let inner = opts
                .trim()
                .strip_prefix("WITH (")
                .and_then(|s| s.strip_suffix(')'))
                .ok_or_else(|| "malformed WITH clause".to_string())?;

            for part in inner.split(',') {
                let part = part.trim();
                if part.starts_with("compression=") {
                    compression = part["compression=".len()..]
                        .trim_matches(|c| c == '\'' || c == '"')
                        .to_lowercase();
                }
            }
        }

        Ok(Command::Export(ExportOptions { path, compression }))
    }
}

impl OpenOptions {
    fn path(&self) -> &str {
        &self.path
    }
    fn table_name(&self) -> &str {
        &self.table_name
    }
}

impl UseOptions {
    fn path(&self) -> &str {
        &self.path
    }
    fn table_name(&self) -> &str {
        &self.table_name
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let mut session = Session::new();

    if args.len() == 2 && args[1].ends_with(".parquet") {
        let path = &args[1];
        let table_name = std::path::Path::new(path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("t")
            .to_string();
        session.do_use(UseOptions { path: path.clone(), table_name }).await?;
    } else if args.get(1).map(|s| s.as_str()) == Some("--register") {
        register_file_association()?;
        return Ok(());
    }

    Session::run(session).await
}

fn register_file_association() -> Result<()> {
    let exe_path = std::env::current_exe()?;
    let exe_str = exe_path.to_string_lossy();

    let content = format!(
        r#"Windows Registry Editor Version 5.00

[HKEY_CLASSES_ROOT\.parquet]
@="z1p.parquet"

[HKEY_CLASSES_ROOT\z1p.parquet\DefaultIcon]
@="{exe_str},0"

[HKEY_CLASSES_ROOT\z1p.parquet\shell\open\command]
@="\"{exe_str}\" \"%1\""
"#);

    let reg_path = std::env::temp_dir().join("z1p_register.reg");
    std::fs::write(&reg_path, &content)?;
    println!("Registry file written to: {}", reg_path.display());
    println!("Double-click the .reg file to register, then restart Explorer.");
    Ok(())
}
