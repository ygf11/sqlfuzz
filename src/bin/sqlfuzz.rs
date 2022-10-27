// Copyright 2022 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow::csv::Writer;
use datafusion::arrow::array::{Array, Int32Array, Int8Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_plan::JoinType;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::{
    common::{DataFusionError, Result},
    dataframe::DataFrame,
    prelude::{
        AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions, SessionContext,
    },
};
use sqlfuzz::{generate_batch, plan_to_sql, FuzzConfig, SQLRelationGenerator, SQLTable};
use std::io::{BufRead, BufReader, Write};
use std::str::FromStr;
use std::{
    fs::File,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::Arc,
};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "sqlfuzz", about = "SQLFuzz: Query Engine Fuzz Testing")]
enum Config {
    /// Generate random queries
    Query(QueryGen),
    /// Generate random data files
    Data(DataGen),
    /// Run SQL queries and capture results
    Execute(ExecuteConfig),
    /// Compare two test runs
    Compare(CompareConfig),
    /// Run SQL queries and capture results
    ExecuteV2(ExecuteConfigV2),
}

#[derive(Debug, StructOpt)]
struct DataGen {
    #[structopt(short, long, required = true)]
    format: FileFormat,
    #[structopt(short, long, required = true)]
    num_files: usize,
    #[structopt(short, long, required = true)]
    path: PathBuf,
    #[structopt(short, long, default_value = "20")]
    row_count: usize,
}

#[derive(Debug, StructOpt)]
struct QueryGen {
    #[structopt(parse(from_os_str), long, required = true, multiple = true)]
    table: Vec<PathBuf>,
    #[structopt(short, long, required = false, multiple = true)]
    join_type: Vec<String>,
    #[structopt(short, long, default_value = "10")]
    count: usize,
    #[structopt(short, long, default_value = "5")]
    max_depth: usize,
    #[structopt(short, long)]
    verbose: bool,
}

#[derive(Debug, StructOpt)]
struct ExecuteConfig {
    #[structopt(parse(from_os_str), long, required = true, multiple = true)]
    table: Vec<PathBuf>,
    #[structopt(short, long, required = true)]
    sql: PathBuf,
    #[structopt(short, long)]
    verbose: bool,
}

/// execute with datafusion-cli
#[derive(Debug, StructOpt)]
struct ExecuteConfigV2 {
    #[structopt(short, long, required = true)]
    datafusion_cli: PathBuf,
    #[structopt(short, long, required = true)]
    create_table: PathBuf,
    #[structopt(short, long, required = true)]
    query: PathBuf,
}

#[derive(Debug, StructOpt)]
struct CompareConfig {
    #[structopt(required = true)]
    report1: PathBuf,
    #[structopt(required = true)]
    report2: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Config::from_args() {
        Config::Query(config) => query_gen(&config).await,
        Config::Data(config) => data_gen(&config).await,
        Config::Execute(config) => execute(&config).await,
        Config::Compare(config) => compare(&config).await,
        Config::ExecuteV2(config) => execute_v2(&config).await,
    }
}

async fn compare(config: &CompareConfig) -> Result<()> {
    let report1 = read_report(&config.report1)?;
    let report2 = read_report(&config.report2)?;
    assert_eq!(report1.results.len(), report2.results.len());
    for i in 0..report1.results.len() {
        let mut result1 = report1.results[i].rows.clone();
        result1.sort();
        let mut result2 = report2.results[i].rows.clone();
        result2.sort();
        println!("COMPARE");
        println!("{:?}", result1);
        println!("WITH");
        println!("{:?}", result2);
        if result1 == result2 {
            println!("VERDICT: SAME");
        } else {
            println!("VERDICT: DIFFERENT");
        }
        println!("-------------------------");
    }
    Ok(())
}

struct ResultSet {
    rows: Vec<Vec<String>>,
}

struct Report {
    results: Vec<ResultSet>,
}

fn read_report(filename: &PathBuf) -> Result<Report> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    let lines = reader.lines();
    let mut report = Report { results: vec![] };
    let mut rows = vec![];
    let mut in_result = true;
    for line in lines {
        let line = line?;
        if line.starts_with("-- BEGIN RESULT --") {
            in_result = true;
        } else if line.starts_with("-- END RESULT --") {
            in_result = false;
            report.results.push(ResultSet { rows });
            rows = vec![];
        } else {
            if in_result {
                rows.push(line.split('\t').map(|s| s.to_string()).collect());
            }
        }
    }
    Ok(report)
}

async fn execute(config: &ExecuteConfig) -> Result<()> {
    // register tables with context
    let (ctx, _) = create_datafusion_context(&config.table, config.verbose).await?;

    let file = File::open(&config.sql)?;
    let reader = BufReader::new(file);
    let lines = reader.lines();
    let mut sql = String::new();
    for line in lines {
        let line = line?;
        if line.starts_with("--") {
            println!("{}", line);
        } else {
            sql.push_str(&line);
            sql.push('\n');
            if sql.trim().ends_with(';') {
                println!("{}", sql);
                println!("-- BEGIN RESULT --");
                match ctx.sql(&sql).await {
                    Ok(df) => match df.collect().await {
                        Ok(batches) => {
                            print_results(batches);
                        }
                        Err(e) => {
                            println!("{:?}", e);
                        }
                    },
                    Err(e) => {
                        println!("{:?}", e);
                    }
                }
                println!("-- END RESULT --");
                sql = String::new();
            }
        }
    }
    Ok(())
}

async fn execute_v2(config: &ExecuteConfigV2) -> Result<()> {
    // let queries = if config.query.is_dir() {
    //     config
    //         .query
    //         .read_dir()?
    //         .filter(|entry| entry.as_ref().unwrap().path().is_file())
    //         .map(|entry| entry.unwrap().path())
    //         .collect::<Vec<_>>()
    // } else {
    //     vec![config.query.clone()]
    // };
    let datafusion_cli = &config.datafusion_cli;
    let create_table = &config.create_table;
    let query = &config.query;
    println!(
        "currenr_dir:{:?}",
        std::env::current_dir().unwrap().to_str()
    );
    let out = execute_single_query(datafusion_cli, create_table, query)?;
    println!("{}", String::from_utf8(out.stdout).unwrap());
    Ok(())
}

fn execute_single_query(
    datafusion_cli: &PathBuf,
    create_table: &PathBuf,
    query: &PathBuf,
) -> Result<Output> {
    println!("datafusion-cli:{}", datafusion_cli.to_string_lossy());
    println!(
        "create_table:{}",
        create_table.to_string_lossy().to_string()
    );
    println!("query:{}", query.to_string_lossy());

    let command = format!(
        "{} -f {} -f {} --format table -q",
        datafusion_cli.to_string_lossy(),
        create_table.to_string_lossy(),
        query.to_string_lossy()
    );

    let output = Command::new("sh").arg("-c").arg(command).output()?;

    Ok(output)
}

fn print_results(batches: Vec<RecordBatch>) {
    for batch in &batches {
        for i in 0..batch.num_rows() {
            let mut csv = String::new();
            for j in 0..batch.num_columns() {
                if j > 0 {
                    csv.push('\t');
                }
                let col = batch.column(j);
                match col.data_type() {
                    DataType::Int8 => {
                        let col = col.as_any().downcast_ref::<Int8Array>().unwrap();
                        if col.is_null(i) {
                            csv.push_str("null");
                        } else {
                            csv.push_str(&format!("{}", col.value(i)))
                        }
                    }
                    DataType::Int32 => {
                        let col = col.as_any().downcast_ref::<Int32Array>().unwrap();
                        if col.is_null(i) {
                            csv.push_str("null");
                        } else {
                            csv.push_str(&format!("{}", col.value(i)))
                        }
                    }
                    DataType::Utf8 => {
                        let col = col.as_any().downcast_ref::<StringArray>().unwrap();
                        if col.is_null(i) {
                            csv.push_str("null");
                        } else {
                            csv.push_str(&format!("{}", col.value(i)))
                        }
                    }
                    _ => unimplemented!(),
                }
            }
            println!("{}", csv);
        }
    }
}

async fn data_gen(config: &DataGen) -> Result<()> {
    //TODO randomize the schema and support more types
    // let schema = Arc::new(Schema::new(vec![
    //     Field::new("c0", DataType::Int8, true),
    //     Field::new("c1", DataType::Int8, true),
    //     Field::new("c2", DataType::Int32, true),
    //     Field::new("c3", DataType::Int32, true),
    //     Field::new("c4", DataType::Utf8, true),
    //     Field::new("c5", DataType::Utf8, true),
    // ]));

    // Using pg compatible data types
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Int16, true), // smallint
        Field::new("c1", DataType::Int16, true), // smallint
        Field::new("c2", DataType::Int32, true), // Int Integer
        Field::new("c3", DataType::Int32, true), // Int Integer
        Field::new("c4", DataType::Utf8, true),  // VARCHAR text
        Field::new("c5", DataType::Utf8, true),  // VARCHAR text
    ]));

    match config.format {
        FileFormat::Csv => generate_csv_file(config, schema),
        FileFormat::Parquet => generate_parquet_file(config, schema),
        _ => unimplemented!(),
    }
}

async fn query_gen(config: &QueryGen) -> Result<()> {
    if config.table.is_empty() {
        panic!("must provide tables to generate queries for");
    }

    let mut join_types = vec![];
    for jt in &config.join_type {
        let jt = match jt.as_str() {
            "anti" => JoinType::Anti,
            "semi" => JoinType::Semi,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "full" => JoinType::Full,
            "inner" => JoinType::Inner,
            other => panic!("invalid join type: {}", other),
        };
        join_types.push(jt);
    }

    // register tables with context
    let (ctx, sql_tables) = create_datafusion_context(&config.table, config.verbose).await?;

    // generate a random SQL query
    let num_queries = config.count;
    let mut rng = rand::thread_rng();

    let fuzz_config = FuzzConfig {
        join_types,
        max_depth: config.max_depth,
    };

    let mut gen = SQLRelationGenerator::new(&mut rng, sql_tables, fuzz_config);

    let mut generated = 0;

    while generated < num_queries {
        let plan = gen.generate_select()?;
        if config.verbose {
            let logical_plan = plan.to_logical_plan();
            println!("Input plan:\n{:?}", logical_plan);
        }
        let sql = plan_to_sql(&plan, 0)?;

        // see if we produced something valid or not (according to DataFusion's
        // SQL query planner)
        match ctx.create_logical_plan(&sql) {
            Ok(_plan) => {
                generated += 1;
                if generated < num_queries {
                    print!("-- SQL Query #{}:\n\n{};\n\n", generated, sql);
                } else {
                    print!("-- SQL Query #{}:\n\n{};", generated, sql);
                };
                // println!("-- SQL Query #{}:\n\n{};\n\n", generated, sql);
                // println!("Plan:\n\n{:?}", plan)
            }
            Err(e) if config.verbose => {
                println!("-- SQL:\n\n{};\n\n", sql);
                println!("-- SQL was not valid: {:?}\n\n", e)
            }
            _ => {
                // ignore
            }
        }
    }

    Ok(())
}

async fn create_datafusion_context(
    table: &[PathBuf],
    verbose: bool,
) -> Result<(SessionContext, Vec<SQLTable>)> {
    let ctx = SessionContext::new();
    let mut sql_tables: Vec<SQLTable> = vec![];
    for path in table {
        let table_name = path
            .file_stem()
            .unwrap()
            .to_str()
            .ok_or_else(|| DataFusionError::Internal("Invalid filename".to_string()))?;
        let table_name = sanitize_table_name(table_name);
        let filename = parse_filename(path)?;
        if verbose {
            println!("Registering table '{}' for {}", table_name, path.display());
        }
        let df = register_table(&ctx, &table_name, filename).await?;
        sql_tables.push(SQLTable::new(&table_name, df.schema().clone()));
    }
    Ok((ctx, sql_tables))
}

fn parse_filename(filename: &Path) -> Result<&str> {
    filename
        .to_str()
        .ok_or_else(|| DataFusionError::Internal("Invalid filename".to_string()))
}

#[derive(Debug)]
enum FileFormat {
    Avro,
    Csv,
    Json,
    Parquet,
}

impl FromStr for FileFormat {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "avro" => Ok(FileFormat::Avro),
            "csv" => Ok(FileFormat::Csv),
            "json" => Ok(FileFormat::Json),
            "parquet" => Ok(FileFormat::Parquet),
            _ => Err("".to_string()),
        }
    }
}

fn file_format(filename: &str) -> Result<FileFormat> {
    match filename.rfind('.') {
        Some(i) => match &filename[i + 1..] {
            "avro" => Ok(FileFormat::Avro),
            "csv" => Ok(FileFormat::Csv),
            "json" => Ok(FileFormat::Json),
            "parquet" => Ok(FileFormat::Parquet),
            other => Err(DataFusionError::Internal(format!(
                "unsupported file extension '{}'",
                other
            ))),
        },
        _ => Err(DataFusionError::Internal(format!(
            "Could not determine file extension for '{}'",
            filename
        ))),
    }
}

fn sanitize_table_name(name: &str) -> String {
    let mut str = String::new();
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            str.push(ch);
        } else {
            str.push('_')
        }
    }
    str
}

fn generate_csv_file(config: &DataGen, schema: SchemaRef) -> Result<()> {
    let mut create_tabel_file = File::create(config.path.join("create_table.sql"))?;
    let mut create_tabel_for_psql_file =
        File::create(config.path.join("create_table_for_psql.sql"))?;

    let mut rng = rand::thread_rng();

    for i in 0..config.num_files {
        let batch = generate_batch(&mut rng, &schema, config.row_count)?;
        let filename = format!("test{}.csv", i);
        let path = config.path.join(&filename);
        println!("Generating {:?}", path);
        let file = File::create(path.clone())?;
        let mut writer = Writer::new(file);
        writer.write(&batch)?;

        // Field::new("c0", DataType::Int16, true),// smallint
        // Field::new("c1", DataType::Int16, true),// smallint
        // Field::new("c2", DataType::Int32, true),// Int Integer
        // Field::new("c3", DataType::Int32, true),// Int Integer
        // Field::new("c4", DataType::Utf8, true),// VARCHAR text
        // Field::new("c5", DataType::Utf8, true),// VARCHAR text

        let create_table_sql = format!(
            "CREATE EXTERNAL TABLE test{} (c0 SMALLINT NULL, c1 SMALLINT NULL, c2 INT NULL, c3 INT NULL, c4 VARCHAR NULL, c5 VARCHAR NULL) STORED AS CSV WITH HEADER ROW
            LOCATION '{}';\n",
            i,
            path.to_string_lossy()
        );
        println!("sql:{}", create_table_sql);
        create_tabel_file.write(create_table_sql.as_bytes())?;

        let create_table_for_psql = format!(
            "CREATE EXTERNAL TABLE test{} (c0 SMALLINT NULL, c1 SMALLINT NULL, c2 INTEGER NULL, c3 Integer NULL, c4 TEXT NULL, c5 TEXT NULL);\n",
            i,
        );
        println!("sql_for_psql:{}", create_table_sql);
        create_tabel_for_psql_file.write(create_table_for_psql.as_bytes())?;
    }

    create_tabel_file.sync_all()?;
    create_tabel_for_psql_file.sync_all()?;

    Ok(())
}

fn generate_parquet_file(config: &DataGen, schema: SchemaRef) -> Result<()> {
    let mut create_tabel_file = File::create(config.path.join("create_table.sql"))?;
    let mut rng = rand::thread_rng();
    let writer_properties = WriterProperties::builder().build();

    for i in 0..config.num_files {
        let batch = generate_batch(&mut rng, &schema, config.row_count)?;
        let filename = format!("test{}.parquet", i);
        let path = config.path.join(&filename);
        println!("Generating {:?}", path);
        let file = File::create(path.clone())?;
        let mut writer =
            ArrowWriter::try_new(file, schema.clone(), Some(writer_properties.clone()))?;
        writer.write(&batch)?;
        writer.close()?;

        let create_table_sql = format!(
            "CREATE EXTERNAL TABLE test{} STORED AS PARQUET LOCATION '{}';\n",
            i,
            path.to_string_lossy()
        );
        println!("sql:{}", create_table_sql);
        create_tabel_file.write(create_table_sql.as_bytes())?;
    }

    create_tabel_file.sync_all()?;

    Ok(())
}

async fn register_table(
    ctx: &SessionContext,
    table_name: &str,
    filename: &str,
) -> Result<Arc<DataFrame>> {
    match file_format(filename)? {
        FileFormat::Avro => {
            ctx.register_avro(table_name, filename, AvroReadOptions::default())
                .await?
        }
        FileFormat::Csv => {
            ctx.register_csv(table_name, filename, CsvReadOptions::default())
                .await?
        }
        FileFormat::Json => {
            ctx.register_json(table_name, filename, NdJsonReadOptions::default())
                .await?
        }
        FileFormat::Parquet => {
            ctx.register_parquet(table_name, filename, ParquetReadOptions::default())
                .await?
        }
    }
    ctx.table(table_name)
}
