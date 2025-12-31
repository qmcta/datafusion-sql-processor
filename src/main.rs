use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::dataframe::DataFrameWriteOptions;
use clap::{Parser, ValueEnum};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the SQL file
    sql_file_path: String,

    /// Output format
    #[arg(short, long, value_enum, default_value_t = OutputFormat::Csv)]
    format: OutputFormat,

    /// Output directory. If not specified, "<input_folder>/output" is used.
    #[arg(short, long)]
    output_dir: Option<PathBuf>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum OutputFormat {
    Csv,
    Parquet,
    Jsonl,
}

#[tokio::main]
async fn main() -> Result<()> {
    // コマンドライン引数をパース
    let args = Args::parse();

    let sql_path = Path::new(&args.sql_file_path);
    if !sql_path.exists() {
        eprintln!("SQL file not found: {}", args.sql_file_path);
        std::process::exit(1);
    }

    // 出力ディレクトリの決定
    let output_dir = match args.output_dir {
        Some(dir) => dir,
        None => {
            let parent = sql_path.parent().unwrap_or_else(|| Path::new("."));
            parent.join("output")
        }
    };

    // 出力ディレクトリの作成
    if !output_dir.exists() {
        fs::create_dir_all(&output_dir).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to create output directory {}: {}",
                output_dir.display(),
                e
            ))
        })?;
    }

    // 出力ファイル名の決定 (stem + extension)
    let stem = sql_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");
    
    let extension = match args.format {
        OutputFormat::Csv => "csv",
        OutputFormat::Parquet => "parquet",
        OutputFormat::Jsonl => "jsonl",
    };
    
    let output_file_path = output_dir.join(format!("{}.{}", stem, extension));

    // SQLファイルの内容を読み込む
    let query = fs::read_to_string(&args.sql_file_path).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to read SQL file at {}: {}",
            args.sql_file_path,
            e
        ))
    })?;

    // SessionContextを初期化
    let ctx = SessionContext::new();

    // SQLファイルの内容を分割して実行（DDL + クエリ対応）
    let statements: Vec<&str> = query
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    if statements.is_empty() {
        eprintln!("No SQL statements found in {}", args.sql_file_path);
        std::process::exit(1);
    }

    let mut final_df = None;

    for (i, stmt) in statements.iter().enumerate() {
        println!("Executing statement {}/{}...", i + 1, statements.len());
        let df = ctx.sql(stmt).await.map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("Error executing statement: {}\nSQL: {}", e, stmt))
        })?;
        
        // 最後のステートメントの結果を保持する
        if i == statements.len() - 1 {
            final_df = Some(df);
        }
    }

    let df = final_df.expect("At least one statement should have executed");

    // 結果を表示（デバッグ用）
    df.clone().show().await?;

    // 指定されたフォーマットで出力
    println!("Writing output to {}...", output_file_path.display());
    
    match args.format {
        OutputFormat::Csv => {
            df.write_csv(
                output_file_path.to_str().unwrap(),
                DataFrameWriteOptions::default(),
                None,
            ).await?;
        }
        OutputFormat::Parquet => {
            df.write_parquet(
                output_file_path.to_str().unwrap(),
                DataFrameWriteOptions::default(),
                None,
            ).await?;
        }
        OutputFormat::Jsonl => {
            df.write_json(
                output_file_path.to_str().unwrap(),
                DataFrameWriteOptions::default(),
                None,
            ).await?;
        }
    }

    println!("Successfully processed {} and saved results to {}", args.sql_file_path, output_file_path.display());

    Ok(())
}
