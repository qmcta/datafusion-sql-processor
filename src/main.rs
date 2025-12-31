use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::dataframe::DataFrameWriteOptions;
use std::env;
use std::fs;

#[tokio::main]
async fn main() -> Result<()> {
    // コマンドライン引数からSQLファイルのパスと出力ベースディレクトリを取得
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <sql_file_path> [output_base_dir]", args[0]);
        std::process::exit(1);
    }
    let sql_file_path = &args[1];
    let output_base_dir = args.get(2).map(|s| s.as_str()).unwrap_or("data");

    // カレントディレクトリを表示（デバッグ用）
    if let Ok(cwd) = env::current_dir() {
        println!("Current working directory: {}", cwd.display());
    }

    // SQLファイルの内容を読み込む
    let query = fs::read_to_string(sql_file_path).map_err(|e| {
        let abs_path = fs::canonicalize(sql_file_path).unwrap_or_else(|_| std::path::PathBuf::from(sql_file_path));
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to read SQL file at {}: {}. (Current directory: {:?})",
            abs_path.display(),
            e,
            env::current_dir().unwrap_or_default()
        ))
    })?;

    // SessionContextを初期化
    let ctx = SessionContext::new();

    // SQLファイルの内容を分割して実行（DDL + クエリ対応）
    // DataFusionのsql()は1ステートメントずつのため、セミコロンで分割します。
    let statements: Vec<&str> = query
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    if statements.is_empty() {
        eprintln!("No SQL statements found in {}", sql_file_path);
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

    // CSVとして出力
    let output_path = format!("{}/output", output_base_dir);
    df.write_csv(&output_path, DataFrameWriteOptions::default(), None).await?;

    println!("Successfully executed all statements from {} and exported final result to {} (directory)", sql_file_path, output_path);

    Ok(())
}
