@echo off
SETLOCAL EnableDelayedExpansion

:: Usage: process_csv.bat <input_dir> <base_sql_file> <format>
:: Example: process_csv.bat data data\query.sql csv

SET INPUT_DIR=%1
SET BASE_SQL=%2
SET FORMAT=%3

if "%INPUT_DIR%"=="" goto usage
if "%BASE_SQL%"=="" goto usage
if "%FORMAT%"=="" SET FORMAT=csv

SET OUTPUT_ROOT=output_%FORMAT%
if not exist %OUTPUT_ROOT% mkdir %OUTPUT_ROOT%

for %%F in (%INPUT_DIR%\*.csv) do (
    set "csv_file=%%F"
    set "filename=%%~nxF"
    set "basename=%%~nF"
    echo Processing !csv_file! ...

    set "TEMP_SQL=temp_!basename!.sql"
    
    :: PowerShellを使用してSQL内のLOCATIONを置換
    powershell -Command "$utf8 = New-Object System.Text.UTF8Encoding($false); $c = (Get-Content -LiteralPath '%BASE_SQL%') -replace 'LOCATION ''.*''', 'LOCATION ''!csv_file:\=/!'''; [System.IO.File]::WriteAllLines('!TEMP_SQL!', $c, $utf8)"

    :: プロセッサの実行 (-f でフォーマットを指定)
    datafusion-sql-processor.exe !TEMP_SQL! -f %FORMAT% -o %OUTPUT_ROOT%

    :: 一時ファイルの削除
    del !TEMP_SQL!
)

echo Done. Results are in %OUTPUT_ROOT%
goto :eof

:usage
echo Usage: %0 ^<input_dir^> ^<base_sql_file^> ^<format^>
exit /b 1