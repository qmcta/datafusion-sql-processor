@echo off
SETLOCAL EnableDelayedExpansion

:: Usage: process_jsonl.bat <input_dir> <base_sql_file>
:: Example: process_jsonl.bat data data\query.sql

SET INPUT_DIR=%1
SET BASE_SQL=%2

if "%INPUT_DIR%"=="" goto usage
if "%BASE_SQL%"=="" goto usage

SET OUTPUT_ROOT=output_csv
if not exist %OUTPUT_ROOT% mkdir %OUTPUT_ROOT%

for %%F in (%INPUT_DIR%\*.jsonl) do (
    set "jsonl_file=%%F"
    set "filename=%%~nxF"
    set "basename=%%~nF"
    echo Processing !jsonl_file! ...

    set "TEMP_SQL=temp_!basename!.sql"
    
    :: Use PowerShell to replace the LOCATION in the SQL file
    :: We use [System.IO.File]::WriteAllLines to ensure UTF-8 without BOM (standard Set-Content adds BOM)
    powershell -Command "$utf8 = New-Object System.Text.UTF8Encoding($false); $c = (Get-Content -LiteralPath '%BASE_SQL%') -replace 'LOCATION ''.*''', 'LOCATION ''!jsonl_file:\=/!'''; [System.IO.File]::WriteAllLines('!TEMP_SQL!', $c, $utf8)"

    :: Run the processor
    datafusion-sql-processor.exe !TEMP_SQL! %OUTPUT_ROOT%\!basename!

    :: Clean up
    del !TEMP_SQL!
)

echo Done. Results are in %OUTPUT_ROOT%
goto :eof

:usage
echo Usage: %0 ^<input_dir^> ^<base_sql_file^>
exit /b 1
