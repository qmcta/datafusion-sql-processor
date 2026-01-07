@echo off
SETLOCAL EnableDelayedExpansion

:: Usage: process_csv_gz.bat <input_dir> <base_sql_file> <format>
:: Example: process_csv_gz.bat data data\query.sql csv

SET INPUT_DIR=%1
SET BASE_SQL=%2
SET FORMAT=%3

if "%INPUT_DIR%"=="" goto usage
if "%BASE_SQL%"=="" goto usage
if "%FORMAT%"=="" SET FORMAT=csv

findstr /i "gzip" "%BASE_SQL%" >nul
if %errorlevel% neq 0 (
    echo WARNING: The SQL file '%BASE_SQL%' does not appear to set 'format.compression gzip'.
    echo          Processing .csv.gz files without this option will likely fail.
    echo          Please ensure your SQL file contains: OPTIONS (format.compression gzip^)
)

SET OUTPUT_ROOT=output_%FORMAT%
if not exist %OUTPUT_ROOT% mkdir %OUTPUT_ROOT%

for %%F in (%INPUT_DIR%\*.csv.gz) do (
    set "csv_file=%%F"
    set "filename=%%~nxF"
    set "stem=%%~nF"
    for %%A in ("!stem!") do set "basename=%%~nA"
    echo Processing !csv_file! ...

    set "TEMP_SQL=!basename!.sql"
    
    :: PowerShell: Replace LOCATION logic and write to temp SQL
    powershell -Command "$utf8 = New-Object System.Text.UTF8Encoding($false); $c = (Get-Content -Encoding UTF8 -LiteralPath '%BASE_SQL%') -replace 'LOCATION ''.*''', 'LOCATION ''!csv_file:\=/!'''; [System.IO.File]::WriteAllLines('!TEMP_SQL!', $c, $utf8)"

    :: Execute Processor (-f specifies format)
    datafusion-sql-processor.exe !TEMP_SQL! -f %FORMAT% -o %OUTPUT_ROOT%

    :: Remove temp SQL
    del !TEMP_SQL!
)

echo Done. Results are in %OUTPUT_ROOT%
goto :eof

:usage
echo Usage: %0 ^<input_dir^> ^<base_sql_file^> ^<format^>
exit /b 1