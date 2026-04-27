@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo ============================================================
echo   Arclap Timelapse Cleaner -- One-Click Installer (Windows)
echo ============================================================
echo.

REM ---- Step 1: Python ------------------------------------------------------
where python >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Python not found on PATH.
  echo.
  echo Install Python 3.10 or later, then re-run install.bat.
  echo Easiest:  winget install -e --id Python.Python.3.12
  echo Or download from:  https://python.org/downloads/
  echo.
  echo IMPORTANT: tick "Add Python to PATH" during install,
  echo then close this window and re-run install.bat.
  pause
  exit /b 1
)

REM ---- Step 2: ffmpeg ------------------------------------------------------
where ffmpeg >nul 2>&1
if errorlevel 1 (
  echo ffmpeg not found on PATH. Attempting install via winget...
  where winget >nul 2>&1
  if errorlevel 1 (
    echo [ERROR] winget is not available either.
    echo.
    echo Install ffmpeg manually from https://www.gyan.dev/ffmpeg/builds/
    echo and add it to your PATH, then re-run install.bat.
    pause
    exit /b 1
  )
  winget install -e --id Gyan.FFmpeg --silent --accept-package-agreements --accept-source-agreements
  REM PATH may need a refresh — re-check
  where ffmpeg >nul 2>&1
  if errorlevel 1 (
    echo.
    echo ffmpeg installed. Windows hasn't refreshed PATH for this session yet.
    echo Please CLOSE this window and re-run install.bat to continue.
    pause
    exit /b 0
  )
)

REM ---- Step 3: Run Python setup --------------------------------------------
python scripts\setup.py
if errorlevel 1 (
  echo.
  echo [FAILED] Setup did not complete successfully. Scroll up for details.
  pause
  exit /b 1
)

echo.
echo ============================================================
echo   Setup complete!  Double-click  run.bat  to start the app.
echo ============================================================
pause
