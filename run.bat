@echo off
cd /d "%~dp0"

if not exist "venv\Scripts\python.exe" (
  echo [ERROR] venv not found. Please run install.bat first.
  pause
  exit /b 1
)

echo Starting Arclap Timelapse Cleaner...
echo Browser will open at http://127.0.0.1:8000
echo Press Ctrl+C in this window to stop the server.
echo.

venv\Scripts\python.exe app.py
pause
