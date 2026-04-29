@echo off
cd /d "%~dp0"

if not exist "venv\Scripts\python.exe" (
  echo [ERROR] venv not found. Please run install.bat first.
  pause
  exit /b 1
)

echo Starting Arclap Vision Suite...
echo Browser will open at http://127.0.0.1:8000
echo Press Ctrl+C in this window to stop the server.
echo.

REM Restart-loop: if app.py exits with code 42 (the in-app Restart button),
REM start it again. Any other exit code (Ctrl+C = 0/1, crash, etc.) drops out
REM of the loop and the window stays open so you can read the error.
:start
venv\Scripts\python.exe app.py
if %ERRORLEVEL% EQU 42 (
  echo.
  echo [run.bat] Vision Suite requested restart -- relaunching...
  echo.
  goto start
)
echo.
echo [run.bat] Vision Suite exited with code %ERRORLEVEL%.
pause
