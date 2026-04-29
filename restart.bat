@echo off
REM Arclap Vision Suite — one-click restart.
REM Double-click this file in File Explorer when you've changed code or
REM the server is acting up. It kills any running Python on port 8000
REM (the Vision Suite) and starts a fresh server in this same window.

cd /d "%~dp0"

echo ============================================================
echo   Arclap Vision Suite -- restarting server
echo ============================================================
echo.

REM ---- Kill anything listening on port 8000 ------------------------------
echo Stopping any running server on port 8000...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8000 ^| findstr LISTENING') do (
  taskkill /F /PID %%a >nul 2>&1
  echo   killed PID %%a
)
echo.

REM ---- Tiny pause so the port is released --------------------------------
ping -n 2 127.0.0.1 >nul

REM ---- Start fresh server ------------------------------------------------
echo Starting fresh server...
echo.
call run.bat
