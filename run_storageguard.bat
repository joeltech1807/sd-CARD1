@echo off
title StorageGuard — SD Card Monitor
echo.
echo  ===================================================
echo   StorageGuard — SD Card Reliability Monitor
echo  ===================================================
echo.
echo  Auto-detecting SD card...
echo  Starting WebSocket backend on ws://localhost:8765
echo.
echo  Open storageguard_dashboard.html in your browser.
echo  Press Ctrl+C to stop.
echo.

cd /d "%~dp0"
python storage_backend.py

pause
