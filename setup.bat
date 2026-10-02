@echo off
rem Delegate to the sibling PowerShell bootstrap; preserve its exit status.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup.ps1"
exit /b %errorlevel%
