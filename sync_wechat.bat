@echo off
REM WeChat data sync -- entry point for Windows Task Scheduler.
REM   sync_wechat.bat            incremental sync (use this for the job)
REM   sync_wechat.bat init       first-time full upload (run once)
REM Config comes from the .env file beside this script (copy .env.example first).
REM Real environment variables take precedence over .env.
REM ASCII only on purpose: UTF-8 comments break under the GBK console codepage.

setlocal
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8

if not exist .env echo [WARN] .env not found, using defaults; copy .env.example to .env

set CMD=%1
if "%CMD%"=="" set CMD=incremental

if not exist logs mkdir logs
set LOG=logs\sync_%CMD%.log

REM Roll the log at 20MB so it cannot grow without bound.
for %%F in ("%LOG%") do if %%~zF GTR 20971520 move /y "%LOG%" "%LOG%.1" >nul 2>&1

py sync_to_server.py %CMD% >> "%LOG%" 2>&1
set RC=%ERRORLEVEL%
if not "%RC%"=="0" echo [%DATE% %TIME%] exit=%RC% >> "%LOG%"
exit /b %RC%
