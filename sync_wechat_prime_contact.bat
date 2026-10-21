@echo off
REM WeChat sync -- Task Scheduler entry for the REMOTE archive service.
REM
REM Uses the default .env, whose WECHAT_SYNC_BASE is the domain form
REM https://primeapi.aizee.cc/api/wechat (archive and gallery are the same host).
REM Local loopback testing is still possible by hand:
REM   set WECHAT_SYNC_ENV_FILE=.env.prime-contact && sync_wechat.bat incremental
REM
REM The registered 10-minute job "WeChatSync" still calls THIS file, so the file
REM name is kept on purpose; only the target config moved from loopback to domain.
REM
REM Create the 10-minute job with:
REM   schtasks /create /tn "WeChatSync" /tr "<this file>" /sc minute /mo 10 /f
REM ASCII only on purpose: UTF-8 comments break under the GBK console codepage.

setlocal
cd /d "%~dp0"
call "%~dp0sync_wechat.bat" %*
exit /b %ERRORLEVEL%
