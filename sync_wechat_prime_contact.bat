@echo off
REM WeChat sync -- Task Scheduler entry for the LOCAL prime-contact target.
REM
REM Reuses sync_wechat.bat but points it at .env.prime-contact, so the production
REM .env (aizee-crm) is left untouched. Point WECHAT_SYNC_ENV_FILE at whatever
REM config this machine should upload to.
REM
REM Create the 10-minute job with:
REM   schtasks /create /tn "WeChatSync" /tr "<this file>" /sc minute /mo 10 /f
REM ASCII only on purpose: UTF-8 comments break under the GBK console codepage.

setlocal
cd /d "%~dp0"
set WECHAT_SYNC_ENV_FILE=.env.prime-contact
call "%~dp0sync_wechat.bat" %*
exit /b %ERRORLEVEL%
