::  @echo off
setlocal
powershell -ExecutionPolicy Bypass -File "%~dp0release_one_click.ps1" %*
endlocal
pause                     