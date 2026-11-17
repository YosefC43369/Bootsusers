@echo off
REM ============================================================================
REM  build.bat  —  alias for build_exe.bat
REM ----------------------------------------------------------------------------
REM  Double-click this (or run `build.bat`) to build dist\Bootsusers.exe.
REM  It simply forwards to build_exe.bat, which does the real work:
REM  create a venv, install deps + PyInstaller, run the spec, locate the .exe.
REM ============================================================================
call "%~dp0build_exe.bat" %*
