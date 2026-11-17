@echo off
REM ============================================================================
REM  build_exe.bat  —  one-click Windows build for Bootsusers.exe
REM ----------------------------------------------------------------------------
REM  Run this ON Windows 11 (double-click, or `build_exe.bat` in a terminal).
REM  It creates an isolated venv, installs the runtime deps + PyInstaller,
REM  and produces  dist\Bootsusers.exe  — one self-contained file, no console.
REM
REM  Requirements: Python 3.9+ on PATH (python.org build; it bundles tkinter,
REM  which customtkinter needs).
REM ============================================================================
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo.
echo  === Bootsusers .exe builder ===
echo.

REM --- locate Python ---------------------------------------------------------
where py >nul 2>nul
if %errorlevel%==0 (set "PYLAUNCH=py -3") else (set "PYLAUNCH=python")

%PYLAUNCH% --version >nul 2>nul
if errorlevel 1 (
    echo [x] Python not found on PATH. Install Python 3.9+ from python.org
    echo     and tick "Add python.exe to PATH".
    pause
    exit /b 1
)

REM --- build venv ------------------------------------------------------------
if not exist ".build_venv\Scripts\python.exe" (
    echo [*] creating build virtualenv ...
    %PYLAUNCH% -m venv .build_venv
)
set "VPY=.build_venv\Scripts\python.exe"

echo [*] upgrading pip / setuptools / wheel ...
"%VPY%" -m pip install --upgrade --disable-pip-version-check pip setuptools wheel

echo [*] installing runtime + build dependencies ...
"%VPY%" -m pip install -r requirements.txt
"%VPY%" -m pip install pyinstaller

echo [*] running PyInstaller ...
"%VPY%" -m PyInstaller --noconfirm --clean bootsusers.spec
if errorlevel 1 (
    echo.
    echo [x] build failed — see the PyInstaller output above.
    pause
    exit /b 1
)

echo.
echo  ============================================================
if exist "%~dp0dist\Bootsusers.exe" (
    echo   DONE.  Your executable is here:
    echo.
    echo       "%~dp0dist\Bootsusers.exe"
    echo.
    echo   Opening the dist folder now ...
    echo  ============================================================
    REM open the folder with the .exe selected
    explorer /select,"%~dp0dist\Bootsusers.exe"
) else (
    echo   [!] BUILD SAID OK, BUT dist\Bootsusers.exe IS MISSING.
    echo  ============================================================
    echo.
    echo   This is almost always your ANTIVIRUS / Windows Defender
    echo   deleting the file right after build. PyInstaller one-file
    echo   .exe files are very often falsely flagged.
    echo.
    echo   Fix:
    echo     1^) Open  Windows Security ^> Virus ^& threat protection
    echo        ^> Protection history  — if "Bootsusers.exe" was
    echo        removed/quarantined, click it and choose RESTORE.
    echo     2^) Add an exclusion for this folder:
    echo          "%~dp0"
    echo        ^(Virus ^& threat protection ^> Manage settings ^>
    echo         Exclusions ^> Add ^> Folder^)
    echo     3^) Run build_exe.bat again.
    echo.
    echo   The folder that SHOULD contain it:
    echo       "%~dp0dist\"
    if exist "%~dp0dist\" explorer "%~dp0dist\"
)
echo.
pause
endlocal
