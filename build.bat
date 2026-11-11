@echo off
:: ============================================================
:: Bootsusers — Windows .exe build script
:: Requires: pip install pyinstaller customtkinter
:: Run from the project root (same folder as launcher.py)
:: ============================================================

setlocal

set NAME=Bootsusers
set ENTRY=launcher.py
set ICON=assets\icon.ico
set DIST=dist
set WORK=build_tmp

echo.
echo  [*] Bootsusers — PyInstaller build
echo  [*] Target: %NAME%.exe
echo.

:: ── dependency check ────────────────────────────────────────
python -m pip show pyinstaller >nul 2>&1
if errorlevel 1 (
    echo  [!] pyinstaller not found — installing...
    python -m pip install pyinstaller
)

python -m pip show customtkinter >nul 2>&1
if errorlevel 1 (
    echo  [!] customtkinter not found — installing...
    python -m pip install customtkinter
)

:: ── locate customtkinter data dir for --add-data ────────────
for /f "delims=" %%i in ('python -c "import customtkinter, os; print(os.path.dirname(customtkinter.__file__))"') do set CTK_PATH=%%i
echo  [*] customtkinter path: %CTK_PATH%

:: ── build ───────────────────────────────────────────────────
pyinstaller ^
    --onefile ^
    --windowed ^
    --name "%NAME%" ^
    --distpath "%DIST%" ^
    --workpath "%WORK%" ^
    --specpath "%WORK%" ^
    --add-data "%CTK_PATH%;customtkinter" ^
    --add-data "auto_react.py;." ^
    --add-data "auto_join.py;." ^
    --add-data "reaction_engine.py;." ^
    --hidden-import telethon ^
    --hidden-import telethon.sessions ^
    --hidden-import telethon.errors ^
    --hidden-import telethon.tl.functions.messages ^
    --hidden-import telethon.tl.functions.channels ^
    --hidden-import telethon.tl.types ^
    --hidden-import socks ^
    --hidden-import python_socks ^
    --hidden-import asyncio ^
    --hidden-import customtkinter ^
    --hidden-import PIL ^
    --hidden-import PIL.Image ^
    --collect-all customtkinter ^
    --collect-all telethon ^
    %ENTRY%

:: ── result ──────────────────────────────────────────────────
if exist "%DIST%\%NAME%.exe" (
    echo.
    echo  [+] BUILD SUCCEEDED
    echo  [+] Output: %DIST%\%NAME%.exe
    echo.
    for %%A in ("%DIST%\%NAME%.exe") do echo  [+] Size: %%~zA bytes
) else (
    echo.
    echo  [!] BUILD FAILED — check output above
    exit /b 1
)

:: ── copy data files next to .exe ───────────────────────────
echo  [*] Copying runtime data to dist\...
if not exist "%DIST%\sessions" mkdir "%DIST%\sessions"
if exist "*.csv" copy /Y "*.csv" "%DIST%\" >nul

echo  [+] Done. Distribute the contents of the dist\ folder.
echo.
endlocal