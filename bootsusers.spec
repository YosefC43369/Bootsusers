# bootsusers.spec — PyInstaller build spec for the single-file suite.
# ------------------------------------------------------------------------------
# Build the Windows .exe ON a Windows machine (PyInstaller is NOT a
# cross-compiler — it emits a binary for the OS it runs on):
#
#     pip install pyinstaller customtkinter telethon aiohttp pysocks
#     pyinstaller bootsusers.spec
#
# Output:  dist\Bootsusers.exe   (one self-contained file, no console window)
#
# The spec bundles:
#   • assets\Hack-Regular.ttf          (the GUI font)
#   • customtkinter's own data files   (themes / widget assets)
#   • telethon submodules + PySocks    (collected so MTProto works when frozen)
# ------------------------------------------------------------------------------

import os
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None

# Use a custom icon only if you drop one at assets/icon.ico; otherwise omit it
# (PyInstaller raises on a missing icon path rather than ignoring it).
_ICON = "assets/icon.ico" if os.path.exists("assets/icon.ico") else None

# --- data files ---------------------------------------------------------------
datas = []
datas += [("assets/Hack-Regular.ttf", "assets")]   # bundled Hack font
datas += collect_data_files("customtkinter")       # ctk themes/assets

# --- hidden / dynamically-imported modules ------------------------------------
hiddenimports = []
hiddenimports += collect_submodules("telethon")     # tl.* request/type classes
hiddenimports += ["socks", "customtkinter"]

a = Analysis(
    ["bootsusers_suite.py"],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="Bootsusers",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,                      # windowed GUI app (no console)
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=_ICON,                         # custom icon only if assets/icon.ico exists
)
