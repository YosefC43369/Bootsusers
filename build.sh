#!/usr/bin/env bash
# ============================================================
# Bootsusers — Linux/macOS .exe cross-build helper
# Actual .exe must be built on Windows; this script packages
# everything needed so the Windows box just runs build.bat.
# ============================================================
set -euo pipefail

DIST="dist_package"
NAME="Bootsusers"

echo ""
echo " [*] Bootsusers — source package for Windows build"
echo ""

# clean
rm -rf "$DIST"
mkdir -p "$DIST"

# copy source
FILES=(
    launcher.py
    auto_react.py
    auto_join.py
    reaction_engine.py
    build.bat
)

for f in "${FILES[@]}"; do
    if [ -f "$f" ]; then
        cp "$f" "$DIST/"
        echo " [+] $f"
    else
        echo " [!] missing: $f (skipping)"
    fi
done

# copy assets if present
if [ -d "assets" ]; then
    cp -r assets "$DIST/"
    echo " [+] assets/"
fi

# copy session files if any
if ls ./*.session 2>/dev/null | head -1 > /dev/null; then
    cp ./*.session "$DIST/" 2>/dev/null || true
    mkdir -p "$DIST/sessions"
fi

# requirements snapshot
python3 -m pip freeze 2>/dev/null | grep -Ei \
    "telethon|customtkinter|pillow|python-socks|pyinstaller" \
    > "$DIST/requirements.txt" || \
    printf "telethon\ncustomtkinter\npillow\npython-socks\npyinstaller\n" \
    > "$DIST/requirements.txt"

echo ""
echo " [+] Package ready: $DIST/"
echo " [i] Transfer to Windows, run:  pip install -r requirements.txt"
echo " [i] Then:                       build.bat"
echo ""