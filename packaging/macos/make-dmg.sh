#!/bin/bash
# Wraps the Mac app from Conveyor's zip in a drag-to-Applications disk image (Conveyor makes
# no DMG). The app keeps Conveyor's signature, and Sparkle updates it wherever it's dragged to.
# Usage: make-dmg.sh <app.zip> <out.dmg>   (on macOS)
set -euo pipefail
zip=$1
out=$2
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir "$work/root"
ditto -x -k "$zip" "$work/root"
app=$(find "$work/root" -maxdepth 1 -name '*.app' -print -quit)
if [[ -z "$app" ]]; then
  echo "no app in $zip" >&2
  exit 1
fi
codesign --verify --deep --strict "$app"
ln -s /Applications "$work/root/Applications"
hdiutil create -volname "Homebase Filesync" -srcfolder "$work/root" -fs HFS+ -format UDZO -ov "$out"
