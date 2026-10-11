#!/bin/bash
# Installs the pinned Conveyor (CONVEYOR_VERSION, checked against CONVEYOR_SHA256) into
# $RUNNER_TEMP and puts it on the job's PATH. For the release workflow.
set -euo pipefail
f=conveyor-$CONVEYOR_VERSION-linux-amd64.tar.gz
curl -sSfLO "https://downloads.hydraulic.dev/conveyor/$f"
echo "$CONVEYOR_SHA256  $f" | sha256sum -c
mkdir -p "$RUNNER_TEMP/conveyor"
tar xzf "$f" -C "$RUNNER_TEMP/conveyor"
rm "$f"
echo "$RUNNER_TEMP/conveyor/conveyor-$CONVEYOR_VERSION/bin" >> "$GITHUB_PATH"
