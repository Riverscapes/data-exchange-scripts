#!/bin/bash
# Installs the GDAL Python package matching the currently installed system GDAL.
#
# Usage:
#   bash install_geo.sh
#
# Prerequisites (run once per machine/codespace before this script):
#   Debian/Ubuntu:  sudo apt-get install -y libgdal-dev gdal-bin
#   macOS:          brew install gdal
#   Windows:        install OSGeo4W or download GDAL binaries from https://www.gisinternals.com/

set -euo pipefail

if ! command -v gdal-config &>/dev/null; then
    echo "ERROR: gdal-config not found."
    echo "Please install the system GDAL development libraries first:"
    echo "  Debian/Ubuntu: sudo apt-get install -y libgdal-dev gdal-bin"
    echo "  macOS:         brew install gdal"
    exit 1
fi

GDAL_VERSION=$(gdal-config --version)
echo "System GDAL version: ${GDAL_VERSION}"

# GDAL < 3.3 Python bindings use the deprecated 'use_2to3' setuptools feature,
# which was removed in setuptools 58+ and is incompatible with Python 3.12.
MAJOR=$(echo "${GDAL_VERSION}" | cut -d. -f1)
MINOR=$(echo "${GDAL_VERSION}" | cut -d. -f2)
if [[ "${MAJOR}" -lt 3 ]] || [[ "${MAJOR}" -eq 3 && "${MINOR}" -lt 3 ]]; then
    echo "ERROR: System GDAL ${GDAL_VERSION} is too old. Python 3.12 requires GDAL >= 3.3."
    echo "Debian Bullseye only ships GDAL 3.2 — upgrade to Debian Bookworm (or use a"
    echo "newer apt source) to get a compatible GDAL version."
    exit 1
fi

echo "Installing Python gdal==${GDAL_VERSION} into the current environment..."
uv pip install "gdal==${GDAL_VERSION}"

echo "Done. GDAL Python package installed successfully."
