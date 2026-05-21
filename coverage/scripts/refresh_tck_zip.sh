#!/usr/bin/env bash
# Build coverage/src/main/resources/v2.1.zip from the SDMX-VTL reference examples.
#
# Same source as GitHub Actions (see .github/workflows/tck-vtl-tf-spark*.yml).
# Run from the Trevas repository root:
#
#   ./coverage/scripts/refresh_tck_zip.sh
#
# Override the VTL git branch (default: fix/doc-examples):
#
#   VTL_TCK_BRANCH=fix/doc-examples ./coverage/scripts/refresh_tck_zip.sh

set -euo pipefail

TREVAS_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
VTL_TCK_BRANCH="${VTL_TCK_BRANCH:-fix/doc-examples}"
DOC_VERSION="${DOC_VERSION:-v2.1}"
WORK_DIR="${TREVAS_ROOT}/.cache/vtl-tck"
RESOURCES_DIR="${TREVAS_ROOT}/coverage/src/main/resources"
ZIP_NAME="${DOC_VERSION}.zip"

rm -rf "${WORK_DIR}"
git clone --depth 1 --branch "${VTL_TCK_BRANCH}" https://github.com/sdmx-twg/vtl.git "${WORK_DIR}"

DOC_VERSION="${DOC_VERSION}" python3 "${WORK_DIR}/scripts/generate_tck_files.py"

mkdir -p "${RESOURCES_DIR}"
cp "${WORK_DIR}/tck/${ZIP_NAME}" "${RESOURCES_DIR}/${ZIP_NAME}"

echo "Updated ${RESOURCES_DIR}/${ZIP_NAME} from sdmx-twg/vtl@${VTL_TCK_BRANCH}"
