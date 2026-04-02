#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="${ROOT_DIR}/tests/tmp/c_library_smoke"
mkdir -p "${TMP_DIR}"

cat > "${TMP_DIR}/atlas_ping_test.c" <<'EOF'
#include <stdio.h>
#include <string.h>
#include "atlas/_c/atlas_sigilo.h"

int main(void) {
    const char *v = atlas_sigilo_ping();
    if (!v || strlen(v) == 0) {
        fprintf(stderr, "FAIL: ping returned empty\n");
        return 1;
    }
    printf("OK: atlas_sigilo_ping() = %s\n", v);

    const char *rv = atlas_render_version();
    if (!rv || strlen(rv) == 0) {
        fprintf(stderr, "FAIL: render version returned empty\n");
        return 1;
    }
    printf("OK: atlas_render_version() = %s\n", rv);
    return 0;
}
EOF

cc -std=c11 -I"${ROOT_DIR}" \
    "${TMP_DIR}/atlas_ping_test.c" \
    "${ROOT_DIR}/atlas/_c/common/diagnostic.c" \
    "${ROOT_DIR}/atlas/_c/common/errors.c" \
    "${ROOT_DIR}/atlas/_c/sigil/sigil_parse.c" \
    "${ROOT_DIR}/atlas/_c/sigil/sigil_validate.c" \
    "${ROOT_DIR}/atlas/_c/atlas_render.c" \
    "${ROOT_DIR}/atlas/_c/atlas_sigilo.c" \
    -lm -o "${TMP_DIR}/atlas_ping_test"

"${TMP_DIR}/atlas_ping_test"
echo "Smoke test passed."
