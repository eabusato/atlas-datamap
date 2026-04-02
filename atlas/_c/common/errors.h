/*
 * CCT — Clavicula Turing
 * Error Handling Definitions
 *
 * FASE 12H: Structural maturity milestone
 *
 * Copyright (c) Erick Andrade Busato. Todos os direitos reservados.
 */

/*
 * ─────────────────────────────────────────────────────────────────────
 * ATLAS VENDORED — Arquivo copiado de ../cct/src/
 * Versão CCT: 0.40.0 (FASE 14A)
 * Sincronização manual. Não modificar — aplicar patches via diff.
 * ─────────────────────────────────────────────────────────────────────
 */

#ifndef CCT_COMMON_ERRORS_H
#define CCT_COMMON_ERRORS_H

#include "types.h"

typedef enum {
    CCT_OK = 0,
    CCT_ERROR_INVALID_ARGUMENT = 1,
    CCT_ERROR_CONTRACT_VIOLATION = 2,
    CCT_ERROR_MISSING_ARGUMENT = 3,
    CCT_ERROR_UNKNOWN_COMMAND = 4,
    CCT_ERROR_FILE_NOT_FOUND = 20,
    CCT_ERROR_FILE_READ = 21,
    CCT_ERROR_FILE_WRITE = 22,
    CCT_ERROR_LEXICAL = 40,
    CCT_ERROR_SYNTAX = 50,
    CCT_ERROR_SEMANTIC = 60,
    CCT_ERROR_CODEGEN = 70,
    CCT_ERROR_INTERNAL = 100,
    CCT_ERROR_NOT_IMPLEMENTED = 101,
    CCT_ERROR_OUT_OF_MEMORY = 102,
} cct_error_code_t;

void cct_error_print(cct_error_code_t code, const char *message);
void cct_error_printf(cct_error_code_t code, const char *format, ...);
void cct_error_at_location(
    cct_error_code_t code,
    const char *filename,
    u32 line,
    u32 column,
    const char *message
);
void cct_error_at_location_with_suggestion(
    cct_error_code_t code,
    const char *filename,
    u32 line,
    u32 column,
    const char *message,
    const char *suggestion
);
const char *cct_error_string(cct_error_code_t code);

#endif /* CCT_COMMON_ERRORS_H */
