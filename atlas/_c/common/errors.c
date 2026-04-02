/*
 * CCT — Clavicula Turing
 * Error Handling Implementation
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

#include "errors.h"
#include "diagnostic.h"

#include <stdarg.h>
#include <stdio.h>

const char *cct_error_string(cct_error_code_t code) {
    switch (code) {
        case CCT_OK:
            return "Success";
        case CCT_ERROR_INVALID_ARGUMENT:
            return "Invalid argument";
        case CCT_ERROR_CONTRACT_VIOLATION:
            return "Contract violation";
        case CCT_ERROR_UNKNOWN_COMMAND:
            return "Unknown command";
        case CCT_ERROR_MISSING_ARGUMENT:
            return "Missing required argument";
        case CCT_ERROR_FILE_NOT_FOUND:
            return "File not found";
        case CCT_ERROR_FILE_READ:
            return "Error reading file";
        case CCT_ERROR_FILE_WRITE:
            return "Error writing file";
        case CCT_ERROR_LEXICAL:
            return "Lexical error";
        case CCT_ERROR_SYNTAX:
            return "Syntax error";
        case CCT_ERROR_SEMANTIC:
            return "Semantic error";
        case CCT_ERROR_CODEGEN:
            return "Code generation error";
        case CCT_ERROR_INTERNAL:
            return "Internal compiler error";
        case CCT_ERROR_NOT_IMPLEMENTED:
            return "Feature not yet implemented";
        case CCT_ERROR_OUT_OF_MEMORY:
            return "Out of memory";
        default:
            return "Unknown error";
    }
}

void cct_error_print(cct_error_code_t code, const char *message) {
    cct_diagnostic_t diag = {
        .level = CCT_DIAG_ERROR,
        .message = message ? message : "unknown error",
        .file_path = NULL,
        .line = 0,
        .column = 0,
        .suggestion = NULL,
        .code_label = (code != CCT_OK) ? cct_error_string(code) : NULL,
    };
    cct_diagnostic_emit(&diag);
}

void cct_error_printf(cct_error_code_t code, const char *format, ...) {
    va_list args;
    char buffer[2048];
    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);
    cct_error_print(code, buffer);
}

void cct_error_at_location(
    cct_error_code_t code,
    const char *filename,
    u32 line,
    u32 column,
    const char *message
) {
    cct_error_at_location_with_suggestion(code, filename, line, column, message, NULL);
}

void cct_error_at_location_with_suggestion(
    cct_error_code_t code,
    const char *filename,
    u32 line,
    u32 column,
    const char *message,
    const char *suggestion
) {
    cct_diagnostic_t diag = {
        .level = CCT_DIAG_ERROR,
        .message = message ? message : "unknown error",
        .file_path = filename,
        .line = line,
        .column = column,
        .suggestion = suggestion,
        .code_label = (code != CCT_OK) ? cct_error_string(code) : NULL,
    };
    cct_diagnostic_emit(&diag);
}
