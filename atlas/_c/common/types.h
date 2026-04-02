/*
 * CCT — Clavicula Turing
 * Common Type Definitions
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

#ifndef CCT_COMMON_TYPES_H
#define CCT_COMMON_TYPES_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define CCT_VERSION_MAJOR 0
#define CCT_VERSION_MINOR 40
#define CCT_VERSION_PATCH 0
#define CCT_VERSION_SUFFIX ""

#define CCT_VERSION_STRING "0.40.0"

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

typedef int8_t i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;

typedef float f32;
typedef double f64;

typedef enum {
    CCT_PROFILE_HOST = 0,
    CCT_PROFILE_FREESTANDING,
} cct_profile_t;

#endif /* CCT_COMMON_TYPES_H */
