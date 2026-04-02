/*
 * Atlas Datamap — Public API facade for Python binding
 *
 * Copyright (c) Erick Andrade Busato. Todos os direitos reservados.
 */

#ifndef ATLAS_SIGILO_H
#define ATLAS_SIGILO_H

#include "atlas_render.h"
#include "sigil/sigil_parse.h"
#include "sigil/sigil_validate.h"

#define ATLAS_SIGILO_ABI_VERSION "3A.0"

ATLAS_SIGILO_API const char *atlas_sigilo_abi_version(void);
ATLAS_SIGILO_API const char *atlas_sigilo_ping(void);
ATLAS_SIGILO_API void atlas_render_compute_layout_force(
    atlas_render_ctx_t *ctx,
    u32 iterations,
    f64 temperature_start,
    f64 cooling_rate
);

#endif /* ATLAS_SIGILO_H */
