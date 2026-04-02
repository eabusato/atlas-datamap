/*
 * Atlas Datamap — Sigilo Renderer for Database Maps
 *
 * Generic node/edge → SVG renderer following CCT visual conventions.
 * Does NOT depend on parser/ast.h or any CCT compiler internals.
 *
 * Copyright (c) Erick Andrade Busato. Todos os direitos reservados.
 */

#ifndef ATLAS_RENDER_H
#define ATLAS_RENDER_H

#include "common/types.h"

#if defined(_WIN32) || defined(__CYGWIN__)
#  if defined(ATLAS_SIGILO_EXPORTS)
#    define ATLAS_SIGILO_API __declspec(dllexport)
#  elif defined(ATLAS_SIGILO_STATIC)
#    define ATLAS_SIGILO_API
#  else
#    define ATLAS_SIGILO_API __declspec(dllimport)
#  endif
#elif defined(__GNUC__) && __GNUC__ >= 4
#  define ATLAS_SIGILO_API __attribute__((visibility("default")))
#else
#  define ATLAS_SIGILO_API
#endif

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>

typedef enum {
    ATLAS_NODE_TABLE = 0,
    ATLAS_NODE_VIEW = 1,
    ATLAS_NODE_MATERIALIZED_VIEW = 2,
    ATLAS_NODE_FOREIGN_TABLE = 3
} atlas_node_type_t;

typedef enum {
    ATLAS_EDGE_FK_DECLARED = 0,
    ATLAS_EDGE_FK_INFERRED = 1
} atlas_edge_type_t;

typedef struct {
    char *name;
    char *type_str;
    bool is_pk;
    bool is_fk;
    bool is_nullable;
    i64 distinct_estimate;
    f64 null_rate;
} atlas_column_desc_t;

typedef struct {
    char *id;
    char *name;
    char *schema;
    char *comment;
    atlas_node_type_t node_type;
    i64 row_estimate;
    i64 size_bytes;
    atlas_column_desc_t *columns;
    u32 column_count;
    u32 fk_count;
    u32 index_count;
    i32 schema_group_idx;
    f64 cx;
    f64 cy;
    f64 r;
} atlas_node_t;

typedef struct {
    char *from_id;
    char *to_id;
    char *from_column;
    char *to_column;
    char *on_delete;
    atlas_edge_type_t edge_type;
    i32 from_idx;
    i32 to_idx;
} atlas_edge_t;

typedef struct {
    char *name;
    u32 *node_indices;
    u32 node_count;
    f64 cx;
    f64 cy;
    f64 ring_r;
} atlas_schema_group_t;

typedef struct {
    u32 canvas_width;
    u32 canvas_height;
    const char *style;
    f64 node_r_min;
    f64 node_r_max;
    i64 node_r_ref_rows;
    bool emit_data_attrs;
    bool emit_titles;
    bool emit_column_dots;
    bool emit_macro_rings;
    f64 ring_opacity;
    const char *ring_stroke_dash;
    f64 schema_orbit_r;
    f64 schema_ring_r_base;
    const char *font_label;
    const char *font_hash;
    const char *core_label;
    const char *core_subtitle;
} atlas_render_config_t;

typedef struct {
    atlas_render_config_t config;
    atlas_node_t *nodes;
    u32 node_count;
    u32 node_capacity;
    atlas_edge_t *edges;
    u32 edge_count;
    u32 edge_capacity;
    atlas_schema_group_t *schemas;
    u32 schema_count;
    u32 schema_capacity;
    bool had_error;
    char error_message[256];
} atlas_render_ctx_t;

ATLAS_SIGILO_API void atlas_render_init(atlas_render_ctx_t *ctx, const atlas_render_config_t *config);
ATLAS_SIGILO_API void atlas_render_dispose(atlas_render_ctx_t *ctx);
ATLAS_SIGILO_API u32 atlas_render_add_node(atlas_render_ctx_t *ctx, const atlas_node_t *node);
ATLAS_SIGILO_API bool atlas_render_add_edge(atlas_render_ctx_t *ctx, const atlas_edge_t *edge);
ATLAS_SIGILO_API void atlas_render_compute_layout(atlas_render_ctx_t *ctx);
ATLAS_SIGILO_API void atlas_render_compute_layout_force(
    atlas_render_ctx_t *ctx,
    u32 iterations,
    f64 temperature_start,
    f64 cooling_rate
);
ATLAS_SIGILO_API bool atlas_render_svg(atlas_render_ctx_t *ctx, FILE *out);
ATLAS_SIGILO_API char *atlas_render_svg_to_buffer(atlas_render_ctx_t *ctx, size_t *out_len);
ATLAS_SIGILO_API const char *atlas_render_version(void);

#endif /* ATLAS_RENDER_H */
