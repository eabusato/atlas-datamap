"""cffi ABI declarations for the vendored libatlas_sigilo shared library."""

# Copyright (c) 2026 Erick Andrade Busato
# SPDX-License-Identifier: MIT

from __future__ import annotations

import cffi  # type: ignore[import-untyped]

ffi = cffi.FFI()

ffi.cdef(
    """
typedef uint8_t  u8;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int32_t  i32;
typedef int64_t  i64;
typedef double   f64;

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

void atlas_render_init(atlas_render_ctx_t *ctx, const atlas_render_config_t *config);
void atlas_render_dispose(atlas_render_ctx_t *ctx);
u32 atlas_render_add_node(atlas_render_ctx_t *ctx, const atlas_node_t *node);
bool atlas_render_add_edge(atlas_render_ctx_t *ctx, const atlas_edge_t *edge);
void atlas_render_compute_layout(atlas_render_ctx_t *ctx);
void atlas_render_compute_layout_force(
    atlas_render_ctx_t *ctx,
    u32 iterations,
    f64 temperature_start,
    f64 cooling_rate
);
char *atlas_render_svg_to_buffer(atlas_render_ctx_t *ctx, size_t *out_len);
const char *atlas_render_version(void);
const char *atlas_sigilo_ping(void);
const char *atlas_sigilo_abi_version(void);

typedef enum {
    CCT_SIGIL_PARSE_MODE_TOLERANT = 0,
    CCT_SIGIL_PARSE_MODE_STRICT = 1,
    CCT_SIGIL_PARSE_MODE_LEGACY_TOLERANT = 2,
    CCT_SIGIL_PARSE_MODE_CURRENT_DEFAULT = 3,
    CCT_SIGIL_PARSE_MODE_STRICT_CONTRACT = 4
} cct_sigil_parse_mode_t;

typedef struct { ...; } cct_sigil_document_t;

bool cct_sigil_parse_file(
    const char *path,
    cct_sigil_parse_mode_t mode,
    cct_sigil_document_t *out_doc
);
bool cct_sigil_document_has_errors(const cct_sigil_document_t *doc);
void cct_sigil_document_dispose(cct_sigil_document_t *doc);

void free(void *ptr);
"""
)

__all__ = ["ffi"]
