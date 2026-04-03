/*
 * Atlas Datamap — Sigilo Renderer for Database Maps (Implementation)
 *
 * Copyright (c) Erick Andrade Busato. Todos os direitos reservados.
 */

#include "atlas_render.h"

#include <ctype.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ATLAS_RENDER_VERSION_STR "3A.0"
#define ATLAS_PI 3.14159265358979323846
#define CANVAS_SINGLE 512
#define COL_DOT_RADIUS 3.0
#define COL_DOT_GAP 8.0
#define MAX_COLUMN_LABELS 12
#define COLUMN_BAND_SIZE 10
#define SCHEMA_GAP 72.0
#define SCHEMA_CANVAS_MARGIN 220.0
#define SCHEMA_LABEL_DEPTH 48.0
#define FONT_SCALE 1.15

static char *ar_strdup(const char *s) {
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    char *out = (char *)malloc(len + 1);
    if (!out) {
        return NULL;
    }
    memcpy(out, s, len + 1);
    return out;
}

static void ar_set_error(atlas_render_ctx_t *ctx, const char *msg) {
    ctx->had_error = true;
    strncpy(ctx->error_message, msg, sizeof(ctx->error_message) - 1);
    ctx->error_message[sizeof(ctx->error_message) - 1] = '\0';
}

static void ar_xml_escape(FILE *out, const char *s) {
    if (!s) {
        return;
    }
    for (const char *p = s; *p; p++) {
        switch (*p) {
            case '<':
                fputs("&lt;", out);
                break;
            case '>':
                fputs("&gt;", out);
                break;
            case '&':
                fputs("&amp;", out);
                break;
            case '"':
                fputs("&quot;", out);
                break;
            case '\'':
                fputs("&apos;", out);
                break;
            default:
                fputc(*p, out);
        }
    }
}

static void ar_fmt_number(char *buf, size_t len, i64 n) {
    if (n < 0) {
        snprintf(buf, len, "unknown");
        return;
    }
    if (n < 1000) {
        snprintf(buf, len, "%lld", (long long)n);
        return;
    }
    if (n < 1000000) {
        snprintf(buf, len, "%lld %03lld", (long long)(n / 1000), (long long)(n % 1000));
        return;
    }
    snprintf(
        buf,
        len,
        "%lld %03lld %03lld",
        (long long)(n / 1000000),
        (long long)((n % 1000000) / 1000),
        (long long)(n % 1000)
    );
}

static void ar_fmt_bytes(char *buf, size_t len, i64 bytes) {
    if (bytes < 0) {
        snprintf(buf, len, "unknown");
        return;
    }
    if (bytes < 1024) {
        snprintf(buf, len, "%lld B", (long long)bytes);
        return;
    }
    if (bytes < 1024 * 1024) {
        snprintf(buf, len, "%.1f KB", bytes / 1024.0);
        return;
    }
    if (bytes < 1024 * 1024 * 1024L) {
        snprintf(buf, len, "%.1f MB", bytes / (1024.0 * 1024));
        return;
    }
    snprintf(buf, len, "%.2f GB", bytes / (1024.0 * 1024 * 1024));
}

static void ar_scale_font_spec(const char *spec, f64 scale, char *buf, size_t len) {
    if (!spec || !*spec) {
        snprintf(buf, len, "%s", "");
        return;
    }

    const char *pos = spec;
    while (*pos && !isdigit((unsigned char)*pos) && *pos != '.') {
        pos++;
    }
    if (!*pos) {
        snprintf(buf, len, "%s", spec);
        return;
    }

    char *endptr = NULL;
    double size = strtod(pos, &endptr);
    if (endptr == pos || strncmp(endptr, "px", 2) != 0) {
        snprintf(buf, len, "%s", spec);
        return;
    }

    snprintf(
        buf,
        len,
        "%.*s%.2fpx%s",
        (int)(pos - spec),
        spec,
        size * scale,
        endptr + 2
    );
}

static f64 ar_clamp(f64 v, f64 lo, f64 hi) {
    return v < lo ? lo : (v > hi ? hi : v);
}

static f64 ar_node_radius(const atlas_render_ctx_t *ctx, i64 row_estimate) {
    f64 r_min = ctx->config.node_r_min;
    f64 r_max = ctx->config.node_r_max;
    i64 r_ref = ctx->config.node_r_ref_rows;
    if (row_estimate <= 0) {
        return r_min;
    }
    f64 scale = log1p((f64)row_estimate) / log1p((f64)r_ref);
    return ar_clamp(r_min + (r_max - r_min) * scale, r_min, r_max);
}

static const char *ar_short_label(const char *name) {
    static char buf[32];
    size_t len = name ? strlen(name) : 0;
    if (!name || len == 0) {
        return "?";
    }
    if (len <= 12) {
        return name;
    }
    snprintf(buf, sizeof(buf), "%.9s...", name);
    return buf;
}

static void ar_emit_column_flags(FILE *out, const atlas_column_desc_t *col) {
    bool first = true;
    if (!col) {
        return;
    }
    if (col->is_pk) {
        fputs("PK", out);
        first = false;
    }
    if (col->is_fk) {
        if (!first) {
            fputs(" | ", out);
        }
        fputs("FK", out);
        first = false;
    }
    if (!first) {
        fputs(" | ", out);
    }
    fputs(col->is_nullable ? "NULL" : "NOT NULL", out);
    first = false;
    if (col->distinct_estimate >= 0) {
        if (!first) {
            fputs(" | ", out);
        }
        fprintf(out, "distinct=%lld", (long long)col->distinct_estimate);
        first = false;
    }
    if (col->null_rate >= 0.0) {
        if (!first) {
            fputs(" | ", out);
        }
        fprintf(out, "null=%.2f", col->null_rate);
    }
}

static void ar_emit_columns_detail_attr(FILE *out, const atlas_node_t *n) {
    if (!n || n->column_count == 0) {
        return;
    }
    fputs(" data-columns-detail=\"", out);
    for (u32 ci = 0; ci < n->column_count; ci++) {
        const atlas_column_desc_t *col = &n->columns[ci];
        if (ci > 0) {
            fputs("||", out);
        }
        ar_xml_escape(out, col->name ? col->name : "?");
        fputs("::", out);
        ar_xml_escape(out, col->type_str ? col->type_str : "");
        fputs("::", out);
        if (col->is_pk) {
            fputs("PK", out);
        }
        if (col->is_fk) {
            if (col->is_pk) {
                fputs("|", out);
            }
            fputs("FK", out);
        }
        if (col->is_pk || col->is_fk) {
            fputs("|", out);
        }
        fputs(col->is_nullable ? "NULL" : "NOT NULL", out);
        if (col->distinct_estimate >= 0) {
            fprintf(out, "|distinct=%lld", (long long)col->distinct_estimate);
        }
        if (col->null_rate >= 0.0) {
            fprintf(out, "|null=%.2f", col->null_rate);
        }
    }
    fputs("\"", out);
}

typedef struct {
    f64 x;
    f64 y;
} ar_vec2_t;

static f64 ar_vec2_len(ar_vec2_t v) {
    return sqrt(v.x * v.x + v.y * v.y);
}

static ar_vec2_t ar_vec2_norm(ar_vec2_t v) {
    f64 len = ar_vec2_len(v);
    if (len < 1e-9) {
        return (ar_vec2_t){0.0, 0.0};
    }
    return (ar_vec2_t){v.x / len, v.y / len};
}

static i32 ar_find_node_idx(atlas_render_ctx_t *ctx, const char *id) {
    if (!id) {
        return -1;
    }
    for (u32 i = 0; i < ctx->node_count; i++) {
        if (ctx->nodes[i].id && strcmp(ctx->nodes[i].id, id) == 0) {
            return (i32)i;
        }
    }
    return -1;
}

static atlas_schema_group_t *ar_find_or_create_schema(atlas_render_ctx_t *ctx, const char *name) {
    for (u32 i = 0; i < ctx->schema_count; i++) {
        if (strcmp(ctx->schemas[i].name, name) == 0) {
            return &ctx->schemas[i];
        }
    }
    if (ctx->schema_count >= ctx->schema_capacity) {
        u32 next_cap = ctx->schema_capacity == 0 ? 8 : ctx->schema_capacity * 2;
        atlas_schema_group_t *grown =
            (atlas_schema_group_t *)realloc(ctx->schemas, next_cap * sizeof(*grown));
        if (!grown) {
            ar_set_error(ctx, "OOM: schemas");
            return NULL;
        }
        ctx->schemas = grown;
        ctx->schema_capacity = next_cap;
    }
    atlas_schema_group_t *sg = &ctx->schemas[ctx->schema_count++];
    memset(sg, 0, sizeof(*sg));
    sg->name = ar_strdup(name);
    sg->node_indices = NULL;
    sg->node_count = 0;
    return sg;
}

static bool ar_schema_add_node(atlas_schema_group_t *sg, u32 node_idx) {
    u32 new_count = sg->node_count + 1;
    u32 *grown = (u32 *)realloc(sg->node_indices, new_count * sizeof(u32));
    if (!grown) {
        return false;
    }
    sg->node_indices = grown;
    sg->node_indices[sg->node_count++] = node_idx;
    return true;
}

void atlas_render_init(atlas_render_ctx_t *ctx, const atlas_render_config_t *config) {
    memset(ctx, 0, sizeof(*ctx));
    if (config) {
        ctx->config = *config;
    } else {
        ctx->config.canvas_width = CANVAS_SINGLE;
        ctx->config.canvas_height = CANVAS_SINGLE;
        ctx->config.style = "network";
        ctx->config.node_r_min = 8.0;
        ctx->config.node_r_max = 32.0;
        ctx->config.node_r_ref_rows = 1000000;
        ctx->config.emit_data_attrs = true;
        ctx->config.emit_titles = true;
        ctx->config.emit_column_dots = true;
        ctx->config.emit_macro_rings = true;
        ctx->config.ring_opacity = 0.32;
        ctx->config.ring_stroke_dash = "";
        ctx->config.schema_orbit_r = 450.0;
        ctx->config.schema_ring_r_base = 180.0;
        ctx->config.font_label = "11px monospace";
        ctx->config.font_hash = "9px monospace";
        ctx->config.core_label = "database";
        ctx->config.core_subtitle = "atlas core";
    }
    if (ctx->config.canvas_width == 0) {
        ctx->config.canvas_width = CANVAS_SINGLE;
    }
    if (ctx->config.canvas_height == 0) {
        ctx->config.canvas_height = CANVAS_SINGLE;
    }
    if (!ctx->config.style) {
        ctx->config.style = "network";
    }
    if (ctx->config.node_r_min <= 0.0) {
        ctx->config.node_r_min = 8.0;
    }
    if (ctx->config.node_r_max < ctx->config.node_r_min) {
        ctx->config.node_r_max = 32.0;
    }
    if (ctx->config.node_r_ref_rows <= 0) {
        ctx->config.node_r_ref_rows = 1000000;
    }
    if (ctx->config.ring_opacity < 0.0) {
        ctx->config.ring_opacity = 0.32;
    }
    if (!ctx->config.ring_stroke_dash) {
        ctx->config.ring_stroke_dash = "";
    }
    if (ctx->config.schema_orbit_r <= 0.0) {
        ctx->config.schema_orbit_r = 450.0;
    }
    if (ctx->config.schema_ring_r_base <= 0.0) {
        ctx->config.schema_ring_r_base = 180.0;
    }
    if (!ctx->config.font_label) {
        ctx->config.font_label = "11px monospace";
    }
    if (!ctx->config.font_hash) {
        ctx->config.font_hash = "9px monospace";
    }
    if (!ctx->config.core_label) {
        ctx->config.core_label = "database";
    }
    if (!ctx->config.core_subtitle) {
        ctx->config.core_subtitle = "atlas core";
    }
}

static void ar_free_column(atlas_column_desc_t *col) {
    free(col->name);
    free(col->type_str);
}

static void ar_free_node(atlas_node_t *n) {
    free(n->id);
    free(n->name);
    free(n->schema);
    free(n->comment);
    for (u32 i = 0; i < n->column_count; i++) {
        ar_free_column(&n->columns[i]);
    }
    free(n->columns);
}

static void ar_free_edge(atlas_edge_t *e) {
    free(e->from_id);
    free(e->to_id);
    free(e->from_column);
    free(e->to_column);
    free(e->on_delete);
}

static void ar_free_schema_group(atlas_schema_group_t *sg) {
    free(sg->name);
    free(sg->node_indices);
}

void atlas_render_dispose(atlas_render_ctx_t *ctx) {
    for (u32 i = 0; i < ctx->node_count; i++) {
        ar_free_node(&ctx->nodes[i]);
    }
    for (u32 i = 0; i < ctx->edge_count; i++) {
        ar_free_edge(&ctx->edges[i]);
    }
    for (u32 i = 0; i < ctx->schema_count; i++) {
        ar_free_schema_group(&ctx->schemas[i]);
    }
    free(ctx->nodes);
    free(ctx->edges);
    free(ctx->schemas);
    memset(ctx, 0, sizeof(*ctx));
}

u32 atlas_render_add_node(atlas_render_ctx_t *ctx, const atlas_node_t *src) {
    if (!src || !src->id || !src->schema) {
        ar_set_error(ctx, "add_node: id and schema are required");
        return UINT32_MAX;
    }
    if (ctx->node_count >= ctx->node_capacity) {
        u32 next_cap = ctx->node_capacity == 0 ? 32 : ctx->node_capacity * 2;
        atlas_node_t *grown = (atlas_node_t *)realloc(ctx->nodes, next_cap * sizeof(*grown));
        if (!grown) {
            ar_set_error(ctx, "OOM: nodes");
            return UINT32_MAX;
        }
        ctx->nodes = grown;
        ctx->node_capacity = next_cap;
    }
    u32 idx = ctx->node_count++;
    atlas_node_t *n = &ctx->nodes[idx];
    memset(n, 0, sizeof(*n));

    n->id = ar_strdup(src->id);
    n->name = ar_strdup(src->name ? src->name : src->id);
    n->schema = ar_strdup(src->schema);
    n->comment = ar_strdup(src->comment);
    n->node_type = src->node_type;
    n->row_estimate = src->row_estimate;
    n->size_bytes = src->size_bytes;
    n->fk_count = src->fk_count;
    n->index_count = src->index_count;
    n->cx = src->cx;
    n->cy = src->cy;
    n->r = src->r;

    if (src->column_count > 0 && src->columns) {
        n->columns = (atlas_column_desc_t *)calloc(src->column_count, sizeof(atlas_column_desc_t));
        if (!n->columns) {
            ar_set_error(ctx, "OOM: columns");
            return UINT32_MAX;
        }
        n->column_count = src->column_count;
        for (u32 i = 0; i < src->column_count; i++) {
            n->columns[i].name = ar_strdup(src->columns[i].name);
            n->columns[i].type_str = ar_strdup(src->columns[i].type_str);
            n->columns[i].is_pk = src->columns[i].is_pk;
            n->columns[i].is_fk = src->columns[i].is_fk;
            n->columns[i].is_nullable = src->columns[i].is_nullable;
            n->columns[i].distinct_estimate = src->columns[i].distinct_estimate;
            n->columns[i].null_rate = src->columns[i].null_rate;
        }
    }

    atlas_schema_group_t *sg = ar_find_or_create_schema(ctx, n->schema);
    if (!sg || !ar_schema_add_node(sg, idx)) {
        ar_set_error(ctx, "schema registration failed");
        return UINT32_MAX;
    }
    n->schema_group_idx = (i32)(sg - ctx->schemas);

    return idx;
}

bool atlas_render_add_edge(atlas_render_ctx_t *ctx, const atlas_edge_t *src) {
    if (!src || !src->from_id || !src->to_id) {
        ar_set_error(ctx, "add_edge: from_id and to_id are required");
        return false;
    }
    if (ctx->edge_count >= ctx->edge_capacity) {
        u32 next_cap = ctx->edge_capacity == 0 ? 64 : ctx->edge_capacity * 2;
        atlas_edge_t *grown = (atlas_edge_t *)realloc(ctx->edges, next_cap * sizeof(*grown));
        if (!grown) {
            ar_set_error(ctx, "OOM: edges");
            return false;
        }
        ctx->edges = grown;
        ctx->edge_capacity = next_cap;
    }
    atlas_edge_t *e = &ctx->edges[ctx->edge_count++];
    memset(e, 0, sizeof(*e));
    e->from_id = ar_strdup(src->from_id);
    e->to_id = ar_strdup(src->to_id);
    e->from_column = ar_strdup(src->from_column);
    e->to_column = ar_strdup(src->to_column);
    e->on_delete = ar_strdup(src->on_delete);
    e->edge_type = src->edge_type;
    e->from_idx = ar_find_node_idx(ctx, src->from_id);
    e->to_idx = ar_find_node_idx(ctx, src->to_id);
    return true;
}

static void ar_refresh_schema_geometry(atlas_render_ctx_t *ctx) {
    for (u32 si = 0; si < ctx->schema_count; si++) {
        atlas_schema_group_t *sg = &ctx->schemas[si];
        if (sg->node_count == 0) {
            continue;
        }

        f64 sum_x = 0.0;
        f64 sum_y = 0.0;
        f64 max_r = 0.0;
        for (u32 ni = 0; ni < sg->node_count; ni++) {
            atlas_node_t *node = &ctx->nodes[sg->node_indices[ni]];
            sum_x += node->cx;
            sum_y += node->cy;
            if (node->r > max_r) {
                max_r = node->r;
            }
        }
        sg->cx = sum_x / (f64)sg->node_count;
        sg->cy = sum_y / (f64)sg->node_count;

        f64 max_dist = 0.0;
        for (u32 ni = 0; ni < sg->node_count; ni++) {
            atlas_node_t *node = &ctx->nodes[sg->node_indices[ni]];
            f64 dx = node->cx - sg->cx;
            f64 dy = node->cy - sg->cy;
            f64 dist = sqrt(dx * dx + dy * dy);
            if (dist > max_dist) {
                max_dist = dist;
            }
        }
        sg->ring_r = ar_clamp(
            max_dist + max_r + 28.0,
            ctx->config.schema_ring_r_base,
            ctx->config.schema_ring_r_base + 220.0
        );
    }
}

static f64 ar_schema_max_node_radius(const atlas_render_ctx_t *ctx, const atlas_schema_group_t *sg) {
    f64 max_r = 0.0;
    for (u32 ni = 0; ni < sg->node_count; ni++) {
        const atlas_node_t *node = &ctx->nodes[sg->node_indices[ni]];
        if (node->r > max_r) {
            max_r = node->r;
        }
    }
    return max_r;
}

static i64 ar_schema_total_rows(const atlas_render_ctx_t *ctx, const atlas_schema_group_t *sg) {
    i64 total = 0;
    for (u32 ni = 0; ni < sg->node_count; ni++) {
        const atlas_node_t *node = &ctx->nodes[sg->node_indices[ni]];
        if (node->row_estimate > 0) {
            total += node->row_estimate;
        }
    }
    return total;
}

static u32 ar_schema_total_fk_count(const atlas_render_ctx_t *ctx, const atlas_schema_group_t *sg) {
    u32 total = 0;
    for (u32 ni = 0; ni < sg->node_count; ni++) {
        const atlas_node_t *node = &ctx->nodes[sg->node_indices[ni]];
        total += node->fk_count;
    }
    return total;
}

static f64 ar_schema_outer_orbit(const atlas_render_ctx_t *ctx) {
    if (ctx->schema_count == 0) {
        return ctx->config.schema_orbit_r;
    }

    f64 max_support = 0.0;
    for (u32 si = 0; si < ctx->schema_count; si++) {
        const atlas_schema_group_t *sg = &ctx->schemas[si];
        f64 support_r = sg->ring_r + ar_schema_max_node_radius(ctx, sg) + 64.0;
        if (support_r > max_support) {
            max_support = support_r;
        }
    }

    if (!ctx->config.emit_macro_rings) {
        if (ctx->schema_count == 1) {
            return 0.0;
        }
        f64 canvas_half = ((f64)ctx->config.canvas_width < (f64)ctx->config.canvas_height
                               ? (f64)ctx->config.canvas_width
                               : (f64)ctx->config.canvas_height) * 0.5;
        f64 available = canvas_half - max_support - 48.0;
        if (available < 0.0) {
            return 0.0;
        }
        f64 reduced_orbit = ctx->config.schema_orbit_r * 0.55;
        return reduced_orbit < available ? reduced_orbit : available;
    }

    f64 outer_orbit = ctx->config.schema_orbit_r;
    if (outer_orbit < max_support + 320.0) {
        outer_orbit = max_support + 320.0;
    }
    if (ctx->schema_count == 1) {
        return outer_orbit;
    }

    f64 angle_step = 2.0 * ATLAS_PI / (f64)ctx->schema_count;
    f64 chord_factor = 2.0 * sin(angle_step * 0.5);
    if (chord_factor <= 0.0) {
        return outer_orbit;
    }

    f64 pairwise_requirement = 0.0;
    for (u32 si = 0; si < ctx->schema_count; si++) {
        const atlas_schema_group_t *left = &ctx->schemas[si];
        const atlas_schema_group_t *right = &ctx->schemas[(si + 1) % ctx->schema_count];
        f64 left_support = left->ring_r + ar_schema_max_node_radius(ctx, left) + 64.0;
        f64 right_support = right->ring_r + ar_schema_max_node_radius(ctx, right) + 64.0;
        f64 required = (left_support + right_support + SCHEMA_GAP) / chord_factor;
        if (required > pairwise_requirement) {
            pairwise_requirement = required;
        }
    }
    return outer_orbit > pairwise_requirement ? outer_orbit : pairwise_requirement;
}

void atlas_render_compute_layout(atlas_render_ctx_t *ctx) {
    u32 ns = ctx->schema_count;
    if (ns == 0) {
        return;
    }

    for (u32 ni = 0; ni < ctx->node_count; ni++) {
        atlas_node_t *n = &ctx->nodes[ni];
        if (n->r <= 0.0) {
            n->r = ar_node_radius(ctx, n->row_estimate);
        }
    }

    f64 max_support = 0.0;
    for (u32 si = 0; si < ns; si++) {
        atlas_schema_group_t *sg = &ctx->schemas[si];
        f64 circumference_need = 0.0;
        for (u32 ni = 0; ni < sg->node_count; ni++) {
            atlas_node_t *n = &ctx->nodes[sg->node_indices[ni]];
            circumference_need += n->r * 2.5 + 18.0;
        }
        f64 max_node_r = ar_schema_max_node_radius(ctx, sg);
        sg->ring_r = circumference_need / (2.0 * ATLAS_PI);
        if (sg->ring_r < ctx->config.schema_ring_r_base * 0.48) {
            sg->ring_r = ctx->config.schema_ring_r_base * 0.48;
        }
        if (sg->ring_r < max_node_r + 28.0) {
            sg->ring_r = max_node_r + 28.0;
        }
        f64 support_r = sg->ring_r + max_node_r + 64.0;
        if (support_r > max_support) {
            max_support = support_r;
        }
    }

    f64 outer_orbit = ar_schema_outer_orbit(ctx);

    f64 required_half = outer_orbit + max_support + SCHEMA_CANVAS_MARGIN + SCHEMA_LABEL_DEPTH;
    if (ctx->config.emit_macro_rings && (f64)ctx->config.canvas_width * 0.5 < required_half) {
        u32 side = (u32)ceil(required_half * 2.0);
        ctx->config.canvas_width = side;
        ctx->config.canvas_height = side;
    }

    f64 cx0 = ctx->config.canvas_width / 2.0;
    f64 cy0 = ctx->config.canvas_height / 2.0;

    for (u32 si = 0; si < ns; si++) {
        atlas_schema_group_t *sg = &ctx->schemas[si];
        f64 schema_angle = 2.0 * ATLAS_PI * si / (f64)ns - ATLAS_PI / 2.0;
        f64 angle_offset = -ATLAS_PI / 2.0 + (f64)si * 0.28;
        sg->cx = cx0 + outer_orbit * cos(schema_angle);
        sg->cy = cy0 + outer_orbit * sin(schema_angle);
        u32 nt = sg->node_count;
        for (u32 ni = 0; ni < nt; ni++) {
            u32 node_idx = sg->node_indices[ni];
            atlas_node_t *n = &ctx->nodes[node_idx];
            f64 angle = angle_offset + 2.0 * ATLAS_PI * ni / (f64)(nt > 1 ? nt : 1);
            f64 ring = sg->ring_r;
            n->cx = sg->cx + ring * cos(angle);
            n->cy = sg->cy + ring * sin(angle);
        }
    }
}

void atlas_render_compute_layout_force(
    atlas_render_ctx_t *ctx,
    u32 iterations,
    f64 temperature_start,
    f64 cooling_rate
) {
    if (!ctx || ctx->node_count == 0) {
        return;
    }
    if (iterations < 1) {
        iterations = 1;
    }
    if (iterations > 10000) {
        iterations = 10000;
    }
    if (temperature_start <= 0.0) {
        temperature_start = 0.5;
    }
    if (cooling_rate <= 0.0) {
        cooling_rate = 0.90;
    }
    if (cooling_rate >= 1.0) {
        cooling_rate = 0.999;
    }
    if (ctx->node_count <= 5) {
        atlas_render_compute_layout(ctx);
        return;
    }

    u32 base_width = ctx->config.canvas_width > 0 ? ctx->config.canvas_width : CANVAS_SINGLE;
    u32 base_height = ctx->config.canvas_height > 0 ? ctx->config.canvas_height : CANVAS_SINGLE;
    atlas_render_compute_layout(ctx);

    if (ctx->config.canvas_width != base_width || ctx->config.canvas_height != base_height) {
        f64 old_width = (f64)ctx->config.canvas_width;
        f64 old_height = (f64)ctx->config.canvas_height;
        f64 old_center_x = old_width * 0.5;
        f64 old_center_y = old_height * 0.5;
        f64 new_center_x = (f64)base_width * 0.5;
        f64 new_center_y = (f64)base_height * 0.5;
        f64 scale_x = old_width > 0.0 ? (f64)base_width / old_width : 1.0;
        f64 scale_y = old_height > 0.0 ? (f64)base_height / old_height : 1.0;
        f64 scale = scale_x < scale_y ? scale_x : scale_y;

        for (u32 i = 0; i < ctx->node_count; i++) {
            ctx->nodes[i].cx = new_center_x + (ctx->nodes[i].cx - old_center_x) * scale;
            ctx->nodes[i].cy = new_center_y + (ctx->nodes[i].cy - old_center_y) * scale;
        }

        ctx->config.canvas_width = base_width;
        ctx->config.canvas_height = base_height;
    }

    u32 n = ctx->node_count;
    f64 width = (f64)ctx->config.canvas_width;
    f64 height = (f64)ctx->config.canvas_height;
    f64 center_x = width * 0.5;
    f64 center_y = height * 0.5;
    f64 canvas_r = (width < height ? width : height) * 0.5 * 0.85;
    f64 area = width * height;
    f64 k = sqrt(area / (f64)(n > 1 ? n : 1));
    f64 alpha_schema = 0.15;
    f64 beta_center = 0.01;
    f64 temperature = temperature_start * canvas_r;

    ar_vec2_t *disp = (ar_vec2_t *)calloc(n, sizeof(ar_vec2_t));
    if (!disp) {
        ar_set_error(ctx, "force layout: displacement allocation failed");
        return;
    }

    for (u32 iter = 0; iter < iterations; iter++) {
        for (u32 i = 0; i < n; i++) {
            disp[i].x = 0.0;
            disp[i].y = 0.0;
        }

        for (u32 i = 0; i < n; i++) {
            for (u32 j = i + 1; j < n; j++) {
                ar_vec2_t delta = {
                    ctx->nodes[i].cx - ctx->nodes[j].cx,
                    ctx->nodes[i].cy - ctx->nodes[j].cy
                };
                f64 d = ar_vec2_len(delta);
                if (d < 1e-6) {
                    delta.x = ((f64)(i + 1) * 0.37) - ((f64)(j + 1) * 0.13);
                    delta.y = ((f64)(i + j + 1) * 0.19);
                    d = ar_vec2_len(delta);
                }
                if (d < 1e-6) {
                    d = 0.1;
                }
                ar_vec2_t dir = ar_vec2_norm(delta);
                f64 f_rep = (k * k) / d;
                disp[i].x += dir.x * f_rep;
                disp[i].y += dir.y * f_rep;
                disp[j].x -= dir.x * f_rep;
                disp[j].y -= dir.y * f_rep;

                if (
                    ctx->nodes[i].schema_group_idx >= 0 &&
                    ctx->nodes[i].schema_group_idx == ctx->nodes[j].schema_group_idx
                ) {
                    f64 f_schema = alpha_schema * d / (k > 1e-6 ? k : 1.0);
                    disp[i].x -= dir.x * f_schema;
                    disp[i].y -= dir.y * f_schema;
                    disp[j].x += dir.x * f_schema;
                    disp[j].y += dir.y * f_schema;
                }
            }
        }

        for (u32 ei = 0; ei < ctx->edge_count; ei++) {
            atlas_edge_t *edge = &ctx->edges[ei];
            if (edge->from_idx < 0 || edge->to_idx < 0) {
                continue;
            }
            u32 from_idx = (u32)edge->from_idx;
            u32 to_idx = (u32)edge->to_idx;
            if (from_idx >= n || to_idx >= n) {
                continue;
            }
            ar_vec2_t delta = {
                ctx->nodes[from_idx].cx - ctx->nodes[to_idx].cx,
                ctx->nodes[from_idx].cy - ctx->nodes[to_idx].cy
            };
            f64 d = ar_vec2_len(delta);
            if (d < 1e-6) {
                continue;
            }
            ar_vec2_t dir = ar_vec2_norm(delta);
            f64 f_att = (d * d) / (k > 1e-6 ? k : 1.0);
            disp[from_idx].x -= dir.x * f_att;
            disp[from_idx].y -= dir.y * f_att;
            disp[to_idx].x += dir.x * f_att;
            disp[to_idx].y += dir.y * f_att;
        }

        for (u32 i = 0; i < n; i++) {
            ar_vec2_t to_center = {
                center_x - ctx->nodes[i].cx,
                center_y - ctx->nodes[i].cy
            };
            f64 d = ar_vec2_len(to_center);
            if (d > 1.0) {
                ar_vec2_t dir = ar_vec2_norm(to_center);
                f64 f_center = beta_center * d;
                disp[i].x += dir.x * f_center;
                disp[i].y += dir.y * f_center;
            }
        }

        for (u32 i = 0; i < n; i++) {
            f64 disp_len = ar_vec2_len(disp[i]);
            if (disp_len > 1e-6) {
                f64 limited = disp_len > temperature ? temperature : disp_len;
                ctx->nodes[i].cx += (disp[i].x / disp_len) * limited;
                ctx->nodes[i].cy += (disp[i].y / disp_len) * limited;
            }

            f64 margin = ctx->nodes[i].r > 0.0 ? ctx->nodes[i].r + 14.0 : 30.0;
            ctx->nodes[i].cx = ar_clamp(ctx->nodes[i].cx, margin, width - margin);
            ctx->nodes[i].cy = ar_clamp(ctx->nodes[i].cy, margin, height - margin);
        }

        temperature *= cooling_rate;
        if (temperature < 0.25) {
            temperature = 0.25;
        }
    }

    free(disp);
    ar_refresh_schema_geometry(ctx);
}

static void ar_emit_stylesheet(atlas_render_ctx_t *ctx, FILE *out) {
    char schema_label_font[64];
    char hash_font[64];
    char db_core_label_font[64];
    char db_core_sub_font[64];
    char schema_core_label_font[64];
    char schema_core_sub_font[64];
    char label_font[64];
    char col_label_font[64];
    char node_metric_font[64];

    ar_scale_font_spec(ctx->config.font_label, FONT_SCALE, schema_label_font, sizeof(schema_label_font));
    ar_scale_font_spec(ctx->config.font_hash, FONT_SCALE, hash_font, sizeof(hash_font));
    ar_scale_font_spec("bold 11px monospace", FONT_SCALE, db_core_label_font, sizeof(db_core_label_font));
    ar_scale_font_spec("8px monospace", FONT_SCALE, db_core_sub_font, sizeof(db_core_sub_font));
    ar_scale_font_spec("8px monospace", FONT_SCALE, schema_core_label_font, sizeof(schema_core_label_font));
    ar_scale_font_spec("7px monospace", FONT_SCALE, schema_core_sub_font, sizeof(schema_core_sub_font));
    ar_scale_font_spec("8px monospace", FONT_SCALE, label_font, sizeof(label_font));
    ar_scale_font_spec("6px monospace", FONT_SCALE, col_label_font, sizeof(col_label_font));
    ar_scale_font_spec("7px monospace", FONT_SCALE, node_metric_font, sizeof(node_metric_font));

    fputs("  <defs>\n    <style><![CDATA[\n", out);
    fprintf(out, "      .bg { fill: #f7f1e5; }\n");
    fprintf(
        out,
        "      .macro-ring { fill: none; stroke: #1f2937; stroke-width: 2.0; opacity: %.2f; stroke-dasharray: %s; }\n",
        ctx->config.ring_opacity,
        (ctx->config.ring_stroke_dash && ctx->config.ring_stroke_dash[0])
            ? ctx->config.ring_stroke_dash
            : "none"
    );
    fprintf(
        out,
        "      .schema-label { fill: #0f172a; font: %s; opacity: 0.75; }\n",
        schema_label_font
    );
    fprintf(
        out,
        "      .hash { fill: #111827; font: %s; letter-spacing: 0.5px; }\n",
        hash_font
    );
    fprintf(
        out,
        "      .foundation-ring { fill: none; stroke: #1f2937; stroke-width: 1.3; opacity: 0.18; }\n"
        "      .db-core-shell { fill: rgba(255,252,246,0.86); stroke: #5a4b3f; stroke-width: 1.0; }\n"
        "      .db-core { fill: #d1fae5; stroke: #0f766e; stroke-width: 1.8; }\n"
        "      .db-core-mark { fill: none; stroke: #0f766e; stroke-width: 0.9; opacity: 0.75; }\n"
        "      .db-core-label { fill: #0f172a; font: %s; }\n"
        "      .db-core-sub { fill: #475569; font: %s; opacity: 0.82; }\n"
        "      .schema-core-shell { fill: rgba(255,252,246,0.82); stroke: #5a4b3f; stroke-width: 0.92; }\n"
        "      .schema-core { fill: #f7efe3; stroke: #57483b; stroke-width: 1.0; }\n"
        "      .schema-core-mark { fill: none; stroke: #8b7763; stroke-width: 0.8; opacity: 0.72; }\n"
        "      .schema-core-label { fill: #0f172a; font: %s; opacity: 0.88; }\n"
        "      .schema-core-sub { fill: #475569; font: %s; opacity: 0.72; }\n"
        "      .base { stroke: #1f1a17; stroke-width: 1.6; fill: none; opacity: 0.7; }\n"
        "      .primary { stroke: #111827; stroke-width: 2.4; fill: none; stroke-linecap: round; }\n"
        "      .call { stroke: #4b5563; stroke-width: 1.5; fill: none; stroke-linecap: round; opacity: 0.82; }\n"
        "      .branch { stroke: #6b7280; stroke-width: 1.2; fill: none; stroke-dasharray: 5 4; opacity: 0.8; }\n"
        "      .node-main { fill: #f8fafc; stroke: #1f2937; stroke-width: 1.65; }\n"
        "      .node-aux  { fill: #f1f5f9; stroke: #475569; stroke-width: 1.15; stroke-dasharray: 4 3; }\n"
        "      .node-loop { fill: #ecfeff; stroke: #0f766e; stroke-width: 1.2; }\n"
        "      .node-fk   { fill: #fef9c3; stroke: #92400e; stroke-width: 1.1; stroke-dasharray: 6 3; }\n"
        "      .node-loop-inner { fill: none; stroke: #0f766e; stroke-width: 0.9; opacity: 0.8; }\n"
        "      .node-shell { fill: none; stroke: #5a4b3f; stroke-width: 1.0; opacity: 0.78; }\n"
        "      .node-foundation { fill: none; stroke: #b7a690; stroke-width: 0.8; opacity: 0.62; }\n"
        "      .col-orbit { fill: none; stroke: #c5b59f; stroke-width: 0.8; opacity: 0.60; }\n"
        "      .col-chord { fill: none; stroke: #7b6a59; stroke-width: 0.72; opacity: 0.52; }\n"
        "      .node-core { fill: #f7efe3; stroke: #57483b; stroke-width: 0.82; }\n"
        "      .col-spoke { stroke: #8d7a68; stroke-width: 0.82; opacity: 0.58; }\n"
        "      .col-pk { fill: #111827; }\n"
        "      .col-fk { fill: #0f766e; }\n"
        "      .col-reg { fill: #64748b; }\n"
        "      .col-nullable { opacity: 0.55; }\n"
        "      .label { fill: #0f172a; font: %s; opacity: 0.68; }\n"
        "      .col-label { fill: #0f172a; font: %s; opacity: 0.78; }\n"
        "      .node-metric { fill: #334155; font: %s; opacity: 0.78; }\n"
        "      .system-node-wrap, .system-edge-wrap, .system-schema-wrap, .system-column-wrap, .system-column-link-wrap { cursor: help; }\n"
        "      .system-node-wrap:hover > circle { opacity: 0.94; }\n"
        "      .system-edge-wrap:hover > line, .system-edge-wrap:hover > path { opacity: 1.0; stroke-width: 2.2; }\n",
        db_core_label_font,
        db_core_sub_font,
        schema_core_label_font,
        schema_core_sub_font,
        label_font,
        col_label_font,
        node_metric_font
    );
    fputs("    ]]></style>\n  </defs>\n", out);
}

static void ar_emit_database_foundation(atlas_render_ctx_t *ctx, FILE *out) {
    f64 cx = (f64)ctx->config.canvas_width * 0.5;
    f64 cy = (f64)ctx->config.canvas_height * 0.5;
    f64 min_side =
        ctx->config.canvas_width < ctx->config.canvas_height ? ctx->config.canvas_width
                                                             : ctx->config.canvas_height;
    f64 max_r = min_side * 0.47;
    f64 rings[] = {max_r * 0.22, max_r * 0.42, max_r * 0.68, max_r};
    f64 core_r = min_side * 0.055;
    if (core_r < 28.0) {
        core_r = 28.0;
    }

    fputs("  <g id=\"database_foundation\">\n", out);
    for (u32 i = 0; i < 4; i++) {
        fprintf(
            out,
            "    <circle class=\"foundation-ring\" cx=\"%.1f\" cy=\"%.1f\" r=\"%.1f\"/>\n",
            cx,
            cy,
            rings[i]
        );
    }
    fprintf(
        out,
        "    <circle class=\"db-core-shell\" cx=\"%.1f\" cy=\"%.1f\" r=\"%.1f\"/>\n",
        cx,
        cy,
        core_r + 8.0
    );
    fprintf(
        out,
        "    <circle class=\"db-core\" cx=\"%.1f\" cy=\"%.1f\" r=\"%.1f\"/>\n",
        cx,
        cy,
        core_r
    );
    fprintf(
        out,
        "    <circle class=\"db-core-mark\" cx=\"%.1f\" cy=\"%.1f\" r=\"%.1f\"/>\n",
        cx,
        cy,
        core_r > 8.0 ? core_r - 8.0 : 8.0
    );
    fprintf(
        out,
        "    <text class=\"db-core-label\" x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\">",
        cx,
        cy - 2.0
    );
    ar_xml_escape(out, ctx->config.core_label);
    fputs("</text>\n", out);
    fprintf(
        out,
        "    <text class=\"db-core-sub\" x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\">",
        cx,
        cy + 12.0
    );
    ar_xml_escape(out, ctx->config.core_subtitle);
    fputs("</text>\n", out);
    fputs("  </g>\n", out);
}

static void ar_emit_macro_rings(atlas_render_ctx_t *ctx, FILE *out) {
    if (!ctx->config.emit_macro_rings) {
        return;
    }
    fputs("  <g id=\"schema_rings\">\n", out);
    for (u32 si = 0; si < ctx->schema_count; si++) {
        atlas_schema_group_t *sg = &ctx->schemas[si];
        f64 max_node_r = ar_schema_max_node_radius(ctx, sg);
        f64 radius = sg->ring_r + max_node_r + 64.0;
        f64 core_r = radius * 0.16;
        if (core_r < 14.0) {
            core_r = 14.0;
        }
        if (core_r > 22.0) {
            core_r = 22.0;
        }
        i64 total_rows = ar_schema_total_rows(ctx, sg);
        u32 total_fk = ar_schema_total_fk_count(ctx, sg);

        fputs("    <g class=\"system-schema-wrap\"", out);
        fprintf(out, " data-schema=\"");
        ar_xml_escape(out, sg->name);
        fprintf(out, "\" data-table-count=\"%u\" data-direct-fk-count=\"%u\" data-total-rows=\"%lld\">\n",
            sg->node_count,
            total_fk,
            (long long)total_rows
        );
        fprintf(
            out,
            "      <circle class=\"macro-ring\" cx=\"%.1f\" cy=\"%.1f\" r=\"%.1f\"/>\n",
            sg->cx,
            sg->cy,
            radius
        );
        fprintf(
            out,
            "      <circle class=\"schema-core-shell\" cx=\"%.1f\" cy=\"%.1f\" r=\"%.1f\"/>\n",
            sg->cx,
            sg->cy,
            core_r + 7.0
        );
        fprintf(
            out,
            "      <circle class=\"schema-core\" cx=\"%.1f\" cy=\"%.1f\" r=\"%.1f\"/>\n",
            sg->cx,
            sg->cy,
            core_r
        );
        fprintf(
            out,
            "      <circle class=\"schema-core-mark\" cx=\"%.1f\" cy=\"%.1f\" r=\"%.1f\"/>\n",
            sg->cx,
            sg->cy,
            core_r > 6.0 ? core_r - 6.0 : 6.0
        );
        fprintf(
            out,
            "      <text class=\"schema-core-label\" x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\">",
            sg->cx,
            sg->cy + 3.0
        );
        ar_xml_escape(out, sg->name);
        fputs("</text>\n", out);
        fprintf(
            out,
            "      <text class=\"schema-label\" x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\">",
            sg->cx,
            sg->cy + radius + 18.0
        );
        ar_xml_escape(out, sg->name);
        fputs("</text>\n", out);
        fprintf(
            out,
            "      <text class=\"schema-core-sub\" x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\">%u tables</text>\n",
            sg->cx,
            sg->cy + radius + 30.0,
            sg->node_count
        );
        if (ctx->config.emit_titles) {
            fputs("      <title>", out);
            ar_xml_escape(out, sg->name);
            fprintf(
                out,
                "\ntables: %u\ndirect_fks: %u\nrows: ~",
                sg->node_count,
                total_fk
            );
            {
                char rows_buf[32];
                ar_fmt_number(rows_buf, sizeof(rows_buf), total_rows);
                ar_xml_escape(out, rows_buf);
            }
            fputs("</title>\n", out);
        }
        fputs("    </g>\n", out);
    }
    fputs("  </g>\n", out);
}

static atlas_node_t *ar_find_node(atlas_render_ctx_t *ctx, const char *id) {
    for (u32 i = 0; i < ctx->node_count; i++) {
        if (ctx->nodes[i].id && strcmp(ctx->nodes[i].id, id) == 0) {
            return &ctx->nodes[i];
        }
    }
    return NULL;
}

static void ar_emit_edges(atlas_render_ctx_t *ctx, FILE *out) {
    fputs("  <g id=\"fk_edges\">\n", out);
    for (u32 ei = 0; ei < ctx->edge_count; ei++) {
        atlas_edge_t *e = &ctx->edges[ei];
        atlas_node_t *from = (e->from_idx >= 0 && (u32)e->from_idx < ctx->node_count)
            ? &ctx->nodes[e->from_idx]
            : ar_find_node(ctx, e->from_id);
        atlas_node_t *to = (e->to_idx >= 0 && (u32)e->to_idx < ctx->node_count)
            ? &ctx->nodes[e->to_idx]
            : ar_find_node(ctx, e->to_id);
        if (!from || !to || from == to) {
            continue;
        }

        const char *edge_class =
            (e->edge_type == ATLAS_EDGE_FK_DECLARED) ? "call" : "branch";
        const char *fk_type =
            (e->edge_type == ATLAS_EDGE_FK_DECLARED) ? "declared" : "inferred";

        fputs("    <g class=\"system-edge-wrap\"", out);
        if (ctx->config.emit_data_attrs) {
            fputs(" data-fk-from=\"", out);
            ar_xml_escape(out, e->from_id);
            fputs("\"", out);
            fputs(" data-fk-to=\"", out);
            ar_xml_escape(out, e->to_id);
            fputs("\"", out);
            if (e->from_column && e->to_column) {
                fputs(" data-fk-columns=\"", out);
                ar_xml_escape(out, e->from_column);
                fputs("->", out);
                ar_xml_escape(out, e->to_column);
                fputs("\"", out);
            }
            fprintf(out, " data-fk-type=\"%s\"", fk_type);
            fprintf(
                out,
                " data-relationship-kind=\"%s\"",
                (e->edge_type == ATLAS_EDGE_FK_DECLARED) ? "direct" : "indirect"
            );
            if (e->on_delete) {
                fputs(" data-on-delete=\"", out);
                ar_xml_escape(out, e->on_delete);
                fputs("\"", out);
            }
        }
        fputs(">\n", out);

        fprintf(
            out,
            "      <line class=\"%s\" x1=\"%.2f\" y1=\"%.2f\" x2=\"%.2f\" y2=\"%.2f\"/>\n",
            edge_class,
            from->cx,
            from->cy,
            to->cx,
            to->cy
        );

        if (ctx->config.emit_titles) {
            fputs("      <title>FK: ", out);
            ar_xml_escape(out, e->from_id);
            if (e->from_column) {
                fputs(".", out);
                ar_xml_escape(out, e->from_column);
            }
            fputs(" -> ", out);
            ar_xml_escape(out, e->to_id);
            if (e->to_column) {
                fputs(".", out);
                ar_xml_escape(out, e->to_column);
            }
            fprintf(out, "\ntype: %s", fk_type);
            if (e->on_delete) {
                fprintf(out, " | ON DELETE %s", e->on_delete);
            }
            fputs("</title>\n", out);
        }
        fputs("    </g>\n", out);
    }
    fputs("  </g>\n", out);
}

static const char *ar_node_css_class(atlas_node_type_t t) {
    switch (t) {
        case ATLAS_NODE_TABLE:
            return "node-main";
        case ATLAS_NODE_VIEW:
            return "node-aux";
        case ATLAS_NODE_MATERIALIZED_VIEW:
            return "node-loop";
        case ATLAS_NODE_FOREIGN_TABLE:
            return "node-fk";
        default:
            return "node-main";
    }
}

static const char *ar_node_type_str(atlas_node_type_t t) {
    switch (t) {
        case ATLAS_NODE_TABLE:
            return "table";
        case ATLAS_NODE_VIEW:
            return "view";
        case ATLAS_NODE_MATERIALIZED_VIEW:
            return "materialized_view";
        case ATLAS_NODE_FOREIGN_TABLE:
            return "foreign_table";
        default:
            return "unknown";
    }
}

static u32 ar_column_band_count(u32 column_count) {
    u32 bands = (column_count + COLUMN_BAND_SIZE - 1) / COLUMN_BAND_SIZE;
    if (bands < 1) {
        bands = 1;
    }
    if (bands > 3) {
        bands = 3;
    }
    return bands;
}

static f64 ar_column_band_radius(f64 node_r, u32 band) {
    f64 start = node_r * 0.36;
    if (start < 16.0) {
        start = 16.0;
    }
    f64 step = node_r * 0.13;
    if (step < 10.0) {
        step = 10.0;
    }
    return start + step * band;
}

static void ar_emit_table_internals(atlas_node_t *n, FILE *out) {
    f64 shell_r = n->r > 6.0 ? n->r - 6.0 : 12.0;
    f64 inner_r = n->r * 0.76;
    if (inner_r < 12.0) {
        inner_r = 12.0;
    }
    f64 core_r = n->r * 0.16;
    if (core_r < 6.0) {
        core_r = 6.0;
    }
    fprintf(
        out,
        "      <circle class=\"node-shell\" cx=\"%.2f\" cy=\"%.2f\" r=\"%.2f\"/>\n",
        n->cx,
        n->cy,
        shell_r
    );
    fprintf(
        out,
        "      <ellipse class=\"node-foundation\" cx=\"%.2f\" cy=\"%.2f\" rx=\"%.2f\" ry=\"%.2f\"/>\n",
        n->cx,
        n->cy,
        inner_r,
        inner_r * 0.74 > 8.0 ? inner_r * 0.74 : 8.0
    );
    fprintf(
        out,
        "      <circle class=\"node-core\" cx=\"%.2f\" cy=\"%.2f\" r=\"%.2f\"/>\n",
        n->cx,
        n->cy,
        core_r
    );
}

static void ar_emit_column_sigils(atlas_render_ctx_t *ctx, atlas_node_t *n, FILE *out) {
    if (n->column_count == 0) {
        return;
    }

    u32 band_count = ar_column_band_count(n->column_count);
    for (u32 band = 0; band < band_count; band++) {
        fprintf(
            out,
            "      <circle class=\"col-orbit\" cx=\"%.2f\" cy=\"%.2f\" r=\"%.2f\"/>\n",
            n->cx,
            n->cy,
            ar_column_band_radius(n->r, band)
        );
    }

    for (u32 band = 0; band < band_count; band++) {
        u32 start = band * COLUMN_BAND_SIZE;
        if (start >= n->column_count) {
            break;
        }
        u32 count = n->column_count - start;
        if (count > COLUMN_BAND_SIZE) {
            count = COLUMN_BAND_SIZE;
        }
        f64 radius = ar_column_band_radius(n->r, band);
        for (u32 idx = 0; idx < count; idx++) {
            f64 angle = 2.0 * ATLAS_PI * idx / (f64)(count > 0 ? count : 1) - ATLAS_PI / 2.0;
            f64 x = n->cx + radius * cos(angle);
            f64 y = n->cy + radius * sin(angle);
            u32 next_idx = (idx + 1) % count;
            f64 next_angle = 2.0 * ATLAS_PI * next_idx / (f64)(count > 0 ? count : 1) - ATLAS_PI / 2.0;
            f64 x2 = n->cx + radius * cos(next_angle);
            f64 y2 = n->cy + radius * sin(next_angle);
            atlas_column_desc_t *left_col = &n->columns[start + idx];
            atlas_column_desc_t *right_col = &n->columns[start + next_idx];
            fputs("    <g class=\"system-column-link-wrap\"", out);
            fputs(" data-table=\"", out);
            ar_xml_escape(out, n->name);
            fputs("\" data-schema=\"", out);
            ar_xml_escape(out, n->schema);
            fputs("\" data-column-left=\"", out);
            ar_xml_escape(out, left_col->name);
            fputs("\" data-column-right=\"", out);
            ar_xml_escape(out, right_col->name);
            fputs("\">\n", out);
            fprintf(
                out,
                "      <path class=\"col-chord\" d=\"M %.2f %.2f Q %.2f %.2f %.2f %.2f\"/>\n",
                x,
                y,
                (n->cx + x2) * 0.5,
                (n->cy + y2) * 0.5,
                x2,
                y2
            );
            if (ctx->config.emit_titles) {
                fputs("      <title>", out);
                ar_xml_escape(out, n->schema);
                fputs(".", out);
                ar_xml_escape(out, n->name);
                fputs("\n", out);
                ar_xml_escape(out, left_col->name);
                fputs(" ↔ ", out);
                ar_xml_escape(out, right_col->name);
                fputs("</title>\n", out);
            }
            fputs("    </g>\n", out);
        }
    }

    for (u32 ci = 0; ci < n->column_count; ci++) {
        atlas_column_desc_t *col = &n->columns[ci];
        u32 band = ci / COLUMN_BAND_SIZE;
        if (band >= band_count) {
            band = band_count - 1;
        }
        u32 within_band = ci % COLUMN_BAND_SIZE;
        u32 total_in_band = n->column_count - band * COLUMN_BAND_SIZE;
        if (total_in_band > COLUMN_BAND_SIZE) {
            total_in_band = COLUMN_BAND_SIZE;
        }
        f64 angle = 2.0 * ATLAS_PI * within_band / (f64)(total_in_band > 0 ? total_in_band : 1)
            - ATLAS_PI / 2.0;
        f64 radius = ar_column_band_radius(n->r, band);
        f64 x = n->cx + radius * cos(angle);
        f64 y = n->cy + radius * sin(angle);
        f64 label_radius = radius - 12.0;
        if (label_radius < n->r * 0.18) {
            label_radius = n->r * 0.18;
        }
        f64 label_x = n->cx + label_radius * cos(angle);
        f64 label_y = n->cy + label_radius * sin(angle) + 2.0;
        const char *dot_class = col->is_pk ? "col-pk" : (col->is_fk ? "col-fk" : "col-reg");
        fputs("    <g class=\"system-column-wrap\"", out);
        fputs(" data-table=\"", out);
        ar_xml_escape(out, n->name);
        fputs("\" data-schema=\"", out);
        ar_xml_escape(out, n->schema);
        fputs("\" data-column-name=\"", out);
        ar_xml_escape(out, col->name);
        fputs("\" data-column-type=\"", out);
        ar_xml_escape(out, col->type_str ? col->type_str : "unknown");
        fputs("\" data-column-flags=\"", out);
        ar_emit_column_flags(out, col);
        fputs("\" data-column-role=\"", out);
        fputs(col->is_pk ? "primary_key" : (col->is_fk ? "foreign_key" : "column"), out);
        fputs("\">\n", out);
        fprintf(
            out,
            "      <line class=\"col-spoke\" x1=\"%.2f\" y1=\"%.2f\" x2=\"%.2f\" y2=\"%.2f\"/>\n",
            n->cx,
            n->cy,
            x,
            y
        );
        fprintf(
            out,
            "      <circle class=\"%s%s\" cx=\"%.2f\" cy=\"%.2f\" r=\"%.1f\"/>\n",
            dot_class,
            (col->is_nullable && !col->is_pk) ? " col-nullable" : "",
            x,
            y,
            COL_DOT_RADIUS
        );
        if (band == 0 && (col->is_pk || col->is_fk)) {
            fprintf(
                out,
                "      <text class=\"col-label\" x=\"%.2f\" y=\"%.2f\" text-anchor=\"middle\">",
                label_x,
                label_y
            );
            ar_xml_escape(out, ar_short_label(col->name));
            fputs("</text>\n", out);
        }
        if (ctx->config.emit_titles) {
            fputs("      <title>", out);
            ar_xml_escape(out, n->schema);
            fputs(".", out);
            ar_xml_escape(out, n->name);
            fputs("\n", out);
            ar_xml_escape(out, col->name);
            fputs(": ", out);
            ar_xml_escape(out, col->type_str ? col->type_str : "unknown");
            fputs("\n", out);
            ar_emit_column_flags(out, col);
            fputs("</title>\n", out);
        }
        fputs("    </g>\n", out);
    }
}

static void ar_emit_nodes(atlas_render_ctx_t *ctx, FILE *out) {
    char row_buf[32];
    char size_buf[32];

    fputs("  <g id=\"table_nodes\">\n", out);
    for (u32 ni = 0; ni < ctx->node_count; ni++) {
        atlas_node_t *n = &ctx->nodes[ni];
        const char *css = ar_node_css_class(n->node_type);
        const char *type_str = ar_node_type_str(n->node_type);

        ar_fmt_number(row_buf, sizeof(row_buf), n->row_estimate);
        ar_fmt_bytes(size_buf, sizeof(size_buf), n->size_bytes);

        fputs("    <g class=\"system-node-wrap\"", out);
        if (ctx->config.emit_data_attrs) {
            fputs(" data-table=\"", out);
            ar_xml_escape(out, n->name);
            fputs("\"", out);
            fputs(" data-schema=\"", out);
            ar_xml_escape(out, n->schema);
            fputs("\"", out);
            fprintf(out, " data-row-estimate=\"%lld\"", (long long)n->row_estimate);
            fprintf(out, " data-size-bytes=\"%lld\"", (long long)n->size_bytes);
            fprintf(out, " data-column-count=\"%u\"", n->column_count);
            fprintf(out, " data-fk-count=\"%u\"", n->fk_count);
            fprintf(out, " data-index-count=\"%u\"", n->index_count);
            fprintf(out, " data-table-type=\"%s\"", type_str);
            ar_emit_columns_detail_attr(out, n);
            if (n->comment) {
                fputs(" data-comment=\"", out);
                ar_xml_escape(out, n->comment);
                fputs("\"", out);
            }
        }
        fputs(">\n", out);

        fprintf(
            out,
            "      <circle class=\"%s\" cx=\"%.2f\" cy=\"%.2f\" r=\"%.2f\"/>\n",
            css,
            n->cx,
            n->cy,
            n->r
        );
        if (n->node_type == ATLAS_NODE_MATERIALIZED_VIEW) {
            fprintf(
                out,
                "      <circle class=\"node-loop-inner\" cx=\"%.2f\" cy=\"%.2f\" r=\"%.2f\"/>\n",
                n->cx,
                n->cy,
                n->r > 4.0 ? (n->r - 4.0) : 2.0
            );
        }
        if (ctx->config.emit_titles) {
            fputs("      <title>", out);
            ar_xml_escape(out, n->name);
            fputs("\nschema: ", out);
            ar_xml_escape(out, n->schema);
            fprintf(out, "\nrows: ~%s\nsize: %s", row_buf, size_buf);
            fprintf(out, "\ncolumns: %u", n->column_count);
            if (n->comment) {
                fputs("\n", out);
                ar_xml_escape(out, n->comment);
            }
            if (n->column_count > 0) {
                fputs("\n\n", out);
                u32 preview = n->column_count < 10 ? n->column_count : 10;
                for (u32 ci = 0; ci < preview; ci++) {
                    atlas_column_desc_t *col = &n->columns[ci];
                    fputs("- ", out);
                    ar_xml_escape(out, col->name);
                    fputs(": ", out);
                    ar_xml_escape(out, col->type_str ? col->type_str : "unknown");
                    fputs(" [", out);
                    ar_emit_column_flags(out, col);
                    fputs("]", out);
                    if (ci + 1 < preview) {
                        fputs("\n", out);
                    }
                }
                if (n->column_count > preview) {
                    fprintf(out, "\n+%u more columns", n->column_count - preview);
                }
            }
            fputs("</title>\n", out);
        }

        fputs("    </g>\n", out);
        ar_emit_table_internals(n, out);
        if (ctx->config.emit_column_dots) {
            ar_emit_column_sigils(ctx, n, out);
        }
        fprintf(
            out,
            "    <text class=\"hash\" x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\">",
            n->cx,
            n->cy + n->r + 14.0
        );
        ar_xml_escape(out, n->name);
        fputs("</text>\n", out);
    }
    fputs("  </g>\n", out);
}

bool atlas_render_svg(atlas_render_ctx_t *ctx, FILE *out) {
    if (!ctx || !out) {
        return false;
    }

    u32 w = ctx->config.canvas_width;
    u32 h = ctx->config.canvas_height;

    fprintf(
        out,
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"%u\" height=\"%u\" viewBox=\"0 0 %u %u\" role=\"img\" aria-label=\"Atlas database sigilo\">\n"
        "  <desc>Atlas Datamap - navigable database sigilo.</desc>\n",
        w,
        h,
        w,
        h
    );

    ar_emit_stylesheet(ctx, out);
    fprintf(out, "  <rect class=\"bg\" width=\"%u\" height=\"%u\"/>\n", w, h);
    ar_emit_database_foundation(ctx, out);
    ar_emit_macro_rings(ctx, out);
    ar_emit_edges(ctx, out);
    ar_emit_nodes(ctx, out);
    fputs("</svg>\n", out);
    return !ctx->had_error;
}

char *atlas_render_svg_to_buffer(atlas_render_ctx_t *ctx, size_t *out_len) {
    if (!ctx) {
        return NULL;
    }

    char *buf = NULL;
    size_t len = 0;
    FILE *stream = tmpfile();
    if (!stream) {
        ar_set_error(ctx, "tmpfile unavailable");
        return NULL;
    }

    bool ok = atlas_render_svg(ctx, stream);
    if (!ok) {
        fclose(stream);
        return NULL;
    }

    if (fflush(stream) != 0) {
        fclose(stream);
        ar_set_error(ctx, "failed to flush svg stream");
        return NULL;
    }
    if (fseek(stream, 0, SEEK_END) != 0) {
        fclose(stream);
        ar_set_error(ctx, "failed to seek svg stream");
        return NULL;
    }
    long end_pos = ftell(stream);
    if (end_pos < 0) {
        fclose(stream);
        ar_set_error(ctx, "failed to measure svg stream");
        return NULL;
    }
    len = (size_t)end_pos;
    if (fseek(stream, 0, SEEK_SET) != 0) {
        fclose(stream);
        ar_set_error(ctx, "failed to rewind svg stream");
        return NULL;
    }

    buf = (char *)malloc(len + 1);
    if (!buf) {
        fclose(stream);
        ar_set_error(ctx, "failed to allocate svg buffer");
        return NULL;
    }
    if (len > 0) {
        size_t bytes_read = fread(buf, 1, len, stream);
        if (bytes_read != len) {
            free(buf);
            fclose(stream);
            ar_set_error(ctx, "failed to read svg stream");
            return NULL;
        }
    }
    buf[len] = '\0';
    fclose(stream);

    if (out_len) {
        *out_len = len;
    }
    return buf;
}

void atlas_render_free_buffer(char *ptr) {
    free(ptr);
}

const char *atlas_render_version(void) {
    return ATLAS_RENDER_VERSION_STR;
}
