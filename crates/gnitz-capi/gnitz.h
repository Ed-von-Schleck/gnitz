#ifndef GNITZ_H
#define GNITZ_H

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
typedef struct GnitzConn           GnitzConn;
typedef struct GnitzSchema         GnitzSchema;
typedef struct GnitzBatch          GnitzBatch;
typedef struct GnitzExprBuilder    GnitzExprBuilder;
typedef struct GnitzExprProgram    GnitzExprProgram;
typedef struct GnitzCircuitBuilder GnitzCircuitBuilder;
typedef struct GnitzCircuit        GnitzCircuit;


#define GNITZ_TYPE_U8 1

#define GNITZ_TYPE_I8 2

#define GNITZ_TYPE_U16 3

#define GNITZ_TYPE_I16 4

#define GNITZ_TYPE_U32 5

#define GNITZ_TYPE_I32 6

#define GNITZ_TYPE_F32 7

#define GNITZ_TYPE_U64 8

#define GNITZ_TYPE_I64 9

#define GNITZ_TYPE_F64 10

#define GNITZ_TYPE_STRING 11

#define GNITZ_TYPE_U128 12

const char *gnitz_last_error(void);

GnitzConn *gnitz_connect(const char *socket_path);

void gnitz_disconnect(GnitzConn *conn);

GnitzSchema *gnitz_schema_new(uint32_t pk_index);

int gnitz_schema_add_col(GnitzSchema *schema, const char *name, int type_code, int nullable);

uint32_t gnitz_schema_col_count(const GnitzSchema *schema);

void gnitz_schema_free(GnitzSchema *schema);

GnitzBatch *gnitz_batch_new(const GnitzSchema *schema);

int gnitz_batch_append_row(GnitzBatch *batch,
                           uint64_t pk_lo,
                           uint64_t pk_hi,
                           int64_t weight,
                           uint64_t null_mask,
                           const void *col_data,
                           size_t col_data_len);

int gnitz_batch_set_string(GnitzBatch *batch, size_t col_idx, const char *value);

size_t gnitz_batch_len(const GnitzBatch *batch);

uint64_t gnitz_batch_get_pk_lo(const GnitzBatch *batch, size_t row);

uint64_t gnitz_batch_get_pk_hi(const GnitzBatch *batch, size_t row);

int64_t gnitz_batch_get_weight(const GnitzBatch *batch, size_t row);

int64_t gnitz_batch_get_i64(const GnitzBatch *batch, size_t col_idx, size_t row);

const char *gnitz_batch_get_string(const GnitzBatch *batch, size_t col_idx, size_t row);

void gnitz_batch_free(GnitzBatch *batch);

uint64_t gnitz_alloc_table_id(GnitzConn *conn);

uint64_t gnitz_alloc_schema_id(GnitzConn *conn);

uint64_t gnitz_create_schema(GnitzConn *conn, const char *name);

int gnitz_drop_schema(GnitzConn *conn, const char *name);

uint64_t gnitz_create_table(GnitzConn *conn,
                            const char *schema_name,
                            const char *table_name,
                            const GnitzSchema *schema,
                            int unique_pk);

int gnitz_drop_table(GnitzConn *conn, const char *schema_name, const char *table_name);

int gnitz_push(GnitzConn *conn,
               uint64_t table_id,
               const GnitzSchema *schema,
               const GnitzBatch *batch);

GnitzBatch *gnitz_scan(GnitzConn *conn, uint64_t table_id, const GnitzSchema *schema);

int gnitz_delete(GnitzConn *conn,
                 uint64_t table_id,
                 const GnitzSchema *schema,
                 const uint64_t *pks_lo,
                 const uint64_t *pks_hi,
                 size_t n_rows);

uint64_t gnitz_create_view(GnitzConn *conn,
                           const char *schema_name,
                           const char *view_name,
                           uint64_t source_table_id,
                           const GnitzSchema *output_schema);

uint64_t gnitz_create_view_with_circuit(GnitzConn *conn,
                                        const char *schema_name,
                                        const char *view_name,
                                        GnitzCircuit *circuit,
                                        const GnitzSchema *output_schema);

int gnitz_drop_view(GnitzConn *conn, const char *schema_name, const char *view_name);

GnitzExprBuilder *gnitz_expr_new(void);

uint32_t gnitz_expr_load_col_int(GnitzExprBuilder *b, uint32_t col_idx);

uint32_t gnitz_expr_load_col_float(GnitzExprBuilder *b, uint32_t col_idx);

uint32_t gnitz_expr_load_const(GnitzExprBuilder *b, int64_t value);

uint32_t gnitz_expr_add(GnitzExprBuilder *b, uint32_t a, uint32_t r);

uint32_t gnitz_expr_sub(GnitzExprBuilder *b, uint32_t a, uint32_t r);

uint32_t gnitz_expr_cmp_eq(GnitzExprBuilder *b, uint32_t a, uint32_t r);

uint32_t gnitz_expr_cmp_ne(GnitzExprBuilder *b, uint32_t a, uint32_t r);

uint32_t gnitz_expr_cmp_gt(GnitzExprBuilder *b, uint32_t a, uint32_t r);

uint32_t gnitz_expr_cmp_ge(GnitzExprBuilder *b, uint32_t a, uint32_t r);

uint32_t gnitz_expr_cmp_lt(GnitzExprBuilder *b, uint32_t a, uint32_t r);

uint32_t gnitz_expr_cmp_le(GnitzExprBuilder *b, uint32_t a, uint32_t r);

uint32_t gnitz_expr_bool_and(GnitzExprBuilder *b, uint32_t a, uint32_t r);

uint32_t gnitz_expr_bool_or(GnitzExprBuilder *b, uint32_t a, uint32_t r);

uint32_t gnitz_expr_bool_not(GnitzExprBuilder *b, uint32_t a);

uint32_t gnitz_expr_is_null(GnitzExprBuilder *b, uint32_t col_idx);

uint32_t gnitz_expr_is_not_null(GnitzExprBuilder *b, uint32_t col_idx);

GnitzExprProgram *gnitz_expr_build(GnitzExprBuilder *builder, uint32_t result_reg);

void gnitz_expr_builder_free(GnitzExprBuilder *b);

void gnitz_expr_program_free(GnitzExprProgram *p);

GnitzCircuitBuilder *gnitz_circuit_new(uint64_t view_id, uint64_t primary_source_id);

uint64_t gnitz_circuit_input_delta(GnitzCircuitBuilder *cb);

uint64_t gnitz_circuit_trace_scan(GnitzCircuitBuilder *cb, uint64_t table_id);

uint64_t gnitz_circuit_filter(GnitzCircuitBuilder *cb, uint64_t input, GnitzExprProgram *expr);

uint64_t gnitz_circuit_map(GnitzCircuitBuilder *cb,
                           uint64_t input,
                           const size_t *projection,
                           size_t n_cols);

uint64_t gnitz_circuit_negate(GnitzCircuitBuilder *cb, uint64_t input);

uint64_t gnitz_circuit_union(GnitzCircuitBuilder *cb, uint64_t a, uint64_t b);

uint64_t gnitz_circuit_delay(GnitzCircuitBuilder *cb, uint64_t input);

uint64_t gnitz_circuit_distinct(GnitzCircuitBuilder *cb, uint64_t input);

uint64_t gnitz_circuit_join(GnitzCircuitBuilder *cb, uint64_t delta, uint64_t trace_table_id);

uint64_t gnitz_circuit_anti_join(GnitzCircuitBuilder *cb, uint64_t delta, uint64_t trace_table_id);

uint64_t gnitz_circuit_semi_join(GnitzCircuitBuilder *cb, uint64_t delta, uint64_t trace_table_id);

uint64_t gnitz_circuit_reduce(GnitzCircuitBuilder *cb,
                              uint64_t input,
                              const size_t *group_cols,
                              size_t n_group_cols,
                              uint64_t agg_func_id,
                              size_t agg_col_idx);

uint64_t gnitz_circuit_shard(GnitzCircuitBuilder *cb,
                             uint64_t input,
                             const size_t *shard_cols,
                             size_t n_shard_cols);

uint64_t gnitz_circuit_gather(GnitzCircuitBuilder *cb, uint64_t input);

uint64_t gnitz_circuit_sink(GnitzCircuitBuilder *cb, uint64_t input);

GnitzCircuit *gnitz_circuit_build(GnitzCircuitBuilder *cb);

void gnitz_circuit_builder_free(GnitzCircuitBuilder *cb);

void gnitz_circuit_free(GnitzCircuit *c);

int gnitz_seek(GnitzConn *conn,
               uint64_t table_id,
               uint64_t pk_lo,
               uint64_t pk_hi,
               GnitzBatch **out_batch);

int gnitz_seek_by_index(GnitzConn *conn,
                        uint64_t table_id,
                        uint64_t col_idx,
                        uint64_t key_lo,
                        uint64_t key_hi,
                        GnitzBatch **out_batch);

int gnitz_execute_sql(GnitzConn *conn, const char *sql, const char *schema, uint64_t *out_id);

void gnitz_free_string(char *s);

#endif  /* GNITZ_H */
