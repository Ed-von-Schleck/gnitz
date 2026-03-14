/*
 * gnitz-capi smoke test.
 *
 * Usage:  c_test <socket_path>
 *
 * Tests: connect → create schema/table → push 10 rows → scan → verify
 *        count and values → drop table → drop schema.
 *        Prints "OK" on success, exits 1 on any failure.
 *
 * Build (after `cargo build -p gnitz-capi`):
 *   cc c_test.c \
 *       -I ../../gnitz-capi \
 *       -L ../../target/debug \
 *       -lgnitz_capi -lpthread -ldl \
 *       -o c_test
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "gnitz.h"

/* ---- helpers ------------------------------------------------------------ */

static void die(const char *msg, GnitzConn *conn) {
    const char *err = gnitz_last_error();
    fprintf(stderr, "FAIL: %s: %s\n", msg, err ? err : "(no error)");
    gnitz_disconnect(conn);
    exit(1);
}

#define CHECK(expr, msg, conn)  do { if (!(expr)) die((msg), (conn)); } while (0)
#define CHECKEQ(a, b, msg, conn) \
    do { if ((a) != (b)) { \
        fprintf(stderr, "FAIL: %s: expected %lld, got %lld\n", \
                (msg), (long long)(b), (long long)(a)); \
        gnitz_disconnect(conn); exit(1); \
    } } while (0)

/* ---- main --------------------------------------------------------------- */

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <socket_path>\n", argv[0]);
        return 1;
    }
    const char *sock = argv[1];

    /* 1. Connect */
    GnitzConn *conn = gnitz_connect(sock);
    CHECK(conn != NULL, "gnitz_connect", NULL);

    /* 2. Create logical schema */
    uint64_t sid = gnitz_create_schema(conn, "ctest");
    CHECK(sid != 0, "gnitz_create_schema", conn);

    /* 3. Build table schema: col 0 = id:U64 (pk), col 1 = val:I64 */
    GnitzSchema *sch = gnitz_schema_new(0 /* pk_index */);
    CHECK(sch != NULL, "gnitz_schema_new", conn);
    CHECK(gnitz_schema_add_col(sch, "id",  GNITZ_TYPE_U64, 0) == 0,
          "add col id",  conn);
    CHECK(gnitz_schema_add_col(sch, "val", GNITZ_TYPE_I64, 0) == 0,
          "add col val", conn);
    CHECKEQ((int)gnitz_schema_col_count(sch), 2, "col_count", conn);

    /* 4. Create table */
    uint64_t tid = gnitz_create_table(conn, "ctest", "nums", sch, 1 /* unique_pk */);
    CHECK(tid != 0, "gnitz_create_table", conn);

    /* 5. Push 10 rows: id=i (U64), val=i*10 (I64), for i=1..10 */
    GnitzBatch *push_batch = gnitz_batch_new(sch);
    CHECK(push_batch != NULL, "gnitz_batch_new", conn);

    for (uint64_t i = 1; i <= 10; i++) {
        int64_t val = (int64_t)i * 10;
        CHECK(gnitz_batch_append_row(push_batch,
                                     i,           /* pk_lo */
                                     0,           /* pk_hi */
                                     1,           /* weight */
                                     0,           /* null_mask */
                                     &val,        /* col_data (non-pk cols) */
                                     sizeof(val)) == 0,
              "batch_append_row", conn);
    }
    CHECKEQ((int)gnitz_batch_len(push_batch), 10, "push batch len", conn);

    CHECK(gnitz_push(conn, tid, sch, push_batch) == 0, "gnitz_push", conn);
    gnitz_batch_free(push_batch);
    push_batch = NULL;

    /* 6. Scan back */
    GnitzBatch *result = gnitz_scan(conn, tid, sch);
    CHECK(result != NULL, "gnitz_scan", conn);

    size_t n = gnitz_batch_len(result);
    CHECKEQ((long long)n, 10LL, "row count", conn);

    /* 7. Verify pk_lo values are in 1..10 and val == pk*10 */
    /* Collect (pk, val) pairs and sort to handle any server ordering. */
    uint64_t got_pk[10];
    int64_t  got_val[10];
    for (size_t r = 0; r < n; r++) {
        got_pk[r]  = gnitz_batch_get_pk_lo(result, r);
        got_val[r] = gnitz_batch_get_i64(result, 1 /* col_idx */, r);
        CHECK(gnitz_last_error() == NULL, "get_i64 error", conn);
    }
    /* Simple selection-sort to normalize ordering */
    for (size_t i = 0; i < n - 1; i++) {
        for (size_t j = i + 1; j < n; j++) {
            if (got_pk[j] < got_pk[i]) {
                uint64_t tmpu = got_pk[i]; got_pk[i] = got_pk[j]; got_pk[j] = tmpu;
                int64_t  tmps = got_val[i]; got_val[i] = got_val[j]; got_val[j] = tmps;
            }
        }
    }
    for (size_t r = 0; r < n; r++) {
        if (got_pk[r] != (uint64_t)(r + 1)) {
            fprintf(stderr, "FAIL: pk[%zu] expected %llu got %llu\n",
                    r, (unsigned long long)(r+1), (unsigned long long)got_pk[r]);
            gnitz_disconnect(conn); exit(1);
        }
        CHECKEQ(got_val[r],       (int64_t)(r + 1) * 10,   "val value", conn);
        CHECKEQ(gnitz_batch_get_weight(result, r), 1LL, "weight", conn);
    }

    gnitz_batch_free(result);
    result = NULL;

    /* 8. Drop table and schema */
    CHECK(gnitz_drop_table(conn, "ctest", "nums") == 0, "gnitz_drop_table", conn);
    CHECK(gnitz_drop_schema(conn, "ctest") == 0, "gnitz_drop_schema", conn);

    /* 9. Clean up */
    gnitz_schema_free(sch);
    gnitz_disconnect(conn);

    puts("OK");
    return 0;
}
