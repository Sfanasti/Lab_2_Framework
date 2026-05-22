/*
 multi.c - Esempio addendum (Fase 6): due mr_t indipendenti nello stesso
 processo, con configurazioni diverse (thread, file di log/stats distinti).

 Nessuno stato globale nel framework => i due handle non interferiscono.
 Dato che l'output non dipende dal grado di parallelismo, i due .mro
 prodotti devono essere identici byte-per-byte.

 Uso: multi <input> <output_prefix>
 produce <prefix>.1.mro (2+2 thread) e <prefix>.2.mro (8+8 thread).
 */

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mr.h"

// Mapper minimo: tokenizza e emette <token, (int)1>.
static int wc_mapper(const mr_file_line_t *line, mr_emit_pair_t emit,
                     void *emit_arg, void *user_arg) {
    (void)user_arg;
    const char *s = line->line;
    size_t n = line->line_len, i = 0;
    while (i < n) {
        if (!isalnum((unsigned char)s[i])) { i++; continue; }
        size_t start = i;
        while (i < n && isalnum((unsigned char)s[i])) i++;
        size_t len = i - start;
        char *tok = malloc(len + 1);
        if (tok == NULL) return -1;
        memcpy(tok, s + start, len);
        tok[len] = '\0';
        int one = 1;
        int rc = emit(tok, &one, sizeof one, emit_arg);
        free(tok);
        if (rc != 0) return -1;
    }
    return 0;
}

// Reducer minimo: somma gli interi del gruppo.
static int wc_reducer(const char *token, const mr_value_t *values,
                      size_t values_count, mr_emit_result_t emit,
                      void *emit_arg, void *user_arg) {
    (void)user_arg;
    long sum = 0;
    for (size_t i = 0; i < values_count; i++) {
        if (values[i].size != sizeof(int)) continue;
        int v;
        memcpy(&v, values[i].data, sizeof v);
        sum += v;
    }
    int total = (int)sum;
    return emit(token, &total, sizeof total, emit_arg);
}

static int run_one(const char *in, const char *out,
                   size_t m, size_t r, const char *log) {
    mr_attr_t a;
    mr_t h;
    if (mr_attr_init(&a) == -1) return -1;
    mr_attr_set_mapper_threads(&a, m);
    mr_attr_set_reducer_threads(&a, r);
    mr_attr_set_log_file(&a, log); // il file .stats e' derivato da qui
    if (mr_create(&h, &a, wc_mapper, wc_reducer, NULL) == -1) {
        mr_attr_destroy(&a);
        return -1;
    }
    int rc = mr_start(h, in, out);
    mr_destroy(h);
    mr_attr_destroy(&a);
    return rc;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "uso: %s <input> <output_prefix>\n", argv[0]);
        return 2;
    }
    char o1[1024], o2[1024];
    snprintf(o1, sizeof o1, "%s.1.mro", argv[2]);
    snprintf(o2, sizeof o2, "%s.2.mro", argv[2]);

    // due handle, config e file diversi: nessuna interferenza (zero globali)
    int r1 = run_one(argv[1], o1, 2, 2, "mr.1.log");
    int r2 = run_one(argv[1], o2, 8, 8, "mr.2.log");

    printf("run1=%d run2=%d\n", r1, r2);
    return (r1 == 0 && r2 == 0) ? 0 : 1;
}
