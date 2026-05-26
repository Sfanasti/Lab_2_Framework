/*
 * wordcount.c - Esempio d'uso del framework libmr.
 *
 * Conta le occorrenze di ogni token (sequenza di caratteri alfanumerici)
 * in un file o in una directory di file.
 *
 *   mapper : per ogni token nella riga emette la coppia <token, 1>
 *   reducer: somma tutti gli interi associati a uno stesso token
 *
 * Nota: la logica del word count vive INTERAMENTE qui. Il framework non
 * sa nulla di parole o di somme: trasporta soltanto byte opachi.
 *
 * Uso:  wordcount <input> <output.mro>
 */

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mr.h"

/* ------------------------------------------------------------------ */
/* Mapper: tokenizza la riga ed emette <token, (int)1> per ogni token. */
/* ------------------------------------------------------------------ */
static int wc_mapper(const mr_file_line_t *line, mr_emit_pair_t emit,
                     void *emit_arg, void *user_arg) {
    (void)user_arg;

    const char *s = line->line;
    size_t      n = line->line_len;
    size_t      i = 0;

    while (i < n) {
        /* salta i caratteri non alfanumerici (separatori) */
        if (!isalnum((unsigned char)s[i])) { i++; continue; }

        /* un token e' una sequenza massimale di caratteri alfanumerici */
        size_t start = i;
        while (i < n && isalnum((unsigned char)s[i])) i++;
        size_t len = i - start;

        char *tok = malloc(len + 1);
        if (tok == NULL) return -1;
        memcpy(tok, s + start, len);
        tok[len] = '\0';

        int one = 1; /* il valore "1" come byte opachi (sizeof(int) byte) */
        int rc = emit(tok, &one, sizeof one, emit_arg);
        free(tok);
        if (rc != 0) return -1; /* il framework copia prima di ritornare */
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* Reducer: somma gli interi del gruppo ed emette <token, (int)somma>.  */
/* ------------------------------------------------------------------ */
static int wc_reducer(const char *token, const mr_value_t *values,
                      size_t values_count, mr_emit_result_t emit,
                      void *emit_arg, void *user_arg) {
    (void)user_arg;

    long sum = 0;
    for (size_t i = 0; i < values_count; i++) {
        if (values[i].size != sizeof(int)) continue; /* difensivo */
        int v;
        memcpy(&v, values[i].data, sizeof v); /* opaco: sempre memcpy */
        sum += v;
    }

    int total = (int)sum;
    return emit(token, &total, sizeof total, emit_arg);
}

/* ------------------------------------------------------------------ */
int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "uso: %s <input> <output.mro>\n", argv[0]);
        return 2;
    }

    mr_t      mr;
    mr_attr_t attr;

    if (mr_attr_init(&attr) == -1) { perror("mr_attr_init"); return 1; }

    /* In Fase 1 thread e queue sono ignorati, ma li impostiamo lo stesso:
       l'interfaccia non cambiera' nelle fasi successive. */
    mr_attr_set_mapper_threads(&attr, 4);
    mr_attr_set_reducer_threads(&attr, 4);
    mr_attr_set_queue_size(&attr, 64);

    if (mr_create(&mr, &attr, wc_mapper, wc_reducer, NULL) == -1) {
        perror("mr_create");
        mr_attr_destroy(&attr);
        return 1;
    }

    if (mr_start(mr, argv[1], argv[2]) == -1) {
        perror("mr_start");
        mr_destroy(mr);
        mr_attr_destroy(&attr);
        return 1;
    }

    mr_destroy(mr);
    mr_attr_destroy(&attr);
    return 0;
}
