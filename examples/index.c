/*
 index.c - Esempio NON-wordcount: indice dei numeri di riga per token.

 Dimostra che il framework e' generico (mapper/reducer arbitrari):
 - il valore emesso e' un unsigned long opaco (8 byte), non un int;
 - il risultato e' una stringa di lunghezza variabile, non un numero;
 - il reducer ordina e deduplica, non somma.

 L'ordinamento nel reducer rende l'output deterministico a prescindere
 dall'ordine di arrivo dei valori (che con mapper paralleli non e' fisso).

 Uso: index <input> <output.mro>
*/

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mr.h"

// mapper: per ogni token emette <token, (unsigned long)numero_di_riga>
static int idx_mapper(const mr_file_line_t *line, mr_emit_pair_t emit,
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
        unsigned long ln = line->line_number; // valore opaco (8 byte)
        int rc = emit(tok, &ln, sizeof ln, emit_arg);
        free(tok);
        if (rc != 0) return -1;
    }
    return 0;
}

static int cmp_ulong(const void *a, const void *b) {
    unsigned long x = *(const unsigned long *)a;
    unsigned long y = *(const unsigned long *)b;
    return (x > y) - (x < y);
}

/*
 reducer: raccoglie i numeri di riga (byte opachi), li ordina, deduplica,
 ed emette una stringa "n1 n2 n3". L'ordinamento rende l'output deterministico
 a prescindere dall'ordine di arrivo dei valori.
*/
static int idx_reducer(const char *token, const mr_value_t *values,
                       size_t values_count, mr_emit_result_t emit,
                       void *emit_arg, void *user_arg) {
    (void)user_arg;
    unsigned long *nums = malloc(values_count * sizeof *nums);
    if (nums == NULL) return -1;

    size_t cnt = 0;
    for (size_t i = 0; i < values_count; i++) {
        if (values[i].size != sizeof(unsigned long)) continue; // difensivo
        memcpy(&nums[cnt++], values[i].data, sizeof(unsigned long)); // opaco
    }
    qsort(nums, cnt, sizeof *nums, cmp_ulong);

    // costruisco "n1 n2 ..." saltando i duplicati
    char buf[4096];
    int pos = 0;
    unsigned long prev = 0;
    int first = 1;
    for (size_t i = 0; i < cnt; i++) {
        if (!first && nums[i] == prev) continue;
        int m = snprintf(buf + pos, sizeof buf - (size_t)pos,
                         first ? "%lu" : " %lu", nums[i]);
        if (m < 0 || (size_t)(pos + m) >= sizeof buf) { free(nums); return -1; }
        pos += m;
        prev = nums[i];
        first = 0;
    }
    free(nums);
    return emit(token, buf, (size_t)pos, emit_arg); // risultato = stringa
}

/*
 Reducer ORDINE-SENSIBILE: concatena i valori nell'ordine in cui arrivano,
 SENZA riordinarli. Serve a verificare che il framework consegni i valori di
 un token in un ordine deterministico (altrimenti due run darebbero output
 diversi). Non e' un esempio "utile", ma una sonda sul determinismo.
*/
static int concat_reducer(const char *token, const mr_value_t *values,
                          size_t values_count, mr_emit_result_t emit,
                          void *emit_arg, void *user_arg) {
    (void)user_arg;
    char buf[8192];
    int pos = 0;
    int first = 1;
    for (size_t i = 0; i < values_count; i++) {
        if (values[i].size != sizeof(unsigned long)) continue;
        unsigned long v;
        memcpy(&v, values[i].data, sizeof v);
        int m = snprintf(buf + pos, sizeof buf - (size_t)pos,
                         first ? "%lu" : " %lu", v);
        if (m < 0 || (size_t)(pos + m) >= sizeof buf) return -1;
        pos += m;
        first = 0;
    }
    return emit(token, buf, (size_t)pos, emit_arg);
}

// legge un intero positivo dall'ambiente, con default
static size_t env_size(const char *name, size_t def) {
    const char *v = getenv(name);
    if (v == NULL || *v == '\0') return def;
    long x = strtol(v, NULL, 10);
    return x > 0 ? (size_t)x : def;
}

/*
 Hash utente d'esempio (djb2) con argomento opzionale (un seed).
 Deterministica, non modifica il token, non dipende dall'ordine dei thread:
 stesso token -> stesso thread reducer, a ogni esecuzione.
*/
static size_t my_hash(const char *token, size_t len, void *user_arg) {
    size_t h = user_arg ? *(size_t *)user_arg : 5381;
    for (size_t i = 0; i < len; i++) h = h * 33 + (unsigned char)token[i];
    return h;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "uso: %s <input> <output.mro>\n", argv[0]);
        return 2;
    }
    mr_t mr;
    mr_attr_t attr;
    if (mr_attr_init(&attr) == -1) { perror("mr_attr_init"); return 1; }
    mr_attr_set_mapper_threads(&attr, env_size("MR_MAPPERS", 4));
    mr_attr_set_reducer_threads(&attr, env_size("MR_REDUCERS", 4));

    // se MR_HASH e' impostato, usa la hash utente (col valore come seed)
    static size_t seed;
    if (getenv("MR_HASH")) {
        seed = env_size("MR_HASH", 5381);
        mr_attr_set_hash_function(&attr, my_hash, &seed);
    }

    // MR_CONCAT seleziona il reducer ordine-sensibile (sonda di determinismo)
    mr_reducer_t red = getenv("MR_CONCAT") ? concat_reducer : idx_reducer;
    if (mr_create(&mr, &attr, idx_mapper, red, NULL) == -1) {
        perror("mr_create"); mr_attr_destroy(&attr); return 1;
    }
    if (mr_start(mr, argv[1], argv[2]) == -1) {
        perror("mr_start"); mr_destroy(mr); mr_attr_destroy(&attr); return 1;
    }
    mr_destroy(mr);
    mr_attr_destroy(&attr);
    return 0;
}
