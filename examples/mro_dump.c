/*
 mro_dump.c - Stampa un .mro come "token<TAB>risultato", interpretando il
 risultato come testo (byte verbatim).

 Per gli esempi il cui risultato e' una stringa (es. index). Per i risultati
 interi usare wc_dump.

 Formato di un record:
 [token_len : u32 LE][token][res_len : u32 LE][res]

 Uso: mro_dump <output.mro>
*/

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

static int read_u32(FILE *f, uint32_t *out) {
    unsigned char b[4];
    if (fread(b, 1, 4, f) != 4) return -1;
    *out = (uint32_t)b[0] | ((uint32_t)b[1] << 8) |
           ((uint32_t)b[2] << 16) | ((uint32_t)b[3] << 24);
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "uso: %s <output.mro>\n", argv[0]);
        return 2;
    }
    FILE *f = fopen(argv[1], "rb");
    if (f == NULL) { perror("fopen"); return 1; }

    for (;;) {
        uint32_t tlen;
        if (read_u32(f, &tlen) != 0) break; // fine file

        char *token = malloc((size_t)tlen + 1);
        if (token == NULL) { perror("malloc"); fclose(f); return 1; }
        if (tlen > 0 && fread(token, 1, tlen, f) != tlen) {
            fprintf(stderr, "record troncato\n"); free(token); fclose(f); return 1;
        }
        token[tlen] = '\0';

        uint32_t rlen;
        if (read_u32(f, &rlen) != 0) {
            fprintf(stderr, "record troncato\n"); free(token); fclose(f); return 1;
        }
        char *res = malloc((size_t)rlen + 1);
        if (res == NULL) { perror("malloc"); free(token); fclose(f); return 1; }
        if (rlen > 0 && fread(res, 1, rlen, f) != rlen) {
            fprintf(stderr, "record troncato\n"); free(res); free(token); fclose(f); return 1;
        }
        res[rlen] = '\0';

        printf("%s\t%s\n", token, res);
        free(res);
        free(token);
    }

    fclose(f);
    return 0;
}
