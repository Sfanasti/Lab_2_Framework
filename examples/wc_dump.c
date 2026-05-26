/*
 * wc_dump.c - Legge un file di output del framework (.mro) e lo stampa in
 * chiaro come "token<TAB>conteggio", interpretando il risultato come int.
 *
 * Serve solo per ispezionare/verificare l'output del word count: il file
 * .mro e' binario (formato a lunghezze esplicite), questo lo rende leggibile.
 *
 * Formato di un record:
 *   [token_len : u32 LE][token][res_len : u32 LE][res]
 *
 * Uso:  wc_dump <output.mro>
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
        if (read_u32(f, &tlen) != 0) break; /* fine file */

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

        long value = 0;
        if (rlen == sizeof(int)) {
            int v;
            if (fread(&v, 1, sizeof v, f) != sizeof v) {
                fprintf(stderr, "record troncato\n"); free(token); fclose(f); return 1;
            }
            value = v;
        } else {
            /* risultato di dimensione inattesa: lo saltiamo */
            if (fseek(f, (long)rlen, SEEK_CUR) != 0) {
                free(token); fclose(f); return 1;
            }
        }

        printf("%s\t%ld\n", token, value);
        free(token);
    }

    fclose(f);
    return 0;
}
