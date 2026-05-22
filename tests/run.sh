#!/bin/sh
# Test Fase 1: wordcount su una batteria di casi limite.
# Per ciascuno: esegue wordcount, decodifica con wc_dump, confronta con l'atteso.
set -e

BUILD=build
INPUT=tests/input
EXPECTED=tests/output

# Casi su singolo file: nome -> tests/input/<nome>.txt e tests/output/<nome>.expected
FILE_CASES="prova_1 empty only_newlines no_final_newline long_line same_token all_unique case_sensitive crlf"

# Casi su directory: nome -> tests/input/<nome>/ e tests/output/<nome>.expected
DIR_CASES="dir_multi dir_with_subdir"

fail=0
for c in $FILE_CASES; do
    in="$INPUT/$c.txt"
    exp="$EXPECTED/$c.expected"
    out="$BUILD/$c.mro"
    got="$BUILD/$c.got.txt"

    "$BUILD/wordcount" "$in" "$out"
    "$BUILD/wc_dump" "$out" > "$got"

    if diff -u "$exp" "$got" > /dev/null; then
        printf "  [OK]   %s\n" "$c"
    else
        printf "  [FAIL] %s\n" "$c"
        diff -u "$exp" "$got" || true
        fail=1
    fi
done

for c in $DIR_CASES; do
    in="$INPUT/$c"
    exp="$EXPECTED/$c.expected"
    out="$BUILD/$c.mro"
    got="$BUILD/$c.got.txt"

    "$BUILD/wordcount" "$in" "$out"
    "$BUILD/wc_dump" "$out" > "$got"

    if diff -u "$exp" "$got" > /dev/null; then
        printf "  [OK]   %s\n" "$c"
    else
        printf "  [FAIL] %s\n" "$c"
        diff -u "$exp" "$got" || true
        fail=1
    fi
done

# Determinismo: due esecuzioni consecutive devono produrre .mro identici byte-per-byte.
# Usiamo dir_multi perche' coinvolge readdir (ordine non garantito dal SO).
"$BUILD/wordcount" "$INPUT/dir_multi" "$BUILD/det1.mro"
"$BUILD/wordcount" "$INPUT/dir_multi" "$BUILD/det2.mro"
if cmp -s "$BUILD/det1.mro" "$BUILD/det2.mro"; then
    printf "  [OK]   determinismo (cmp su due run con directory)\n"
else
    printf "  [FAIL] determinismo: i due .mro differiscono\n"
    fail=1
fi

# Invarianza rispetto ai thread: l'output non deve dipendere dalla concorrenza.
# Stesso input a 1+1 e a 8+8 thread -> .mro identici byte-per-byte.
MR_MAPPERS=1 MR_REDUCERS=1 "$BUILD/wordcount" "$INPUT/dir_multi" "$BUILD/seq.mro"
MR_MAPPERS=8 MR_REDUCERS=8 "$BUILD/wordcount" "$INPUT/dir_multi" "$BUILD/par.mro"
if cmp -s "$BUILD/seq.mro" "$BUILD/par.mro"; then
    printf "  [OK]   invarianza thread (1+1 vs 8+8)\n"
else
    printf "  [FAIL] invarianza thread: l'output dipende dalla concorrenza\n"
    fail=1
fi

# Path d'errore: input inesistente -> exit non-zero (senza crash).
# (dentro 'if' set -e e' sospeso, quindi l'uscita non-zero non aborta lo script)
if "$BUILD/wordcount" /percorso/che/non/esiste "$BUILD/err.mro" 2>/dev/null; then
    printf "  [FAIL] errore: input inesistente accettato\n"
    fail=1
else
    printf "  [OK]   errore: input inesistente rifiutato\n"
fi

# Addendum 6D: due mr_t indipendenti nello stesso processo, config diverse.
# I due .mro (2+2 e 8+8 thread) devono coincidere byte-per-byte.
"$BUILD/multi" "$INPUT/dir_multi" "$BUILD/multi" >/dev/null
if cmp -s "$BUILD/multi.1.mro" "$BUILD/multi.2.mro"; then
    printf "  [OK]   addendum: due mr_t indipendenti (output identico)\n"
else
    printf "  [FAIL] addendum: i due mr_t danno output diversi\n"
    fail=1
fi

# Addendum 6C: il file statistiche (derivato da log_file: mr.log.stats)
# viene prodotto e contiene le metriche.
rm -f mr.log.stats
"$BUILD/wordcount" "$INPUT/dir_multi" "$BUILD/stats.mro" >/dev/null
if grep -q "pairs=" mr.log.stats && grep -q "lines=" mr.log.stats; then
    printf "  [OK]   addendum: file statistiche prodotto\n"
else
    printf "  [FAIL] addendum: statistiche mancanti in mr.log.stats\n"
    fail=1
fi

# Genericita': un esempio NON-wordcount (indice numeri di riga per token).
# mapper/reducer arbitrari, valore opaco unsigned long, risultato stringa.
"$BUILD/index" "$INPUT/index_basic.txt" "$BUILD/index.mro"
"$BUILD/mro_dump" "$BUILD/index.mro" > "$BUILD/index.got.txt"
if diff -u "$EXPECTED/index_basic.expected" "$BUILD/index.got.txt" > /dev/null; then
    printf "  [OK]   genericita': esempio non-wordcount (indice)\n"
else
    printf "  [FAIL] genericita': output indice diverso dall'atteso\n"
    diff -u "$EXPECTED/index_basic.expected" "$BUILD/index.got.txt" || true
    fail=1
fi

# Invarianza thread sull'esempio non-wordcount: il reducer e' ordine-sensibile
# ma ordina internamente, quindi 1+1 e 8+8 devono dare lo stesso .mro.
MR_MAPPERS=1 MR_REDUCERS=1 "$BUILD/index" "$INPUT/index_basic.txt" "$BUILD/idx_seq.mro"
MR_MAPPERS=8 MR_REDUCERS=8 "$BUILD/index" "$INPUT/index_basic.txt" "$BUILD/idx_par.mro"
if cmp -s "$BUILD/idx_seq.mro" "$BUILD/idx_par.mro"; then
    printf "  [OK]   genericita': invarianza thread (indice)\n"
else
    printf "  [FAIL] genericita': l'indice dipende dalla concorrenza\n"
    fail=1
fi

# Addendum hashing: una hash utente diversa deve dare lo STESSO .mro
# (la hash cambia solo quale thread reducer processa un token, non l'output).
"$BUILD/index" "$INPUT/index_basic.txt" "$BUILD/idx_defhash.mro"
MR_HASH=7 "$BUILD/index" "$INPUT/index_basic.txt" "$BUILD/idx_userhash.mro"
if cmp -s "$BUILD/idx_defhash.mro" "$BUILD/idx_userhash.mro"; then
    printf "  [OK]   addendum: hash utente -> stesso output\n"
else
    printf "  [FAIL] addendum: l'output dipende dalla funzione di hash\n"
    fail=1
fi

# Determinismo con reducer ORDINE-SENSIBILE (concatena senza riordinare).
# A 8+8 thread l'arrivo delle coppie e' non deterministico, ma il framework
# ordina i valori per contenuto: due run devono dare lo stesso .mro.
MR_CONCAT=1 MR_MAPPERS=8 MR_REDUCERS=8 "$BUILD/index" "$INPUT/index_dense.txt" "$BUILD/cc1.mro"
MR_CONCAT=1 MR_MAPPERS=8 MR_REDUCERS=8 "$BUILD/index" "$INPUT/index_dense.txt" "$BUILD/cc2.mro"
if cmp -s "$BUILD/cc1.mro" "$BUILD/cc2.mro"; then
    printf "  [OK]   determinismo con reducer ordine-sensibile\n"
else
    printf "  [FAIL] determinismo: reducer ordine-sensibile non riproducibile\n"
    fail=1
fi

if [ "$fail" -eq 0 ]; then
    echo "TEST OK"
else
    echo "TEST FALLITO"
    exit 1
fi
