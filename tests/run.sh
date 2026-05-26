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

if [ "$fail" -eq 0 ]; then
    echo "TEST OK"
else
    echo "TEST FALLITO"
    exit 1
fi
