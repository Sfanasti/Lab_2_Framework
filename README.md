# libmr — Framework MapReduce in C

`libmr` è un framework, scritto in C (C11), per l'analisi di file di testo su
singolo calcolatore secondo il modello **MapReduce**. È distribuito come
**libreria statica** `libmr.a` con un header pubblico `mr.h`: un programma
esterno include `mr.h`, linka `libmr.a` e fornisce due funzioni applicative
(`mapper` e `reducer`). Il framework resta **generico** — nessuna logica
specifica di un esempio (es. word count) vive dentro `src/`.

    file_line --> <token, value>*  -->  <token, value[]>  -->  result

- il **mapper** riceve una riga logica ed emette zero o più coppie `<token, value>`;
- il framework raggruppa i valori per token;
- il **reducer** riceve un token con tutti i suoi valori ed emette uno o più risultati.

## Requisiti

- Linux (sviluppato/testato su Ubuntu 24.04), `gcc`, `make`.
- Compilazione con `-std=c11 -Wall -Wextra -pthread`.

## Build

    make            # libmr.a + tutti gli esempi
    make lib        # solo build/libmr.a
    make test       # batteria di test automatici (stampa "TEST OK")
    make clean      # rimuove build/, log, statistiche e .mro temporanei

Gli artefatti finiscono in `build/`: la libreria è `build/libmr.a`.

## Usare il framework da un programma esterno

Bastano `mr.h` e `libmr.a`:

    gcc -std=c11 -Iinclude mioprog.c build/libmr.a -o mioprog -pthread

Schema d'uso (vedi `examples/` per esempi completi):

```c
#include "mr.h"

int my_mapper(const mr_file_line_t *line, mr_emit_pair_t emit,
              void *emit_arg, void *user_arg);
int my_reducer(const char *token, const mr_value_t *values, size_t n,
               mr_emit_result_t emit, void *emit_arg, void *user_arg);

int main(void) {
    mr_t mr; mr_attr_t attr;
    mr_attr_init(&attr);
    mr_attr_set_mapper_threads(&attr, 4);
    mr_attr_set_reducer_threads(&attr, 4);
    mr_create(&mr, &attr, my_mapper, my_reducer, NULL);
    mr_start(mr, "input", "output.mro");   // bloccante: 0 = ok, -1 = errore (errno)
    mr_destroy(mr);
    mr_attr_destroy(&attr);
    return 0;
}
```

Tutte le funzioni pubbliche ritornano `0` (successo) o `-1` (errore, con
`errno` impostato ove appropriato). I puntatori passati alle callback sono
validi solo durante l'invocazione: il framework copia i dati prima che `emit`
ritorni. Le funzioni applicative **non devono scrivere su `stdout`** (riservato
al protocollo interno tra i processi): per eventuali messaggi diagnostici usare
`stderr` o il log del framework.

### Contratto importante: byte opachi

Il `token` è una C-string (alfanumerica) usata solo per il raggruppamento. I
**valori** (`value`) e i **risultati** (`result`) sono invece sequenze di byte
**opache** di lunghezza nota: vanno copiati con `memcpy`+size, **mai** trattati
con `strlen`/`strcmp`/`printf("%s")`. Possono contenere byte nulli e non sono
necessariamente C-string.

## Architettura

Pipeline di **3 processi** collegati da **3 pipe** unidirezionali:

    processo principale --pipe righe-->  processo mapper  --pipe coppie-->  processo reducer
            ^                            (multithread C11)                  (multithread C11)
            |                                                                     |
            +---------------------------- pipe risultati -------------------------+

- `fork()` di mapper e reducer, `dup2()` delle pipe su stdin/stdout, chiusura
  accurata dei descrittori, EOF a cascata per segnalare la fine, `waitpid()`.
- Il **processo mapper** usa 1 thread lettore + N thread worker su una coda
  bounded (`mtx_t`/`cnd_t`); la scrittura sulla pipe è sotto mutex.
- Il **processo reducer** raccoglie tutte le coppie, le raggruppa per token e
  processa i gruppi con N thread (assegnati per hash del token), accumulando
  l'output in buffer per-gruppo riversati in ordine (determinismo).
- Solo thread C11 (`threads.h`); **nessun** pthread, socket, memoria condivisa
  o `exec`.

## Determinismo

A parità di input, funzioni e parametri, l'output è **identico tra esecuzioni
diverse**, indipendentemente dal numero di thread. L'output è ordinato
lessicograficamente per token; a parità di token i valori sono ordinati per
**contenuto** (non per ordine di arrivo, che con i mapper paralleli non è
deterministico). Se un reducer emette più risultati per uno stesso token,
compaiono nell'ordine di emissione.

## Formato dei file prodotti

- **Output `.mro`** (binario, a lunghezze esplicite). Un record per risultato:

      [token_len u32][token][result_len u32][result]

  La lunghezza del token non include il terminatore.
- **Log** (`mr.log` di default, configurabile con `mr_attr_set_log_file`):
  righe `[timestamp] pid=N ruolo messaggio`, scritte atomicamente. Registra
  creazione di pipe/processi/thread, apertura/chiusura file, conteggi ed errori.
- **Statistiche** (`<log_file>.stats`): tempi, righe lette, coppie, token
  distinti, risultati. File separato e per-istanza.

## Esempi (`examples/`)

Esempi d'uso del framework (chiamano `mr_create`/`mr_start`):

| File | Cosa fa |
|---|---|
| `wordcount.c` | Conta le occorrenze dei token (esempio consigliato). |
| `index.c` | Indice dei numeri di riga per token (valore opaco `unsigned long`, risultato stringa, hash utente opzionale). |
| `multi.c` | Due `mr_t` indipendenti nello stesso processo. |

Utility di lettura dell'output (non usano il framework: decodificano un
`.mro`, che è binario):

| File | Cosa fa |
|---|---|
| `wc_dump.c` | Decodifica un `.mro` interpretando il risultato come `int`. |
| `mro_dump.c` | Decodifica un `.mro` interpretando il risultato come testo. |

Esecuzione tipica:

    ./build/wordcount tests/input/dir_multi out.mro
    ./build/wc_dump out.mro          # token <TAB> conteggio

Gli esempi leggono il numero di thread da variabili d'ambiente
(`MR_MAPPERS`, `MR_REDUCERS`, `MR_QUEUE`), utile per i test di invarianza.

## Test (`make test`)

La suite (in `tests/run.sh`) esercita casi limite (file vuoti, righe vuote,
ultima riga senza `\n`, CRLF, token unici/ripetuti), input a directory con
**scansione ricorsiva**, **determinismo**, **invarianza rispetto al numero di
thread**, gestione degli errori, più `mr_t` indipendenti, file di statistiche e
hashing dei token. Include esempi **diversi dal word count** per verificare la
genericità del framework.

## Configurazione (`mr_attr_t`)

| Funzione | Effetto |
|---|---|
| `mr_attr_set_mapper_threads(attr, n)` | thread del processo mapper (≥1) |
| `mr_attr_set_reducer_threads(attr, n)` | thread del processo reducer (≥1) |
| `mr_attr_set_queue_size(attr, n)` | capacità delle code interne (≠0) |
| `mr_attr_set_log_file(attr, path)` | nome del file di log |
| `mr_attr_set_hash_function(attr, hash, hash_arg)` | hash utente per assegnare i token ai thread reducer (default interno: FNV-1a) |

La hash deve essere deterministica (stesso token → stesso thread), non
modificare il token e non dipendere dall'ordine dei thread.

## Struttura del repository

    include/   mr.h            interfaccia pubblica
    src/       mr.c            implementazione del framework
    examples/                  esempi d'uso + utility di dump
    tests/     run.sh          test automatici (input/ e output/ attesi)
    Makefile
    

