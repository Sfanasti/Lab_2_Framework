# Progetto di Laboratorio 2 A
## Framework per analisi di file di testo — paradigma MapReduce
**A.A. 2025-26**

---

## Premessa

Il progetto consiste nella realizzazione di un piccolo **framework in linguaggio C** (`libmr`) per l'analisi di file testuali su un singolo calcolatore, utilizzabile come libreria da un normale programma C.

**Vincoli tecnologici:**
- ✅ Consentito: `fork()`, `pipe()`, `dup2()`, `waitpid()`, thread C11, semafori POSIX
- ❌ Vietato: socket, memoria condivisa, `exec()`, thread POSIX (`pthread`)

---

## 1. Modello MapReduce — Introduzione concettuale

Il modello separa la computazione in tre fasi:

1. **Map** — trasforma l'input in coppie `<chiave, valore>` intermedie
2. **Raggruppamento** — raccoglie insieme tutte le coppie con la stessa chiave
3. **Reduce** — combina i valori di ogni gruppo in un risultato finale

Nel progetto: **un solo processo mapper** e **un solo processo reducer**, entrambi multithread C11.

---

## 2. Descrizione generale

Il framework `libmr` deve:

```
file_line → <token, processed_token>*  →  <token, processed_token[]>  →  result
```

**Il programma utente fornisce:**
- `mapper(riga)` → emette coppie `<token, valore>`
- `reducer(token, valori[])` → produce risultati finali

**Il framework si occupa di:**
- Leggere l'input riga per riga
- Creare la pipeline di processi
- Trasferire righe al mapper via pipe
- Trasferire coppie al reducer via pipe
- Raggruppare i valori per token
- Invocare il reducer sui gruppi completi
- Raccogliere e scrivere i risultati in output
- Gestire errori, chiusura/duplicazione descrittori, sincronizzazione, terminazione

> ⚠️ Il framework **non deve** contenere logica specifica per nessun esempio (es. word count).

---

## 3. Token, valori intermedi e risultati

- **Token**: sequenza non vuota di caratteri alfanumerici ASCII (`A-Z`, `a-z`, `0-9`), stringa C valida terminata da `'\0'`
- **processed_token** (valore): dato **opaco** — sequenza di byte di lunghezza nota; non assumere che sia stringa, intero, stampabile, o terminato da `'\0'`
- **result**: sequenza di byte opaca, il framework la trasporta senza interpretarla

---

## 4. Input

L'input può essere:
- Un **singolo file regolare**
- Una **directory** (elabora tutti i file regolari diretti, in ordine lessicografico)

> Non è richiesta la scansione ricorsiva (solo per l'addendum).

**Riga logica** (`mr_file_line_t`):
- Separata da `'\n'` (non incluso nel contenuto passato al mapper)
- L'ultima riga può non avere `'\n'`

```c
typedef struct {
    const char *file_name;
    size_t      file_name_len;
    unsigned long line_number;   /* parte da 1 */
    const char *line;
    size_t      line_len;
} mr_file_line_t;
```

**Casi da gestire obbligatoriamente:** file vuoti, righe vuote, file con una sola riga, ultima riga senza `'\n'`.

---

## 5. Architettura dei processi

```
Processo principale  ──pipe: righe serializzate──►  Processo mapper (multithread C11)
                                                            │
                                               pipe: coppie serializzate
                                                            │
                                                            ▼
Processo principale  ◄──pipe: risultati serializzati──  Processo reducer (multithread C11)
```

### Inizializzazione

```c
int main_to_mapper[2];
int mapper_to_reducer[2];
int reducer_to_main[2];

pipe(main_to_mapper);
pipe(mapper_to_reducer);
pipe(reducer_to_main);
```

### Fork del mapper

```c
pid_t mapper_pid = fork();
if (mapper_pid == 0) {
    dup2(main_to_mapper[0],    STDIN_FILENO);
    dup2(mapper_to_reducer[1], STDOUT_FILENO);
    /* chiudere tutti i descrittori non necessari */
    mapper_process_main(...);
    _exit(0);
}
```

### Fork del reducer

```c
pid_t reducer_pid = fork();
if (reducer_pid == 0) {
    dup2(mapper_to_reducer[0], STDIN_FILENO);
    dup2(reducer_to_main[1],   STDOUT_FILENO);
    /* chiudere tutti i descrittori non necessari */
    reducer_process_main(...);
    _exit(0);
}
```

### Regole sui descrittori

- **Processo principale**: tenere aperti solo lato scrittura → mapper e lato lettura ← reducer
- Tutti gli altri descrittori devono essere **chiusi** in ogni processo
- Un descrittore aperto nel processo sbagliato può bloccare la pipeline (manca EOF)

### Ordine di avvio

1. Creare le pipe
2. `fork()` del mapper
3. `fork()` del reducer
4. `dup2()` nei figli
5. Chiudere i descrittori non utilizzati in ogni processo
6. Avviare i thread C11 nei figli
7. Inviare righe serializzate al mapper
8. Raccogliere risultati dal reducer
9. `waitpid()` per entrambi i figli

> ⚠️ **Non usare `exec()`** — i figli ereditano codice e puntatori a funzione via `fork()`.  
> ⚠️ **Non fare `fork()` dopo aver creato thread C11 attivi.**

### Segnalazione fine input (via EOF, non messaggi speciali)

1. Processo principale chiude la pipe verso il mapper → mapper riceve EOF su stdin
2. Thread lettore mapper marca la coda come chiusa
3. Thread mapper completano le righe in coda e terminano
4. **Solo dopo** la terminazione di tutti i thread mapper, il mapper chiude stdout (pipe → reducer)
5. Reducer riceve EOF su stdin → completa il raggruppamento, invoca reducer, scrive risultati
6. Reducer chiude stdout → processo principale riceve EOF

---

## 6. Thread C11

```c
#include <threads.h>
```

**Vietato:** `pthread_create`, `pthread_mutex_t`, `pthread_cond_t`  
**Obbligatorio:** `thrd_create`, `mtx_t`, `cnd_t`

Firma dei thread worker:
```c
static int mapper_worker_main(void *arg);
static int reducer_worker_main(void *arg);
static int reader_main(void *arg);
```

Le code interne sono strutture dati **private al processo**, protette da `mtx_t` e `cnd_t` (produttore-consumatore). Non sono pipe né memoria condivisa.

### 6.1 Processo mapper

- **Thread lettore**: legge righe da stdin → coda condivisa
- **N thread mapper**: estraggono righe, invocano la funzione mapper, producono coppie
- Le coppie vengono scritte su stdout (pipe → reducer)
- La scrittura verso la pipe deve essere **sincronizzata** (un messaggio logico non deve essere mescolato con un altro)

### 6.2 Processo reducer

- **Thread lettore**: legge coppie da stdin
- Raggruppa i valori per token
- Quando l'input è terminato, crea i gruppi `<token, processed_token[]>`
- **N thread reducer**: elaborano i gruppi invocando la funzione reducer
- I risultati vengono scritti su stdout (pipe → processo principale)
- La funzione reducer viene invocata **una sola volta per token**, dopo aver raccolto tutti i valori

---

## 7. Protocollo interno sulle pipe

La comunicazione usa un **protocollo a lunghezze esplicite** (non testo libero, i valori possono contenere byte arbitrari).

### Header di una coppia (mapper → reducer)

```c
typedef struct {
    int token_len;
    int value_len;
} mr_pair_header_t;
```

Seguito da:
- `token_len` byte del token
- `value_len` byte del valore opaco

> `token_len` **non** include `'\0'`. Il ricevente deve allocare `token_len + 1` byte e aggiungere `'\0'`.  
> `value_len` è il numero esatto di byte opachi (nessun terminatore implicito).

### Letture/scritture parziali

Obbligatorio implementare:
```c
ssize_t readn(int fd, void *buf, size_t n);
ssize_t writen(int fd, const void *buf, size_t n);
```

### Validazione degli header

- Le lunghezze negli header sono di tipo `int`
- **Verificare** che non siano negative e che non superino limiti ragionevoli (documentare i limiti)
- Convertire a `size_t` **solo dopo** la validazione

---

## 8. Output

Ogni record di output contiene (formato a lunghezze esplicite):
1. Lunghezza del token (senza `'\0'`)
2. Il token
3. Lunghezza del risultato
4. I byte del risultato

**Determinismo**: a parità di input, il file di output deve essere identico tra esecuzioni diverse. Scrivere i risultati **ordinati lessicograficamente per token**. Documentare l'ordine relativo se il reducer emette più risultati per lo stesso token.

---

## 9. Esempio: word count

```c
/* Mapper: emette <token, 1> per ogni token nella riga */
int one = 1;
emit(token, &one, sizeof(one), emit_arg);

/* Reducer: somma tutti gli interi associati al token */
```

> Il framework **non** deve contenere logica specifica per il word count.

---

## 10. Precisazioni importanti sul contratto

- `processed_token` è una sequenza opaca di byte — **mai** usare `strlen`, `strcpy`, `strcmp`, `printf("%s")` sui valori intermedi
- I valori intermedi **possono** contenere byte nulli
- La funzione reducer viene invocata **una sola volta** per token distinto
- Niente race condition, perdita di risultati, duplicazioni o dipendenza dallo scheduling
- **Stdout è riservato al protocollo interno**: mapper e reducer applicativi usano `stderr` per diagnostica
- Gestire correttamente letture/scritture parziali

---

## 11. Log di esecuzione

Configurabile con `mr_attr_set_log_file()`. Default: `mr.log`.

**Formato suggerito:**
```
[timestamp] [processo] [thread] [evento] messaggio
```

**Deve registrare almeno:**
- Creazione pipe, processi mapper e reducer
- Avvio e terminazione thread
- Apertura/chiusura file di input e output
- Numero righe inviate al mapper
- Numero coppie prodotte dal mapper
- Numero token distinti raggruppati dal reducer
- Numero risultati finali prodotti
- Errori rilevati

L'accesso al log da thread/processi diversi deve essere **sincronizzato** (semafori POSIX, lock su file, o log centralizzato).

---

## 12. Requisiti tecnici

- Linguaggio C, compilabile su **Linux Ubuntu 24.04**
- Ogni chiamata di sistema deve essere controllata
- Usare macro o funzioni wrapper per la gestione degli errori

---

## 13. Test e valutazione

- Produrre una **libreria statica `libmr.a`**
- La valutazione usa mapper e reducer **non noti in anticipo** conformi all'interfaccia pubblica
- Target `make test` deve eseguire una batteria di test automatizzati

---

## 14. Addendum (per chi non ha superato le prove in itinere)

Funzionalità aggiuntive richieste:
1. Scansione ricorsiva delle directory di input
2. Più elaborazioni indipendenti nello stesso processo (`mr_t` senza interferenze)
3. File di statistiche separato (tempi, righe lette, coppie prodotte, token distinti, risultati)
4. Funzione di hashing deterministica per l'assegnazione dei token ai thread reducer

```c
typedef size_t (*mr_hash_t)(
    const char *token,
    size_t      token_len,
    void       *user_arg
);

typedef struct {
    size_t      mapper_threads;
    size_t      reducer_threads;
    size_t      queue_size;
    const char *log_file;
    mr_hash_t   hash;
    void       *hash_arg;
} mr_attr_t;

int mr_attr_set_hash_function(mr_attr_t *attr, mr_hash_t hash, void *hash_arg);
```

---

## A. Interfaccia pubblica (`include/mr.h`)

```c
#ifndef MR_H
#define MR_H

#include <stddef.h>

/* Handle opaco di una elaborazione */
typedef struct mr *mr_t;

typedef struct {
    size_t      mapper_threads;
    size_t      reducer_threads;
    size_t      queue_size;      /* capacità code interne, non dimensione pipe */
    const char *log_file;        /* NULL = nome di default */
} mr_attr_t;

typedef struct {
    const char   *file_name;
    size_t        file_name_len;
    unsigned long line_number;   /* parte da 1 */
    const char   *line;
    size_t        line_len;
} mr_file_line_t;

typedef struct {
    const void *data;
    size_t      size;
} mr_value_t;

/* Funzione per emettere una coppia dal mapper */
typedef int (*mr_emit_pair_t)(
    const char *token,
    const void *value,
    size_t      value_size,
    void       *emit_arg
);

/* Funzione per emettere un risultato dal reducer */
typedef int (*mr_emit_result_t)(
    const char *token,
    const void *result,
    size_t      result_size,
    void       *emit_arg
);

/* Funzione mapper fornita dall'utente */
typedef int (*mr_mapper_t)(
    const mr_file_line_t *line,
    mr_emit_pair_t        emit,
    void                 *emit_arg,
    void                 *user_arg
);

/* Funzione reducer fornita dall'utente */
typedef int (*mr_reducer_t)(
    const char       *token,
    const mr_value_t *values,
    size_t            values_count,
    mr_emit_result_t  emit,
    void             *emit_arg,
    void             *user_arg
);

int mr_attr_init(mr_attr_t *attr);
int mr_attr_destroy(mr_attr_t *attr);

int mr_attr_set_mapper_threads(mr_attr_t *attr, size_t n);
int mr_attr_set_reducer_threads(mr_attr_t *attr, size_t n);
int mr_attr_set_queue_size(mr_attr_t *attr, size_t n);
int mr_attr_set_log_file(mr_attr_t *attr, const char *path);

int mr_create(
    mr_t          *mr,
    const mr_attr_t *attr,
    mr_mapper_t    mapper,
    mr_reducer_t   reducer,
    void          *user_arg
);

int mr_start(mr_t mr, const char *input_path, const char *output_path);
int mr_destroy(mr_t mr);

#endif
```

**Note sull'interfaccia:**
- Tutte le funzioni restituiscono `0` in caso di successo, `-1` in caso di errore (impostando `errno`)
- `mr_t` è opaco; `mr_attr_t` è completamente definita nell'header
- `mr_attr_init()` deve inizializzare con valori di default validi (almeno 1 thread mapper e 1 reducer)
- `mr_attr_set_*()` rifiuta valori non validi (es. 0 thread, queue_size 0)
- `mr_create()` copia internamente la configurazione; dopo il ritorno, l'utente può modificare `mr_attr_t`
- `mr_start()` è **bloccante**: restituisce quando l'elaborazione è terminata o si è verificato un errore
- I puntatori passati alle callback sono validi **solo durante l'invocazione**; il framework copia i dati prima che `emit` ritorni

---

## B. Uso previsto del framework

```c
#include <stdio.h>
#include "mr.h"

int main(int argc, char **argv) {
    mr_t     mr;
    mr_attr_t attr;

    if (mr_attr_init(&attr) == -1) { perror("mr_attr_init"); return 1; }

    mr_attr_set_mapper_threads(&attr, 4);
    mr_attr_set_reducer_threads(&attr, 4);
    mr_attr_set_queue_size(&attr, 64);
    mr_attr_set_log_file(&attr, "mr.log");

    if (mr_create(&mr, &attr, my_mapper, my_reducer, NULL) == -1) {
        perror("mr_create"); mr_attr_destroy(&attr); return 1;
    }

    if (mr_start(mr, "input", "output.mro") == -1) {
        perror("mr_start"); mr_destroy(mr); mr_attr_destroy(&attr); return 1;
    }

    mr_destroy(mr);
    mr_attr_destroy(&attr);
    return 0;
}
```

---

## 15. Consegna

**Struttura archivio `.zip`:**
```
include/        ← mr.h
src/            ← implementazione framework
examples/       ← almeno un esempio d'uso
Makefile
README
relazione.pdf
```

**Target Makefile:**
- `make` — compila framework, `libmr.a` e almeno un esempio
- `make test` — esegue i test automatici
- `make clean` — rimuove oggetti, eseguibili, file temporanei

---

## 16. Relazione (max 10 pagine PDF)

Deve contenere:
1. Architettura generale del framework
2. Interfaccia pubblica
3. Organizzazione processi (`fork`, `pipe`, `dup2`, `waitpid`)
4. Organizzazione thread C11 in mapper e reducer
5. Struttura code interne e meccanismi di sincronizzazione
6. Formato messaggi sulle pipe
7. Struttura dati per il raggruppamento per token
8. Formato file di output
9. Formato file di log
10. Descrizione dei test
