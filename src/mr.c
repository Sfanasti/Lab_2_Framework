#define _POSIX_C_SOURCE 202405L

#define MR_MAX_TOKEN_L 4096
#define MR_MAX_VALUE_L (16u << 20)

#define MR_MAX_FNAME_L 4096
#define MR_MAX_LINE_L (16u << 20)
#define MR_MAX_THREADS 1024

#define CHECK(cond, err) \
    do { \
        if (!(cond)) { \
            errno = (err); \
            return -1; \
        } \
    } while (0)


#include "mr.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/wait.h>
#include <threads.h>
#include <stdarg.h>
#include <time.h>
#include <fcntl.h>
#include <signal.h>

// ==================================================================
// Strutture Dati
// ==================================================================


// struttura dell'handle
struct mr {
    size_t mapper_threads;
    size_t reducer_threads;
    size_t queue_size;
    char *log_file;
    int log_fd;
    char *stats_file;
    int stats_fd;
    mr_hash_t hash;
    void *hash_arg;

    mr_mapper_t mapper;
    mr_reducer_t reducer;

    void *user_arg;
};

// coppia raccolta in RAM
typedef struct {
    char *token;
    void *value;
    size_t value_size;
}pair_t;

typedef struct{
    mr_file_line_t fl;
}qline_t;

typedef struct{
    int tok_len;
    int val_len;
}pair_header_t;

typedef struct {
    int file_name_len;
    int line_len;
    unsigned long line_number;
}line_header_t;
// collettore passato al mapper come emit_arg

typedef struct{
    pair_t *items;
    size_t count;
    size_t max;
}collector_t;

// scrittore passato al reducer come emit_arg

typedef struct{
    FILE *output;
    size_t results;
    int error;
}writer_t;

typedef struct {
    void **items;
    size_t cap;
    size_t head;
    size_t tail;
    size_t count;
    int closed;
    mtx_t mtx;
    cnd_t not_full;
    cnd_t not_empty;
}queue_t;

typedef struct {
    int in_fd;
    int log_fd;
    queue_t *q;
}reader_arg_t;

typedef struct {
    mr_t mr;
    queue_t *q;
    int out_fd;
    mtx_t *write_mtx;
}mapper_worker_arg_t;

// contesto passato come emit_arg quando l'emit scrive su una pipe
typedef struct{
    int fd;
    int error;
    mtx_t *mtx;
}pipe_emit_t;

// gruppo di coppie con lo stesso token + buffer di output del reducer
typedef struct{
    char *token; // puntatore dentro il collector (non copiato)
    mr_value_t *vals; // view dei valori del gruppo
    size_t n; // numero di valori
    char *out; // buffer dei risultati serializzati
    size_t out_len;
    size_t out_cap;
    int error;
}group_t;

// stato condiviso dai worker reducer
typedef struct{
    group_t *groups;
    size_t count;
    size_t nthreads;
    mr_hash_t hash;
    void *hash_arg;
    mr_t mr;
    int error; // alzato dal primo worker che fallisce
    mtx_t mtx; // protegge error
}reduce_pool_t;

// argomento per-worker del reducer: pool condiviso + indice del worker
typedef struct{
    reduce_pool_t *pool;
    size_t id;
}reduce_worker_arg_t;

// ==================================================================
// Protocollo Interno
// ==================================================================

/*
  Legge esattamente n byte, oppure si ferma a EOF.
  Ritorna: n in caso di successo;
           k in [0, n) se EOF arriva dopo k byte (k=0 = EOF "pulito");
           -1 in caso di errore (errno impostato).
 */

static ssize_t read_n(int fd, void *buf, size_t n){
    size_t got = 0;
    char *p = buf;

    while (got < n){
        ssize_t r = read(fd, p + got, n - got);
        if (r < 0){ //la syscall ha fallito
            if (errno == EINTR) continue; //era solo un segnale: riprova, non è un errore
            return -1; //errore vero: propaga
        }
        if (r == 0) break; //EOF raggiunto
        got += (size_t)r;
    }

    return (ssize_t)got;
}

/*
  Scrive esattamente n byte. Non esiste un "EOF" in scrittura: un write che
  ritorna 0 è anomalo e lo trattiamo come errore (EIO).
 */

static ssize_t write_n(int fd, const void *buf, size_t n){
    size_t sent = 0;
    const char *p = buf;

    while (sent < n){
        ssize_t w = write(fd, p + sent, n - sent);
        if (w < 0){
            if (errno == EINTR) continue;
            return -1;
        }
        CHECK (w !=0, EIO);
        sent += (size_t)w;
    }

    return (ssize_t)n;
}

/*
 Scrive una riga di log in modo atomico (singola write su fd O_APPEND).
 Best-effort: gli errori di logging non interrompono l'elaborazione.
*/
static void mr_log(int fd, const char *role, const char *fmt, ...){
    if (fd < 0) return;

    char buf[1024];
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    int n = snprintf(buf, sizeof buf, "[%ld.%03ld] pid=%d %-7s ",
                     (long)ts.tv_sec, ts.tv_nsec / 1000000,
                     (int)getpid(), role);
    if (n < 0) return;
    if ((size_t)n >= sizeof buf) n = sizeof buf - 1;

    va_list ap;
    va_start(ap, fmt);
    int m = vsnprintf(buf + n, sizeof buf - n, fmt, ap);
    va_end(ap);
    if (m > 0){
        n += m;
        if ((size_t)n >= sizeof buf) n = sizeof buf - 1;
    }

    if ((size_t)n < sizeof buf - 1){
        buf[n++] = '\n';
    } else {
        buf[sizeof buf - 1] = '\n';
        n = sizeof buf;
    }

    // UNA sola write: atomicità garantita da O_APPEND sotto PIPE_BUF
    ssize_t w = write(fd, buf, (size_t)n);
    (void)w; // best-effort: ignoriamo l'esito
}

/*
 Logga un errore di system call con errno tradotto. Thread-safe:
 strerror_r scrive in un buffer locale (no buffer statico condiviso).
*/
static void mr_log_err(int fd, const char *role, const char *what){
    int e = errno; // catturo subito: mr_log puo' toccare errno
    char ebuf[128];
    if (strerror_r(e, ebuf, sizeof ebuf) != 0)
        snprintf(ebuf, sizeof ebuf, "errno %d", e);
    mr_log(fd, role, "ERRORE %s: %s (errno=%d)", what, ebuf, e);
}

/*
 Wrapper per syscall che falliscono con valore < 0: su errore logga e
 salta al cleanup. Richiede 'mr', 'role' in scope e una label 'fail:'.
*/
#define SYS(call, what) \
    do { \
        if ((call) < 0){ \
            mr_log_err(mr->log_fd, role, (what)); \
            goto fail; \
        } \
    } while (0)

// Serializza una coppia sulla pipe

static int pair_w(int fd, const char *token, const void *value,
                  size_t value_size){
    CHECK(token, EINVAL);

    size_t tlen = strlen(token);
    CHECK(tlen > 0 && tlen <= MR_MAX_TOKEN_L, EINVAL);
    CHECK(value_size <= MR_MAX_VALUE_L, EINVAL);

    pair_header_t h;
    h.tok_len = (int)tlen;
    h.val_len = (int)value_size;

    if (write_n(fd, &h, sizeof(h)) < 0) return -1;
    if (write_n(fd, token, tlen) < 0) return -1;
    if (value_size > 0 && write_n(fd, value, value_size) < 0) return -1;
    return 0;
}

// Riceve e deserializza una coppia

/*
 *token_out: C-string allocata con malloc (token_len+1, terminata da '\0').
 *value_out: buffer opaco allocato con malloc; NULL se value_size == 0.
 *value_size_out: numero di byte del valore.
 Ritorna 1 se ha letto una coppia completa.
 Ritorna 0 se EOF pulito (nessun header iniziato).
 Ritorna -1 in caso di errore (errno impostato; niente da liberare per il chiamante).
*/

static int pair_r(int fd, char **token_out, void **value_out,
                  size_t *value_size_out){
    pair_header_t h;
    ssize_t r = read_n(fd, &h, sizeof(h));

    if (r == 0) return 0; //EOF pulito
    if (r == -1) return -1; // errore di I/O: errno gia' impostato da read_n
    CHECK(r == (ssize_t)sizeof(h), EPROTO); // arrivati qui, e' troncato

    CHECK(h.tok_len > 0 && h.tok_len <= MR_MAX_TOKEN_L &&
          h.val_len >= 0 && h.val_len <= (int)MR_MAX_VALUE_L, EPROTO);

    size_t tlen = (size_t)h.tok_len;
    size_t vlen = (size_t)h.val_len;

    char *tok = malloc(tlen + 1);
    if (tok == NULL) return -1;

    void *val = NULL;
    if (vlen > 0){
        val = malloc(vlen);
        if(val == NULL){
            free(tok);
            return -1;
        }
    }

    if (read_n(fd, tok, tlen) != (ssize_t)tlen){
        int e = errno ? errno : EPROTO;
        free(tok);
        free(val);
        errno = e;
        return -1;
    }
    tok[tlen] = '\0';

     if (vlen > 0 && read_n(fd, val, vlen) != (ssize_t)vlen) {
        int e = errno ? errno : EPROTO;
        free(tok); free(val); errno = e;
        return -1;
    }

    *token_out = tok;
    *value_out = val;
    *value_size_out = vlen;
    return 1;
}

// Scrive una mr_file_line_t sulla pipe
static int line_w(int fd, const mr_file_line_t *fl){
    CHECK(fl && fl -> file_name, EINVAL);
    CHECK(fl -> file_name_len > 0 && fl->file_name_len <= MR_MAX_FNAME_L, EINVAL);
    CHECK(fl -> line_len <= MR_MAX_LINE_L, EINVAL);

    line_header_t h;
    h.file_name_len = (int)fl->file_name_len;
    h.line_len = (int)fl->line_len;
    h.line_number = fl->line_number;

    if (write_n(fd, &h, sizeof(h)) < 0) return -1;
    if (write_n(fd, fl -> file_name, fl -> file_name_len) < 0) return -1;
    if (fl -> line_len > 0 && write_n(fd, fl -> line, fl -> line_len) < 0) return -1;
    return 0;
}


static int line_r(int fd, mr_file_line_t *fl_out,
                  char **fname_out, char **line_out){
    line_header_t h;
    ssize_t r = read_n(fd, &h, sizeof(h));

    if (r == 0) return 0;
    if (r == -1) return -1;
    CHECK(r == (ssize_t)sizeof(h), EPROTO);
    CHECK(h.file_name_len > 0 && h.file_name_len <= MR_MAX_FNAME_L &&
          h.line_len >= 0 && h.line_len <= (int)MR_MAX_LINE_L, EPROTO);

    size_t flen = (size_t)h.file_name_len;
    size_t llen = (size_t)h.line_len;

    char *fname = malloc(flen + 1);
    if (fname == NULL) return -1;

    char *line = NULL;
    if (llen > 0){
        line = malloc(llen);
        if (line == NULL){ free(fname); return -1; }
    }

    if (read_n(fd, fname, flen) != (ssize_t)flen){
        int e = errno ? errno : EPROTO;
        free(fname); free(line); errno = e; return -1;
    }

    fname[flen] = '\0';

    if (llen > 0 && read_n(fd, line, llen) != (ssize_t)llen){
        int e = errno ? errno : EPROTO;
        free(fname); free(line); errno = e; return -1;
    }

    fl_out -> file_name = fname;
    fl_out -> file_name_len = flen;
    fl_out -> line_number = h.line_number;
    fl_out -> line = line;
    fl_out -> line_len = llen;
    *fname_out = fname;
    *line_out = line;
    return 1;
}

// FNV-1a 64-bit: hash deterministica di default per i token.
static size_t mr_default_hash(const char *token, size_t len, void *user_arg){
    (void)user_arg;
    uint64_t h = 1469598103934665603ULL; // offset basis
    for (size_t i = 0; i < len; i++){
        h ^= (unsigned char)token[i];
        h *= 1099511628211ULL; // prime
    }
    return (size_t)h;
}

// ==================================================================
// Gestione attributi
// ==================================================================

int mr_attr_init (mr_attr_t *attr){
    CHECK(attr, EINVAL);
    attr -> mapper_threads = 1;
    attr -> reducer_threads = 1;
    attr -> queue_size = 64;
    attr -> log_file = NULL;
    attr -> hash = NULL;
    attr -> hash_arg = NULL;

    return 0;
}

int mr_attr_destroy(mr_attr_t *attr){
    CHECK(attr, EINVAL);
    return 0;
}

int mr_attr_set_mapper_threads(mr_attr_t *attr, size_t n){
    CHECK(attr && n && n <= MR_MAX_THREADS, EINVAL);
    attr -> mapper_threads = n;
    return 0;
}
int mr_attr_set_reducer_threads(mr_attr_t *attr, size_t n){
    CHECK(attr && n && n <= MR_MAX_THREADS, EINVAL);
    attr -> reducer_threads = n;
    return 0;
}
int mr_attr_set_queue_size(mr_attr_t *attr, size_t n){
    CHECK(attr && n, EINVAL);
    attr -> queue_size = n;
    return 0;
}
int mr_attr_set_log_file(mr_attr_t *attr, const char *path){
    CHECK(attr && path, EINVAL);
    attr ->log_file = path;
    return 0;
}
int mr_attr_set_hash_function(mr_attr_t *attr, mr_hash_t hash, void *hash_arg){
    CHECK(attr && hash, EINVAL);
    attr -> hash = hash;
    attr -> hash_arg = hash_arg;
    return 0;
}

// ==================================================================
// Handle
// ==================================================================

int mr_create(mr_t *mr_u,const mr_attr_t *attr, mr_mapper_t mapper,
    mr_reducer_t reducer, void *user_arg){

    CHECK(mr_u && mapper && reducer, EINVAL);

    mr_t mr_f = calloc(1, sizeof(*mr_f));
    if (mr_f == NULL) return -1;

    if (attr != NULL){
        mr_f -> mapper_threads = attr -> mapper_threads ? attr -> mapper_threads : 1;
        mr_f -> reducer_threads = attr -> reducer_threads ? attr -> reducer_threads : 1;
        mr_f -> queue_size = attr -> queue_size ? attr -> queue_size : 64;
    }
    else{
        mr_f -> mapper_threads = 1;
        mr_f -> reducer_threads = 1;
        mr_f -> queue_size = 64;
    }

    const char *lf = (attr != NULL && attr->log_file != NULL)
                         ? attr->log_file
                         : "mr.log";
    mr_f -> log_file = strdup(lf);
    if (mr_f -> log_file == NULL) {
        free(mr_f);
        return -1;
    }

    /*
     Il file statistiche e' separato dal log e non e' un campo pubblico
     dell'attr: lo deriviamo da log_file (<log_file>.stats), cosi' resta
     per-istanza (no interferenze tra piu' mr_t) senza estendere mr_attr_t.
    */
    size_t lflen = strlen(mr_f -> log_file);
    mr_f -> stats_file = malloc(lflen + sizeof(".stats"));
    if (mr_f -> stats_file == NULL) {
        free(mr_f -> log_file);
        free(mr_f);
        return -1;
    }
    memcpy(mr_f -> stats_file, mr_f -> log_file, lflen);
    memcpy(mr_f -> stats_file + lflen, ".stats", sizeof(".stats"));

    mr_f -> hash = (attr != NULL && attr->hash != NULL)
                       ? attr->hash
                       : mr_default_hash;
    mr_f -> hash_arg = (attr != NULL) ? attr->hash_arg : NULL;

    mr_f -> mapper = mapper;
    mr_f -> reducer = reducer;
    mr_f -> user_arg = user_arg;
    mr_f -> log_fd = -1;
    mr_f -> stats_fd = -1;

    *mr_u = mr_f;
    return 0;
}

int mr_destroy (mr_t mr){
    if (mr == NULL) return 0;
    free (mr->log_file);
    free (mr->stats_file);
    free (mr);
    return 0;
}

// ==================================================================
// Emit
// ==================================================================

/*
 emit usata sia dal mapper (token,value) sia dal reducer (token,result):
 la serializzazione e' la stessa (pair_w).
*/
static int emit_to_pipe(const char *token, const void *data,
                        size_t size, void *emit_arg){
    pipe_emit_t *e = emit_arg;
    if (e -> error) return -1;
    if (token == NULL){
        e -> error = 1;
        errno = EINVAL;
        return -1;
    }

    if (e -> mtx) mtx_lock(e -> mtx);
    int r = pair_w(e -> fd, token, data, size);
    if (e -> mtx) mtx_unlock(e -> mtx);

    if (r != 0){
        e -> error = 1;
        return -1;
    }
    return 0;
}

static int write_result(const char *token, const void *result,
                        size_t result_size, void *emit_arg) {
    writer_t *w = emit_arg;
    if (w -> error) return -1;
    if (token == NULL){
        w->error = 1;
        errno = EINVAL;
        return -1;
    }

    uint32_t toklen = (uint32_t)strlen(token);
    uint32_t redlen = (uint32_t)result_size;

    if ((fwrite(&toklen, sizeof(toklen), 1, w->output) != 1) ||
       (toklen > 0 && fwrite(token, 1, toklen, w->output) != toklen) ||
       (fwrite(&redlen, sizeof(redlen), 1, w->output) != 1) ||
       (redlen > 0 && fwrite(result, 1, redlen, w->output) != redlen)){

        w->error = 1;
        return -1;
    }

    w -> results++;
    return 0;

}

// ==================================================================
// Lettura dell'input
// ==================================================================

static int send_file_lines(const char *path, int fd, size_t *nlines, int log_fd){

    FILE *f = fopen(path, "rb");
    if (f == NULL){ mr_log_err(log_fd, "parent", "apertura file input"); return -1; }
    mr_log(log_fd, "parent", "apertura file input %s", path);

    char *buf = NULL;
    size_t max_l = 0;
    unsigned long lineno = 1;
    int rcode = 0;
    ssize_t n;

    while ((n = getline(&buf, &max_l, f)) != -1) {
        size_t len = (size_t)n;
        if (len > 0 && buf[len - 1] == '\n') len--;

        mr_file_line_t fl;
        fl.file_name = path;
        fl.file_name_len = strlen(path);
        fl.line_number = lineno;
        fl.line = buf;
        fl.line_len = len;

        if (line_w(fd, &fl) != 0) {
            rcode = -1;
            break;
        }
        if (nlines) (*nlines)++;
        lineno++;
    }

    if (rcode == 0 && ferror(f)) rcode = -1;

    free(buf);
    fclose(f);
    mr_log(log_fd, "parent", "chiusura file input %s", path);
    return rcode;
}

static int cmp_str(const void *a, const void *b) {
    const char *const *sa = a;
    const char *const *sb = b;
    return strcmp(*sa, *sb);
}


static int send_dir_lines(const char *dirpath, int fd, size_t *nlines, int log_fd){

    DIR *d = opendir(dirpath);
    if (d == NULL) return -1;

    char **names = NULL;
    size_t count = 0;
    size_t max_l = 0;
    int rcode = 0;

    struct dirent *e;
    errno = 0;

    while ((e = readdir(d)) != NULL) {
        if (strcmp(e -> d_name, ".") == 0 || strcmp(e -> d_name, "..") == 0)
            continue;

        size_t pathlen = strlen(dirpath) + 1 + strlen(e->d_name) + 1;
        char *full = malloc(pathlen);
        if (full == NULL) { rcode = -1; break; }
        snprintf(full, pathlen, "%s/%s", dirpath, e->d_name);

        /*
         raccogliamo TUTTE le voci (file e sottodir): il tipo lo
         decidiamo dopo l'ordinamento, al momento dell'elaborazione
        */
        if (count == max_l) {
            size_t newmax_l = max_l ? max_l * 2 : 16;
            char **tmp = realloc(names, newmax_l * sizeof *tmp);
            if (tmp == NULL) { free(full); rcode = -1; break; }
            names = tmp;
            max_l = newmax_l;
        }
        names[count++] = full; // trasferiamo la proprieta'

        errno = 0;
    }

    if (rcode == 0 && errno != 0) rcode = -1; // errore di readdir
    closedir(d);

    if (rcode == 0) {
        qsort(names, count, sizeof *names, cmp_str); // ordine deterministico
        for (size_t i = 0; i < count; i++) {
            struct stat st;
            if (stat(names[i], &st) != 0) { rcode = -1; break; }

            if (S_ISREG(st.st_mode)) {
                if (send_file_lines(names[i], fd, nlines, log_fd) != 0) { rcode = -1; break; }
            } else if (S_ISDIR(st.st_mode)) {
                if (send_dir_lines(names[i], fd, nlines, log_fd) != 0) { rcode = -1; break; } // RICORSIONE
            }
            // altri tipi (fifo, socket, link...) ignorati
        }
    }

    for (size_t i = 0; i < count; i++){
        free(names[i]);
    }
    free(names);

    return rcode;

}

// ==================================================================
// Group & reduce
// ==================================================================

/*
 Ordina per token; a parità di token, spareggio sul CONTENUTO del valore
 (byte opachi, memcmp + lunghezza). NON usiamo l'ordine di arrivo: con
 mapper paralleli sarebbe non deterministico. Cosi' l'ordine dei valori
 dato al reducer e' identico a ogni esecuzione -> output deterministico
 anche per reducer sensibili all'ordine.
*/
static int cmp_pair(const void *a, const void *b) {
    const pair_t *pa = a;
    const pair_t *pb = b;
    int c = strcmp(pa->token, pb->token);
    if (c != 0) return c;
    size_t m = pa->value_size < pb->value_size ? pa->value_size : pb->value_size;
    int d = (m > 0) ? memcmp(pa->value, pb->value, m) : 0;
    if (d != 0) return d;
    if (pa->value_size < pb->value_size) return -1;
    if (pa->value_size > pb->value_size) return 1;
    return 0;
}

//Libera tutta la memoria del collettore.
static void collector_free(collector_t *c) {
    for (size_t i = 0; i < c->count; i++) {
        free(c->items[i].token);
        free(c->items[i].value);
    }
    free(c->items);
    c->items = NULL;
    c->count = 0;
    c->max = 0;
}

// ==================================================================
// Coda
// ==================================================================


static int queue_init(queue_t *q, size_t cap){
    q -> items = calloc(cap, sizeof(*q -> items));
    if (q -> items == NULL) return -1;
    q -> cap = cap;
    q -> head = q -> tail = q -> count = 0;
    q -> closed = 0;

    if (mtx_init(&q -> mtx, mtx_plain) != thrd_success){
        free(q -> items); return -1;
    }
    if (cnd_init(&q -> not_full) != thrd_success){
        mtx_destroy(&q -> mtx); free(q -> items); return -1;
    }
    if (cnd_init(&q -> not_empty) != thrd_success){
        cnd_destroy(&q -> not_full);
        mtx_destroy(&q -> mtx);
        free(q -> items);
        return -1;
    }
    return 0;
}

static void queue_destroy(queue_t *q){
    cnd_destroy(&q -> not_empty);
    cnd_destroy(&q -> not_full);
    mtx_destroy(&q -> mtx);
    free(q -> items);
}

/*
 Inserisce item nella coda. Si blocca se piena.
 Ritorna 0 in caso di successo, -1 se la coda e' stata chiusa.
*/
static int queue_push(queue_t *q, void *item){
    mtx_lock(&q -> mtx);
    while (!q -> closed && q -> count == q -> cap){
        cnd_wait(&q -> not_full, &q -> mtx);
    }
    if (q -> closed){
        mtx_unlock(&q -> mtx);
        return -1;
    }
    q -> items[q -> tail] = item;
    q -> tail = (q -> tail + 1) % q -> cap;
    q -> count++;
    cnd_signal(&q -> not_empty);
    mtx_unlock(&q -> mtx);
    return 0;
}

/*
 Estrae un item dalla coda. Si blocca se vuota.
 Ritorna 0 se ha estratto, 1 se coda chiusa e vuota (drain completato).
*/
static int queue_pop(queue_t *q, void **out){
    mtx_lock(&q -> mtx);
    while (!q -> closed && q -> count == 0){
        cnd_wait(&q -> not_empty, &q -> mtx);
    }
    if (q -> count == 0){ // qui: closed && empty
        mtx_unlock(&q -> mtx);
        return 1;
    }
    *out = q -> items[q -> head];
    q -> head = (q -> head + 1) % q -> cap;
    q -> count--;
    cnd_signal(&q -> not_full);
    mtx_unlock(&q -> mtx);
    return 0;
}

// Marca la coda come chiusa. Sveglia tutti i thread bloccati.
static void queue_close(queue_t *q){
    mtx_lock(&q -> mtx);
    q -> closed = 1;
    cnd_broadcast(&q -> not_empty);
    cnd_broadcast(&q -> not_full);
    mtx_unlock(&q -> mtx);
}

static void qline_free(qline_t *qi){
    if (qi == NULL) return;
    free((void *)qi -> fl.file_name);
    free((void *)qi -> fl.line);
    free(qi);
}

// ==================================================================
// Buffer dei risultati
// ==================================================================

// appende n byte al buffer di un gruppo, crescendo se necessario
static int buf_append(group_t *g, const void *src, size_t n){
    if (g -> out_len + n > g -> out_cap){
        size_t newcap = g -> out_cap ? g -> out_cap * 2 : 256;
        while (newcap < g -> out_len + n) newcap *= 2;
        char *tmp = realloc(g -> out, newcap);
        if (tmp == NULL) return -1;
        g -> out = tmp;
        g -> out_cap = newcap;
    }
    memcpy(g -> out + g -> out_len, src, n);
    g -> out_len += n;
    return 0;
}

/*
 emit del reducer: serializza (token,result) nel buffer del gruppo,
 con lo stesso formato di pair_w. NON scrive sulla pipe (determinismo).
*/
static int emit_to_buf(const char *token, const void *result,
                       size_t result_size, void *emit_arg){
    group_t *g = emit_arg;
    if (g -> error) return -1;
    if (token == NULL){
        g -> error = 1;
        errno = EINVAL;
        return -1;
    }

    size_t tlen = strlen(token);
    if (tlen == 0 || tlen > MR_MAX_TOKEN_L || result_size > MR_MAX_VALUE_L){
        g -> error = 1;
        errno = EINVAL;
        return -1;
    }

    pair_header_t h;
    h.tok_len = (int)tlen;
    h.val_len = (int)result_size;

    if (buf_append(g, &h, sizeof(h)) != 0 ||
        buf_append(g, token, tlen) != 0 ||
        (result_size > 0 && buf_append(g, result, result_size) != 0)){
        g -> error = 1;
        return -1;
    }
    return 0;
}

// ==================================================================
// Mapper Threads
// ==================================================================

static int mapper_reader_t(void *arg){
    reader_arg_t *a = arg;
    int rcode = 0;

    mr_log(a -> log_fd, "mapper", "reader thread avviato");

    while(true){
        qline_t *qi = malloc(sizeof(*qi));
        if (qi == NULL){
            rcode = -1;
            break;
        }
        memset(qi, 0, sizeof(*qi));

        char *fname = NULL;
        char *line = NULL;

        int got = line_r(a -> in_fd, &qi -> fl, &fname, &line);
        if (got == 0){
            free(qi);
            break;
        }
        if (got == -1){
            rcode = -1;
            free(qi);
            break;
        }

        if (queue_push(a -> q, qi) != 0){
            qline_free(qi);
            rcode = -1;
            break;
        }
    }

    queue_close(a -> q);
    mr_log(a -> log_fd, "mapper", "reader thread terminato rc=%d", rcode);
    return rcode;
}

static int mapper_worker_t(void *arg){
    mapper_worker_arg_t *a = arg;
    pipe_emit_t emit_ctx;
    emit_ctx.fd = a -> out_fd;
    emit_ctx.error = 0;
    emit_ctx.mtx = a -> write_mtx;

    int rcode = 0;

    mr_log(a -> mr -> log_fd, "mapper", "worker thread avviato");

    while(true){
        void *item = NULL;
        int r = queue_pop(a -> q, &item);
        if(r == 1) break;
        qline_t *qi = item;

        if (a -> mr -> mapper(&qi -> fl, emit_to_pipe, &emit_ctx,
            a -> mr -> user_arg) != 0 || emit_ctx.error){
                qline_free(qi);
                rcode = -1;
                queue_close(a -> q); //chiudo cosi reader e altri worker si fermano
                break;
            }
        qline_free(qi);
    }

    mr_log(a -> mr -> log_fd, "mapper", "worker thread terminato rc=%d", rcode);
    return rcode;
}

// ==================================================================
// Reducer Threads
// ==================================================================

/*
 worker reducer: ogni thread processa solo i gruppi che la hash gli assegna,
 e accumula i risultati nel buffer privato di ciascun gruppo.
*/
static int reducer_worker_t(void *arg){
    reduce_worker_arg_t *wa = arg;
    reduce_pool_t *pool = wa -> pool;
    size_t id = wa -> id;
    int rcode = 0;

    mr_log(pool -> mr -> log_fd, "reducer", "worker thread avviato");

    for (size_t i = 0; i < pool -> count; i++){
        mtx_lock(&pool -> mtx);
        int stop = pool -> error;
        mtx_unlock(&pool -> mtx);
        if (stop) break;

        group_t *g = &pool -> groups[i];

        // questo gruppo spetta a me? indice = hash(token) % nthreads
        if (pool -> hash(g -> token, strlen(g -> token), pool -> hash_arg) % pool -> nthreads != id)
            continue;

        if (pool -> mr -> reducer(g -> token, g -> vals, g -> n,
                                  emit_to_buf, g, pool -> mr -> user_arg) != 0 ||
            g -> error){
            mtx_lock(&pool -> mtx);
            pool -> error = 1;
            mtx_unlock(&pool -> mtx);
            rcode = -1;
            break;
        }
    }

    mr_log(pool -> mr -> log_fd, "reducer", "worker thread terminato rc=%d", rcode);
    return rcode;
}


// ==================================================================
// Figli
// ==================================================================

/*
 Figlio mapper: legge righe da in_fd, chiama il mapper utente,
 scrive coppie su out_fd. Esce con 0 a EOF pulito.
*/
static int mapper_child(mr_t mr, int in_fd, int out_fd){
    int rcode = 0;
    queue_t q;
    mtx_t write_mtx;

    if (queue_init(&q, mr -> queue_size) != 0) return -1;
    if (mtx_init(&write_mtx, mtx_plain) != thrd_success){
        queue_destroy(&q);
        return -1;
    }

    // avvio del reader
    reader_arg_t r_arg;
    r_arg.in_fd = in_fd;
    r_arg.log_fd = mr -> log_fd;
    r_arg.q = &q;

    thrd_t reader;
    if (thrd_create(&reader, mapper_reader_t, &r_arg) != thrd_success){
        mtx_destroy(&write_mtx);
        queue_destroy(&q);
        return -1;
    }

    // avvio dei worker
    size_t n = mr -> mapper_threads;
    thrd_t *workers = calloc(n, sizeof(*workers));
    mapper_worker_arg_t *w_args = calloc(n, sizeof(*w_args));
    if (workers == NULL || w_args == NULL){
        queue_close(&q);
        thrd_join(reader, NULL);
        free(workers);
        free(w_args);
        mtx_destroy(&write_mtx);
        queue_destroy(&q);
        return -1;
    }

    size_t spawned = 0;
    for (size_t i = 0; i < n; i++){
        w_args[i].mr = mr;
        w_args[i].q = &q;
        w_args[i].out_fd = out_fd;
        w_args[i].write_mtx = &write_mtx;
        if (thrd_create(&workers[i], mapper_worker_t, &w_args[i])
            != thrd_success){
            rcode = -1;
            break;
        }
        spawned++;
    }

    // se non sono riuscito a spawnare tutti, chiudo per sbloccare reader
    if (spawned < n) queue_close(&q);

    int rres = 0;
    thrd_join(reader, &rres);
    if (rres != 0) rcode = -1;

    for (size_t i = 0; i < spawned; i++){
        int wres = 0;
        thrd_join(workers[i], &wres);
        if (wres != 0) rcode = -1;
    }

    // drain di sicurezza: se la coda ha residui (es. spawned == 0), liberali
    void *item;
    while (queue_pop(&q, &item) == 0) qline_free(item);

    free(workers);
    free(w_args);
    mtx_destroy(&write_mtx);
    queue_destroy(&q);
    return rcode;
}

/*
 Figlio reducer: raccoglie tutte le coppie da in_fd, ordina, raggruppa,
 processa i gruppi in parallelo (buffer per-gruppo) e scrive i risultati
 in ordine su out_fd.
*/
static int reducer_child(mr_t mr, int in_fd, int out_fd){
    collector_t coll;
    memset(&coll, 0, sizeof coll);

    // fase A: raccolgo tutte le coppie dalla pipe
    for (;;){
        char *tok = NULL;
        void *val = NULL;
        size_t vsize = 0;
        int got = pair_r(in_fd, &tok, &val, &vsize);
        if (got == 0) break;
        if (got == -1){ collector_free(&coll); return -1; }

        if (coll.count == coll.max){
            size_t newmax = coll.max ? coll.max * 2 : 16;
            pair_t *tmp = realloc(coll.items, newmax * sizeof(*tmp));
            if (tmp == NULL){
                free(tok); free(val); collector_free(&coll); return -1;
            }
            coll.items = tmp;
            coll.max = newmax;
        }
        pair_t *p = &coll.items[coll.count++];
        p -> token = tok;
        p -> value = val;
        p -> value_size = vsize;
    }

    qsort(coll.items, coll.count, sizeof *coll.items, cmp_pair);

    // fase B.1: costruisco l'array dei gruppi
    group_t *groups = NULL;
    size_t gcount = 0, gcap = 0;
    int rcode = 0;
    size_t i = 0;

    while (i < coll.count){
        size_t j = i;
        while (j < coll.count &&
               strcmp(coll.items[j].token, coll.items[i].token) == 0) j++;
        size_t group_n = j - i;

        if (gcount == gcap){
            size_t newcap = gcap ? gcap * 2 : 16;
            group_t *tmp = realloc(groups, newcap * sizeof(*tmp));
            if (tmp == NULL){ rcode = -1; break; }
            groups = tmp;
            gcap = newcap;
        }

        group_t *g = &groups[gcount];
        g -> token = coll.items[i].token;
        g -> n = group_n;
        g -> out = NULL;
        g -> out_len = 0;
        g -> out_cap = 0;
        g -> error = 0;
        g -> vals = malloc(group_n * sizeof(*g -> vals));
        if (g -> vals == NULL){ rcode = -1; break; }
        for (size_t k = 0; k < group_n; k++){
            g -> vals[k].data = coll.items[i + k].value;
            g -> vals[k].size = coll.items[i + k].value_size;
        }
        gcount++;
        i = j;
    }

    // fase B.2: pool di worker con partizionamento per hash
    if (rcode == 0){
        reduce_pool_t pool;
        pool.groups = groups;
        pool.count = gcount;
        pool.nthreads = mr -> reducer_threads;
        pool.hash = mr -> hash;
        pool.hash_arg = mr -> hash_arg;
        pool.mr = mr;
        pool.error = 0;

        if (mtx_init(&pool.mtx, mtx_plain) != thrd_success){
            rcode = -1;
        } else {
            size_t n = mr -> reducer_threads;
            thrd_t *workers = calloc(n, sizeof(*workers));
            reduce_worker_arg_t *wargs = calloc(n, sizeof(*wargs));
            if (workers == NULL || wargs == NULL){
                rcode = -1;
            } else {
                size_t spawned = 0;
                for (size_t t = 0; t < n; t++){
                    wargs[t].pool = &pool;
                    wargs[t].id = t;
                    if (thrd_create(&workers[t], reducer_worker_t, &wargs[t])
                        != thrd_success){
                        rcode = -1;
                        break;
                    }
                    spawned++;
                }

                for (size_t t = 0; t < spawned; t++){
                    int wres = 0;
                    thrd_join(workers[t], &wres);
                    if (wres != 0) rcode = -1;
                }
                if (pool.error) rcode = -1;
            }
            free(workers);
            free(wargs);
            mtx_destroy(&pool.mtx);
        }
    }

    // fase B.3: flush sequenziale dei buffer in ordine di token
    if (rcode == 0){
        for (size_t k = 0; k < gcount; k++){
            if (groups[k].out_len > 0 &&
                write_n(out_fd, groups[k].out, groups[k].out_len) < 0){
                rcode = -1;
                break;
            }
        }
    }

    // statistiche del reducer: coppie ricevute, token distinti
    mr_log(mr -> stats_fd, "reducer", "pairs=%zu tokens=%zu", coll.count, gcount);
    // gli stessi conteggi anche nel log (richiesto dalla specifica del log)
    mr_log(mr -> log_fd, "reducer", "coppie ricevute=%zu token distinti=%zu",
           coll.count, gcount);

    // cleanup
    for (size_t k = 0; k < gcount; k++){
        free(groups[k].vals);
        free(groups[k].out);
    }
    free(groups);
    collector_free(&coll);
    return rcode;
}

// ==================================================================
// Start
// ==================================================================

int mr_start(mr_t mr, const char *input_path, const char *output_path) {
    CHECK(mr && input_path && output_path, EINVAL);

    const char *role = "parent";

    // SIGPIPE ignorato: write su pipe senza lettore -> EPIPE a write_n, non segnale
    struct sigaction sa_ign, sa_old;
    memset(&sa_ign, 0, sizeof sa_ign);
    sa_ign.sa_handler = SIG_IGN;
    sigemptyset(&sa_ign.sa_mask);
    int pipe_saved = (sigaction(SIGPIPE, &sa_ign, &sa_old) == 0);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    size_t nlines = 0;

    mr->log_fd = open(mr->log_file, O_WRONLY | O_CREAT | O_APPEND, 0644);
    // il log e' opzionale: se fallisce, log_fd = -1 e mr_log diventa no-op

    // file statistiche: O_TRUNC -> fresco a ogni run
    mr->stats_fd = open(mr->stats_file,
                        O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);

    mr_log(mr->log_fd, role, "run start input=%s output=%s mappers=%zu reducers=%zu",
           input_path, output_path, mr->mapper_threads, mr->reducer_threads);

    // risorse: tutte a "non aperto", cosi' il cleanup chiude solo il dovuto
    int pl[2] = {-1, -1};
    int pp[2] = {-1, -1};
    int pr[2] = {-1, -1};
    FILE *out = NULL;
    pid_t pid_m = -1;
    pid_t pid_r = -1;
    int rcode = 0;

    struct stat st;
    SYS(stat(input_path, &st), "stat input");

    // 1) Apertura output.
    out = fopen(output_path, "wb");
    if (out == NULL){ mr_log_err(mr->log_fd, role, "fopen output"); goto fail; }
    mr_log(mr->log_fd, role, "apertura file output %s", output_path);

    /*
     2) Creazione delle 3 pipe.
     pl = lines (padre -> mapper)
     pp = pairs (mapper -> reducer)
     pr = results (reducer -> padre)
    */
    SYS(pipe(pl), "pipe lines");
    SYS(pipe(pp), "pipe pairs");
    SYS(pipe(pr), "pipe results");
    mr_log(mr->log_fd, role, "create 3 pipe (lines, pairs, results)");

    // 3) Fork del mapper.
    pid_m = fork();
    if (pid_m < 0){ mr_log_err(mr->log_fd, role, "fork mapper"); goto fail; }
    if (pid_m == 0){
        // MAPPER CHILD: usa pl[0] (in), pp[1] (out)
        close(pl[1]); close(pp[0]);
        close(pr[0]); close(pr[1]);
        fclose(out);

        if (dup2(pl[0], STDIN_FILENO) < 0){ mr_log_err(mr->log_fd, "mapper", "dup2 stdin"); _exit(1); }
        if (dup2(pp[1], STDOUT_FILENO) < 0){ mr_log_err(mr->log_fd, "mapper", "dup2 stdout"); _exit(1); }
        close(pl[0]); close(pp[1]);

        mr_log(mr->log_fd, "mapper", "child avviato");
        int rc = mapper_child(mr, STDIN_FILENO, STDOUT_FILENO);
        mr_log(mr->log_fd, "mapper", "child terminato rc=%d", rc);
        _exit(rc == 0 ? 0 : 1);
    }

    // 4) Fork del reducer.
    pid_r = fork();
    if (pid_r < 0){ mr_log_err(mr->log_fd, role, "fork reducer"); goto fail; }
    if (pid_r == 0){
        // REDUCER CHILD: usa pp[0] (in), pr[1] (out)
        close(pl[0]); close(pl[1]);
        close(pp[1]); close(pr[0]);
        fclose(out);

        if (dup2(pp[0], STDIN_FILENO) < 0){ mr_log_err(mr->log_fd, "reducer", "dup2 stdin"); _exit(1); }
        if (dup2(pr[1], STDOUT_FILENO) < 0){ mr_log_err(mr->log_fd, "reducer", "dup2 stdout"); _exit(1); }
        close(pp[0]); close(pr[1]);

        mr_log(mr->log_fd, "reducer", "child avviato");
        int rc = reducer_child(mr, STDIN_FILENO, STDOUT_FILENO);
        mr_log(mr->log_fd, "reducer", "child terminato rc=%d", rc);
        _exit(rc == 0 ? 0 : 1);
    }

    // 5) PARENT: chiude gli fd che non gli servono.
    close(pl[0]); pl[0] = -1;
    close(pp[0]); pp[0] = -1;
    close(pp[1]); pp[1] = -1;
    close(pr[1]); pr[1] = -1;

    mr_log(mr->log_fd, role, "fork mapper pid=%d reducer pid=%d",
           (int)pid_m, (int)pid_r);

    // 6) Invio le righe al mapper.
    if (S_ISDIR(st.st_mode)) {
        rcode = send_dir_lines(input_path, pl[1], &nlines, mr->log_fd);
    } else if (S_ISREG(st.st_mode)) {
        rcode = send_file_lines(input_path, pl[1], &nlines, mr->log_fd);
    } else {
        errno = EINVAL;
        mr_log_err(mr->log_fd, role, "input non valido");
        rcode = -1;
    }

    // 7) Chiudo pl[1]: segnala EOF al mapper, a cascata al reducer.
    close(pl[1]); pl[1] = -1;

    // 8) Leggo i risultati dal reducer e scrivo il .mro.
    writer_t w;
    w.output = out; w.results = 0; w.error = 0;

    if (rcode == 0){
        for (;;){
            char *tok = NULL;
            void *res = NULL;
            size_t rsize = 0;
            int got = pair_r(pr[0], &tok, &res, &rsize);
            if (got == 0) break;
            if (got == -1){ mr_log_err(mr->log_fd, role, "pair_r risultati"); rcode = -1; break; }

            int wr = write_result(tok, res, rsize, &w);
            free(tok); free(res);
            if (wr != 0){ rcode = -1; break; }
        }
    }
    close(pr[0]); pr[0] = -1;

    // 9) Attendo i figli.
    int status;
    if (waitpid(pid_m, &status, 0) < 0 ||
        !WIFEXITED(status) || WEXITSTATUS(status) != 0){
        mr_log(mr->log_fd, role, "mapper terminato male");
        rcode = -1;
    }
    if (waitpid(pid_r, &status, 0) < 0 ||
        !WIFEXITED(status) || WEXITSTATUS(status) != 0){
        mr_log(mr->log_fd, role, "reducer terminato male");
        rcode = -1;
    }

    if (fclose(out) != 0){ mr_log_err(mr->log_fd, role, "fclose output"); rcode = -1; }
    out = NULL;
    mr_log(mr->log_fd, role, "chiusura file output");

    // conteggi nel log (lato parent: righe inviate, risultati raccolti)
    mr_log(mr->log_fd, role, "righe inviate al mapper=%zu risultati raccolti=%zu",
           nlines, w.results);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    long elapsed_ms = (t1.tv_sec - t0.tv_sec) * 1000
                    + (t1.tv_nsec - t0.tv_nsec) / 1000000;
    mr_log(mr->stats_fd, role, "lines=%zu results=%zu elapsed_ms=%ld",
           nlines, w.results, elapsed_ms);
    if (mr->stats_fd >= 0){ close(mr->stats_fd); mr->stats_fd = -1; }

    mr_log(mr->log_fd, role, "run end status=%d", rcode);
    if (mr->log_fd >= 0){ close(mr->log_fd); mr->log_fd = -1; }
    if (pipe_saved) sigaction(SIGPIPE, &sa_old, NULL);
    return rcode;

fail:
    rcode = -1;
    if (out) fclose(out);
    if (pl[0] >= 0) close(pl[0]);
    if (pl[1] >= 0) close(pl[1]);
    if (pp[0] >= 0) close(pp[0]);
    if (pp[1] >= 0) close(pp[1]);
    if (pr[0] >= 0) close(pr[0]);
    if (pr[1] >= 0) close(pr[1]);
    if (pid_m > 0) waitpid(pid_m, NULL, 0);
    if (pid_r > 0) waitpid(pid_r, NULL, 0);
    if (mr->stats_fd >= 0){ close(mr->stats_fd); mr->stats_fd = -1; }
    mr_log(mr->log_fd, role, "run end status=%d", rcode);
    if (mr->log_fd >= 0){ close(mr->log_fd); mr->log_fd = -1; }
    if (pipe_saved) sigaction(SIGPIPE, &sa_old, NULL);
    return rcode;
}
