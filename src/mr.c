#define _POSIX_C_SOURCE 202405L

#define MR_MAX_TOKEN_L 4096
#define MR_MAX_VALUE_L (16u << 20)

#define MR_MAX_FNAME_L 4096
#define MR_MAX_LINE_L (16u << 20)

#define CHECK(cond, err)       \
    do {                       \
        if (!(cond)) {         \
            errno = (err);     \
            return -1;         \
        }                      \
    } while (0)


#include "mr.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/wait.h>

/* ================================================================== */
/*                          Strutture Dati                            */
/* ================================================================== */


/*struttura dell'handle*/
struct mr {
    size_t mapper_threads;
    size_t reducer_threads;
    size_t queue_size;
    char *log_file;

    mr_mapper_t mapper;
    mr_reducer_t reducer;

    void *user_arg;
};

/*coppia raccolta in RAM*/
typedef struct {
    char *token; 
    size_t token_len; 
    void *value;
    size_t value_size;
    size_t order;
}pair_t;

typedef struct{
    int tok_len;
    int val_len;
}pair_header_t;

typedef struct {
    int           file_name_len;
    int           line_len;
    unsigned long line_number;
}line_header_t;
/*collettore passato al mapper come emit_arg*/

typedef struct{
    pair_t *items;
    size_t count;
    size_t max;
    size_t next_order;
    int error;
}collector_t;

/*scrittore passato al reducer come emit_arg*/

typedef struct{
    FILE *output;
    size_t results;
    int error;
}writer_t;

/*contesto passato come emit_arg quando l'emit scrive su una pipe*/
typedef struct{
    int fd;
    int error;
}pipe_emit_t;

/* ================================================================== */
/*               Forward declarations protocollo                      */
/* ================================================================== */

static int pair_w(int fd, const char *token, const void *value, size_t value_size);
static int pair_r(int fd, char **token_out, void **value_out, size_t *value_size_out);
static int line_w(int fd, const mr_file_line_t *fl);
static int line_r(int fd, mr_file_line_t *fl_out, char **fname_out, char **line_out);

/* ================================================================== */
/*                         Gestione attributi                         */
/* ================================================================== */

int mr_attr_init (mr_attr_t *attr){
    CHECK(attr, EINVAL);
    attr -> mapper_threads = 1;
    attr -> reducer_threads = 1;
    attr -> queue_size = 64;
    attr -> log_file = NULL;

    return 0;
}

int mr_attr_destroy(mr_attr_t *attr){
    CHECK(attr, EINVAL);
    return 0;
}

int mr_attr_set_mapper_threads(mr_attr_t *attr, size_t n){
    CHECK(attr && n, EINVAL);
    attr -> mapper_threads = n;
    return 0;
}
int mr_attr_set_reducer_threads(mr_attr_t *attr, size_t n){
    CHECK(attr && n, EINVAL);
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

/* ================================================================== */
/*                                Handle                              */
/* ================================================================== */

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
 
    mr_f->mapper   = mapper;
    mr_f->reducer  = reducer;
    mr_f->user_arg = user_arg;
 
    *mr_u = mr_f;
    return 0;
}

int mr_destroy (mr_t mr){
    if (mr == NULL) return 0;
    free (mr->log_file);
    free (mr);
    return 0;
}

/* ================================================================== */
/*                                 Emit                               */
/* ================================================================== */

/*emit usata sia dal mapper (token,value) sia dal reducer (token,result):
  la serializzazione e' la stessa (pair_w).*/
static int emit_to_pipe(const char *token, const void *data,
                        size_t size, void *emit_arg){
    pipe_emit_t *e = emit_arg;
    if (e -> error) return -1;
    if (token == NULL){
        e -> error = 1;
        errno = EINVAL;
        return -1;
    }
    if (pair_w(e -> fd, token, data, size) != 0){
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

/* ================================================================== */
/*                         Lettura dell'input                         */
/* ================================================================== */

static int send_file_lines(const char *path, int fd){

    FILE *f = fopen(path, "rb");
    if (f == NULL) return -1;

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
        lineno++;
    }

    if (rcode == 0 && ferror(f)) rcode = -1;

    free(buf);
    fclose(f);
    return rcode;
}

static int cmp_str(const void *a, const void *b) {
    const char *const *sa = a;
    const char *const *sb = b;
    return strcmp(*sa, *sb);
}


static int send_dir_lines(const char *dirpath, int fd){

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

        struct stat st;
        if (stat(full, &st) == 0 && S_ISREG(st.st_mode)) {
            if (count == max_l) {
                size_t newmax_l = max_l ? max_l * 2 : 16;
                char **tmp = realloc(names, newmax_l * sizeof *tmp);
                if (tmp == NULL) { free(full); rcode = -1; break; }
                names = tmp;
                max_l = newmax_l;
            }
            names[count++] = full; /* trasferiamo la proprieta' */
        } else {
            free(full); /* non e' un file regolare: lo ignoriamo */
        }

        errno = 0;
    }

    if (rcode == 0 && errno != 0) rcode = -1; /* errore di readdir */
    closedir(d);

    if (rcode == 0) {
        qsort(names, count, sizeof *names, cmp_str); /* ordine deterministico */
        for (size_t i = 0; i < count; i++) {
            if (send_file_lines(names[i], fd) != 0) { rcode = -1; break; }
        }
    }

    for (size_t i = 0; i < count; i++){
        free(names[i]);
    }
    free(names);

    return rcode;

}

/* ================================================================== */
/*                           Group & reduce                           */
/* ================================================================== */

static int cmp_pair(const void *a, const void *b) {
    const pair_t *pa = a;
    const pair_t *pb = b;
    int c = strcmp(pa->token, pb->token);
    if (c != 0) return c;
    if (pa -> order < pb -> order) return -1;
    if (pa -> order > pb -> order) return 1;
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

/* ================================================================== */
/*                              Figli                                 */
/* ================================================================== */

/*Figlio mapper: legge righe da in_fd, chiama il mapper utente,
  scrive coppie su out_fd. Esce con 0 a EOF pulito.*/
static int mapper_child(mr_t mr, int in_fd, int out_fd){
    pipe_emit_t emit_ctx;
    emit_ctx.fd = out_fd; emit_ctx.error = 0;
    int rcode = 0;

    for (;;){
        mr_file_line_t fl;
        char *fname = NULL;
        char *line = NULL;
        int got = line_r(in_fd, &fl, &fname, &line);
        if (got == 0) break;                 /* EOF pulito */
        if (got == -1){ rcode = -1; break; }

        if (mr -> mapper(&fl, emit_to_pipe, &emit_ctx, mr -> user_arg) != 0 ||
            emit_ctx.error){
            free(fname); free(line);
            rcode = -1;
            break;
        }
        free(fname);
        free(line);
    }
    return rcode;
}

/*Figlio reducer: raccoglie tutte le coppie da in_fd, ordina, raggruppa,
  chiama il reducer utente, scrive risultati su out_fd.*/
static int reducer_child(mr_t mr, int in_fd, int out_fd){
    collector_t coll;
    memset(&coll, 0, sizeof coll);

    /* fase A: raccolgo tutte le coppie dalla pipe */
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
        p -> token_len = strlen(tok);
        p -> value = val;
        p -> value_size = vsize;
        p -> order = coll.next_order++;
    }

    /* fase B: ordino + raggruppo + chiamo reducer utente */
    qsort(coll.items, coll.count, sizeof *coll.items, cmp_pair);

    pipe_emit_t emit_ctx;
    emit_ctx.fd = out_fd; emit_ctx.error = 0;
    int rcode = 0;
    size_t i = 0;

    while (i < coll.count){
        size_t j = i;
        while (j < coll.count &&
               strcmp(coll.items[j].token, coll.items[i].token) == 0) j++;

        size_t group_n = j - i;

        mr_value_t *vals = malloc(group_n * sizeof *vals);
        if (vals == NULL){
            rcode = -1;
            break;
        }

        for (size_t k = 0; k < group_n; k++){
            vals[k].data = coll.items[i + k].value;
            vals[k].size = coll.items[i + k].value_size;
        }

        int r = mr -> reducer(coll.items[i].token, vals, group_n,
                              emit_to_pipe, &emit_ctx, mr -> user_arg);
        free(vals);
        if (r != 0 || emit_ctx.error){
            rcode = -1;
            break;
        }
        i = j;
    }

    collector_free(&coll);
    return rcode;
}

/* ================================================================== */
/*                                Start                               */
/* ================================================================== */

int mr_start(mr_t mr, const char *input_path, const char *output_path) {
    CHECK(mr && input_path && output_path, EINVAL);

    struct stat st;
    if (stat(input_path, &st) != 0) return -1;

    /* 1) Apertura output. */
    FILE *out = fopen(output_path, "wb");
    if (out == NULL) return -1;

    /* 2) Creazione delle 3 pipe.
       pl = lines (padre -> mapper)
       pp = pairs (mapper -> reducer)
       pr = results (reducer -> padre) */
    int pl[2], pp[2], pr[2];
    if (pipe(pl) < 0){ fclose(out); return -1; }
    if (pipe(pp) < 0){
        close(pl[0]); close(pl[1]);
        fclose(out); return -1;
    }
    if (pipe(pr) < 0){
        close(pl[0]); close(pl[1]);
        close(pp[0]); close(pp[1]);
        fclose(out); return -1;
    }

    /* 3) Fork del mapper. */
    pid_t pid_m = fork();
    if (pid_m < 0){
        close(pl[0]); close(pl[1]);
        close(pp[0]); close(pp[1]);
        close(pr[0]); close(pr[1]);
        fclose(out); return -1;
    }
    if (pid_m == 0){
        /* MAPPER CHILD: usa pl[0] (in), pp[1] (out) */
        close(pl[1]); close(pp[0]);
        close(pr[0]); close(pr[1]);
        fclose(out);

        if (dup2(pl[0], STDIN_FILENO)  < 0) _exit(1);
        if (dup2(pp[1], STDOUT_FILENO) < 0) _exit(1);
        close(pl[0]); close(pp[1]);

        int rc = mapper_child(mr, STDIN_FILENO, STDOUT_FILENO);
        _exit(rc == 0 ? 0 : 1);
    }

    /* 4) Fork del reducer. */
    pid_t pid_r = fork();
    if (pid_r < 0){
        close(pl[0]); close(pl[1]);
        close(pp[0]); close(pp[1]);
        close(pr[0]); close(pr[1]);
        fclose(out);
        waitpid(pid_m, NULL, 0);
        return -1;
    }
    if (pid_r == 0){
        /* REDUCER CHILD: usa pp[0] (in), pr[1] (out) */
        close(pl[0]); close(pl[1]);
        close(pp[1]); close(pr[0]);
        fclose(out);

        if (dup2(pp[0], STDIN_FILENO)  < 0) _exit(1);
        if (dup2(pr[1], STDOUT_FILENO) < 0) _exit(1);
        close(pp[0]); close(pr[1]);

        int rc = reducer_child(mr, STDIN_FILENO, STDOUT_FILENO);
        _exit(rc == 0 ? 0 : 1);
    }

    /* 5) PARENT: chiude tutti gli fd che non gli servono. */
    close(pl[0]);
    close(pp[0]); close(pp[1]);
    close(pr[1]);

    int rcode = 0;

    /* 6) Invio le righe al mapper. */
    if (S_ISDIR(st.st_mode)) {
        rcode = send_dir_lines(input_path, pl[1]);
    } else if (S_ISREG(st.st_mode)) {
        rcode = send_file_lines(input_path, pl[1]);
    } else {
        errno = EINVAL;
        rcode = -1;
    }

    /* 7) Chiudo pl[1]: segnala EOF al mapper, a cascata al reducer. */
    close(pl[1]);

    /* 8) Leggo i risultati dal reducer e scrivo il .mro. */
    writer_t w;
    w.output = out; w.results = 0; w.error = 0;

    if (rcode == 0){
        for (;;){
            char *tok = NULL;
            void *res = NULL;
            size_t rsize = 0;
            int got = pair_r(pr[0], &tok, &res, &rsize);
            if (got == 0) break;
            if (got == -1){ rcode = -1; break; }

            int wr = write_result(tok, res, rsize, &w);
            free(tok); free(res);
            if (wr != 0){ rcode = -1; break; }
        }
    }
    close(pr[0]);

    /* 9) Attendo i figli. */
    int status;
    if (waitpid(pid_m, &status, 0) < 0 ||
        !WIFEXITED(status) || WEXITSTATUS(status) != 0) rcode = -1;
    if (waitpid(pid_r, &status, 0) < 0 ||
        !WIFEXITED(status) || WEXITSTATUS(status) != 0) rcode = -1;

    if (fclose(out) != 0) rcode = -1;
    return rcode;
}

/* ================================================================== */
/*                         Protocollo Interno                         */
/* ================================================================== */

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
        if (r < 0){                         //la syscall ha fallito
            if (errno == EINTR) continue;   //era solo un segnale: riprova, non è un errore
            return -1;                      //errore vero: propaga
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


/* Serializza una coppia sulla pipe */

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

/* Riceve e deserializza una coppia */

/*
 * *token_out  : C-string allocata con malloc (token_len+1, terminata da '\0').
 * *value_out  : buffer opaco allocato con malloc; NULL se value_size == 0.
 * *value_size_out: numero di byte del valore.
 *
 * Ritorna  1 se ha letto una coppia completa,
 *          0 se EOF pulito (nessun header iniziato),
 *         -1 in caso di errore (errno impostato; nessuna memoria da liberare
 *            per il chiamante).
 */

static int pair_r(int fd, char **token_out, void **value_out,
                  size_t *value_size_out){
    pair_header_t h;
    ssize_t r = read_n(fd, &h, sizeof(h));

    if (r == 0) return 0; //EOF pulito
    if (r == -1) return -1;           /* errore di I/O: errno gia' impostato da read_n */
    CHECK(r == (ssize_t)sizeof(h), EPROTO);   /* arrivati qui, e' troncato */
    
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

/* Scrive una mr_file_line_t sulla pipe */
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

    if (r == 0)  return 0;
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
    *line_out  = line;
    return 1;
}
