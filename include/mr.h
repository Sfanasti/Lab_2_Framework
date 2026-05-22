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