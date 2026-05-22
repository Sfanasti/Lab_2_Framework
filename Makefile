CC      = gcc
CFLAGS  = -std=c11 -Wall -Wextra -Iinclude
LDFLAGS = -pthread
AR      = ar
ARFLAGS = rcs

BUILD   = build
LIB     = $(BUILD)/libmr.a
LIBOBJ  = $(BUILD)/mr.o

EXAMPLES = $(BUILD)/wordcount $(BUILD)/wc_dump $(BUILD)/multi \
           $(BUILD)/index $(BUILD)/mro_dump

.PHONY: all lib test clean

all: $(LIB) $(EXAMPLES)

# Produce solo la libreria statica
lib: $(LIB)

$(BUILD):
	mkdir -p $(BUILD)

# Oggetti del framework
$(BUILD)/%.o: src/%.c | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

# Libreria statica
$(LIB): $(LIBOBJ)
	$(AR) $(ARFLAGS) $@ $^

# Esempio: linkato contro la libreria statica
$(BUILD)/wordcount: examples/wordcount.c $(LIB) | $(BUILD)
	$(CC) $(CFLAGS) $< $(LIB) -o $@ $(LDFLAGS)

# Esempio addendum: due mr_t indipendenti
$(BUILD)/multi: examples/multi.c $(LIB) | $(BUILD)
	$(CC) $(CFLAGS) $< $(LIB) -o $@ $(LDFLAGS)

# Esempio NON-wordcount: indice numeri di riga per token (prova genericita')
$(BUILD)/index: examples/index.c $(LIB) | $(BUILD)
	$(CC) $(CFLAGS) $< $(LIB) -o $@ $(LDFLAGS)

# Dumper generico: risultato come testo
$(BUILD)/mro_dump: examples/mro_dump.c | $(BUILD)
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)

# Strumento di verifica: indipendente dalla libreria
$(BUILD)/wc_dump: examples/wc_dump.c | $(BUILD)
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)

test: all
	@sh tests/run.sh

clean:
	rm -rf $(BUILD)
	rm -f mr.log mr.stats mr.*.log mr.*.stats *.mro