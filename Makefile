CC      = gcc
CFLAGS  = -std=c11 -Wall -Wextra -Iinclude
LDFLAGS = -pthread
AR      = ar
ARFLAGS = rcs

BUILD   = build
LIB     = $(BUILD)/libmr.a
LIBOBJ  = $(BUILD)/mr.o

EXAMPLES = $(BUILD)/wordcount $(BUILD)/wc_dump

.PHONY: all test clean

all: $(LIB) $(EXAMPLES)

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

# Strumento di verifica: indipendente dalla libreria
$(BUILD)/wc_dump: examples/wc_dump.c | $(BUILD)
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)

test: all
	@sh tests/run.sh

clean:
	rm -rf $(BUILD)