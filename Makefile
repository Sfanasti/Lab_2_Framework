CC      = gcc
CFLAGS  = -std=c11 -Wall -Wextra -Iinclude
LDFLAGS = -pthread

all: build/stub

build/stub: src/stub.c | build
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)

build:
	mkdir -p build

clean:
	rm -rf build