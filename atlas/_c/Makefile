CC      ?= gcc
CFLAGS  ?= -std=c11 -Wall -Wextra -O2 -fPIC
ATLAS_SIGILO_ARCHFLAGS ?=

UNAME := $(shell uname -s 2>/dev/null || echo Unknown)
ifeq ($(UNAME),Darwin)
    LIB_EXT = dylib
    LDFLAGS ?= -dynamiclib -lm
    CFLAGS += $(ATLAS_SIGILO_ARCHFLAGS)
    LDFLAGS += $(ATLAS_SIGILO_ARCHFLAGS)
else ifeq ($(OS),Windows_NT)
    LIB_EXT = dll
    CFLAGS += -DATLAS_SIGILO_EXPORTS
    LDFLAGS ?= -shared
else
    LIB_EXT = so
    CFLAGS += -DATLAS_SIGILO_EXPORTS
    LDFLAGS ?= -shared -lm
endif

ifeq ($(UNAME),Darwin)
    CFLAGS += -DATLAS_SIGILO_EXPORTS
endif

LIB_NAME = libatlas_sigilo.$(LIB_EXT)

SRCS = \
	common/diagnostic.c \
	common/errors.c \
	sigil/sigil_parse.c \
	sigil/sigil_validate.c \
	atlas_render.c \
	atlas_sigilo.c

OBJS = $(SRCS:.c=.o)

.PHONY: all clean

all: $(LIB_NAME)

$(LIB_NAME): $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^
	cp $@ ../

%.o: %.c
	$(CC) $(CFLAGS) -I. -c $< -o $@

clean:
	rm -f $(OBJS) $(LIB_NAME) ../$(LIB_NAME) ../libatlas_sigilo.so ../libatlas_sigilo.dylib ../libatlas_sigilo.dll
