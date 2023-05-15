CFLAGS  = -Wall -Wextra -std=c++17
LFLAGS  =
CC      = g++
RM      = /bin/rm -rf
UNAME_S := $(shell uname -s)

LIBRARY = ../bin/libgtfs.a

TESTS = test

# Platform Specific Compiler Flags
ifeq ($(UNAME_S),Linux)
    LFLAGS += -lstdc++fs
endif

all: $(TESTS)

test : test.cpp
	$(CC) $(CFLAGS) test.cpp $(LIBRARY) -o test $(LFLAGS)

clean:
	$(RM) *.o $(TESTS)
