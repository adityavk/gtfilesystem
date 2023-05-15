CFLAGS  = -Wall -Wextra -std=c++17
LFLAGS  =
CC      = g++
RM      = /bin/rm -rf
AR      = ar rc
RANLIB  = ranlib
BINDIR  = bin

LIBRARY = bin/libgtfs.a

LIB_SRC = src/gtfs.cpp

LIB_OBJ = $(patsubst %.cpp,%.o,$(LIB_SRC))

# pattern rule for object files
%.o: %.cpp
	$(CC) -c $(CFLAGS) $< -o $@

all: $(LIBRARY) 

$(LIBRARY): $(LIB_OBJ)
	$(AR) $(LIBRARY) $(LIB_OBJ)
	$(RANLIB) $(LIBRARY)

$(LIB_OBJ) : $(LIB_SRC)
	@mkdir -p $(BINDIR)
	$(CC) -c $(CFLAGS) $< -o $@

clean:
	$(RM) $(LIBRARY) src/*.o tests/test
	$(RM) -r $(BINDIR)
