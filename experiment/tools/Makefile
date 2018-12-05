CC=gcc
CFLAGS= -Wall -g -fPIC -O3 -fno-common -fno-strict-aliasing -march=nocona -rdynamic -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE -DAS_USE_LIBUV
INC=-I/usr/local/include
LDFLAGS=-laerospike -luv -lssl -lcrypto -lpthread -lrt -ldl -lm -lz

all: AerospikeScanCache.c 
	$(CC) $(CFLAGS) $(INC) -o aero_scan AerospikeScanCache.c $(LDFLAGS)

clean:
	rm -f aero_scan *.o
