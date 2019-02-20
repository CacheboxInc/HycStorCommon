CC=gcc
CFLAGS= -Wall -g -fPIC -O3 -fno-common -fno-strict-aliasing -march=nocona -rdynamic -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE -DAS_USE_LIBUV
INC=-I/usr/local/include -L/usr/lib/x86_64-linux-gnu/ -L../../build/thirdparty/hyc-datapack/src/compression/ -L../../build/thirdparty/hyc-datapack/src/encryption/
#LDFLAGS=-laerospike -luv -lssl -lpthread -lrt -ldl -lm -lz -lHycCompress -lsnappy -llz4 -lHycEncryption -lcrypto

LDFLAGS=/usr/lib/libaerospike.a /usr/lib/x86_64-linux-gnu/libuv.a -lssl -lpthread -lrt -ldl -lm -lz -lHycCompress -lsnappy -llz4 -lHycEncryption -lcrypto

all: AerospikeScanCache.c 
	$(CC) $(CFLAGS) $(INC) -o aero_scan AerospikeScanCache.c $(LDFLAGS)

clean:
	rm -f aero_scan *.o
