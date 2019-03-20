CC = cc

OPTFLAGS = -g
INCLUDE_FLAGS = -I .. -I ../third_party/murmur3-master
CLIENTLIB = ../libReadAhead.a  ../third_party/murmur3-master
LD_FLAGS = -L .. -lReadAhead -L ../third_party/murmur3-master -lmurmur3

TARGETS = gtest

default: targets

.PHONY: targets
targets: $(TARGETS)

gtest:	gtest.c $(CLIENTLIB)
	$(CC) $(OPTFLAGS) $(INCLUDE_FLAGS) -o $@ $< $(LD_FLAGS)	

clean:
	-rm -rf *~ $(TARGETS)
