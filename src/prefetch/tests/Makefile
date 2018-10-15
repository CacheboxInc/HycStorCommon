CC = cc

OPTFLAGS = -g
INCLUDE_FLAGS = -I .. -I ../third_party/murmur3-master
CLIENTLIB = ../libghb.a
LD_FLAGS = -L .. -lghb

TARGETS = gtest

default: targets

.PHONY: targets
targets: $(TARGETS)

gtest:	gtest.c $(CLIENTLIB)
	$(CC) $(OPTFLAGS) $(INCLUDE_FLAGS) -o $@ $< $(LD_FLAGS)	

clean:
	-rm -rf *~ $(TARGETS)
