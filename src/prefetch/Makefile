CC = cc
LD = ld
M4 = m4
AR = ar


OPTFLAGS = -g

TARGET = libghb.a

SUBDIRS = third_party

DEFINES =

HEADERS = defs.h ghb.h dc.h

SOURCES = ghb.c dc.c

OBJECTS = $(patsubst %.c, %.o, $(SOURCES))

INCLUDE_FLAGS :=										\
  $(foreach dir,$(SUBDIRS),									\
    $(subst <ROOT>,$(dir),									\
      $(shell (sed -n '/^INCLUDE_FLAGS\ =/,/^$$/p' $(dir)/PATH_OPTS | sed '1d;/^$$/d'))))

LD_FLAGS :=											\
  $(foreach dir,$(SUBDIRS),									\
    $(subst <ROOT>,$(dir),									\
      $(shell (sed -n '/^LD_FLAGS\ =/,/^$$/p' $(dir)/PATH_OPTS | sed '1d;/^$$/d'))))


default: subdirs $(TARGET) tests

$(TARGET): $(OBJECTS)
	$(LD) -r $^ $(LD_FLAGS) -o $@

%.o : %.c $(HEADERS)
	$(CC) -c $(OPTFLAGS) $(DEFINES) $(INCLUDE_FLAGS) $< -o $@

%.c : %.cm
	$(M4) $(DEFINES) macros.m4 $< > $@

.PHONY:	subdirs $(SUBDIRS)
subdirs: $(SUBDIRS)
$(SUBDIRS):
	$(MAKE) -C $@

.PHONY:	tests
tests:
	$(MAKE) -C tests

.PHONY: clean
clean:
	-rm -f *~ $(OBJECTS) $(TARGET)

.PHONY: tests_clean
tests_clean:
	$(MAKE) -C tests clean

.PHONY: distclean
distclean: clean tests_clean
	for dir in $(SUBDIRS); do $(MAKE) -C $$dir clean; done
