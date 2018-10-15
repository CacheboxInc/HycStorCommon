UNZIP = unzip
PATCH = patch
CMAKE = cmake

# murmur3
MURMUR3_ZIP = downloads/murmur3-master.zip
MURMUR3_PATCH = patches/murmur3-master.patch
MURMUR3_DIR = murmur3-master
MURMUR3_MAKEOPTS = static

TARGETS = $(MURMUR3_DIR)

default: $(TARGETS)

$(MURMUR3_DIR): $(MURMUR3_ZIP)
	$(UNZIP) -DD $<
	$(PATCH) -s -p0 < $(MURMUR3_PATCH)
	$(MAKE) -C $@ $(MURMUR3_MAKEOPTS)

clean:
	-rm -rf *~ $(TARGETS)
