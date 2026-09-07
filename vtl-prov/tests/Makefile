# Render provenance golden graphs to styled images.
#
# The expected.dot goldens are the source of truth (hand-written / asserted);
# styling is applied at RENDER time via style.gvpr, so the goldens themselves are
# never modified. Node labels show role/type/src; condition edges are dashed red.
#
#   make            # styled PNGs for every case
#   make svg        # styled SVGs instead
#   make clean      # remove generated images
#
# Requires Graphviz (dot + gvpr).

DOTS  := $(wildcard */expected.dot)
PNGS  := $(DOTS:.dot=.png)
SVGS  := $(DOTS:.dot=.svg)
STYLE := style.gvpr

.PHONY: all png svg clean
all: png

png: $(PNGS)
svg: $(SVGS)

%.png: %.dot $(STYLE)
	gvpr -f $(STYLE) $< | dot -Tpng -o $@

%.svg: %.dot $(STYLE)
	gvpr -f $(STYLE) $< | dot -Tsvg -o $@

clean:
	rm -f $(PNGS) $(SVGS)
