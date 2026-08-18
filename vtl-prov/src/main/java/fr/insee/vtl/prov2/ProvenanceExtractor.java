package fr.insee.vtl.prov2;

import java.util.List;

/**
 * Extracts a provenance graph from a VTL script and its input bindings.
 *
 * <p>Implementations must throw {@link UnsupportedOperationException} with an {@code unsupported:
 * …} message on syntax they do not handle — never a plausible-but-wrong graph.
 */
@FunctionalInterface
public interface ProvenanceExtractor {

  ProvGraph extract(String script, List<InputDataset> inputs);
}
