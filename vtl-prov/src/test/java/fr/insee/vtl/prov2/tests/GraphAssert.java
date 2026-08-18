package fr.insee.vtl.prov2.tests;

import org.assertj.core.api.AbstractAssert;

/**
 * AssertJ assertion for {@link Graph}, so tests read {@code
 * assertThat(actual).isSameGraphAs(expected)} and a mismatch reports the graph-level differences
 * (missing/unexpected vertices and edges, attribute diffs) instead of a raw collection dump.
 */
public class GraphAssert extends AbstractAssert<GraphAssert, Graph> {

  private GraphAssert(Graph actual) {
    super(actual, GraphAssert.class);
  }

  public static GraphAssert assertThat(Graph actual) {
    return new GraphAssert(actual);
  }

  public GraphAssert isSameGraphAs(Graph expected) {
    isNotNull();
    var differences = expected.diff(actual);
    if (!differences.isEmpty()) {
      failWithMessage(
          "Provenance graph differs from the golden:%n  %s",
          String.join(System.lineSeparator() + "  ", differences));
    }
    return this;
  }
}
