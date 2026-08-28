package fr.insee.vtl.engine.semantics.udo;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.util.List;
import org.junit.jupiter.api.Test;

class UdoStructureCheckTest {

  private static final Positioned POS = () -> new Positioned.Position("test", 1, 1, 0, 0);

  @Test
  void matchingStructurePasses() {
    UdoDatasetSignature expected =
        new UdoDatasetSignature(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("long1", Long.class, Dataset.Role.MEASURE)),
            List.of());
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("long1", Long.class, Dataset.Role.MEASURE)),
            List.of("x", 1L));

    assertThatCode(
            () ->
                UdoStructureCheck.requireDatasetMatches(expected, dataset, "argument 'ds'", POS))
        .doesNotThrowAnyException();
  }

  @Test
  void missingComponentIsRejected() {
    UdoDatasetSignature expected =
        new UdoDatasetSignature(
            List.of(new Structured.Component("long1", Long.class, Dataset.Role.MEASURE)), List.of());
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER)),
            List.of("x"));

    assertThatThrownBy(
            () ->
                UdoStructureCheck.requireDatasetMatches(expected, dataset, "argument 'ds'", POS))
        .isInstanceOf(VtlScriptException.class)
        .hasMessageContaining("missing component 'long1'");
  }

  @Test
  void wrongRoleIsRejected() {
    UdoDatasetSignature expected =
        new UdoDatasetSignature(
            List.of(new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER)), List.of());
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(new Structured.Component("id", String.class, Dataset.Role.MEASURE)),
            List.of("x"));

    assertThatThrownBy(
            () ->
                UdoStructureCheck.requireDatasetMatches(expected, dataset, "argument 'ds'", POS))
        .isInstanceOf(VtlScriptException.class)
        .hasMessageContaining("has role")
        .hasMessageContaining("expected");
  }

  @Test
  void wildcardRequiresExactlyOneMeasure() throws VtlScriptException {
    UdoDatasetSignature expected =
        new UdoDatasetSignature(
            List.of(new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER)),
            List.of(
                new UdoDatasetSignature.Wildcard(
                    Dataset.Role.MEASURE,
                    Long.class,
                    UdoDatasetSignature.WildcardMultiplicity.EXACTLY_ONE)));
    InMemoryDataset ok =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("long1", Long.class, Dataset.Role.MEASURE)),
            List.of("x", 1L));
    InMemoryDataset missingMeasure =
        new InMemoryDataset(
            List.of(new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER)),
            List.of("x"));
    InMemoryDataset twoMeasures =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("m1", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("m2", Long.class, Dataset.Role.MEASURE)),
            List.of("x", 1L, 2L));

    assertThatCode(
            () -> UdoStructureCheck.requireDatasetMatches(expected, ok, "argument 'ds'", POS))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                UdoStructureCheck.requireDatasetMatches(
                    expected, missingMeasure, "argument 'ds'", POS))
        .hasMessageContaining("exactly one");
    assertThatThrownBy(
            () ->
                UdoStructureCheck.requireDatasetMatches(expected, twoMeasures, "argument 'ds'", POS))
        .hasMessageContaining("exactly one");
  }
}
