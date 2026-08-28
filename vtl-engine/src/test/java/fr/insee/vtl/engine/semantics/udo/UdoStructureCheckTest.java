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
    Structured.DataStructure expected =
        new Structured.DataStructure(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("long1", Long.class, Dataset.Role.MEASURE)));
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
    Structured.DataStructure expected =
        new Structured.DataStructure(
            List.of(new Structured.Component("long1", Long.class, Dataset.Role.MEASURE)));
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
    Structured.DataStructure expected =
        new Structured.DataStructure(
            List.of(new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER)));
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
}
