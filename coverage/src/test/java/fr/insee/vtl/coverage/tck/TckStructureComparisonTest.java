package fr.insee.vtl.coverage.tck;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.List;
import org.junit.jupiter.api.Test;

class TckStructureComparisonTest {

  @Test
  void strictElsewhereRejectsIntegerVsNumber() {
    DataStructure longMe = structure(Long.class);
    DataStructure doubleMe = structure(Double.class);
    String path = "Numeric operators" + TckPaths.SEGMENT_SEP + "Modulo" + TckPaths.SEGMENT_SEP + "ex_1";

    assertThat(TckStructureComparison.isSumIntegerNumberDriftCase(path)).isFalse();
    assertThat(TckStructureComparison.sameStructure(longMe, doubleMe, path)).isFalse();
  }

  @Test
  void sumAllowlistAcceptsIntegerVsNumber() {
    DataStructure longMe = structure(Long.class);
    DataStructure doubleMe = structure(Double.class);
    String path =
        "Aggregate and Analytic operators"
            + TckPaths.SEGMENT_SEP
            + "Sum"
            + TckPaths.SEGMENT_SEP
            + "ex_1"
            + TckPaths.SEGMENT_SEP
            + "ex_1";

    assertThat(TckStructureComparison.isSumIntegerNumberDriftCase(path)).isTrue();
    assertThat(TckStructureComparison.sameStructure(longMe, doubleMe, path)).isTrue();
    assertThat(TckStructureComparison.sameStructure(doubleMe, longMe, path)).isTrue();
  }

  @Test
  void sumAllowlistStillRejectsRoleOrNameMismatch() {
    DataStructure measure =
        new DataStructure(
            List.of(new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE)));
    DataStructure asId =
        new DataStructure(
            List.of(new Structured.Component("Me_1", Long.class, Dataset.Role.IDENTIFIER)));
    String path =
        "Aggregate and Analytic operators"
            + TckPaths.SEGMENT_SEP
            + "Sum"
            + TckPaths.SEGMENT_SEP
            + "ex_1";

    assertThat(TckStructureComparison.sameStructure(measure, asId, path)).isFalse();
  }

  @Test
  void sumAllowlistStillRejectsStringVsNumber() {
    DataStructure number =
        new DataStructure(
            List.of(new Structured.Component("Me_1", Double.class, Dataset.Role.MEASURE)));
    DataStructure string =
        new DataStructure(
            List.of(new Structured.Component("Me_1", String.class, Dataset.Role.MEASURE)));
    String path =
        "Aggregate and Analytic operators"
            + TckPaths.SEGMENT_SEP
            + "Analytic invocation"
            + TckPaths.SEGMENT_SEP
            + "ex_1";

    assertThat(TckStructureComparison.sameStructure(number, string, path)).isFalse();
  }

  private static DataStructure structure(Class<?> meType) {
    return new DataStructure(
        List.of(
            new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("Me_1", meType, Dataset.Role.MEASURE)));
  }
}
