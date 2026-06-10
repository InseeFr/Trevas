package fr.insee.vtl.engine.membership;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.processors.InMemoryProcessingEngine;
import fr.insee.vtl.engine.utils.DefaultMeasureNames;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.Structured;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** In-memory scenarios aligned with VTL 2.1 Reference Manual membership examples (TCK 86–91). */
class MembershipTckScenariosTest {

  private static final Positioned POS = () -> new Positioned.Position("test", 1, 1, 0, 0);

  private InMemoryProcessingEngine engine;
  private DatasetExpression ds1;
  private DatasetExpression ds2;

  @BeforeEach
  void setUp() {
    engine = new InMemoryProcessingEngine();
    ds1 = DatasetExpression.of(ds1(), POS);
    ds2 = DatasetExpression.of(ds2(), POS);
  }

  @Test
  void ds1MeasureMembershipEx1() {
    var result = MembershipOperations.execute(engine, ds1, "Me_1").resolve(Map.of());

    assertThat(result.getColumnNames()).containsExactly("Id_1", "Id_2", "Me_1");
    assertThat(result.getDataAsMap().get(0))
        .containsEntry("Id_1", 1L)
        .containsEntry("Id_2", "A")
        .containsEntry("Me_1", 1L);
  }

  @Test
  void ds1IdentifierMembershipEx2() {
    var result = MembershipOperations.execute(engine, ds1, "Id_1").resolve(Map.of());

    assertThat(result.getColumnNames())
        .containsExactly("Id_1", "Id_2", DefaultMeasureNames.INT_VAR);
    assertThat(result.getDataAsMap().get(1).get(DefaultMeasureNames.INT_VAR)).isEqualTo(1L);
    assertThat(result.getDataStructure().containsKey("Me_1")).isFalse();
  }

  @Test
  void ds1AttributeMembershipEx3() {
    var result = MembershipOperations.execute(engine, ds1, "At_1").resolve(Map.of());

    assertThat(result.getColumnNames())
        .containsExactly("Id_1", "Id_2", DefaultMeasureNames.STRING_VAR);
    assertThat(result.getDataStructure().containsKey("At_1")).isFalse();
    assertThat(result.getDataAsMap().get(1).get(DefaultMeasureNames.STRING_VAR)).isEqualTo("P");
  }

  @Test
  void ds2MeasureMembershipKeepsViralEx4() {
    var result = MembershipOperations.execute(engine, ds2, "Me_1").resolve(Map.of());

    assertThat(result.getColumnNames()).containsExactly("Id_1", "Id_2", "Me_1", "At_1");
    assertThat(result.getDataAsMap().get(1).get("At_1")).isEqualTo("P");
  }

  @Test
  void ds2IdentifierMembershipKeepsViralEx5() {
    var result = MembershipOperations.execute(engine, ds2, "Id_1").resolve(Map.of());

    assertThat(result.getColumnNames())
        .containsExactly("Id_1", "Id_2", DefaultMeasureNames.INT_VAR, "At_1");
    assertThat(result.getDataAsMap().get(1).get("At_1")).isEqualTo("P");
  }

  @Test
  void ds2ViralAttributeMembershipEx6() {
    var result = MembershipOperations.execute(engine, ds2, "At_1").resolve(Map.of());

    assertThat(result.getColumnNames())
        .containsExactly("Id_1", "Id_2", DefaultMeasureNames.STRING_VAR, "At_1");
    assertThat(result.getDataAsMap().get(1).get(DefaultMeasureNames.STRING_VAR)).isEqualTo("P");
    assertThat(result.getDataAsMap().get(1).get("At_1")).isEqualTo("P");
  }

  private static InMemoryDataset ds1() {
    return new InMemoryDataset(
        List.of(
            new Structured.Component("Id_1", Long.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE),
            new Structured.Component("Me_2", Long.class, Dataset.Role.MEASURE),
            new Structured.Component("At_1", String.class, Dataset.Role.ATTRIBUTE)),
        row(1L, "A", 1L, 5L, null),
        row(1L, "B", 2L, 10L, "P"),
        row(2L, "A", 3L, 12L, null));
  }

  private static InMemoryDataset ds2() {
    return new InMemoryDataset(
        List.of(
            new Structured.Component("Id_1", Long.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE),
            new Structured.Component("Me_2", Long.class, Dataset.Role.MEASURE),
            new Structured.Component("At_1", String.class, Dataset.Role.VIRALATTRIBUTE)),
        row(1L, "A", 1L, 5L, null),
        row(1L, "B", 2L, 10L, "P"),
        row(2L, "A", 3L, 12L, null));
  }

  private static List<Object> row(Object... values) {
    return Arrays.asList(values);
  }
}
