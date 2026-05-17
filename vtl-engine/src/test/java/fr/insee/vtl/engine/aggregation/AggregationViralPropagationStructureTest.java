package fr.insee.vtl.engine.aggregation;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.AggregationViralPropagation;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** TCK-aligned viral structure for aggregate invocation vs {@code aggr} clause (tests 162–165). */
class AggregationViralPropagationStructureTest {

  private static final Positioned.Position POS = new Positioned.Position("test", 1, 1, 0, 0);

  private static Structured.DataStructure inputWithViral() {
    return new Structured.DataStructure(
        List.of(
            new Structured.Component(
                "Id_1", org.threeten.extra.Interval.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("Id_3", String.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE),
            new Structured.Component("At_1", String.class, Dataset.Role.VIRALATTRIBUTE)));
  }

  private static Map<String, AggregationExpression> avgCollector() {
    return Map.of(
        "Me_1",
        AggregationExpression.avg(
            ResolvableExpression.withType(Long.class)
                .withPosition(POS)
                .using(c -> Long.class.cast(c.get("Me_1")))));
  }

  @Test
  void tck162_globalAvgIncludesViralAsAttribute() {
    var result =
        AggregationResultStructureBuilder.build(
            inputWithViral(),
            List.of(),
            avgCollector(),
            AggregationViralPropagation.INVOCATION_GLOBAL);
    assertThat(result.containsKey("At_1")).isTrue();
    assertThat(result.get("At_1").getRole()).isEqualTo(Dataset.Role.ATTRIBUTE);
  }

  @Test
  void tck163_aggrClauseGroupByKeepsViralAttributeRole() {
    var result =
        AggregationResultStructureBuilder.build(
            inputWithViral(),
            List.of("Id_1"),
            Map.of(
                "Me_2",
                AggregationExpression.max(
                    ResolvableExpression.withType(Long.class)
                        .withPosition(POS)
                        .using(c -> Long.class.cast(c.get("Me_1"))))),
            AggregationViralPropagation.AGGR_CLAUSE_GROUPED);
    assertThat(result.get("At_1").getRole()).isEqualTo(Dataset.Role.VIRALATTRIBUTE);
  }

  @Test
  void tck164_sumInvocationGroupByOmitsViral() {
    var result =
        AggregationResultStructureBuilder.build(
            inputWithViral(),
            List.of("Id_1", "Id_3"),
            Map.of(
                "Me_1",
                AggregationExpression.sum(
                    ResolvableExpression.withType(Long.class)
                        .withPosition(POS)
                        .using(c -> Long.class.cast(c.get("Me_1"))))),
            AggregationViralPropagation.INVOCATION_GROUPED);
    assertThat(result.containsKey("At_1")).isFalse();
  }

  @Test
  void tck165_avgInvocationPartialGroupByOmitsViral() {
    var result =
        AggregationResultStructureBuilder.build(
            inputWithViral(),
            List.of("Id_1"),
            avgCollector(),
            AggregationViralPropagation.INVOCATION_GROUPED);
    assertThat(result.containsKey("At_1")).isFalse();
  }
}
