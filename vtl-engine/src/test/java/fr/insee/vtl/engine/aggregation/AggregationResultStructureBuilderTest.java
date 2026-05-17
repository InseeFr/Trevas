package fr.insee.vtl.engine.aggregation;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AggregationResultStructureBuilderTest {

  private static final Positioned.Position TEST_POSITION =
      new Positioned.Position("test", 1, 1, 0, 0);

  @Test
  void groupedSumKeepsLongMeasureType() {
    Structured.DataStructure input =
        new Structured.DataStructure(
            List.of(
                new Structured.Component("id_1", Long.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("me_1", Long.class, Dataset.Role.MEASURE)));

    Structured.DataStructure result =
        AggregationResultStructureBuilder.build(
            input,
            List.of("id_1"),
            Map.of(
                "me_1",
                AggregationExpression.sum(
                    ResolvableExpression.withType(Long.class)
                        .withPosition(TEST_POSITION)
                        .using(c -> Long.class.cast(c.get("me_1"))))));

    assertThat(result.get("me_1").getType()).isEqualTo(Double.class);
    assertThat(result.get("me_1").getRole()).isEqualTo(Dataset.Role.MEASURE);
  }

  @Test
  void globalAggregationPromotesMeasuresToIdentifiers() {
    Structured.DataStructure input =
        new Structured.DataStructure(
            List.of(
                new Structured.Component("me_1", Double.class, Dataset.Role.MEASURE),
                new Structured.Component("at_1", String.class, Dataset.Role.ATTRIBUTE)));

    Structured.DataStructure result =
        AggregationResultStructureBuilder.build(
            input,
            List.of(),
            Map.of(
                "me_1",
                AggregationExpression.avg(
                    ResolvableExpression.withType(Double.class)
                        .withPosition(TEST_POSITION)
                        .using(c -> Double.class.cast(c.get("me_1"))))));

    assertThat(result.get("me_1").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);
    assertThat(result.get("at_1")).isNull();
  }

  @Test
  void groupedAggregationPreservesViralAttributesOnly() {
    Structured.DataStructure input =
        new Structured.DataStructure(
            List.of(
                new Structured.Component("id_1", Long.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("me_1", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("at_plain", String.class, Dataset.Role.ATTRIBUTE),
                new Structured.Component("at_viral", String.class, Dataset.Role.VIRALATTRIBUTE)));

    Structured.DataStructure result =
        AggregationResultStructureBuilder.build(
            input,
            List.of("id_1"),
            Map.of(
                "me_1",
                AggregationExpression.sum(
                    ResolvableExpression.withType(Long.class)
                        .withPosition(TEST_POSITION)
                        .using(c -> Long.class.cast(c.get("me_1"))))));

    assertThat(result.get("at_plain")).isNull();
    assertThat(result.get("at_viral").getRole()).isEqualTo(Dataset.Role.ATTRIBUTE);
  }

  @Test
  void countInvocationUsesIntVarMeasureName() {
    Structured.DataStructure input =
        new Structured.DataStructure(
            List.of(
                new Structured.Component("id_1", Long.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("me_1", Long.class, Dataset.Role.MEASURE)));

    Structured.DataStructure result =
        AggregationResultStructureBuilder.build(
            input, List.of("id_1"), Map.of("int_var", AggregationExpression.count()));

    assertThat(result.get("int_var")).isNotNull();
    assertThat(result.get("int_var").getType()).isEqualTo(Long.class);
    assertThat(result.get("me_1")).isNull();
  }
}
