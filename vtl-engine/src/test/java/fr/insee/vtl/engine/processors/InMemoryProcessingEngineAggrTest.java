package fr.insee.vtl.engine.processors;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.semantics.aggregation.AggregationPlan;
import fr.insee.vtl.engine.semantics.aggregation.AggregationResults;
import fr.insee.vtl.model.*;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InMemoryProcessingEngineAggrTest {

  private static final Positioned.Position TEST_POSITION =
      new Positioned.Position("test", 1, 1, 0, 0);
  private static final Positioned POSITION = () -> TEST_POSITION;

  private final InMemoryProcessingEngine engine = new InMemoryProcessingEngine();

  @Test
  void groupedSumKeepsLongMeasureType() {
    Structured.DataStructure input =
        new Structured.DataStructure(
            List.of(
                new Structured.Component("id_1", Long.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("me_1", Long.class, Dataset.Role.MEASURE)));
    DatasetExpression dataset =
        DatasetExpression.of(new InMemoryDataset(List.<List<Object>>of(), input), POSITION);

    Map<String, AggregationExpression> measureCollectors =
        Map.of(
            "me_1",
            AggregationExpression.sum(
                ResolvableExpression.withType(Long.class)
                    .withPosition(TEST_POSITION)
                    .using(c -> Long.class.cast(c.get("me_1")))));
    var plan =
        AggregationPlan.prepare(
            input,
            List.of("id_1"),
            measureCollectors,
            AggregationViralPropagation.INVOCATION_GROUPED);

    DatasetExpression mechanical = engine.executeAggr(dataset, List.of("id_1"), plan.collectors());
    DatasetExpression result = AggregationResults.withStructure(mechanical, plan.structure());

    assertThat(mechanical.getDataStructure().get("me_1").getRole()).isEqualTo(Dataset.Role.MEASURE);
    assertThat(result.getDataStructure()).isEqualTo(plan.structure());
    assertThat(result.getDataStructure().get("me_1").getType()).isEqualTo(Double.class);
  }

  @Test
  void globalAggregationDropsNonViralAttributesFromMechanicalResult() {
    Structured.DataStructure input =
        new Structured.DataStructure(
            List.of(
                new Structured.Component("me_1", Double.class, Dataset.Role.MEASURE),
                new Structured.Component("at_1", String.class, Dataset.Role.ATTRIBUTE)));
    DatasetExpression dataset =
        DatasetExpression.of(new InMemoryDataset(List.<List<Object>>of(), input), POSITION);

    Map<String, AggregationExpression> measureCollectors =
        Map.of(
            "me_1",
            AggregationExpression.avg(
                ResolvableExpression.withType(Double.class)
                    .withPosition(TEST_POSITION)
                    .using(c -> Double.class.cast(c.get("me_1")))));
    var plan =
        AggregationPlan.prepare(
            input, List.of(), measureCollectors, AggregationViralPropagation.INVOCATION_GLOBAL);

    DatasetExpression mechanical = engine.executeAggr(dataset, List.of(), plan.collectors());
    DatasetExpression result = AggregationResults.withStructure(mechanical, plan.structure());

    assertThat(mechanical.getDataStructure().get("at_1")).isNull();
    assertThat(result.getDataStructure().get("me_1").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);
    assertThat(result.getDataStructure().get("at_1")).isNull();
  }

  @Test
  void countInvocationUsesIntVarMeasureName() {
    Structured.DataStructure input =
        new Structured.DataStructure(
            List.of(
                new Structured.Component("id_1", Long.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("me_1", Long.class, Dataset.Role.MEASURE)));
    DatasetExpression dataset =
        DatasetExpression.of(new InMemoryDataset(List.<List<Object>>of(), input), POSITION);

    Map<String, AggregationExpression> measureCollectors =
        Map.of("int_var", AggregationExpression.count());
    var plan =
        AggregationPlan.prepare(
            input,
            List.of("id_1"),
            measureCollectors,
            AggregationViralPropagation.INVOCATION_GROUPED);

    DatasetExpression result =
        AggregationResults.withStructure(
            engine.executeAggr(dataset, List.of("id_1"), plan.collectors()), plan.structure());

    assertThat(result.getDataStructure().get("int_var")).isNotNull();
    assertThat(result.getDataStructure().get("int_var").getType()).isEqualTo(Long.class);
    assertThat(result.getDataStructure().get("me_1")).isNull();
  }
}
