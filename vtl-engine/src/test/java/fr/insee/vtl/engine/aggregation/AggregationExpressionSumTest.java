package fr.insee.vtl.engine.aggregation;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.Structured.DataPoint;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class AggregationExpressionSumTest {

  private static final Positioned.Position POSITION = new Positioned.Position("test", 1, 1, 0, 0);

  @Test
  void sumLongOperandTypeIsLong() {
    ResolvableExpression longMeasure =
        ResolvableExpression.withType(Long.class)
            .withPosition(POSITION)
            .using(ctx -> Long.class.cast(ctx.get("me_1")));

    assertThat(AggregationExpression.sum(longMeasure).getType()).isEqualTo(Long.class);
  }

  @Test
  void sumDoubleOperandTypeIsDouble() {
    ResolvableExpression doubleMeasure =
        ResolvableExpression.withType(Double.class)
            .withPosition(POSITION)
            .using(ctx -> Double.class.cast(ctx.get("me_1")));

    assertThat(AggregationExpression.sum(doubleMeasure).getType()).isEqualTo(Double.class);
  }

  @Test
  void sumLongValuesStaysLong() {
    DataStructure structure =
        new DataStructure(
            List.of(new Structured.Component("me_1", Long.class, Dataset.Role.MEASURE)));
    ResolvableExpression longMeasure =
        ResolvableExpression.withType(Long.class)
            .withPosition(POSITION)
            .using(ctx -> Long.class.cast(ctx.get("me_1")));

    AggregationExpression sum = AggregationExpression.sum(longMeasure);
    Object result =
        Stream.of(
                new DataPoint(structure, Map.of("me_1", 2L)),
                new DataPoint(structure, Map.of("me_1", 3L)))
            .collect(sum);

    assertThat(result).isEqualTo(5L);
    assertThat(sum.getType()).isEqualTo(Long.class);
  }

  @Test
  void sumDoubleOperandProducesDouble() {
    DataStructure structure =
        new DataStructure(
            List.of(new Structured.Component("me_1", Double.class, Dataset.Role.MEASURE)));
    ResolvableExpression doubleMeasure =
        ResolvableExpression.withType(Double.class)
            .withPosition(POSITION)
            .using(ctx -> Double.class.cast(ctx.get("me_1")));

    AggregationExpression sum = AggregationExpression.sum(doubleMeasure);
    Object result =
        Stream.of(
                new DataPoint(structure, Map.of("me_1", 2D)),
                new DataPoint(structure, Map.of("me_1", 1.5D)))
            .collect(sum);

    assertThat(result).isEqualTo(3.5D);
    assertThat(sum.getType()).isEqualTo(Double.class);
  }
}
