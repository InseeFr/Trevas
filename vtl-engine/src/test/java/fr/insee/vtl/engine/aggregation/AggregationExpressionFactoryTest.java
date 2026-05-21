package fr.insee.vtl.engine.aggregation;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.AggregationExpression;
import org.junit.jupiter.api.Test;

class AggregationExpressionFactoryTest {

  @Test
  void countRowsReturnsCountAggregation() {
    AggregationExpression count = AggregationExpressionFactory.countRows();
    assertThat(count).isNotNull();
  }
}
