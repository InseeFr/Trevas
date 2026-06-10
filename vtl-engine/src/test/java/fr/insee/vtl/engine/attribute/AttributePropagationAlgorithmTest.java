package fr.insee.vtl.engine.attribute;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class AttributePropagationAlgorithmTest {

  @Test
  void reduceGroupedStringsUsesMinimumIncludingEmpty() {
    assertThat(
            AttributePropagationAlgorithm.reduceGroupedValues(
                List.of("", "H", "A", ""), String.class))
        .isEqualTo("");
    assertThat(
            AttributePropagationAlgorithm.reduceGroupedValues(
                List.of("P", "N", "P", "Z"), String.class))
        .isEqualTo("N");
    assertThat(AttributePropagationAlgorithm.reduceGroupedValues(List.of("P"), String.class))
        .isEqualTo("P");
  }

  @Test
  void reduceGroupedValuesUsesNullsFirstLikeMinAggregation() {
    assertThat(
            AttributePropagationAlgorithm.reduceGroupedValues(
                Arrays.asList(null, "A", null), String.class))
        .isNull();
    assertThat(
            AttributePropagationAlgorithm.reduceGroupedValues(
                Arrays.asList(null, null), String.class))
        .isNull();
    assertThat(
            AttributePropagationAlgorithm.reduceGroupedValues(
                Arrays.asList("P", "N", "P", "Z"), String.class))
        .isEqualTo("N");
  }
}
