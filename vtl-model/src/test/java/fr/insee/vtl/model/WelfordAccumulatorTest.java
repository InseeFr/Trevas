package fr.insee.vtl.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class WelfordAccumulatorTest {

  @Test
  void sampleVarianceMatchesTwoPassReference() {
    List<Long> values = Arrays.asList(2L, 4L, 4L, 4L, 5L, 5L, 7L, 9L);
    Double welford = values.stream().collect(WelfordAccumulator.longVarianceCollector(false));
    double mean = values.stream().mapToLong(v -> v).average().orElse(0);
    double ref =
        values.stream().mapToDouble(v -> Math.pow(v - mean, 2)).sum() / (values.size() - 1);
    assertThat(welford).isCloseTo(ref, org.assertj.core.data.Offset.offset(1e-9));
  }

  @Test
  void populationStdDevReturnsNullWhenAnyNull() {
    List<Double> values = Arrays.asList(1.0, 2.0, null, 4.0);
    Double result = values.stream().collect(WelfordAccumulator.doubleStdDevCollector(true));
    assertThat(result).isNull();
  }

  @Test
  void singleValueGroupReturnsZero() {
    assertThat(List.of(42L).stream().collect(WelfordAccumulator.longVarianceCollector(true)))
        .isEqualTo(0D);
  }
}
