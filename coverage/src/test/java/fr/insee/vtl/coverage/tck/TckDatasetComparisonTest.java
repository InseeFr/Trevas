package fr.insee.vtl.coverage.tck;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TckDatasetComparisonTest {

  @Test
  void sameRowsIgnoresOrder() {
    List<Map<String, Object>> a =
        List.of(
            row("Id_1", 2010L, "Id_3", "XX", "Me_1", 22.0),
            row("Id_1", 2010L, "Id_3", "YY", "Me_1", 23.0));
    List<Map<String, Object>> b =
        List.of(
            row("Id_1", 2010L, "Id_3", "YY", "Me_1", 23.0),
            row("Id_1", 2010L, "Id_3", "XX", "Me_1", 22.0));
    assertThat(TckDatasetComparison.sameRows(a, b)).isTrue();
  }

  @Test
  void sameRowsRejectsDifferentValues() {
    List<Map<String, Object>> a = List.of(row("Id_1", 2010L, "Me_1", 22.0));
    List<Map<String, Object>> b = List.of(row("Id_1", 2010L, "Me_1", 23.0));
    assertThat(TckDatasetComparison.sameRows(a, b)).isFalse();
  }

  @Test
  void sameRowsHandlesDuplicateRows() {
    Map<String, Object> row = row("Id_1", 1L, "Me_1", 2.0);
    List<Map<String, Object>> a = List.of(row, row("Id_1", 1L, "Me_1", 2.0));
    List<Map<String, Object>> b =
        List.of(row("Id_1", 1L, "Me_1", 2.0), row("Id_1", 1L, "Me_1", 2.0));
    assertThat(TckDatasetComparison.sameRows(a, b)).isTrue();
    assertThat(TckDatasetComparison.sameRows(a, List.of(row))).isFalse();
  }

  @Test
  void sameRowsUsesNumericTolerance() {
    List<Map<String, Object>> a = List.of(row("Me_1", 22.799999999999997));
    List<Map<String, Object>> b = List.of(row("Me_1", 22.8));
    assertThat(TckDatasetComparison.sameRows(a, b)).isTrue();
  }

  private static Map<String, Object> row(Object... keyValuePairs) {
    if (keyValuePairs.length % 2 != 0) {
      throw new IllegalArgumentException("even number of arguments required");
    }
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < keyValuePairs.length; i += 2) {
      map.put((String) keyValuePairs[i], keyValuePairs[i + 1]);
    }
    return map;
  }
}
