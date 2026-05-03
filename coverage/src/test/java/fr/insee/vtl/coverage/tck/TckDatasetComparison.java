package fr.insee.vtl.coverage.tck;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Pure comparison helpers for TCK expected vs actual row maps (order-sensitive). */
public final class TckDatasetComparison {

  private TckDatasetComparison() {}

  public static boolean sameRowOrder(
      List<Map<String, Object>> actual, List<Map<String, Object>> expected) {
    if (actual.size() != expected.size()) {
      return false;
    }
    for (int i = 0; i < actual.size(); i++) {
      if (!Objects.equals(actual.get(i), expected.get(i))) {
        return false;
      }
    }
    return true;
  }
}
