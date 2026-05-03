package fr.insee.vtl.coverage.tck;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Row-by-row comparison for TCK outputs: same length, same key sets per row, values equal modulo
 * numeric tolerance (IEEE-754 / Spark vs CSV literals).
 */
public final class TckDatasetComparison {

  /**
   * Absolute tolerance for floating-point noise (e.g. {@code -22.8} vs {@code
   * -22.799999999999997}).
   */
  private static final double ABS_EPS = 1e-9;

  /** Relative tolerance scaled by magnitude (stable on larger measures). */
  private static final double REL_EPS = 1e-12;

  private TckDatasetComparison() {}

  public static boolean sameRowOrder(
      List<Map<String, Object>> actual, List<Map<String, Object>> expected) {
    if (actual.size() != expected.size()) {
      return false;
    }
    for (int i = 0; i < actual.size(); i++) {
      if (!rowMapsEqual(actual.get(i), expected.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean rowMapsEqual(Map<String, Object> actual, Map<String, Object> expected) {
    if (actual.size() != expected.size()) {
      return false;
    }
    if (!actual.keySet().equals(expected.keySet())) {
      return false;
    }
    for (String key : actual.keySet()) {
      if (!valuesEqual(actual.get(key), expected.get(key))) {
        return false;
      }
    }
    return true;
  }

  private static boolean valuesEqual(Object a, Object b) {
    if (a == b) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    if (a instanceof Number na && b instanceof Number nb) {
      return numbersClose(na, nb);
    }
    return Objects.equals(a, b);
  }

  private static boolean numbersClose(Number a, Number b) {
    double x = a.doubleValue();
    double y = b.doubleValue();
    if (Double.isNaN(x) && Double.isNaN(y)) {
      return true;
    }
    if (Double.isNaN(x) || Double.isNaN(y)) {
      return false;
    }
    if (Double.isInfinite(x) && Double.isInfinite(y) && x == y) {
      return true;
    }
    double diff = Math.abs(x - y);
    if (diff <= ABS_EPS) {
      return true;
    }
    double scale = Math.max(Math.abs(x), Math.abs(y));
    return diff <= REL_EPS * Math.max(1.0, scale);
  }
}
