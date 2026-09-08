package fr.insee.vtl.coverage.tck;

import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.Objects;
import java.util.Set;

/**
 * Structure comparison for TCK outputs. Strict by default ({@link DataStructure#equals(Object)}).
 *
 * <p><b>Hack (VTL 2.1 only):</b> a small allowlist of {@code sum} examples declare Integer vs
 * Number inconsistently for the same pattern (see sdmx-twg/vtl#708; clarified in 2.2 via PR #713).
 * For those paths only, {@link Long} and {@link Double} measure/component types are treated as
 * compatible when name and role match. All other cases stay strict. Remove when targeting VTL 2.2
 * fixtures.
 */
public final class TckStructureComparison {

  /**
   * Display-path markers for the known 2.1 {@code sum} Integer/Number fixture drift. Matched with
   * {@link String#contains(CharSequence)} against {@link TckPaths} labels.
   */
  private static final Set<String> SUM_INTEGER_NUMBER_DRIFT_PATH_MARKERS =
      Set.of(
          "Aggregate and Analytic operators" + TckPaths.SEGMENT_SEP + "Sum" + TckPaths.SEGMENT_SEP,
          "Aggregate and Analytic operators"
              + TckPaths.SEGMENT_SEP
              + "Aggregate invocation"
              + TckPaths.SEGMENT_SEP
              + "ex_2",
          "Aggregate and Analytic operators"
              + TckPaths.SEGMENT_SEP
              + "Analytic invocation"
              + TckPaths.SEGMENT_SEP
              + "ex_1",
          "Clause operators" + TckPaths.SEGMENT_SEP + "Aggregation" + TckPaths.SEGMENT_SEP + "ex_1",
          "Clause operators"
              + TckPaths.SEGMENT_SEP
              + "Aggregation"
              + TckPaths.SEGMENT_SEP
              + "ex_3");

  private TckStructureComparison() {}

  /** Whether this leaf is in the temporary {@code sum} Integer/Number allowlist. */
  public static boolean isSumIntegerNumberDriftCase(String displayPath) {
    if (displayPath == null || displayPath.isEmpty()) {
      return false;
    }
    for (String marker : SUM_INTEGER_NUMBER_DRIFT_PATH_MARKERS) {
      if (displayPath.contains(marker)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Strict structure equality, except for allowlisted {@code sum} drift cases where Long↔Double is
   * accepted on otherwise equal components.
   */
  public static boolean sameStructure(
      DataStructure actual, DataStructure expected, String displayPath) {
    if (Objects.equals(actual, expected)) {
      return true;
    }
    if (!isSumIntegerNumberDriftCase(displayPath)) {
      return false;
    }
    return sameStructureAllowingIntegerNumber(actual, expected);
  }

  static boolean sameStructureAllowingIntegerNumber(DataStructure actual, DataStructure expected) {
    if (actual == null || expected == null || actual.size() != expected.size()) {
      return false;
    }
    if (!actual.keySet().equals(expected.keySet())) {
      return false;
    }
    for (String name : actual.keySet()) {
      Component a = actual.get(name);
      Component e = expected.get(name);
      if (a == null || e == null) {
        return false;
      }
      if (!a.getName().equals(e.getName()) || a.getRole() != e.getRole()) {
        return false;
      }
      if (!typesCompatibleForSumDrift(a.getType(), e.getType())) {
        return false;
      }
    }
    return true;
  }

  private static boolean typesCompatibleForSumDrift(Class<?> actual, Class<?> expected) {
    if (Objects.equals(actual, expected)) {
      return true;
    }
    return isIntegerOrNumber(actual) && isIntegerOrNumber(expected);
  }

  private static boolean isIntegerOrNumber(Class<?> type) {
    return Long.class.equals(type) || Double.class.equals(type);
  }
}
