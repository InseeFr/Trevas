package fr.insee.vtl.engine.attribute;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * VTL 2.1 attribute propagation — value rules (User Manual, attribute components).
 *
 * <p><strong>Grouped aggregation</strong> (TCK ex. 163): for each viral component, reduce the
 * group's values with the minimum ({@code null} first, then natural order). Empty strings
 * participate like any other string value.
 *
 * <p><strong>Warning:</strong> using {@code min} as the group reducer is an implementation aligned
 * with today's TCK golden files, not a verbatim copy of the User Manual algorithm. It may need to
 * change if the VTL 2.1 spec, User Manual, or reference examples are updated — see {@code
 * package-info.java} in this package.
 *
 * <p><strong>Unary</strong> operations copy viral column values unchanged (handled by retaining
 * columns through operators; see {@link AttributePropagation}).
 *
 * <p><strong>Binary</strong> propagation after joins is not implemented here (Phase 5).
 */
public final class AttributePropagationAlgorithm {

  private AttributePropagationAlgorithm() {}

  /**
   * Reduces viral attribute values within one aggregation group.
   *
   * @return {@code null} when the group is empty; minimum with {@code nullsFirst} otherwise (same
   *     rule as {@link fr.insee.vtl.model.AggregationExpression#min}).
   */
  public static Object reduceGroupedValues(List<Object> values, Class<?> type) {
    if (values == null || values.isEmpty()) {
      return null;
    }
    if (String.class.equals(type)) {
      return minNullsFirst(values, String.class);
    }
    if (Long.class.equals(type)) {
      return minNullsFirst(values, Long.class);
    }
    if (Double.class.equals(type)) {
      return minNullsFirst(values, Double.class);
    }
    if (Boolean.class.equals(type)) {
      return minNullsFirst(values, Boolean.class);
    }
    if (Instant.class.equals(type)) {
      return minNullsFirst(values, Instant.class);
    }
    throw new IllegalArgumentException(
        "viral attribute type is not comparable for aggregation: " + type.getName());
  }

  /** Stream variant used by collectors. */
  public static Object reduceGroupedValues(Stream<Object> values, Class<?> type) {
    return reduceGroupedValues(values.toList(), type);
  }

  private static <T extends Comparable<? super T>> T minNullsFirst(
      List<Object> values, Class<T> type) {
    Comparator<T> comparator = Comparator.nullsFirst(Comparator.naturalOrder());
    T min = null;
    boolean seen = false;
    for (Object value : values) {
      T cast = type.cast(value);
      if (!seen || comparator.compare(cast, min) < 0) {
        min = cast;
        seen = true;
      }
    }
    return seen ? min : null;
  }
}
