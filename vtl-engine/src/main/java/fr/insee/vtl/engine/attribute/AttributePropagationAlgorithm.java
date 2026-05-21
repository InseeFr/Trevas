package fr.insee.vtl.engine.attribute;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Viral attribute values: grouped reduction uses {@code min} (nulls first); unary copy is by
 * column.
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

  /**
   * Binary join / multi-operand merge: same {@code min} (nulls first) rule as grouped reduction.
   */
  public static Object propagateBinaryValue(Object left, Object right, Class<?> type) {
    List<Object> values = new ArrayList<>(2);
    values.add(left);
    values.add(right);
    return reduceGroupedValues(values, type);
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
