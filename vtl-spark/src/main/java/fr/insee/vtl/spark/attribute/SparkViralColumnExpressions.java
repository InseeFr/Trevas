package fr.insee.vtl.spark.attribute;

import static org.apache.spark.sql.functions.when;

import java.time.Instant;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL expressions matching {@link
 * fr.insee.vtl.engine.attribute.AttributePropagationAlgorithm} (min with nulls first).
 */
public final class SparkViralColumnExpressions {

  private SparkViralColumnExpressions() {}

  /** Merges two viral values on one row (binary join / multi-column projection). */
  public static Column merge(Column left, Column right, Class<?> type) {
    if (String.class.equals(type)) {
      return mergeComparable(left, right);
    }
    if (Long.class.equals(type)) {
      return mergeComparable(left, right);
    }
    if (Double.class.equals(type)) {
      return mergeComparable(left, right);
    }
    if (Boolean.class.equals(type)) {
      return mergeComparable(left, right);
    }
    if (Instant.class.equals(type)) {
      return mergeComparable(
          left.cast(DataTypes.TimestampType), right.cast(DataTypes.TimestampType));
    }
    throw new IllegalArgumentException(
        "unsupported viral attribute type for merge: " + type.getName());
  }

  private static Column mergeComparable(Column left, Column right) {
    return when(left.isNull(), left)
        .when(right.isNull(), right)
        .when(left.lt(right), left)
        .otherwise(right);
  }
}
