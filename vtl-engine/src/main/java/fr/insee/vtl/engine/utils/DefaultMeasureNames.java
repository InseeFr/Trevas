package fr.insee.vtl.engine.utils;

/**
 * Default measure names when a dataset operator changes the scalar type (VTL 2.1 typical
 * behaviour).
 */
public final class DefaultMeasureNames {

  public static final String BOOL_VAR = "bool_var";
  public static final String INT_VAR = "int_var";
  public static final String NUM_VAR = "num_var";
  public static final String STRING_VAR = "string_var";

  private DefaultMeasureNames() {}

  /** Returns whether the result measure must use a default name because the scalar type changed. */
  public static boolean changesMeasureType(Class<?> operandMeasureType, Class<?> resultType) {
    if (operandMeasureType == null || resultType == null) {
      return false;
    }
    return !operandMeasureType.equals(resultType);
  }

  /**
   * Default measure name for a result scalar type after a type-changing operator on a mono-measure
   * dataset.
   */
  public static String forType(Class<?> resultType) {
    if (Boolean.class.equals(resultType)) {
      return BOOL_VAR;
    }
    if (Long.class.equals(resultType)) {
      return INT_VAR;
    }
    if (Double.class.equals(resultType)) {
      return NUM_VAR;
    }
    if (String.class.equals(resultType)) {
      return STRING_VAR;
    }
    throw new UnsupportedOperationException(
        "no default measure name for result type " + resultType.getName());
  }

  /** Resolves the output measure name according to the naming policy. */
  public static String resolveOutputMeasureName(
      String homonymousName,
      Class<?> operandMeasureType,
      Class<?> resultType,
      MeasureNamingPolicy policy) {
    if (policy == MeasureNamingPolicy.TYPE_CHANGING
        && changesMeasureType(operandMeasureType, resultType)) {
      return forType(resultType);
    }
    return homonymousName;
  }
}
