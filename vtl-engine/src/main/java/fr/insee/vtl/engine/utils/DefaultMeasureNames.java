package fr.insee.vtl.engine.utils;

import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import java.util.Collection;
import java.util.List;

/**
 * Default measure names for VTL 2.1 dataset-scoped operators ({@code bool_var}, {@code int_var}, …).
 *
 * <p>Naming is decided in {@link #resolveOutputMeasureName} from three pieces of information — not
 * from operand/result types alone:
 *
 * <ul>
 *   <li><strong>Mono vs multi-measure</strong> ({@code monoMeasureOperands}): the spec only applies
 *       default names on mono-measure dataset operands; multi-measure calls keep homonymous names.
 *   <li><strong>Which operand type</strong> ({@link #operandMeasureType}): when several dataset
 *       parameters are present (e.g. {@code if ds1 > ds2 then ds1 else ds2}), the comparison result
 *       is also a dataset and would be picked first from metadata — we must use the branch measure
 *       type ({@code Long}) instead of the condition's ({@code Boolean}).
 *   <li><strong>Scalar family</strong> ({@link #requiresDefaultMeasureName}): even when JVM types
 *       differ, homonymous naming is kept inside the same VTL family (numeric {@code Long}/{@code
 *       Double}, e.g. {@code ceil}); only a cross-family change (numeric → boolean, string →
 *       integer) triggers a {@code *_var} name per {@code typical_behaviour.rst}.
 * </ul>
 *
 * <p>Membership ({@code DS # component}) uses {@link #forType(Class)} directly.
 */
public final class DefaultMeasureNames {

  public static final String BOOL_VAR = "bool_var";
  public static final String INT_VAR = "int_var";
  public static final String NUM_VAR = "num_var";
  public static final String STRING_VAR = "string_var";

  private DefaultMeasureNames() {}

  /**
   * Operand measure type used for naming — not the first dataset in {@code parameters}.
   *
   * <p>Needed because {@code FunctionExpression#getType()} only gives the scalar result; among
   * dataset parameters, the condition of {@code if cond then dsA else dsB} is itself a dataset whose
   * measure type ({@code Boolean}) would wrongly drive naming for a {@code Long} result.
   */
  public static Class<?> operandMeasureType(
      List<ResolvableExpression> parameters,
      Collection<String> homonymousMeasureNames,
      Class<?> resultType) {
    List<Structured.Component> measures =
        parameters.stream()
            .filter(DatasetExpression.class::isInstance)
            .map(DatasetExpression.class::cast)
            .map(de -> de.getMeasures().get(0))
            .filter(m -> homonymousMeasureNames.contains(m.getName()))
            .toList();
    return measures.stream()
        .filter(m -> resultType.equals(m.getType()))
        .map(Structured.Component::getType)
        .findFirst()
        .orElse(measures.isEmpty() ? null : measures.get(0).getType());
  }

  /**
   * Whether operand and result differ across VTL scalar families.
   *
   * <p>Comparing raw JVM types is insufficient: {@code ceil} is {@code Double} → {@code Long} but
   * stays homonymous; {@code DS > 20} is {@code Long} → {@code Boolean} and must become {@code
   * bool_var}.
   */
  public static boolean requiresDefaultMeasureName(Class<?> operandType, Class<?> resultType) {
    if (operandType == null || resultType == null || operandType.equals(resultType)) {
      return false;
    }
    return !sameScalarFamily(operandType, resultType);
  }

  /** Default measure name for a scalar result type (membership promotion, or rule above). */
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

  /**
   * Output measure name after a dataset-scoped function.
   *
   * @param monoMeasureOperands {@code false} for multi-measure per-column invocation (homonymous
   *     measures per spec)
   */
  public static String resolveOutputMeasureName(
      String homonymousName,
      Class<?> operandMeasureType,
      Class<?> resultType,
      boolean monoMeasureOperands) {
    if (monoMeasureOperands
        && requiresDefaultMeasureName(operandMeasureType, resultType)) {
      return forType(resultType);
    }
    return homonymousName;
  }

  private static boolean sameScalarFamily(Class<?> left, Class<?> right) {
    if (isNumeric(left) && isNumeric(right)) {
      return true;
    }
    return left.equals(right);
  }

  private static boolean isNumeric(Class<?> type) {
    return Long.class.equals(type)
        || Double.class.equals(type)
        || Number.class.isAssignableFrom(type);
  }
}
