package fr.insee.vtl.engine.utils;

import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ResolvableExpression;
import java.util.List;
import java.util.Set;

/**
 * Selects the measure naming policy for a dataset-scoped function call (VTL 2.1 *Typical
 * behaviour*).
 *
 * <p><strong>Homonymous</strong> ({@code Me_1}, …): type preserved on the operand measure (e.g.
 * {@code DS_1 xor DS_2} on boolean), explicit {@code calc} target, multi-measure per-column
 * invocation, or operators that keep the measure name by convention in Trevas ({@code len},
 * arithmetic, …).
 *
 * <p><strong>Type-changing</strong> ({@code bool_var}, …): mono-measure dataset-level operator
 * whose scalar result type differs from the operand measure — comparisons ({@code DS_1 > 20},
 * {@code DS_1 = DS_2} on numeric), {@code between(DS_1, …)}, {@code isnull(DS_1)}, {@code DS_1 in
 * {…}}, etc. Actual rename still depends on {@link DefaultMeasureNames#changesMeasureType}.
 */
public final class MeasureNamingPolicies {

  /**
   * Comparisons and comparison-like functions that may yield {@code bool_var} on mono-measure DS.
   */
  private static final Set<String> TYPE_CHANGING_WHEN_MONO_MEASURE =
      Set.of(
          "isEqual",
          "isNotEqual",
          "isLessThan",
          "isGreaterThan",
          "isGreaterThanOrEqual",
          "isLessThanOrEqual",
          "between",
          "charsetMatch",
          "isNull",
          "in",
          "notIn");

  /** Boolean combinators: type stays boolean → homonymous measure name. */
  private static final Set<String> BOOLEAN_COMBINATORS = Set.of("and", "or", "xor", "not");

  private MeasureNamingPolicies() {}

  public static MeasureNamingPolicy policyFor(
      String functionName, List<ResolvableExpression> parameters) {
    if (BOOLEAN_COMBINATORS.contains(functionName)) {
      return MeasureNamingPolicy.HOMONYMOUS;
    }
    if (!TYPE_CHANGING_WHEN_MONO_MEASURE.contains(functionName)) {
      return MeasureNamingPolicy.HOMONYMOUS;
    }
    List<DatasetExpression> datasetOperands =
        parameters.stream()
            .filter(DatasetExpression.class::isInstance)
            .map(DatasetExpression.class::cast)
            .toList();
    if (datasetOperands.isEmpty()) {
      return MeasureNamingPolicy.HOMONYMOUS;
    }
    boolean allMonoMeasure =
        datasetOperands.stream().allMatch(de -> Boolean.TRUE.equals(de.isMonoMeasure()));
    return allMonoMeasure ? MeasureNamingPolicy.TYPE_CHANGING : MeasureNamingPolicy.HOMONYMOUS;
  }
}
