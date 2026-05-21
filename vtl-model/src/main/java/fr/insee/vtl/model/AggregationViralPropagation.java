package fr.insee.vtl.model;

/**
 * How viral attributes appear in aggregation results (TCK-aligned).
 *
 * <ul>
 *   <li>{@link #INVOCATION_GLOBAL} — {@code avg(DS)}: viral as {@link Dataset.Role#ATTRIBUTE}
 *   <li>{@link #AGGR_CLAUSE_GROUPED} — {@code DS[aggr … group by …]}: viral as {@link
 *       Dataset.Role#VIRALATTRIBUTE}
 *   <li>{@link #INVOCATION_GROUPED} — {@code sum(DS group by …)}: no viral columns
 * </ul>
 */
public enum AggregationViralPropagation {

  /** Aggregate invocation with a {@code group by} clause: viral attributes are not propagated. */
  INVOCATION_GROUPED,

  /**
   * Global aggregate invocation ({@code avg(DS)}, etc.): viral as {@link Dataset.Role#ATTRIBUTE}.
   */
  INVOCATION_GLOBAL,

  /**
   * Dataset {@code aggr} clause with {@code group by}: viral stays {@link
   * Dataset.Role#VIRALATTRIBUTE}.
   */
  AGGR_CLAUSE_GROUPED;

  public boolean propagatesViralAttributes() {
    return this != INVOCATION_GROUPED;
  }

  public Dataset.Role propagatedViralRole() {
    return switch (this) {
      case INVOCATION_GROUPED -> throw new IllegalStateException("viral not propagated");
      case INVOCATION_GLOBAL -> Dataset.Role.ATTRIBUTE;
      case AGGR_CLAUSE_GROUPED -> Dataset.Role.VIRALATTRIBUTE;
    };
  }
}
