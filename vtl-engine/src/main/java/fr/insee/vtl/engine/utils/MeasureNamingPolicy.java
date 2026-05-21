package fr.insee.vtl.engine.utils;

/** How to name the measure column after a dataset-scoped function invocation. */
public enum MeasureNamingPolicy {

  /** Keep the operand measure name (VTL homonymous measure behaviour). */
  HOMONYMOUS,

  /**
   * Use default names ({@code bool_var}, {@code int_var}, …) when the scalar type changes (VTL
   * typical behaviour for dataset comparisons).
   */
  TYPE_CHANGING
}
