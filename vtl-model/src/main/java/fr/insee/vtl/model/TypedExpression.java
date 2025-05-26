package fr.insee.vtl.model;

/** <code>TypedExpression</code> is the base interface for typed VTL expressions. */
public interface TypedExpression {

  /**
   * Returns the class corresponding to the type of the expression.
   *
   * @return The class corresponding to the type of the expression.
   */
  Class<?> getType();
}
