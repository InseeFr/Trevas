package fr.insee.vtl.model;

/**
 * <code>TypedContainerExpression</code> is the base interface for containers (e.g. collections)
 * exposing the type of the contained objects.
 */
public interface TypedContainerExpression {

  /**
   * Returns the class corresponding to the type of the contained objects.
   *
   * @return The class corresponding to the type of the contained objects.
   */
  Class<?> containedType();
}
