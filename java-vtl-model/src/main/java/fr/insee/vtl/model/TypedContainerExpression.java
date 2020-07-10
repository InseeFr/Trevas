package fr.insee.vtl.model;

/**
 * Exposes the type inside a container.
 */
public interface TypedContainerExpression {
    Class<?> containedType();
}
