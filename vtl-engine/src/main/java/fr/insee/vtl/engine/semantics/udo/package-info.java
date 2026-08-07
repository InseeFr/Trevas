/**
 * User-defined operators (UDO).
 *
 * <p><b>Pattern under validation:</b> define builds an {@link UdoDefinition} into bindings and
 * registers a reflective trampoline {@link java.lang.reflect.Method}; call sites build a {@link
 * fr.insee.vtl.engine.expressions.UdoFunctionExpression} ({@code FunctionExpression}) so resolution
 * goes through {@code Method.invoke} → body re-entry via {@code ExpressionVisitor}.
 */
package fr.insee.vtl.engine.semantics.udo;
