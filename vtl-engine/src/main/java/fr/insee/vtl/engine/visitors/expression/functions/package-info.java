/**
 * Parse-tree dispatch for VTL dataset function families. Each {@code *FunctionsVisitor} delegates
 * to a {@code <Domain>Executor} in the matching {@code fr.insee.vtl.engine.semantics.*} subpackage.
 *
 * <p>Scalar function visitors ({@code StringFunctionsVisitor}, {@code NumericFunctionsVisitor}, …)
 * build {@link fr.insee.vtl.model.ResolvableExpression} trees without {@link
 * fr.insee.vtl.model.ProcessingEngine}.
 */
package fr.insee.vtl.engine.visitors.expression.functions;
