package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.BooleanExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.Objects;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;
import static fr.insee.vtl.engine.utils.TypeChecking.isNull;

/**
 * <code>IfVisitor</code> is the base visitor for if-then-else expressions.
 */
public class ConditionalVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor The visitor for the enclosing expression.
     */
    public ConditionalVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    /**
     * Visits if-then-else expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the if or else clause resolution depending on the condition resolution.
     */
    @Override
    public ResolvableExpression visitIfExpr(VtlParser.IfExprContext ctx) {

        ResolvableExpression conditionalExpr = exprVisitor.visit(ctx.conditionalExpr);
        if (isNull(conditionalExpr)) {
            return BooleanExpression.of(fromContext(ctx), (Boolean) null);
        }

        try {
            conditionalExpr.checkAssignableFrom(Boolean.class);
        } catch (InvalidTypeException e) {
            throw new VtlRuntimeException(e);
        }

        // Find the common non-null type.
        ResolvableExpression thenExpression = exprVisitor.visit(ctx.thenExpr);
        ResolvableExpression elseExpression = exprVisitor.visit(ctx.elseExpr);

        // Normalize the type if we have nulls.
        if (isNull(elseExpression) && !isNull(thenExpression)) {
            elseExpression = assertTypeExpression(elseExpression, thenExpression.getType(),
                    ctx.elseExpr);
        } else if (isNull(thenExpression)) {
            thenExpression = assertTypeExpression(thenExpression, elseExpression.getType(),
                    ctx.thenExpr);
        }

        try {
            elseExpression.checkAssignableFrom(thenExpression.getType());
        } catch (InvalidTypeException e) {
            throw new VtlRuntimeException(e);
        }

        ResolvableExpression finalThenExpression = thenExpression;
        ResolvableExpression finalElseExpression = elseExpression;
        return ResolvableExpression.withTypeCasting(thenExpression.getType(), (clazz, context) -> {
            Boolean conditionalValue = (Boolean) conditionalExpr.resolve(context);
            return Boolean.TRUE.equals(conditionalValue) ?
                    clazz.cast(finalThenExpression.resolve(context)) :
                    clazz.cast(finalElseExpression.resolve(context));
        });
    }

    /**
     * Visits nvl expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the null value clause.
     */
    @Override
    public ResolvableExpression visitNvlAtom(VtlParser.NvlAtomContext ctx) {
        ResolvableExpression expression = exprVisitor.visit(ctx.left);
        ResolvableExpression defaultExpression = exprVisitor.visit(ctx.right);

        if (isNull(expression)) {
            return ResolvableExpression.withTypeCasting(defaultExpression.getType(), (clazz, context) ->
                    clazz.cast(defaultExpression.resolve(context))
            );
        }

        Class<?> expressionType = expression.getType();
        Class<?> defaultExpressionType = defaultExpression.getType();

        if (!expressionType.equals(defaultExpressionType)) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(expressionType, defaultExpressionType, fromContext(ctx.right))
            );
        }

        return ResolvableExpression.withTypeCasting(defaultExpressionType, (clazz, context) -> {
            var resolvedExpression = expression.resolve(context);
            return resolvedExpression == null ?
                    clazz.cast(defaultExpression.resolve(context)) :
                    clazz.cast(resolvedExpression);
        });
    }
}
