package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.Map;
import java.util.Objects;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
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

    static class ItThenExpression extends ResolvableExpression {

        final ResolvableExpression conditionExpr;
        final ResolvableExpression thenExpr;
        final ResolvableExpression elseExpr;

        final Class<?> type;

        ItThenExpression(Positioned position,
                         ResolvableExpression conditionExpr,
                         ResolvableExpression thenExpr,
                         ResolvableExpression elseExpr) throws InvalidTypeException {
            super(position);
            this.conditionExpr = conditionExpr.checkInstanceOf(Boolean.class);

            // Normalize the types of the branches.
            if (!isNull(thenExpr)) {
                this.elseExpr = elseExpr.tryCast(thenExpr.getType());
                this.thenExpr = thenExpr;
            } else if (!isNull(elseExpr)) {
                this.thenExpr = thenExpr.tryCast(elseExpr.getType());
                this.elseExpr = elseExpr;
            }
            this.type = thenExpr.getType();
        }

        @Override
        public Object resolve(Map<String, Object> context) {
            var cond = (Boolean) conditionExpr.resolve(context);
            return Boolean.TRUE.equals(cond)
                    ? thenExpr.resolve(context)
                    : elseExpr.resolve(context);
        }

        @Override
        public Class<?> getType() {
            return type;
        }
    }

    /**
     * Visits if-then-else expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the if or else clause resolution depending on the condition resolution.
     */
    @Override
    public ResolvableExpression visitIfExpr(VtlParser.IfExprContext ctx) {

        try {
            var conditionalExpr = exprVisitor.visit(ctx.conditionalExpr).checkInstanceOf(Boolean.class);
            var thenExpression = exprVisitor.visit(ctx.thenExpr);
            var elseExpression = exprVisitor.visit(ctx.elseExpr);


            ResolvableExpression finalThenExpression = thenExpression;
            ResolvableExpression finalElseExpression = elseExpression;
            Class<?> type = thenExpression.getType();
            return ResolvableExpression.withType(type).withPosition(fromContext(ctx)).using((Map<String, Object> context) -> {
                Boolean conditionalValue = (Boolean) conditionalExpr.resolve(context);
                return Boolean.TRUE.equals(conditionalValue) ?
                        type.cast(finalThenExpression.resolve(context)) :
                        type.cast(finalElseExpression.resolve(context));
            });

        } catch (InvalidTypeException e) {
            throw new VtlRuntimeException(e);
        }
    }

    /**
     * Visits nvl expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the null value clause.
     */
    @Override
    public ResolvableExpression visitNvlAtom(VtlParser.NvlAtomContext ctx) {
        // TODO: Rewrite using if.
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
