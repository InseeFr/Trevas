package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.Objects;

/**
 * <code>IfVisitor</code> is the base visitor for if-then-else expressions.
 */
public class IfVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking a scripting context.
     *
     * @param context The expression visitor.
     */
    public IfVisitor(ExpressionVisitor expressionVisitor) {
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

        ResolvableExpression conditionalExpression = TypeChecking.assertTypeExpression(
                exprVisitor.visit(ctx.conditionalExpr),
                Boolean.class,
                ctx.conditionalExpr
        );

        ResolvableExpression thenExpression = exprVisitor.visit(ctx.thenExpr);
        ResolvableExpression elseExpression = exprVisitor.visit(ctx.elseExpr);

        if (!thenExpression.getType().equals(elseExpression.getType())) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(thenExpression.getType(), elseExpression.getType(), ctx.elseExpr)
            );
        }

        return ResolvableExpression.withTypeCasting(thenExpression.getType(), (clazz, context) -> {
            Boolean conditionalValue = (Boolean) conditionalExpression.resolve(context);
            return Boolean.TRUE.equals(conditionalValue) ?
                    clazz.cast(thenExpression.resolve(context)) :
                    clazz.cast(elseExpression.resolve(context));
        });
    }
}
