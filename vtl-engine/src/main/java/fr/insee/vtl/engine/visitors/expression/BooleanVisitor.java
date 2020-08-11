package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;

/**
 * <code>BooleanVisitor</code> is the base visitor for expressions involving boolean operations.
 */
public class BooleanVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking a scripting context.
     *
     * @param context The scripting context for the visitor.
     */
    public BooleanVisitor(ScriptContext context) {
        exprVisitor = new ExpressionVisitor(context);
    }

    /**
     * Visits expressions with boolean operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the boolean operation.
     */
    @Override
    public ResolvableExpression visitBooleanExpr(VtlParser.BooleanExprContext ctx) {
        ResolvableExpression leftExpression = exprVisitor.visit(ctx.left);
        ResolvableExpression rightExpression = exprVisitor.visit(ctx.right);
        if (!leftExpression.getType().equals(Boolean.class)) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(Boolean.class, leftExpression.getType(), ctx.left)
            );
        }
        if (!rightExpression.getType().equals(Boolean.class)) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(Boolean.class, rightExpression.getType(), ctx.right)
            );
        }

        switch (ctx.op.getType()) {
            case VtlParser.AND:
                return ResolvableExpression.withType(Boolean.class, context -> {
                    Boolean leftValue = (Boolean) leftExpression.resolve(context);
                    Boolean rightValue = (Boolean) rightExpression.resolve(context);
                    return leftValue && rightValue;
                });
            case VtlParser.OR:
                return ResolvableExpression.withType(Boolean.class, context -> {
                    Boolean leftValue = (Boolean) leftExpression.resolve(context);
                    Boolean rightValue = (Boolean) rightExpression.resolve(context);
                    return leftValue || rightValue;
                });
            case VtlParser.XOR:
                return ResolvableExpression.withType(Boolean.class, context -> {
                    Boolean leftValue = (Boolean) leftExpression.resolve(context);
                    Boolean rightValue = (Boolean) rightExpression.resolve(context);
                    return leftValue ^ rightValue;
                });
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }
}
