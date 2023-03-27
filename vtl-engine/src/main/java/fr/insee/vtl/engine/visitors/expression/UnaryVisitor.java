package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.Objects;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>UnaryVisitor</code> is the base visitor for unary expressions (plus, minus, not).
 */
public class UnaryVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor The visitor for the enclosing expression.
     */
    public UnaryVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    /**
     * Visits unary expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the unary operation.
     */
    @Override
    public ResolvableExpression visitUnaryExpr(VtlParser.UnaryExprContext ctx) {
        try {
            switch (ctx.op.getType()) {
                case VtlParser.PLUS:
                    return handleUnaryPlus(ctx.right);
                case VtlParser.MINUS:
                    return handleUnaryMinus(ctx.right);
                case VtlParser.NOT:
                    return handleUnaryNot(ctx.right);
                default:
                    throw new UnsupportedOperationException("unknown operator " + ctx);
            }
        } catch (InvalidTypeException e) {
            throw new VtlRuntimeException(e);
        }
    }

    private ResolvableExpression handleUnaryPlus(VtlParser.ExprContext exprContext) throws InvalidTypeException {
        ResolvableExpression expression = exprVisitor.visit(exprContext).checkInstanceOf(Number.class);
        if (TypeChecking.isLong(expression)) {
            return ResolvableExpression.withType(Long.class).withPosition(fromContext(exprContext)).using(context -> {
                        Object resolved = expression.resolve(context);
                        if (resolved == null) return null;
                        return (Long) resolved;
                    }

            );
        }
        return ResolvableExpression.withType(Double.class).withPosition(fromContext(exprContext)).using(context -> {
                    Object resolved = expression.resolve(context);
                    if (resolved == null) return null;
                    return (Double) resolved;
                }

        );
    }

    private ResolvableExpression handleUnaryMinus(VtlParser.ExprContext exprContext) throws InvalidTypeException {
        ResolvableExpression expression = exprVisitor.visit(exprContext).checkInstanceOf(Number.class);
        if (TypeChecking.isLong(expression)) {

            return ResolvableExpression.withType(Long.class).withPosition(fromContext(exprContext)).using(context -> {
                        Object resolved = expression.resolve(context);
                        if (resolved == null) return null;
                        return -((Long) resolved);
                    }
            );
        } else {
            return ResolvableExpression.withType(Double.class).withPosition(fromContext(exprContext)).using(context -> {
                        Object resolved = expression.resolve(context);
                        if (resolved == null) return null;
                        return -((Double) resolved);
                    }
            );
        }
    }

    private ResolvableExpression handleUnaryNot(VtlParser.ExprContext exprContext) throws InvalidTypeException {
        ResolvableExpression expression = exprVisitor.visit(exprContext).checkInstanceOf(Boolean.class);
        return ResolvableExpression.withType(Boolean.class).withPosition(fromContext(exprContext)).using(context -> {
                    Object resolved = expression.resolve(context);
                    if (resolved == null) return null;
                    return !((Boolean) resolved);
                }
        );
    }
}
