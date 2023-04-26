package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.List;
import java.util.Objects;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>UnaryVisitor</code> is the base visitor for unary expressions (plus, minus, not).
 */
public class UnaryVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;
    private final GenericFunctionsVisitor genericFunctionsVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor The visitor for the enclosing expression.
     */
    public UnaryVisitor(ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
        this.genericFunctionsVisitor = genericFunctionsVisitor;
    }

    public static Number plus(Number right) {
        if (right == null) {
            return null;
        }
        if (right instanceof Long) {
            return right.longValue();
        }
        return right.doubleValue();
    }

    public static Number minus(Number right) {
        if (right == null) {
            return null;
        }
        if (right instanceof Long) {
            return -right.longValue();
        }
        return -right.doubleValue();
    }

    public static Boolean not(Boolean right) {
        if (right == null) {
            return null;
        }
        return !right;
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
            var pos = fromContext(ctx);
            var parameters = List.of(exprVisitor.visit(ctx.right));
            return getResolvableExpression(ctx, pos, parameters);
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }

    private ResolvableExpression getResolvableExpression(VtlParser.UnaryExprContext ctx, Positioned pos, List<ResolvableExpression> parameters) throws VtlScriptException {
        switch (ctx.op.getType()) {
            case VtlParser.PLUS:
                return new ArithmeticVisitor.ArithmeticExpression(
                        genericFunctionsVisitor.invokeFunction("plus", parameters, pos),
                        parameters
                );
            case VtlParser.MINUS:
                return new ArithmeticVisitor.ArithmeticExpression(
                        genericFunctionsVisitor.invokeFunction("minus", parameters, pos),
                        parameters
                );
            case VtlParser.NOT:
                return genericFunctionsVisitor.invokeFunction("not", parameters, pos);
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
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
