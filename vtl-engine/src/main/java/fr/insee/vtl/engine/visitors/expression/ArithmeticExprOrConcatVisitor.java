package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.Objects;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.isLong;

/**
 * <code>ArithmeticExprOrConcatVisitor</code> is the base visitor for plus, minus or concatenation expressions.
 */
public class ArithmeticExprOrConcatVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor The visitor for the enclosing expression.
     */
    public ArithmeticExprOrConcatVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    /**
     * Visits expressions with plus, minus or concatenation operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the plus, minus or concatenation operation.
     */
    @Override
    public ResolvableExpression visitArithmeticExprOrConcat(VtlParser.ArithmeticExprOrConcatContext ctx) {
        try {
            switch (ctx.op.getType()) {
                case VtlParser.PLUS:
                    return handlePlus(ctx);
                case VtlParser.MINUS:
                    return handleMinus(ctx);
                case VtlParser.CONCAT:
                    return handleConcat(ctx);
                default:
                    throw new UnsupportedOperationException("unknown operator " + ctx);
            }
        } catch (InvalidTypeException e) {
            throw new VtlRuntimeException(e);
        }
    }

    private ResolvableExpression handlePlus(VtlParser.ArithmeticExprOrConcatContext ctx) throws InvalidTypeException {
        var leftExpression = exprVisitor.visit(ctx.left).checkInstanceOf(Number.class);
        var rightExpression = exprVisitor.visit(ctx.right).checkInstanceOf(Number.class);
        if (isLong(leftExpression) && isLong(rightExpression)) {
            return ResolvableExpression.withType(Long.class).withPosition(fromContext(ctx)).using(context -> {
                Long leftValue = (Long) leftExpression.resolve(context);
                Long rightValue = (Long) rightExpression.resolve(context);
                if (TypeChecking.hasNullArgs(leftValue, rightValue)) return null;
                return leftValue + rightValue;
            });
        } else {
            return ResolvableExpression.withType(Double.class).withPosition(fromContext(ctx)).using(context -> {
                var leftValue = leftExpression.resolve(context);
                var rightValue = rightExpression.resolve(context);
                if (TypeChecking.hasNullArgs(leftValue, rightValue)) return null;
                var leftDouble = leftValue instanceof Long ? ((Long) leftValue).doubleValue() : (Double) leftValue;
                var rightDouble = rightValue instanceof Long ? ((Long) rightValue).doubleValue() : (Double) rightValue;
                return leftDouble + rightDouble;
            });
        }
    }

    private ResolvableExpression handleMinus(VtlParser.ArithmeticExprOrConcatContext ctx) throws InvalidTypeException {
        var leftExpression = exprVisitor.visit(ctx.left).checkInstanceOf(Number.class);
        var rightExpression = exprVisitor.visit(ctx.right).checkInstanceOf(Number.class);
        if (isLong(leftExpression) && isLong(rightExpression)) {
            return ResolvableExpression.withType(Long.class).withPosition(fromContext(ctx)).using(context -> {
                Long leftValue = (Long) leftExpression.resolve(context);
                Long rightValue = (Long) rightExpression.resolve(context);
                if (TypeChecking.hasNullArgs(leftValue, rightValue)) return null;
                return leftValue - rightValue;
            });
        } else {
            return ResolvableExpression.withType(Double.class).withPosition(fromContext(ctx)).using(context -> {
                var leftValue = leftExpression.resolve(context);
                var rightValue = rightExpression.resolve(context);
                if (TypeChecking.hasNullArgs(leftValue, rightValue)) return null;
                var leftDouble = leftValue instanceof Long ? ((Long) leftValue).doubleValue() : (Double) leftValue;
                var rightDouble = rightValue instanceof Long ? ((Long) rightValue).doubleValue() : (Double) rightValue;
                return leftDouble - rightDouble;
            });
        }
    }

    private ResolvableExpression handleConcat(VtlParser.ArithmeticExprOrConcatContext ctx) throws InvalidTypeException {
        var leftExpression = exprVisitor.visit(ctx.left).checkInstanceOf(String.class);
        var rightExpression = exprVisitor.visit(ctx.right).checkInstanceOf(String.class);
        return ResolvableExpression.withType(String.class).withPosition(fromContext(ctx)).using(context -> {
            String leftValue = (String) leftExpression.resolve(context);
            String rightValue = (String) rightExpression.resolve(context);
            if (TypeChecking.hasNullArgs(leftValue, rightValue)) return null;
            return leftValue.concat(rightValue);
        });
    }
}
