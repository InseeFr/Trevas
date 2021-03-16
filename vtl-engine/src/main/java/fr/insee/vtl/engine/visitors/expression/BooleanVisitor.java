package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.model.BooleanExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.Objects;

import static fr.insee.vtl.engine.utils.TypeChecking.assertBoolean;

/**
 * <code>BooleanVisitor</code> is the base visitor for expressions involving boolean operations.
 */
public class BooleanVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking a scripting context.
     *
     * @param expressionVisitor the parent expression visitor.
     */
    public BooleanVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    private static boolean nullFalse(Boolean b) {
        return b != null ? b : false;
    }

    /**
     * Visits expressions with boolean operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the boolean operation.
     */
    @Override
    public ResolvableExpression visitBooleanExpr(VtlParser.BooleanExprContext ctx) {
        switch (ctx.op.getType()) {
            case VtlParser.AND:
                return handleAnd(ctx.left, ctx.right);
            case VtlParser.OR:
                return handleOr(ctx.left, ctx.right);
            case VtlParser.XOR:
                return handleXor(ctx.left, ctx.right);
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }

    private ResolvableExpression handleAnd(VtlParser.ExprContext left, VtlParser.ExprContext right) {
        var leftExpression = assertBoolean(exprVisitor.visit(left), left);
        var rightExpression = assertBoolean(exprVisitor.visit(right), right);
        return BooleanExpression.of(context -> {

            var leftValue = (Boolean) leftExpression.resolve(context);
            if (leftValue != null && !leftValue) return false;

            var rightValue = (Boolean) rightExpression.resolve(context);
            if (rightValue != null && !rightValue) return false;

            if (TypeChecking.hasNullArgs(rightValue, leftValue)) return null;

            return true;
        });
    }

    private ResolvableExpression handleOr(VtlParser.ExprContext left, VtlParser.ExprContext right) {
        var leftExpression = assertBoolean(exprVisitor.visit(left), left);
        var rightExpression = assertBoolean(exprVisitor.visit(right), right);
        return BooleanExpression.of(context -> {
            var leftValue = (Boolean) leftExpression.resolve(context);
            if (leftValue != null && leftValue) {
                return true;
            }
            var rightValue = (Boolean) rightExpression.resolve(context);
            if (rightValue != null && rightValue) {
                return true;
            }
            if (TypeChecking.hasNullArgs(rightValue, leftValue)) {
                return null;
            }
            return false;
        });
    }

    private ResolvableExpression handleXor(VtlParser.ExprContext left, VtlParser.ExprContext right) {
        var leftExpression = assertBoolean(exprVisitor.visit(left), left);
        var rightExpression = assertBoolean(exprVisitor.visit(right), right);
        return BooleanExpression.of(context -> {
            var leftValue = (Boolean) leftExpression.resolve(context);
            if (leftValue == null) {
                return null;
            }
            var rightValue = (Boolean) rightExpression.resolve(context);
            if (rightValue == null) {
                return null;
            }
            return nullFalse(leftValue) ^ nullFalse(rightValue);
        });
    }
}
