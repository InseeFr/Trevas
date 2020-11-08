package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.DoubleExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.Objects;

import static fr.insee.vtl.engine.utils.TypeChecking.assertNumber;

public class NumericFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking a scripting context.
     *
     * @param context The expression visitor.
     */
    public NumericFunctionsVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    /**
     * Visits a 'unaryNumeric' expression.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to a double).
     */
    @Override
    public ResolvableExpression visitUnaryNumeric(VtlParser.UnaryNumericContext ctx) {

        switch (ctx.op.getType()) {
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }

    }

    /**
     * Visits a 'unaryWithOptionalNumeric' expression.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to a double).
     */
    @Override
    public ResolvableExpression visitUnaryWithOptionalNumeric(VtlParser.UnaryWithOptionalNumericContext ctx) {

        switch (ctx.op.getType()) {
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }

    }

    /**
     * Visits a 'binaryNumeric' expression.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to a double).
     */
    @Override
    public ResolvableExpression visitBinaryNumeric(VtlParser.BinaryNumericContext ctx) {

        switch (ctx.op.getType()) {
            case VtlParser.MOD:
                // TODO: Support dataset.
                return handleModulo(ctx.left, ctx.right);
            case VtlParser.POWER:
                // TODO: Support dataset.
                return handlePower(ctx.left, ctx.right);
            case VtlParser.LOG:
                // TODO: Support dataset.
                return handleLog(ctx.left, ctx.right);
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }

    }

    private ResolvableExpression handleModulo(VtlParser.ExprContext left, VtlParser.ExprContext right) {
        var leftExpression = assertNumber(exprVisitor.visit(left), left);
        var rightExpression = assertNumber(exprVisitor.visit(right), right);
        return DoubleExpression.of(context -> {
            var leftValue = leftExpression.resolve(context);
            var rightValue = rightExpression.resolve(context);
            Double leftDouble = leftValue instanceof Long ? ((Long) leftValue).doubleValue() : (Double) leftValue;
            Double rightDouble = rightValue instanceof Long ? ((Long) rightValue).doubleValue() : (Double) rightValue;
            return rightDouble.equals(0D) ? leftDouble : leftDouble % rightDouble;
        });
    }

    private ResolvableExpression handlePower(VtlParser.ExprContext left, VtlParser.ExprContext right) {
        var leftExpression = assertNumber(exprVisitor.visit(left), left);
        var rightExpression = assertNumber(exprVisitor.visit(right), right);
        return DoubleExpression.of(context -> {
            var leftValue = leftExpression.resolve(context);
            var rightValue = rightExpression.resolve(context);
            Double leftDouble = leftValue instanceof Long ? ((Long) leftValue).doubleValue() : (Double) leftValue;
            Double rightDouble = rightValue instanceof Long ? ((Long) rightValue).doubleValue() : (Double) rightValue;
            return Math.pow(leftDouble, rightDouble);
        });
    }

    private ResolvableExpression handleLog(VtlParser.ExprContext left, VtlParser.ExprContext right) {
        var leftExpression = assertNumber(exprVisitor.visit(left), left);
        var baseExpression = assertNumber(exprVisitor.visit(right), right);
        return DoubleExpression.of(context -> {
            var leftValue = leftExpression.resolve(context);
            var baseValue = baseExpression.resolve(context);
            Double leftDouble = leftValue instanceof Long ? ((Long) leftValue).doubleValue() : (Double) leftValue;
            Double baseDouble = baseValue instanceof Long ? ((Long) baseValue).doubleValue() : (Double) baseValue;
            //TODO:
            // If leftDouble <= 0
            // If baseDouble <= 1
            return Math.log(leftDouble) / Math.log(baseDouble);
        });
    }

}
