package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.DoubleExpression;
import fr.insee.vtl.model.LongExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

import static fr.insee.vtl.engine.utils.TypeChecking.assertLong;
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
            case VtlParser.CEIL:
                // TODO: Support dataset.
                return handleCeil(ctx.expr());
            case VtlParser.FLOOR:
                // TODO: Support dataset.
                return handleFloor(ctx.expr());
            case VtlParser.ABS:
                // TODO: Support dataset.
                return handleAbs(ctx.expr());
            case VtlParser.EXP:
                // TODO: Support dataset.
                return handleExp(ctx.expr());
            case VtlParser.LN:
                // TODO: Support dataset.
                return handleLn(ctx.expr());
            case VtlParser.SQRT:
                // TODO: Support dataset.
                return handleSqrt(ctx.expr());
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }

    }

    private ResolvableExpression handleCeil(VtlParser.ExprContext expr) {
        var expression = assertNumber(exprVisitor.visit(expr), expr);
        return LongExpression.of(context -> {
            Double exprDouble = ((Number) expression.resolve(context)).doubleValue();
            return ((Double) (Math.ceil(exprDouble))).longValue();
        });
    }

    private ResolvableExpression handleFloor(VtlParser.ExprContext expr) {
        var expression = assertNumber(exprVisitor.visit(expr), expr);
        return LongExpression.of(context -> {
            Double exprDouble = ((Number) expression.resolve(context)).doubleValue();
            return ((Double) (Math.floor(exprDouble))).longValue();
        });
    }

    private ResolvableExpression handleAbs(VtlParser.ExprContext expr) {
        var expression = assertNumber(exprVisitor.visit(expr), expr);
        return DoubleExpression.of(context -> {
            Double exprDouble = ((Number) expression.resolve(context)).doubleValue();
            return Math.abs(exprDouble);
        });
    }

    private ResolvableExpression handleExp(VtlParser.ExprContext expr) {
        var expression = assertNumber(exprVisitor.visit(expr), expr);
        return DoubleExpression.of(context -> {
            Double exprDouble = ((Number) expression.resolve(context)).doubleValue();
            return Math.exp(exprDouble);
        });
    }

    private ResolvableExpression handleLn(VtlParser.ExprContext expr) {
        var expression = assertNumber(exprVisitor.visit(expr), expr);
        return DoubleExpression.of(context -> {
            Double exprDouble = ((Number) expression.resolve(context)).doubleValue();
            return Math.log(exprDouble);
        });
    }

    private ResolvableExpression handleSqrt(VtlParser.ExprContext expr) {
        var expression = assertNumber(exprVisitor.visit(expr), expr);
        return DoubleExpression.of(context -> {
            Double exprDouble = ((Number) expression.resolve(context)).doubleValue();
            if (exprDouble < 0)
                throw new VtlRuntimeException(new InvalidArgumentException("Sqrt operand has to be 0 or positive", expr));
            return Math.sqrt(exprDouble);
        });
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
            case VtlParser.ROUND:
                // TODO: Support dataset.
                return handleRound(ctx.expr(), ctx.optionalExpr());
            case VtlParser.TRUNC:
                // TODO: Support dataset.
                return handleTrunc(ctx.expr(), ctx.optionalExpr());
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }

    }

    private ResolvableExpression handleRound(VtlParser.ExprContext expr, VtlParser.OptionalExprContext decimal) {
        var expression = assertNumber(exprVisitor.visit(expr), expr);
        var decimalValue = decimal == null ? LongExpression.of(0L) : assertLong(exprVisitor.visit(decimal), decimal);
        return DoubleExpression.of(context -> {
            Double exprDouble = ((Number) expression.resolve(context)).doubleValue();
            Long decimalLong = ((Long) decimalValue.resolve(context)).longValue();
            BigDecimal bd = new BigDecimal(Double.toString(exprDouble));
            bd = bd.setScale(decimalLong.intValue(), RoundingMode.HALF_UP);
            return bd.doubleValue();
        });
    }

    private ResolvableExpression handleTrunc(VtlParser.ExprContext expr, VtlParser.OptionalExprContext decimal) {
        var expression = assertNumber(exprVisitor.visit(expr), expr);
        var decimalValue = decimal == null ? LongExpression.of(0L) : assertLong(exprVisitor.visit(decimal), decimal);
        return DoubleExpression.of(context -> {
            Double exprDouble = ((Number) expression.resolve(context)).doubleValue();
            Long decimalLong = ((Long) decimalValue.resolve(context)).longValue();
            BigDecimal bd = new BigDecimal(Double.toString(exprDouble));
            bd = bd.setScale(decimalLong.intValue(), RoundingMode.DOWN);
            return bd.doubleValue();
        });
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
            Double leftDouble = ((Number) leftExpression.resolve(context)).doubleValue();
            Double rightDouble = ((Number) rightExpression.resolve(context)).doubleValue();
            if (rightDouble.equals(0D)) return leftDouble;
            return (leftDouble % rightDouble) * (rightDouble < 0 ? -1 : 1);
        });
    }

    private ResolvableExpression handlePower(VtlParser.ExprContext left, VtlParser.ExprContext right) {
        var leftExpression = assertNumber(exprVisitor.visit(left), left);
        var rightExpression = assertNumber(exprVisitor.visit(right), right);
        return DoubleExpression.of(context -> {
            Double leftDouble = ((Number) leftExpression.resolve(context)).doubleValue();
            Double rightDouble = ((Number) rightExpression.resolve(context)).doubleValue();
            return Math.pow(leftDouble, rightDouble);
        });
    }

    private ResolvableExpression handleLog(VtlParser.ExprContext left, VtlParser.ExprContext base) {
        var leftExpression = assertNumber(exprVisitor.visit(left), left);
        var baseExpression = assertNumber(exprVisitor.visit(base), base);
        return DoubleExpression.of(context -> {
            Double leftDouble = ((Number) leftExpression.resolve(context)).doubleValue();
            Double baseDouble = ((Number) baseExpression.resolve(context)).doubleValue();
            if (leftDouble <= 0)
                throw new VtlRuntimeException(new InvalidArgumentException("Log operand has to be positive", left));
            if (baseDouble < 1)
                throw new VtlRuntimeException(new InvalidArgumentException("Log base has to be greater or equal than 1", base));
            return Math.log(leftDouble) / Math.log(baseDouble);
        });
    }

}
