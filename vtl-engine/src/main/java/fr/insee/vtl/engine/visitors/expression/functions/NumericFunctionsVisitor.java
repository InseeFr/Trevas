package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.LongExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Objects;

import static fr.insee.vtl.engine.utils.TypeChecking.assertLong;
import static fr.insee.vtl.engine.utils.TypeChecking.assertNumber;

/**
 * <code>NumericFunctionsVisitor</code> is the visitor for expressions involving numeric functions.
 */
public class NumericFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    // TODO: Support dataset as argument of unary numeric.
    //          ceil_ds := ceil(ds) -> ds[calc m1 := ceil(m1), m2 := ...]
    //          ceil_ds := ceil(ds#measure)
    //       Evaluate when we have a proper function abstraction:
    //          var function = findFunction(name);
    //          function.run(ctx.expr());
    //

    private final ExpressionVisitor exprVisitor;
    private final GenericFunctionsVisitor genericFunctionsVisitor;

    private final String UNKNOWN_OPERATOR = "unknown operator ";

    /**
     * Constructor taking a scripting context.
     *
     * @param expressionVisitor       The expression visitor.
     * @param genericFunctionsVisitor
     */
    public NumericFunctionsVisitor(ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
        this.exprVisitor = Objects.requireNonNull(expressionVisitor);
        this.genericFunctionsVisitor = genericFunctionsVisitor;
    }

    /**
     * Visits a 'unaryNumeric' expressi
     * on.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to a double.
     */
    @Override
    public ResolvableExpression visitUnaryNumeric(VtlParser.UnaryNumericContext ctx) {
        VtlParser.ExprContext expr = ctx.expr();
        List<ResolvableExpression> parameter = List.of(exprVisitor.visit(expr));
        switch (ctx.op.getType()) {
            case VtlParser.CEIL:
                return genericFunctionsVisitor.invoke("ceil", parameter);
            case VtlParser.FLOOR:
                return genericFunctionsVisitor.invoke("floor", parameter);
            case VtlParser.ABS:
                return genericFunctionsVisitor.invoke("abs", parameter);
            case VtlParser.EXP:
                return genericFunctionsVisitor.invoke("exp", parameter);
            case VtlParser.LN:
                return genericFunctionsVisitor.invoke("ln", parameter);
            case VtlParser.SQRT:
                return genericFunctionsVisitor.invoke("sqrt", parameter);
            default:
                throw new UnsupportedOperationException(UNKNOWN_OPERATOR + ctx);
        }
    }
    public static Double ceil(Number value) {
        if (value == null) {
            return null;
        }
        return Math.ceil(value.doubleValue());
    }

    public static Long floor(Number value) {
        if (value == null) {
            return null;
        }
        return (long) Math.floor(value.doubleValue());
    }

    public static Double abs(Number value) {
        if (value == null) {
            return null;
        }
        return Math.abs(value.doubleValue());
    }

    public static Double exp(Number value) {
        if (value == null) {
            return null;
        }
        return Math.exp(value.doubleValue());
    }

    public static Double ln(Number value) {
        if (value == null) {
            return null;
        }
        return Math.log(value.doubleValue());
    }

    public static Double sqrt(Number value) {
        if (value == null) {
            return null;
        }
        if (value.doubleValue() < 0) {
            throw new IllegalArgumentException("operand has to be 0 or positive");
        }
        return Math.sqrt(value.doubleValue());
    }

    /**
     * Visits a 'unaryWithOptionalNumeric' expression.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to a double).
     */
    @Override
    public ResolvableExpression visitUnaryWithOptionalNumeric(VtlParser.UnaryWithOptionalNumericContext ctx) {

        List<ResolvableExpression> parameters = List.of(
                assertNumber(exprVisitor.visit(ctx.expr()), ctx.expr()),
                ctx.optionalExpr() == null ? LongExpression.of(0L) : assertLong(exprVisitor.visit(ctx.optionalExpr()), ctx.optionalExpr())
        );
        switch (ctx.op.getType()) {
            case VtlParser.ROUND:
                return genericFunctionsVisitor.invoke("round", parameters);
            case VtlParser.TRUNC:
                return genericFunctionsVisitor.invoke("trunc", parameters);
            default:
                throw new UnsupportedOperationException(UNKNOWN_OPERATOR + ctx);
        }
    }

    public static Double round(Number value, Long decimal) {
        if (decimal == null) {
            decimal = 0L;
        }
        if (value == null) {
            return null;
        }
        BigDecimal bd = new BigDecimal(Double.toString(value.doubleValue()));
        bd = bd.setScale(decimal.intValue(), RoundingMode.HALF_UP);
        return bd.doubleValue();
    }

    public static Double trunc(Number value, Long decimal) {
        if (decimal == null) {
            decimal = 0L;
        }
        if (value == null) {
            return null;
        }
        BigDecimal bd = new BigDecimal(Double.toString(value.doubleValue()));
        bd = bd.setScale(decimal.intValue(), RoundingMode.DOWN);
        return bd.doubleValue();
    }

    /**
     * Visits a 'binaryNumeric' expression.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to a double.
     */
    @Override
    public ResolvableExpression visitBinaryNumeric(VtlParser.BinaryNumericContext ctx) {
        List<ResolvableExpression> parameters = List.of(
                exprVisitor.visit(ctx.left),
                exprVisitor.visit(ctx.right)
        );
        switch (ctx.op.getType()) {
            case VtlParser.MOD:
                return genericFunctionsVisitor.invoke("mod", parameters);
            case VtlParser.POWER:
                return genericFunctionsVisitor.invoke("power", parameters);
            case VtlParser.LOG:
                return genericFunctionsVisitor.invoke("log", parameters);
            default:
                throw new UnsupportedOperationException(UNKNOWN_OPERATOR + ctx);
        }
    }

    public static Double mod(Number left, Number right) {
        if (left == null || right == null) {
            return null;
        }
        if (right.doubleValue() == 0) {
            return left.doubleValue();
        }
        return (left.doubleValue() % right.doubleValue()) * (right.doubleValue() < 0 ? -1 : 1);
    }

    public static Double power(Number left, Number right) {
        if (left == null || right == null) {
            return null;
        }
        return Math.pow(left.doubleValue(), right.doubleValue());
    }

    public static Double log(Number operand, Number base) {
        if (operand == null || base == null) {
            return null;
        }
        if (operand.doubleValue() <= 0)
            throw new IllegalArgumentException("operand must be positive");
        if (base.doubleValue() < 1)
            throw new IllegalArgumentException("base must be greater or equal than 1");
        return Math.log(operand.doubleValue()) / Math.log(base.doubleValue());
    }
}
