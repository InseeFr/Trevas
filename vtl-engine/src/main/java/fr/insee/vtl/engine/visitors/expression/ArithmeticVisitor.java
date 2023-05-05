package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.TypedExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>ArithmeticVisitor</code> is the base visitor for multiplication or division expressions.
 */
public class ArithmeticVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;
    private final GenericFunctionsVisitor genericFunctionsVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor The visitor for the enclosing expression.
     */
    public ArithmeticVisitor(ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
        this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
    }

    public static Long multiplication(Long valueA, Long valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        return valueA * valueB;
    }

    public static Double multiplication(Long valueA, Double valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        return valueA.doubleValue() * valueB;
    }

    public static Double multiplication(Double valueA, Long valueB) {
        return multiplication(valueB, valueA);
    }

    public static Double multiplication(Double valueA, Double valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        return valueA * valueB;
    }

    public static Double division(Long valueA, Double valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        return valueA.doubleValue() / valueB;
    }

    public static Double division(Double valueA, Long valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        return valueA / valueB.doubleValue();
    }


    public static Double division(Long valueA, Long valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        return ((double) valueA / valueB);
    }

    public static Double division(Double valueA, Double valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        return valueA / valueB;
    }

    /**
     * Visits expressions with multiplication or division operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the multiplication or division operation.
     */
    @Override
    public ResolvableExpression visitArithmeticExpr(VtlParser.ArithmeticExprContext ctx) {
        try {
            List<ResolvableExpression> parameters = List.of(
                    exprVisitor.visit(ctx.left),
                    exprVisitor.visit(ctx.right)
            );
            var pos = fromContext(ctx);
            boolean hasDsParameter = parameters.stream()
                    .map(ResolvableExpression::getType)
                    .anyMatch(Double.class::equals);
            if (hasDsParameter) {
                switch (ctx.op.getType()) {
                    case VtlParser.MUL:
                        return new ArithmeticVisitor.ArithmeticExpression(
                                genericFunctionsVisitor.invokeFunction("multiplication", parameters, pos),
                                parameters
                        );
                    case VtlParser.DIV:
                        return new ArithmeticVisitor.ArithmeticExpression(
                                genericFunctionsVisitor.invokeFunction("division", parameters, pos),
                                parameters
                        );
                    default:
                        throw new UnsupportedOperationException("unknown operator " + ctx);
                }
            }
            switch (ctx.op.getType()) {
                case VtlParser.MUL:
                    return genericFunctionsVisitor.invokeFunction("multiplication", parameters, fromContext(ctx));
                case VtlParser.DIV:
                    return genericFunctionsVisitor.invokeFunction("division", parameters, fromContext(ctx));
                default:
                    throw new UnsupportedOperationException("unknown operator " + ctx);
            }
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }

    static class ArithmeticExpression extends ResolvableExpression {

        final ResolvableExpression expr;
        final Class<?> type;

        ArithmeticExpression(ResolvableExpression expr, List<? extends TypedExpression> typedExpressions) {
            super(expr);
            try {
                this.expr = expr.checkInstanceOf(Number.class);
            } catch (InvalidTypeException e) {
                throw new VtlRuntimeException(e);
            }
            this.type = typedExpressions.stream()
                    .map(TypedExpression::getType)
                    .anyMatch(Double.class::equals) ? Double.class : Long.class;
        }

        @Override
        public Object resolve(Map<String, Object> context) {
            return expr.resolve(context);
        }

        @Override
        public Class<?> getType() {
            return type;
        }
    }
}
