package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.List;
import java.util.Objects;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>ArithmeticExprOrConcatVisitor</code> is the base visitor for plus, minus or concatenation expressions.
 */
public class ArithmeticExprOrConcatVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;
    private final GenericFunctionsVisitor genericFunctionsVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor       The visitor for the enclosing expression.
     * @param genericFunctionsVisitor
     */
    public ArithmeticExprOrConcatVisitor(ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
        this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
    }

    public static Long addition(Long valueA, Long valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        return valueA + valueB;
    }

    public static Double addition(Long valueA, Double valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        return valueA + valueB;
    }

    public static Double addition(Double valueA, Long valueB) {
        return addition(valueB, valueA);
    }

    public static Double addition(Double valueA, Double valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        return valueA + valueB;
    }

    public static Long subtraction(Long valueA, Long valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        return valueA - valueB;
    }

    public static Double subtraction(Long valueA, Double valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        return valueA - valueB;
    }

    public static Double subtraction(Double valueA, Long valueB) {
        return subtraction(valueB, valueA);
    }

    public static Double subtraction(Double valueA, Double valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        return valueA - valueB;
    }

    public static String concat(String valueA, String valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        return valueA + valueB;
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
            var pos = fromContext(ctx);
            var parameters = List.of(exprVisitor.visit(ctx.left), exprVisitor.visit(ctx.right));
            boolean hasDsParameter = parameters.stream()
                    .map(ResolvableExpression::getType)
                    .anyMatch(Double.class::equals);
            if (hasDsParameter) {
                switch (ctx.op.getType()) {
                    case VtlParser.PLUS:
                        return new ArithmeticVisitor.ArithmeticExpression(
                                genericFunctionsVisitor.invokeFunction("addition", parameters, pos),
                                parameters
                        );
                    case VtlParser.MINUS:
                        return new ArithmeticVisitor.ArithmeticExpression(
                                genericFunctionsVisitor.invokeFunction("subtraction", parameters, pos),
                                parameters
                        );
                    case VtlParser.CONCAT:
                        return genericFunctionsVisitor.invokeFunction("concat", parameters, pos);
                    default:
                        throw new UnsupportedOperationException("unknown operator " + ctx);
                }
            }
            switch (ctx.op.getType()) {
                case VtlParser.PLUS:
                    return genericFunctionsVisitor.invokeFunction("addition", parameters, pos);
                case VtlParser.MINUS:
                    return genericFunctionsVisitor.invokeFunction("subtraction", parameters, pos);
                case VtlParser.CONCAT:
                    return genericFunctionsVisitor.invokeFunction("concat", parameters, pos);
                default:
                    throw new UnsupportedOperationException("unknown operator " + ctx);
            }
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }
}
