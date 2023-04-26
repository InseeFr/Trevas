package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.Positioned;
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

    public static Number addition(Number valueA, Number valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        if (valueA instanceof Long && valueB instanceof Long) {
            return valueA.longValue() + valueB.longValue();
        }
        return valueA.doubleValue() + valueB.doubleValue();
    }

    public static Number subtraction(Number valueA, Number valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        if (valueA instanceof Long && valueB instanceof Long) {
            return valueA.longValue() + valueB.longValue();
        }
        return valueA.doubleValue() + valueB.doubleValue();
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
            return getResolvableExpression(ctx, pos, parameters);
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }

    private ResolvableExpression getResolvableExpression(VtlParser.ArithmeticExprOrConcatContext ctx, Positioned pos, List<ResolvableExpression> parameters) throws VtlScriptException {
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
}
