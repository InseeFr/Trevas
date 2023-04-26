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

    public static Number multiplication(Number valueA, Number valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        if (valueA instanceof Long && valueB instanceof Long) {
            return valueA.longValue() * valueB.longValue();
        }
        return valueA.doubleValue() * valueB.doubleValue();
    }

    public static Number division(Number valueA, Number valueB) {
        if (valueA == null || valueB == null) {
            return null;
        }
        if (valueA instanceof Long && valueB instanceof Long) {
            return valueA.longValue() / valueB.longValue();
        }
        return valueA.doubleValue() / valueB.doubleValue();
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
            return new ArithmeticExpression(getResolvableExpression(ctx, parameters), parameters);
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }

    private ResolvableExpression getResolvableExpression(VtlParser.ArithmeticExprContext ctx, List<ResolvableExpression> parameters) throws VtlScriptException {
        switch (ctx.op.getType()) {
            case VtlParser.MUL:
                return genericFunctionsVisitor.invokeFunction("multiplication", parameters, fromContext(ctx));
            case VtlParser.DIV:
                return genericFunctionsVisitor.invokeFunction("division", parameters, fromContext(ctx));
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
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
