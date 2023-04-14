package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.List;
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
        this.genericFunctionsVisitor = genericFunctionsVisitor;
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

    /**
     * Visits expressions with multiplication or division operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the multiplication or division operation.
     */
    @Override
    public ResolvableExpression visitArithmeticExpr(VtlParser.ArithmeticExprContext ctx) {
        try {
            var pos = fromContext(ctx);
            List<ResolvableExpression> parameters = List.of(
                    exprVisitor.visit(ctx.left),
                    exprVisitor.visit(ctx.right)
            );
            switch (ctx.op.getType()) {
                case VtlParser.MUL:
                    return genericFunctionsVisitor.invokeFunction("multiplication", parameters, fromContext(ctx));
                case VtlParser.DIV:
                    return handleDivision(ctx);
                default:
                    throw new UnsupportedOperationException("unknown operator " + ctx);
            }
        } catch (InvalidTypeException e) {
            throw new VtlRuntimeException(e);
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }

    private ResolvableExpression handleDivision(VtlParser.ArithmeticExprContext ctx) throws InvalidTypeException {
        var leftExpression = exprVisitor.visit(ctx.left).checkInstanceOf(Number.class);
        var rightExpression = exprVisitor.visit(ctx.right).checkInstanceOf(Number.class);
        return ResolvableExpression.withType(Double.class).withPosition(fromContext(ctx)).using(context -> {
            var leftValue = leftExpression.resolve(context);
            var rightValue = rightExpression.resolve(context);
            if (TypeChecking.hasNullArgs(leftValue, rightValue)) return null;
            var leftDouble = leftValue instanceof Long ? ((Long) leftValue).doubleValue() : (Double) leftValue;
            var rightDouble = rightValue instanceof Long ? ((Long) rightValue).doubleValue() : (Double) rightValue;
            return leftDouble / rightDouble;
        });
    }
}
