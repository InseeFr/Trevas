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
 * <code>ArithmeticVisitor</code> is the base visitor for multiplication or division expressions.
 */
public class ArithmeticVisitor extends VtlBaseVisitor<ResolvableExpression> {

    // TODO: Support dataset as argument of unary numeric.
    //          ceil_ds := ceil(ds) -> ds[calc m1 := ceil(m1), m2 := ...]
    //          ceil_ds := ceil(ds#measure)
    //       Evaluate when we have a proper function abstraction:
    //          var function = findFunction(name);
    //          function.run(ctx.expr());
    //

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor The visitor for the enclosing expression.
     */
    public ArithmeticVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
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
            switch (ctx.op.getType()) {
                case VtlParser.MUL:
                    return handleMultiplication(ctx);

                case VtlParser.DIV:
                    return handleDivision(ctx);
                default:
                    throw new UnsupportedOperationException("unknown operator " + ctx);
            }
        } catch (InvalidTypeException e) {
            throw new VtlRuntimeException(e);
        }
    }

    private ResolvableExpression handleMultiplication(VtlParser.ArithmeticExprContext ctx) throws InvalidTypeException {
        var leftExpression = exprVisitor.visit(ctx.left).checkInstanceOf(Number.class);
        var rightExpression = exprVisitor.visit(ctx.right).checkInstanceOf(Number.class);

        if (isLong(leftExpression) && isLong(rightExpression)) {
            return ResolvableExpression.withType(Long.class).withPosition(fromContext(ctx)).using(context -> {
                Long leftValue = (Long) leftExpression.resolve(context);
                Long rightValue = (Long) rightExpression.resolve(context);
                if (TypeChecking.hasNullArgs(leftValue, rightValue)) return null;
                return leftValue * rightValue;
            });
        } else {
            return ResolvableExpression.withType(Double.class).withPosition(fromContext(ctx)).using(context -> {
                var leftValue = leftExpression.resolve(context);
                var rightValue = rightExpression.resolve(context);
                if (TypeChecking.hasNullArgs(leftValue, rightValue)) return null;
                var leftDouble = leftValue instanceof Long ? ((Long) leftValue).doubleValue() : (Double) leftValue;
                var rightDouble = rightValue instanceof Long ? ((Long) rightValue).doubleValue() : (Double) rightValue;
                return leftDouble * rightDouble;
            });
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
