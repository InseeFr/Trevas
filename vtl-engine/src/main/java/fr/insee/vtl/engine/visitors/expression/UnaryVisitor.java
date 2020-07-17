package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;
import java.util.List;

public class UnaryVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    public UnaryVisitor(ScriptContext context) {
        exprVisitor = new ExpressionVisitor(context);
    }

    @Override
    public ResolvableExpression visitUnaryExpr(VtlParser.UnaryExprContext ctx) {

        ResolvableExpression rightExpression = exprVisitor.visit(ctx.right);

        switch (ctx.op.getType()) {
            case VtlParser.PLUS:
                if (!TypeChecking.isNumber(rightExpression))
                    throw new VtlRuntimeException(
                            new InvalidTypeException(ctx.right, List.of(Long.class, Double.class), rightExpression.getType())
                    );
                return handleUnaryPlus(rightExpression);
            case VtlParser.MINUS:
                if (!TypeChecking.isNumber(rightExpression)) {
                    throw new VtlRuntimeException(
                            new InvalidTypeException(ctx.right, List.of(Long.class, Double.class), rightExpression.getType())
                    );
                }
                return handleUnaryMinus(rightExpression);
            case VtlParser.NOT:
                // TODO: handle null right value (not null has to return null)
                if (!TypeChecking.isBoolean(rightExpression)) {
                    throw new VtlRuntimeException(
                            new InvalidTypeException(ctx.right, Boolean.class, rightExpression.getType())
                    );
                }
                return handleUnaryNot(rightExpression);
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }

    private ResolvableExpression handleUnaryPlus(ResolvableExpression rightExpression) {
        if (TypeChecking.isLong(rightExpression))
            return ResolvableExpression.withType(Long.class, context ->
                    (Long) rightExpression.resolve(context)
            );
        return ResolvableExpression.withType(Double.class, context ->
                (Double) rightExpression.resolve(context)
        );
    }

    private ResolvableExpression handleUnaryMinus(ResolvableExpression rightExpression) {
        if (TypeChecking.isLong(rightExpression))
            return ResolvableExpression.withType(Long.class, context ->
                    - ((Long) rightExpression.resolve(context))
            );
        return ResolvableExpression.withType(Double.class, context ->
                - ((Double) rightExpression.resolve(context))
        );
    }

    private ResolvableExpression handleUnaryNot(ResolvableExpression rightExpression) {
        return ResolvableExpression.withType(Boolean.class, context ->
                !((Boolean) rightExpression.resolve(context)));
    }
}
