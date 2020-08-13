package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.Objects;

public class IfVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    public IfVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    @Override
    public ResolvableExpression visitIfExpr(VtlParser.IfExprContext ctx) {
        ResolvableExpression conditionalExpression = exprVisitor.visit(ctx.conditionalExpr);
        if (!conditionalExpression.getType().equals(Boolean.class)) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(Boolean.class, conditionalExpression.getType(), ctx.conditionalExpr)
            );
        }

        ResolvableExpression thenExpression = exprVisitor.visit(ctx.thenExpr);
        ResolvableExpression elseExpression = exprVisitor.visit(ctx.elseExpr);
        if (!thenExpression.getType().equals(elseExpression.getType())) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(thenExpression.getType(), elseExpression.getType(), ctx.elseExpr)
            );
        }

        return ResolvableExpression.withType(Object.class, context -> {
            Boolean conditionalValue = (Boolean) conditionalExpression.resolve(context);
            return Boolean.TRUE.equals(conditionalValue) ?
                    thenExpression.resolve(context) : elseExpression.resolve(context);
        });
    }
}
