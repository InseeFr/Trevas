package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

public class IfVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor = new ExpressionVisitor();

    @Override
    public ResolvableExpression visitIfExpr(VtlParser.IfExprContext ctx) {
        ResolvableExpression conditionalExpression = exprVisitor.visit(ctx.conditionalExpr);
        ResolvableExpression thenExpression = exprVisitor.visit(ctx.thenExpr);
        ResolvableExpression elseExpression = exprVisitor.visit(ctx.elseExpr);
        return ResolvableExpression.withType(Object.class, context -> {
            Boolean conditionalValue = (Boolean) conditionalExpression.resolve(context);
            return conditionalValue ? thenExpression.resolve(context) : elseExpression.resolve(context);
        });
    }
}
