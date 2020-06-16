package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

public class ArithmeticVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor = new ExpressionVisitor();

    @Override
    public ResolvableExpression visitArithmeticExpr(VtlParser.ArithmeticExprContext ctx) {
        // TODO: deal with Long & Double dynamically
        ResolvableExpression leftExpression = exprVisitor.visit(ctx.left);
        ResolvableExpression rightExpression = exprVisitor.visit(ctx.right);
        switch (ctx.op.getType()) {
            case VtlParser.MUL:
                return ResolvableExpression.withType(Long.class, context -> {
                    Long leftValue = (Long) leftExpression.resolve(context);
                    Long rightValue = (Long) rightExpression.resolve(context);
                    return leftValue * rightValue;
                });
            case VtlParser.DIV:
                return ResolvableExpression.withType(Long.class, context -> {
                    Long leftValue = (Long) leftExpression.resolve(context);
                    Long rightValue = (Long) rightExpression.resolve(context);
                    return leftValue / rightValue;
                });
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }
}
