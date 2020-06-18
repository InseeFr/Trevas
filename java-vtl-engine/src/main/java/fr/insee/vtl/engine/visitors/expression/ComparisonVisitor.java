package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;

public class ComparisonVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    public ComparisonVisitor(ScriptContext context) {
        exprVisitor = new ExpressionVisitor(context);;
    }

    @Override
    public ResolvableExpression visitComparisonExpr(VtlParser.ComparisonExprContext ctx) {
        ResolvableExpression leftExpression = exprVisitor.visit(ctx.left);
        ResolvableExpression rightExpression = exprVisitor.visit(ctx.right);
        // TODO : improve how to get the type of operand
        switch (ctx.comparisonOperand().getText()) {
            case "=":
                return ResolvableExpression.withType(Boolean.class, context -> {
                    Object leftValue = leftExpression.resolve(context);
                    Object rightValue = rightExpression.resolve(context);
                    return leftValue.equals(rightValue);
                });
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }
}
