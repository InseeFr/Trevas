package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;

public class ArithmeticExprOrConcatVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    public ArithmeticExprOrConcatVisitor(ScriptContext context) {
        exprVisitor = new ExpressionVisitor(context);;
    }

    @Override
    public ResolvableExpression visitArithmeticExprOrConcat(VtlParser.ArithmeticExprOrConcatContext ctx) {
        // TODO: deal with Long & Double dynamically
        ResolvableExpression leftExpression = exprVisitor.visit(ctx.left);
        ResolvableExpression rightExpression = exprVisitor.visit(ctx.right);
        switch (ctx.op.getType()) {
            case VtlParser.PLUS:
                return ResolvableExpression.withType(Long.class, context -> {
                    Long leftValue = (Long) leftExpression.resolve(context);
                    Long rightValue = (Long) rightExpression.resolve(context);
                    return leftValue + rightValue;
                });
            case VtlParser.MINUS:
                return ResolvableExpression.withType(Long.class, context -> {
                    Long leftValue = (Long) leftExpression.resolve(context);
                    Long rightValue = (Long) rightExpression.resolve(context);
                    return leftValue - rightValue;
                });
            case VtlParser.CONCAT:
                return ResolvableExpression.withType(Long.class, context -> {
                    Long leftValue = (Long) leftExpression.resolve(context);
                    Long rightValue = (Long) rightExpression.resolve(context);
                    return Long.parseLong((leftValue.toString() + rightValue.toString()));
                });
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }
}
