package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;

public class IfVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    public IfVisitor(ScriptContext context) {
        exprVisitor = new ExpressionVisitor(context);
    }

    @Override
    public ResolvableExpression visitIfExpr(VtlParser.IfExprContext ctx) {
        ResolvableExpression conditionalExpression = exprVisitor.visit(ctx.conditionalExpr);
        if (!conditionalExpression.getType().equals(Boolean.class)) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(ctx.conditionalExpr, Boolean.class, conditionalExpression.getType())
            );
        }

        ResolvableExpression thenExpression = exprVisitor.visit(ctx.thenExpr);
        ResolvableExpression elseExpression = exprVisitor.visit(ctx.elseExpr);
        if (!thenExpression.getType().equals(elseExpression.getType())) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(ctx.elseExpr, thenExpression.getType(), elseExpression.getType())
            );
        }

        return ResolvableExpression.withType(Object.class, context -> {
            Boolean conditionalValue = (Boolean) conditionalExpression.resolve(context);
            return conditionalValue ? thenExpression.resolve(context) : elseExpression.resolve(context);
        });
    }
}
