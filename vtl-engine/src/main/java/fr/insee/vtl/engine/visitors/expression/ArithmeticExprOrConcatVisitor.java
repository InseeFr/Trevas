package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;

/**
 * <code>ArithmeticExprOrConcatVisitor</code> is the base visitor for plus, minus or concatenation expressions.
 */
public class ArithmeticExprOrConcatVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking a scripting context.
     *
     * @param context The scripting context for the visitor.
     */
    public ArithmeticExprOrConcatVisitor(ScriptContext context) {
        exprVisitor = new ExpressionVisitor(context);
    }

    /**
     * Visits expressions with plus, minus or concatenation operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the plus, minus or concatenation operation.
     */
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
                return ResolvableExpression.withType(String.class, context -> {
                    String leftValue = (String) leftExpression.resolve(context);
                    String rightValue = (String) rightExpression.resolve(context);
                    return leftValue + rightValue;
                });
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }
}
