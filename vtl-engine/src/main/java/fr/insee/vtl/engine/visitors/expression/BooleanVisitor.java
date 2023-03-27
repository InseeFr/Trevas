package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.Objects;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>BooleanVisitor</code> is the base visitor for expressions involving boolean operations.
 */
public class BooleanVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor the parent expression visitor.
     */
    public BooleanVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    /**
     * Visits expressions with boolean operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the boolean operation.
     */
    @Override
    public ResolvableExpression visitBooleanExpr(VtlParser.BooleanExprContext ctx) {
        var pos = fromContext(ctx);
        try {
            var leftExpr = exprVisitor.visit(ctx.left).checkAssignableFrom(Boolean.class);
            var rightExpr = exprVisitor.visit(ctx.right).checkAssignableFrom(Boolean.class);

            return ResolvableExpression.withType(Boolean.class).withPosition(pos).using(
                    context -> {
                        var leftValue = (Boolean) leftExpr.resolve(context);
                        var rightValue = (Boolean) rightExpr.resolve(context);
                        switch (ctx.op.getType()) {
                            case VtlParser.AND:
                                return handleAnd(leftValue, rightValue);
                            case VtlParser.OR:
                                return handleOr(leftValue, rightValue);
                            case VtlParser.XOR:
                                return handleXor(leftValue, rightValue);
                            default:
                                throw new UnsupportedOperationException("unknown operator " + ctx);
                        }
                    }
            );
        } catch (InvalidTypeException e) {
            throw new VtlRuntimeException(e);
        }
    }

    private Boolean handleAnd(Boolean left, Boolean right) {
        if (left != null && !left) return false;
        if (right != null && !right) return false;
        if (TypeChecking.hasNullArgs(left, right)) {
            return null;
        }
        return true;
    }

    private Boolean handleOr(Boolean left, Boolean right) {
        if (left != null && left) {
            return true;
        }
        if (right != null && right) {
            return true;
        }
        if (TypeChecking.hasNullArgs(left, right)) {
            return null;
        }
        return false;
    }

    private Boolean handleXor(Boolean left, Boolean right) {
        if (TypeChecking.hasNullArgs(left, right)) {
            return null;
        }
        return left ^ right;
    }
}
