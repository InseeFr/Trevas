package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.utils.Java8Helpers;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.List;
import java.util.Objects;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>BooleanVisitor</code> is the base visitor for expressions involving boolean operations.
 */
public class BooleanVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;
    private final GenericFunctionsVisitor genericFunctionsVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor       the parent expression visitor.
     * @param genericFunctionsVisitor the parent generic functions visitor.
     */
    public BooleanVisitor(ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
        this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
    }

    public static Boolean and(Boolean left, Boolean right) {
        if (left != null && !left) return false;
        if (right != null && !right) return false;
        if (left == null || right == null) return null;
        return true;
    }

    public static Boolean or(Boolean left, Boolean right) {
        if (left != null && left) {
            return true;
        }
        if (right != null && right) {
            return true;
        }
        if (left == null || right == null) {
            return null;
        }
        return false;
    }

    public static Boolean xor(Boolean left, Boolean right) {
        if (left == null || right == null) {
            return null;
        }
        return left ^ right;
    }

    /**
     * Visits expressions with boolean operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the boolean operation.
     */
    @Override
    public ResolvableExpression visitBooleanExpr(VtlParser.BooleanExprContext ctx) {
        try {
            List<ResolvableExpression> parameters = Java8Helpers.listOf(
                    exprVisitor.visit(ctx.left),
                    exprVisitor.visit(ctx.right)
            );
            switch (ctx.op.getType()) {
                case VtlParser.AND:
                    return genericFunctionsVisitor.invokeFunction("and", parameters, fromContext(ctx));
                case VtlParser.OR:
                    return genericFunctionsVisitor.invokeFunction("or", parameters, fromContext(ctx));
                case VtlParser.XOR:
                    return genericFunctionsVisitor.invokeFunction("xor", parameters, fromContext(ctx));
                default:
                    throw new UnsupportedOperationException("unknown operator " + ctx);
            }
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }
}
