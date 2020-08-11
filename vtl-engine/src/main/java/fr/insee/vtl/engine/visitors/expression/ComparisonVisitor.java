package fr.insee.vtl.engine.visitors.expression;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.script.ScriptContext;
import javax.script.ScriptException;

import fr.insee.vtl.engine.exceptions.ConflictingTypesException;
import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.model.BooleanExpression;
import fr.insee.vtl.model.ListExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.TypedExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * <code>ComparisonVisitor</code> is the base visitor for comparison, 'element of' and list expressions.
 */
public class ComparisonVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking a scripting context.
     *
     * @param context The scripting context for the visitor.
     */
    public ComparisonVisitor(ScriptContext context) {
        exprVisitor = new ExpressionVisitor(context);
    }

    /**
     * Visits expressions with comparisons.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the boolean result of the comparison.
     */
    @Override
    public ResolvableExpression visitComparisonExpr(VtlParser.ComparisonExprContext ctx) {
        ResolvableExpression leftExpression = exprVisitor.visit(ctx.left);
        ResolvableExpression rightExpression = exprVisitor.visit(ctx.right);

        if (!leftExpression.getType().equals(rightExpression.getType())) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(leftExpression.getType(), rightExpression.getType(), ctx.right)
            );
        }

        // Get the type of the Token.
        // TODO(hadrien): Reported to ANTLR: https://github.com/antlr/antlr4/issues/2862
        Token type = ((TerminalNode) ctx.op.getChild(0)).getSymbol();

        switch (type.getType()) {
            case VtlParser.EQ:
                return ResolvableExpression.withType(Boolean.class, context -> {
                    Object leftValue = leftExpression.resolve(context);
                    Object rightValue = rightExpression.resolve(context);
                    return leftValue.equals(rightValue);
                });
            case VtlParser.NEQ:
                return ResolvableExpression.withType(Boolean.class, context -> {
                    Object leftValue = leftExpression.resolve(context);
                    Object rightValue = rightExpression.resolve(context);
                    return !leftValue.equals(rightValue);
                });
            case VtlParser.LT:
                return ResolvableExpression.withType(Boolean.class, context -> {
                    if (TypeChecking.isLong(leftExpression) &&  TypeChecking.isLong(rightExpression)) {
                        Long leftValue = (Long) leftExpression.resolve(context);
                        Long rightValue = (Long) rightExpression.resolve(context);
                        return leftValue < rightValue;
                    }
                    else if (TypeChecking.isDouble(leftExpression) &&  TypeChecking.isDouble(rightExpression)) {
                        Double leftValue = (Double) leftExpression.resolve(context);
                        Double rightValue = (Double) rightExpression.resolve(context);
                        return leftValue < rightValue;
                    }
                    throw new VtlRuntimeException(
                            new InvalidTypeException(leftExpression.getType(), rightExpression.getType(), ctx.right)
                    );
                });
            case VtlParser.MT:
                return ResolvableExpression.withType(Boolean.class, context -> {
                    if (TypeChecking.isLong(leftExpression) &&  TypeChecking.isLong(rightExpression)) {
                        Long leftValue = (Long) leftExpression.resolve(context);
                        Long rightValue = (Long) rightExpression.resolve(context);
                        return leftValue > rightValue;
                    }
                    else if (TypeChecking.isDouble(leftExpression) &&  TypeChecking.isDouble(rightExpression)) {
                        Double leftValue = (Double) leftExpression.resolve(context);
                        Double rightValue = (Double) rightExpression.resolve(context);
                        return leftValue > rightValue;
                    }
                    throw new VtlRuntimeException(
                            new InvalidTypeException(leftExpression.getType(), rightExpression.getType(), ctx.right)
                    );
                });
            case VtlParser.LE:
                return ResolvableExpression.withType(Boolean.class, context -> {
                    if (TypeChecking.isLong(leftExpression) &&  TypeChecking.isLong(rightExpression)) {
                        Long leftValue = (Long) leftExpression.resolve(context);
                        Long rightValue = (Long) rightExpression.resolve(context);
                        return leftValue <= rightValue;
                    }
                    else if (TypeChecking.isDouble(leftExpression) &&  TypeChecking.isDouble(rightExpression)) {
                        Double leftValue = (Double) leftExpression.resolve(context);
                        Double rightValue = (Double) rightExpression.resolve(context);
                        return leftValue <= rightValue;
                    }
                    throw new VtlRuntimeException(
                            new InvalidTypeException(leftExpression.getType(), rightExpression.getType(), ctx.right)
                    );
                });
            case VtlParser.ME:
                return ResolvableExpression.withType(Boolean.class, context -> {
                    if (TypeChecking.isLong(leftExpression) &&  TypeChecking.isLong(rightExpression)) {
                        Long leftValue = (Long) leftExpression.resolve(context);
                        Long rightValue = (Long) rightExpression.resolve(context);
                        return leftValue >= rightValue;
                    }
                    else if (TypeChecking.isDouble(leftExpression) &&  TypeChecking.isDouble(rightExpression)) {
                        Double leftValue = (Double) leftExpression.resolve(context);
                        Double rightValue = (Double) rightExpression.resolve(context);
                        return leftValue >= rightValue;
                    }
                    throw new VtlRuntimeException(
                            new InvalidTypeException(leftExpression.getType(), rightExpression.getType(), ctx.right)
                    );
                });
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }

    /**
     * Visits 'element of' ('In' or 'Not in') expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the boolean result of the 'element of' expression.
     */
    @Override
    public ResolvableExpression visitInNotInExpr(VtlParser.InNotInExprContext ctx) {
        ResolvableExpression operand = exprVisitor.visit(ctx.left);
        ListExpression listExpression = (ListExpression) visit(ctx.lists());

        if (!operand.getType().equals(listExpression.containedType())) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(operand.getType(), listExpression.containedType(), ctx.lists())
            );
        }

        switch (ctx.op.getType()) {
            case VtlParser.IN:
                return new BooleanExpression() {
                    @Override
                    public Boolean resolve(Map<String, Object> context) {
                        List<?> list = listExpression.resolve(context);
                        Object value = operand.resolve(context);
                        return list.contains(value);
                    }
                };
            case VtlParser.NOT_IN:
                return new BooleanExpression() {
                    @Override
                    public Boolean resolve(Map<String, Object> context) {
                        List<?> list = listExpression.resolve(context);
                        Object value = operand.resolve(context);
                        return !list.contains(value);
                    }
                };
            default:
                throw new IllegalStateException("Unexpected value: " + ctx.op.getType());
        }
    }

    /**
     * Visits list expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ListExpression</code> resolving to the list of given values with the given contained type.
     */
    @Override
    public ResolvableExpression visitLists(VtlParser.ListsContext ctx) {

        // Transform all the constants.
        List<ResolvableExpression> listExpressions = ctx.constant().stream()
                .map(exprVisitor::visitConstant)
                .collect(Collectors.toList());

        // Check that all list elements are of the same type
        Set<Class<?>> types = listExpressions.stream().map(TypedExpression::getType)
                .collect(Collectors.toSet());
        if (types.size() > 1) {
            throw new VtlRuntimeException(
                    new ConflictingTypesException(types, ctx)
            );
        }

        // The grammar defines lists with at minimum one constant so list of types will never be empty.
        Class<?> type = types.iterator().next();

        // Since all expressions are constant we don't need any context.
        List<Object> values = listExpressions
                .stream()
                .map(expression -> expression.resolve(Map.of()))
                .collect(Collectors.toList());

        return new ListExpression() {
            @Override
            public List<?> resolve(Map<String, Object> context) {
                return values;
            }

            @Override
            public Class<?> containedType() {
                return type;
            }
        };
    }
}
