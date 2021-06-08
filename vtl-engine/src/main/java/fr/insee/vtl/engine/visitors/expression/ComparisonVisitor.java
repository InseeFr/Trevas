package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.ConflictingTypesException;
import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.BooleanExpression;
import fr.insee.vtl.model.ListExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.TypedExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;
import static fr.insee.vtl.engine.utils.TypeChecking.hasNullArgs;

/**
 * <code>ComparisonVisitor</code> is the base visitor for comparison, 'element of' and list expressions.
 */
public class ComparisonVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor the parent expression visitor.
     */
    public ComparisonVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    private static <T extends Comparable<T>> boolean isEqual(T left, T right) {
        return left.compareTo(right) == 0;
    }

    private static <T extends Comparable<T>> boolean isNotEqual(T left, T right) {
        return !isEqual(left, right);
    }

    private static <T extends Comparable<T>> boolean isLessThan(T left, T right) {
        return left.compareTo(right) < 0;
    }

    private static <T extends Comparable<T>> boolean isGreaterThan(T left, T right) {
        return left.compareTo(right) > 0;
    }

    private static <T extends Comparable<T>> boolean isLessThanOrEqual(T left, T right) {
        return !isGreaterThan(left, right);
    }

    private static <T extends Comparable<T>> boolean isGreaterThanOrEqual(T left, T right) {
        return !isLessThan(left, right);
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

        // Get the type of the Token.
        // TODO(hadrien): Reported to ANTLR: https://github.com/antlr/antlr4/issues/2862
        Token type = ((TerminalNode) ctx.op.getChild(0)).getSymbol();

        // Special case with nulls.
        if (leftExpression.getType().equals(Object.class) || rightExpression.getType().equals(Object.class)) {
            return BooleanExpression.of((Boolean) null);
        }

        assertTypeExpression(rightExpression, leftExpression.getType(), ctx.right);

        // As long as both types return Comparable<TYPE>.
        if (Comparable.class.isAssignableFrom(leftExpression.getType())) {
            switch (type.getType()) {
                case VtlParser.EQ:
                    return BooleanExpression.of(context -> {
                        // TODO: factorize null handling in equal function
                        Comparable leftValue = (Comparable) leftExpression.resolve(context);
                        Comparable rightValue = (Comparable) rightExpression.resolve(context);
                        if (hasNullArgs(leftValue, rightValue)) return null;
                        return isEqual(leftValue, rightValue);
                    });
                case VtlParser.NEQ:
                    return BooleanExpression.of(context -> {
                        Comparable leftValue = (Comparable) leftExpression.resolve(context);
                        Comparable rightValue = (Comparable) rightExpression.resolve(context);
                        if (hasNullArgs(leftValue, rightValue)) return null;
                        return isNotEqual(leftValue, rightValue);
                    });
                case VtlParser.LT:
                    return BooleanExpression.of(context -> {
                        Comparable leftValue = (Comparable) leftExpression.resolve(context);
                        Comparable rightValue = (Comparable) rightExpression.resolve(context);
                        if (hasNullArgs(leftValue, rightValue)) return null;
                        return isLessThan(leftValue, rightValue);
                    });
                case VtlParser.MT:
                    return BooleanExpression.of(context -> {
                        Comparable leftValue = (Comparable) leftExpression.resolve(context);
                        Comparable rightValue = (Comparable) rightExpression.resolve(context);
                        if (hasNullArgs(leftValue, rightValue)) return null;
                        return isGreaterThan(leftValue, rightValue);
                    });
                case VtlParser.LE:
                    return BooleanExpression.of(context -> {
                        Comparable leftValue = (Comparable) leftExpression.resolve(context);
                        Comparable rightValue = (Comparable) rightExpression.resolve(context);
                        if (hasNullArgs(leftValue, rightValue)) return null;
                        return isLessThanOrEqual(leftValue, rightValue);
                    });
                case VtlParser.ME:
                    return BooleanExpression.of(context -> {
                        Comparable leftValue = (Comparable) leftExpression.resolve(context);
                        Comparable rightValue = (Comparable) rightExpression.resolve(context);
                        if (hasNullArgs(leftValue, rightValue)) return null;
                        return isGreaterThanOrEqual(leftValue, rightValue);
                    });
                default:
                    throw new UnsupportedOperationException("unknown operator " + ctx);
            }
        } else {
            throw new Error("type " + leftExpression.getType() + " must implement Comparable");
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
                return BooleanExpression.of(context -> {
                    List<?> list = listExpression.resolve(context);
                    Object value = operand.resolve(context);
                    return list.contains(value);
                });
            case VtlParser.NOT_IN:
                return BooleanExpression.of(context -> {
                    List<?> list = listExpression.resolve(context);
                    Object value = operand.resolve(context);
                    return !list.contains(value);
                });
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

        // Find the type of the list.
        Set<Class<?>> types = listExpressions.stream().map(TypedExpression::getType)
                .collect(Collectors.toSet());

        if (types.size() > 1) {
            throw new VtlRuntimeException(
                    new ConflictingTypesException(types, ctx)
            );
        }

        // The grammar defines list with minimum one constant so the types will never
        // be empty.
        Class<?> type = types.iterator().next();

        // Since all expression are constant we don't need any context.
        List<Object> values = listExpressions
                .stream()
                .map(expression -> expression.resolve(Map.of()))
                .collect(Collectors.toList());

        return ListExpression.withContainedType(values, type);
    }
}
