package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.ConflictingTypesException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.BooleanExpression;
import fr.insee.vtl.model.ListExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.TypedExpression;
import fr.insee.vtl.model.VtlBiFunction;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.NumberConvertors.asBigDecimal;
import static fr.insee.vtl.engine.utils.TypeChecking.assertNumberOrTypeExpression;
import static fr.insee.vtl.engine.utils.TypeChecking.hasNullArgs;
import static fr.insee.vtl.engine.utils.TypeChecking.isNull;
import static fr.insee.vtl.engine.utils.TypeChecking.isNumber;

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
     * This method handles the comparison between two expressions
     *
     * @param leftExpression
     * @param rightExpression
     * @param comparisonFn    a lambda or a method reference
     * @return a VTL boolean expression
     */
    public BooleanExpression compareExpressions(
            ResolvableExpression leftExpression,
            ResolvableExpression rightExpression,
            VtlBiFunction<Comparable, Comparable, Boolean> comparisonFn) {
        return BooleanExpression.of(() -> {throw new UnsupportedOperationException();}, context -> {
            Comparable leftValue;
            Comparable rightValue;

            if (isNumber(leftExpression)) {
                leftValue = asBigDecimal(leftExpression, leftExpression.resolve(context));
                rightValue = asBigDecimal(rightExpression, rightExpression.resolve(context));

            } else {
                leftValue = (Comparable) leftExpression.resolve(context);
                rightValue = (Comparable) rightExpression.resolve(context);
            }

            if (hasNullArgs(leftValue, rightValue)) return null;

            return comparisonFn.apply(leftValue, rightValue);
        });
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
        Token type = ((TerminalNode) ctx.op.getChild(0)).getSymbol();

        // Special case with nulls.
        if (isNull(leftExpression) || isNull(rightExpression)) {
            return BooleanExpression.of(fromContext(ctx), (Boolean) null);
        }

        assertNumberOrTypeExpression(rightExpression, leftExpression.getType(), ctx.right);

        // As long as both types return Comparable<TYPE>.
        if (Comparable.class.isAssignableFrom(leftExpression.getType())) {
            switch (type.getType()) {
                case VtlParser.EQ:
                    return compareExpressions(leftExpression, rightExpression, ComparisonVisitor::isEqual);
                case VtlParser.NEQ:
                    return compareExpressions(leftExpression, rightExpression, ComparisonVisitor::isNotEqual);
                case VtlParser.LT:
                    return compareExpressions(leftExpression, rightExpression, ComparisonVisitor::isLessThan);
                case VtlParser.MT:
                    return compareExpressions(leftExpression, rightExpression, ComparisonVisitor::isGreaterThan);
                case VtlParser.LE:
                    return compareExpressions(leftExpression, rightExpression, ComparisonVisitor::isLessThanOrEqual);
                case VtlParser.ME:
                    return compareExpressions(leftExpression, rightExpression, ComparisonVisitor::isGreaterThanOrEqual);
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

        Positioned pos = fromContext(ctx);
        if (!operand.getType().equals(listExpression.containedType())) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(operand.getType(), listExpression.containedType(), pos)
            );
        }

        switch (ctx.op.getType()) {
            case VtlParser.IN:
                return BooleanExpression.of(pos, context -> {
                    List<?> list = listExpression.resolve(context);
                    Object value = operand.resolve(context);
                    return list.contains(value);
                });
            case VtlParser.NOT_IN:
                return BooleanExpression.of(pos, context -> {
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
                    new ConflictingTypesException(types, fromContext(ctx))
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
