package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.ConflictingTypesException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.ListExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.TypedExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>ComparisonVisitor</code> is the base visitor for comparison, 'element of' and list expressions.
 */
public class ComparisonVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;
    private final GenericFunctionsVisitor genericFunctionsVisitor;

    private final String UNKNOWN_OPERATOR = "unknown operator ";

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor the parent expression visitor.
     */
    public ComparisonVisitor(ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
        this.genericFunctionsVisitor = genericFunctionsVisitor;
    }

    public static Boolean isEqual(Double left, Long right) {
        return BigDecimal.valueOf(left).compareTo(BigDecimal.valueOf(right)) == 0;
    }

    public static Boolean isEqual(Long left, Double right) {
        return BigDecimal.valueOf(left).compareTo(BigDecimal.valueOf(right)) == 0;
    }

    public static <T extends Comparable<T>> Boolean isEqual(T left, T right) {
        if (left == null || right == null) {
            return null;
        }
        return left.compareTo(right) == 0;
    }

    public static Boolean isNotEqual(Double left, Long right) {
        return !isEqual(left, right);
    }

    public static Boolean isNotEqual(Long left, Double right) {
        return !isEqual(left, right);
    }

    public static <T extends Comparable<T>> Boolean isNotEqual(T left, T right) {
        return !isEqual(left, right);
    }

    public static Boolean isLessThan(Double left, Long right) {
        return BigDecimal.valueOf(left).compareTo(BigDecimal.valueOf(right)) < 0;
    }

    public static Boolean isLessThan(Long left, Double right) {
        return BigDecimal.valueOf(left).compareTo(BigDecimal.valueOf(right)) < 0;
    }

    public static <T extends Comparable<T>> Boolean isLessThan(T left, T right) {
        if (left == null || right == null) {
            return null;
        }
        return left.compareTo(right) < 0;
    }

    public static Boolean isGreaterThan(Double left, Long right) {
        return BigDecimal.valueOf(left).compareTo(BigDecimal.valueOf(right)) > 0;
    }

    public static Boolean isGreaterThan(Long left, Double right) {
        return BigDecimal.valueOf(left).compareTo(BigDecimal.valueOf(right)) > 0;
    }

    public static <T extends Comparable<T>> Boolean isGreaterThan(T left, T right) {
        if (left == null || right == null) {
            return null;
        }
        return left.compareTo(right) > 0;
    }

    public static Boolean isLessThanOrEqual(Double left, Long right) {
        return !isGreaterThan(left, right);
    }

    public static Boolean isLessThanOrEqual(Long left, Double right) {
        return !isGreaterThan(left, right);
    }

    public static <T extends Comparable<T>> Boolean isLessThanOrEqual(T left, T right) {
        return !isGreaterThan(left, right);
    }

    public static Boolean isGreaterThanOrEqual(Double left, Long right) {
        return !isLessThan(left, right);
    }

    public static Boolean isGreaterThanOrEqual(Long left, Double right) {
        return !isLessThan(left, right);
    }

    public static <T extends Comparable<T>> Boolean isGreaterThanOrEqual(T left, T right) {
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
        try {
            Token type = ((TerminalNode) ctx.op.getChild(0)).getSymbol();
            var leftExpression = exprVisitor.visit(ctx.left);
            List<ResolvableExpression> parameters = List.of(
                    leftExpression,
                    exprVisitor.visit(ctx.right));
            // As long as both types return Comparable<TYPE>.

            // If a parameter is the null token
            if (parameters.stream().map(TypedExpression::getType).anyMatch(Object.class::equals)) {
                return ResolvableExpression
                        .withType(Boolean.class)
                        .withPosition(fromContext(ctx))
                        .using(c -> null);
            }

            if (Comparable.class.isAssignableFrom(leftExpression.getType())) {
                switch (type.getType()) {
                    case VtlParser.EQ:
                        return genericFunctionsVisitor.invokeFunction("isEqual", parameters, fromContext(ctx));
                    case VtlParser.NEQ:
                        return genericFunctionsVisitor.invokeFunction("isNotEqual", parameters, fromContext(ctx));
                    case VtlParser.LT:
                        return genericFunctionsVisitor.invokeFunction("isLessThan", parameters, fromContext(ctx));
                    case VtlParser.MT:
                        return genericFunctionsVisitor.invokeFunction("isGreaterThan", parameters, fromContext(ctx));
                    case VtlParser.LE:
                        return genericFunctionsVisitor.invokeFunction("isLessThanOrEqual", parameters, fromContext(ctx));
                    case VtlParser.ME:
                        return genericFunctionsVisitor.invokeFunction("isGreaterThanOrEqual", parameters, fromContext(ctx));
                    default:
                        throw new UnsupportedOperationException(UNKNOWN_OPERATOR + ctx);
                }
            } else {
                throw new Error("type " + leftExpression.getType() + " must implement Comparable");
            }
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
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
                return ResolvableExpression.withType(Boolean.class).withPosition(pos).using(context -> {
                    List<?> list = listExpression.resolve(context);
                    Object value = operand.resolve(context);
                    return list.contains(value);
                });
            case VtlParser.NOT_IN:
                return ResolvableExpression.withType(Boolean.class).withPosition(pos).using(context -> {
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

        var pos = fromContext(ctx);

        if (types.size() > 1) {
            throw new VtlRuntimeException(
                    new ConflictingTypesException(types, pos)
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

        return ListExpression.withContainedType(values, type, pos);
    }
}
