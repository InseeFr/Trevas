package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.ConflictingTypesException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.ListExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.TypedExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Date;
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

    private static final String unknownOperator = "unknown operator ";
    private final ExpressionVisitor exprVisitor;
    private final GenericFunctionsVisitor genericFunctionsVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor the parent expression visitor.
     */
    public ComparisonVisitor(ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
        this.genericFunctionsVisitor = genericFunctionsVisitor;
    }

    private static Integer compare(Object left, Object right) throws Exception {
        if (left == null || right == null) {
            return null;
        }
        if (left instanceof Number number && right instanceof Number number1) {
            if (left instanceof Long long1 && right instanceof Long long2) {
                return Long.compare(long1.longValue(), long2.longValue());
            }
            return Double.compare(number.doubleValue(), number1.doubleValue());
        }
        if (left instanceof Boolean boolean1 && right instanceof Boolean boolean2) {
            return Boolean.compare(boolean1, boolean2);
        }
        if (left instanceof String string && right instanceof String string1) {
            return string.compareTo(string1);
        }
        if (left instanceof Date date && right instanceof Date date1) {
            return date.compareTo(date1);
        } else {
            throw new Exception("Comparisons require Comparable params");
        }
    }

    public static Boolean isEqual(Object left, Object right) throws Exception {
        Integer compare = compare(left, right);
        if (compare == null) {
            return null;
        }
        return compare == 0;
    }

    public static Boolean isNotEqual(Object left, Object right) throws Exception {
        Integer compare = compare(left, right);
        if (compare == null) {
            return null;
        }
        return compare != 0;
    }

    public static Boolean isLessThan(Object left, Object right) throws Exception {
        Integer compare = compare(left, right);
        if (compare == null) {
            return null;
        }
        return compare < 0;
    }

    public static Boolean isGreaterThan(Object left, Object right) throws Exception {
        Integer compare = compare(left, right);
        if (compare == null) {
            return null;
        }
        return compare > 0;
    }

    public static Boolean isLessThanOrEqual(Object left, Object right) throws Exception {
        Integer compare = compare(left, right);
        if (compare == null) {
            return null;
        }
        return compare <= 0;
    }

    public static Boolean isGreaterThanOrEqual(Object left, Object right) throws Exception {
        Integer compare = compare(left, right);
        if (compare == null) {
            return null;
        }
        return compare >= 0;
    }

    public static Boolean in(Object obj, List<?> list) {
        if (obj == null) {
            return null;
        }
        return list.contains(obj);
    }

    public static Boolean notIn(Object obj, List<?> list) {
        if (obj == null) {
            return null;
        }
        return !list.contains(obj);
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
            // If a parameter is the null token
            if (parameters.stream().map(TypedExpression::getType).anyMatch(Object.class::equals)) {
                return ResolvableExpression
                        .withType(Boolean.class)
                        .withPosition(fromContext(ctx))
                        .using(c -> null);
            }
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
                    throw new UnsupportedOperationException(unknownOperator + ctx);
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
        try {
            List<ResolvableExpression> parameters = List.of(
                    exprVisitor.visit(ctx.left),
                    visit(ctx.lists()));
            Positioned pos = fromContext(ctx);

            switch (ctx.op.getType()) {
                case VtlParser.IN:
                    return genericFunctionsVisitor.invokeFunction("in", parameters, pos);
                case VtlParser.NOT_IN:
                    return genericFunctionsVisitor.invokeFunction("notIn", parameters, pos);
                default:
                    throw new IllegalStateException("Unexpected value: " + ctx.op.getType());
            }
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
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
