package fr.insee.vtl.engine.visitors.expression;

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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;

/**
 * <code>ComparisonVisitor</code> is the base visitor for comparison, 'element of' and list expressions.
 */
public class ComparisonVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking a scripting context.
     *
     * @param expressionVisitor the parent expression visitor.
     */
    public ComparisonVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
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
        ResolvableExpression rightExpression = assertTypeExpression(
                exprVisitor.visit(ctx.right),
                leftExpression.getType(),
                ctx.right);

        // Get the type of the Token.
        // TODO(hadrien): Reported to ANTLR: https://github.com/antlr/antlr4/issues/2862
        Token type = ((TerminalNode) ctx.op.getChild(0)).getSymbol();

        switch (type.getType()) {
            case VtlParser.EQ:
                return handleEqual(leftExpression, rightExpression);
            case VtlParser.NEQ:
                return handleNotEqual(leftExpression, rightExpression);
            case VtlParser.LT:
                if (TypeChecking.isLong(leftExpression) && TypeChecking.isLong(rightExpression)) {
                    return BooleanExpression.of(context -> {
                        Long leftValue = (Long) leftExpression.resolve(context);
                        Long rightValue = (Long) rightExpression.resolve(context);
                        return leftValue < rightValue;
                    });
                } else if (TypeChecking.isDouble(leftExpression) && TypeChecking.isDouble(rightExpression)) {
                    return BooleanExpression.of(context -> {
                        Double leftValue = (Double) leftExpression.resolve(context);
                        Double rightValue = (Double) rightExpression.resolve(context);
                        return leftValue < rightValue;
                    });
                } else {
                    throw new VtlRuntimeException(
                            new InvalidTypeException(leftExpression.getType(), rightExpression.getType(), ctx.right)
                    );
                }
            case VtlParser.MT:
                if (TypeChecking.isLong(leftExpression) && TypeChecking.isLong(rightExpression)) {
                    return BooleanExpression.of(context -> {
                        Long leftValue = (Long) leftExpression.resolve(context);
                        Long rightValue = (Long) rightExpression.resolve(context);
                        return leftValue > rightValue;
                    });

                } else if (TypeChecking.isDouble(leftExpression) && TypeChecking.isDouble(rightExpression)) {
                    return BooleanExpression.of(context -> {
                        Double leftValue = (Double) leftExpression.resolve(context);
                        Double rightValue = (Double) rightExpression.resolve(context);
                        return leftValue > rightValue;
                    });
                } else {
                    throw new VtlRuntimeException(
                            new InvalidTypeException(leftExpression.getType(), rightExpression.getType(), ctx.right)
                    );
                }
            case VtlParser.LE:
                if (TypeChecking.isLong(leftExpression) && TypeChecking.isLong(rightExpression)) {
                    return BooleanExpression.of(context -> {
                        Long leftValue = (Long) leftExpression.resolve(context);
                        Long rightValue = (Long) rightExpression.resolve(context);
                        return leftValue <= rightValue;
                    });
                } else if (TypeChecking.isDouble(leftExpression) && TypeChecking.isDouble(rightExpression)) {
                    return BooleanExpression.of(context -> {
                        Double leftValue = (Double) leftExpression.resolve(context);
                        Double rightValue = (Double) rightExpression.resolve(context);
                        return leftValue <= rightValue;
                    });
                } else {
                    throw new VtlRuntimeException(
                            new InvalidTypeException(leftExpression.getType(), rightExpression.getType(), ctx.right)
                    );
                }
            case VtlParser.ME:
                if (TypeChecking.isLong(leftExpression) && TypeChecking.isLong(rightExpression)) {
                    return BooleanExpression.of(context -> {
                        Long leftValue = (Long) leftExpression.resolve(context);
                        Long rightValue = (Long) rightExpression.resolve(context);
                        return leftValue >= rightValue;
                    });
                } else if (TypeChecking.isDouble(leftExpression) && TypeChecking.isDouble(rightExpression)) {
                    return BooleanExpression.of(context -> {
                        Double leftValue = (Double) leftExpression.resolve(context);
                        Double rightValue = (Double) rightExpression.resolve(context);
                        return leftValue >= rightValue;
                    });
                } else {
                    throw new VtlRuntimeException(
                            new InvalidTypeException(leftExpression.getType(), rightExpression.getType(), ctx.right)
                    );
                }
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }

    private ResolvableExpression handleEqual(ResolvableExpression left, ResolvableExpression right) {
        return BooleanExpression.of(context -> {
            Object leftValue = left.resolve(context);
            Object rightValue = right.resolve(context);
            return leftValue.equals(rightValue);
        });
    }

    private ResolvableExpression handleNotEqual(ResolvableExpression left, ResolvableExpression right) {
        return BooleanExpression.of(context -> {
            Object leftValue = left.resolve(context);
            Object rightValue = right.resolve(context);
            return !leftValue.equals(rightValue);
        });
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
