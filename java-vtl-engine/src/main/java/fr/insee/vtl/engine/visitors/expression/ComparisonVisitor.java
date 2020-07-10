package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.BooleanExpression;
import fr.insee.vtl.model.ListExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.TypedExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ComparisonVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    public ComparisonVisitor(ScriptContext context) {
        exprVisitor = new ExpressionVisitor(context);
    }

    @Override
    public ResolvableExpression visitComparisonExpr(VtlParser.ComparisonExprContext ctx) {
        ResolvableExpression leftExpression = exprVisitor.visit(ctx.left);
        ResolvableExpression rightExpression = exprVisitor.visit(ctx.right);
        // TODO : improve how to get the type of operand
        switch (ctx.comparisonOperand().getText()) {
            case "=":
                return ResolvableExpression.withType(Boolean.class, context -> {
                    Object leftValue = leftExpression.resolve(context);
                    Object rightValue = rightExpression.resolve(context);
                    return leftValue.equals(rightValue);
                });
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }

    @Override
    public ResolvableExpression visitInNotInExpr(VtlParser.InNotInExprContext ctx) {
        ResolvableExpression operand = exprVisitor.visit(ctx.left);
        ListExpression listExpression = (ListExpression) visit(ctx.lists());

        if (!operand.getType().equals(listExpression.containedType())) {
            // TODO: Define runtime exception.
            // TODO: Inject context in exception:
            throw new RuntimeException(
                    new ScriptException("TODO: incompatible types, expected " + listExpression.containedType())
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
                new BooleanExpression() {
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

    @Override
    public ResolvableExpression visitLists(VtlParser.ListsContext ctx) {

        // Transform all the constants.
        List<ResolvableExpression> listExpressions = ctx.constant().stream()
                .map(exprVisitor::visitConstant)
                .collect(Collectors.toList());

        // Find the type of the list.
        Set<? extends Class<?>> types = listExpressions.stream().map(TypedExpression::getType)
                .collect(Collectors.toSet());

        if (types.size() > 1) {
            // TODO: Define runtime exception.
            // TODO: Inject context in exception:
            throw new RuntimeException(new ScriptException("TODO: incompatible types:"));
        }

        // The grammar defines list with minimum one constant so the types will never
        // be empty.
        Class<?> type = types.iterator().next();

        // Since all expression are constant we don't need any context.
        List<Object> values = listExpressions.stream().map(expression -> {
            return expression.resolve(Map.of());
        }).collect(Collectors.toList());

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
