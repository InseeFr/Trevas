package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.UnimplementedException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.exceptions.VtlScriptException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.script.ScriptContext;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * <code>GenericFunctionsVisitor</code> is the base visitor for cast expressions.
 */
public class GenericFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ScriptContext context;
    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param context
     * @param expressionVisitor The visitor for the enclosing expression.
     */
    public GenericFunctionsVisitor(ExpressionVisitor expressionVisitor, ScriptContext context) {
        this.context = context;
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    /**
     * Method to map basic scalar types and classes.
     *
     * @param basicScalarType Basic scalar type.
     * @param basicScalarText Basic scalar text.
     */
    private static Class<?> getOutputClass(Integer basicScalarType, String basicScalarText) {
        switch (basicScalarType) {
            case VtlParser.STRING:
                return String.class;
            case VtlParser.INTEGER:
                return Long.class;
            case VtlParser.NUMBER:
                return Double.class;
            case VtlParser.BOOLEAN:
                return Boolean.class;
            case VtlParser.DATE:
                return Instant.class;
            default:
                throw new UnsupportedOperationException("basic scalar type " + basicScalarText + " unsupported");
        }
    }

    public Optional<Method> findMethod(String name) {
        // TODO: Cache the methods
        // TODO: Use another scope?
        return context.getBindings(ScriptContext.ENGINE_SCOPE).entrySet().stream()
                .filter(entry -> entry.getValue() instanceof Method)
                .filter(entry -> entry.getKey().startsWith("$vtl.functions."))
                .filter(entry -> entry.getKey().endsWith(name))
                .map(Map.Entry::getValue)
                .map(o -> (Method) o)
                .findFirst();
    }

    @Override
    public ResolvableExpression visitCallDataset(VtlParser.CallDatasetContext ctx) {
        // Strange name, this is the generic function syntax; fnName ( param, * ).

        Method method = findMethod(ctx.operatorID().getText()).orElseThrow(() -> {
            throw new VtlRuntimeException(new UnimplementedException("could not find function", ctx.operatorID()));
        });
        List<ResolvableExpression> parameters = ctx.parameter().stream().map(exprVisitor::visit).collect(Collectors.toList());
        if (method.getReturnType().equals(Dataset.class)) {
            return new DatasetExpression() {

                private Dataset dataset;
                @Override
                public Dataset resolve(Map<String, Object> context) {
                    if (dataset == null) {
                        Object[] evaluatedParameters = parameters.stream().map(p -> p.resolve(context)).toArray();
                        try {
                            dataset = (Dataset) method.invoke(null, evaluatedParameters);
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            throw new VtlRuntimeException(new VtlScriptException(e, ctx));
                        }
                    }
                    return dataset;
                }

                @Override
                public DataStructure getDataStructure() {
                    return null;
                }
            };
        } else {
            return new ResolvableExpression() {

                @Override
                public Class<?> getType() {
                    return method.getReturnType();
                }

                @Override
                public Object resolve(Map<String, Object> context) {
                    Object[] evaluatedParameters = parameters.stream().map(p -> p.resolve(context)).toArray();
                    try {
                        return method.invoke(null, evaluatedParameters);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new VtlRuntimeException(new VtlScriptException(e, ctx));
                    }
                }
            };
        }

    }

    /**
     * Visits expressions with cast operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the cast operation.
     */
    @Override
    public ResolvableExpression visitCastExprDataset(VtlParser.CastExprDatasetContext ctx) {
        ResolvableExpression expression = exprVisitor.visit(ctx.expr());
        TerminalNode maskNode = ctx.STRING_CONSTANT();
        // STRING_CONSTANT().getText return null or a string wrapped by quotes
        String mask = maskNode == null ? null :
                maskNode.getText()
                        .replaceAll("\"", "")
                        .replace("YYYY", "yyyy")
                        .replace("DD", "dd");
        Token symbol = ((TerminalNode) ctx.basicScalarType().getChild(0)).getSymbol();
        Integer basicScalarType = symbol.getType();
        String basicScalarText = symbol.getText();

        Class<?> outputClass = getOutputClass(basicScalarType, basicScalarText);

        if (Object.class.equals(expression.getType())) {
            return ResolvableExpression.ofType(outputClass, null);
        }
        if (String.class.equals(expression.getType())) {
            return StringExpression.castTo(expression, outputClass, mask)
                    .handleException(NumberFormatException.class, nfe -> new VtlRuntimeException(
                            new InvalidArgumentException("cannot cast to number: " + nfe.getMessage(), ctx)));
        }
        if (Boolean.class.equals(expression.getType())) {
            return BooleanExpression.castTo(expression, outputClass);
        }
        if (Long.class.equals(expression.getType())) {
            return LongExpression.castTo(expression, outputClass);
        }
        if (Double.class.equals(expression.getType())) {
            return DoubleExpression.castTo(expression, outputClass);
        }
        if (Instant.class.equals(expression.getType())) {
            if (mask == null || mask.isEmpty()) {
                throw new VtlRuntimeException(new InvalidArgumentException("cannot cast date: no mask specified", ctx));
            }
            return InstantExpression.castTo(expression, outputClass, mask);
        }
        throw new UnsupportedOperationException("cast unsupported on expression of type: " + expression.getType());
    }

}
