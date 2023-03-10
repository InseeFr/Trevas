package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.UnimplementedException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.BooleanExpression;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.DoubleExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.InstantExpression;
import fr.insee.vtl.model.LongExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.StringExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static fr.insee.vtl.engine.exceptions.VtlScriptException.fromContext;

/**
 * <code>GenericFunctionsVisitor</code> is the base visitor for cast expressions.
 */
public class GenericFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final VtlScriptEngine engine;
    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor The visitor for the enclosing expression.
     * @param engine            The {@link VtlScriptEngine}.
     */
    public GenericFunctionsVisitor(ExpressionVisitor expressionVisitor, VtlScriptEngine engine) {
        this.engine = Objects.requireNonNull(engine);
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

    // TODO: Extract to model.
    public static class FunctionExpression implements ResolvableExpression {

        private final Method method;
        private final List<ResolvableExpression> parameters;

        public FunctionExpression(Method method, List<ResolvableExpression> parameters) {
            this.method = Objects.requireNonNull(method);

            // Type check.
            // TODO: Add context.
            Class<?>[] methodParameterTypes = this.method.getParameterTypes();
            if (parameters.size() < methodParameterTypes.length) {
                throw new RuntimeException("too many parameters");
            } else if (parameters.size() > methodParameterTypes.length) {
                throw new RuntimeException("missing parameters");
            }
            for (int i = 0; i < parameters.size(); i++) {
                if (parameters.get(i).getType().equals(Object.class)) {
                    continue;
                }
                if (!methodParameterTypes[i].isAssignableFrom(parameters.get(i).getType())) {
                    throw new RuntimeException(String.format("invalid parameter type %s, need %s",
                            parameters.get(i).getType(),
                            methodParameterTypes[i])
                    );
                }
            }

            this.parameters = Objects.requireNonNull(parameters);
        }

        @Override
        public Object resolve(Map<String, Object> context) {
            Object[] evaluatedParameters = parameters.stream().map(p -> p.resolve(context)).toArray();
            try {
                return method.invoke(null, evaluatedParameters);
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof RuntimeException) {
                    throw  (RuntimeException) e.getCause();
                } else {
                    throw new RuntimeException(e.getCause());
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Class<?> getType() {
            return method.getReturnType();
        }
    }

    public static class DatasetFunctionExpression extends DatasetExpression {

        private final DatasetExpression operand;
        private final Map<Component, ResolvableExpression> expressions;
        private final DataStructure structure;

        public DatasetFunctionExpression(Method method, DatasetExpression operand) {
            // TODO: Check that method is serializable.
            Objects.requireNonNull(method);
            this.operand = Objects.requireNonNull(operand);

            if (method.getParameterTypes().length != 1) {
                throw new RuntimeException("only supports unary operators");
            }
            Class<?> parameterType = method.getParameterTypes()[0];

            // TODO: Empty expression should be an error
            this.expressions = createExpressionMap(method, operand, parameterType);

            List<Component> components = new ArrayList<>();
            for (Component component : operand.getDataStructure().values()) {
                if (expressions.containsKey(component)) {
                    components.add(new Component(
                            component.getName(),
                            expressions.get(component).getType(),
                            component.getRole()
                    ));
                } else {
                    components.add(component);
                }
            }
            this.structure = new DataStructure(components);

        }

        @NotNull
        private Map<Component, ResolvableExpression> createExpressionMap(Method method, DatasetExpression operand, Class<?> parameterType) {
            // TODO: test with function that changes the type.
            Map<Component, ResolvableExpression> parameters = new LinkedHashMap<>();
            for (Component component : operand.getDataStructure().values()) {
                if (!component.isMeasure() || !parameterType.isAssignableFrom(component.getType())) {
                    continue;
                }
                parameters.put(component, new GenericFunctionsVisitor.FunctionExpression(method, List.of(new ComponentExpression(component))));
            }
            return Map.copyOf(parameters);
        }

        @Override
        public Dataset resolve(Map<String, Object> context) {
            var dataset = operand.resolve(context);
            List<List<Object>> result = dataset.getDataPoints().stream().map(dataPoint -> {
                var newDataPoint = new DataPoint(getDataStructure(), dataPoint);
                for (Component component : expressions.keySet()) {
                    newDataPoint.set(component.getName(), expressions.get(component).resolve(dataPoint));
                }
                return newDataPoint;
            }).collect(Collectors.toList());
            return new InMemoryDataset(result, getDataStructure());
        }

        @Override
        public DataStructure getDataStructure() {
            return this.structure;
        }
    }

    // TODO: Extract to model
    // TODO: Check that we don't already have something like that.
    public static class ComponentExpression implements ResolvableExpression {

        private final Structured.Component component;

        public ComponentExpression(Structured.Component component) {
            this.component = Objects.requireNonNull(component);
        }

        @Override
        public Object resolve(Map<String, Object> context) {
            return context.get(component.getName());
        }

        @Override
        public Class<?> getType() {
            return component.getType();
        }
    }

    public ResolvableExpression invoke(String methodName, List<ResolvableExpression> parameter) {
        Method method = engine.findMethod(methodName).orElseThrow();
        return invokeFunction(method, parameter);
    }
    
    private ResolvableExpression invokeFunction(Method method, List<ResolvableExpression> parameters) {
        var parameterTypes = parameters.stream().map(ResolvableExpression::getType).collect(Collectors.toList());
        // TODO: Method with more that one dataset parameters.
        //          ie: parameterTypes.contains(Dataset.class)
        if (parameterTypes.size() == 1 && parameterTypes.get(0).equals(Dataset.class)) {
            return new DatasetFunctionExpression(method, (DatasetExpression) parameters.get(0));
        } else {
            return new FunctionExpression(method, parameters);
        }
    }


    @Override
    public ResolvableExpression visitCallDataset(VtlParser.CallDatasetContext ctx) {
        // Strange name, this is the generic function syntax; fnName ( param, * ).
        // TODO: Use parameters to find functions so we can override them.
        Method method = engine.findMethod(ctx.operatorID().getText()).orElseThrow(() -> {
            throw new VtlRuntimeException(new UnimplementedException("could not find function", fromContext(ctx.operatorID())));
        });
        List<ResolvableExpression> parameters = ctx.parameter().stream().map(exprVisitor::visit).collect(Collectors.toList());
        return invokeFunction(method, parameters);
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
            return StringExpression.castTo(expression, outputClass, mask);
            // Antlr context is not serializable
            // TODO: Find a way to make ctx serializable
            //        .handleException(NumberFormatException.class, nfe -> new VtlRuntimeException(
            //                new InvalidArgumentException("cannot cast to number: " + nfe.getMessage(), ctx)));
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
                throw new VtlRuntimeException(new InvalidArgumentException("cannot cast date: no mask specified", fromContext(ctx)));
            }
            return InstantExpression.castTo(expression, outputClass, mask);
        }
        throw new UnsupportedOperationException("cast unsupported on expression of type: " + expression.getType());
    }

}
