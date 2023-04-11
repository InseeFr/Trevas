package fr.insee.vtl.engine.expressions;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class DatasetFunctionExpression extends DatasetExpression {

    private final DatasetExpression operand;
    private final Map<Component, ResolvableExpression> expressions;
    private final DataStructure structure;

    public DatasetFunctionExpression(Method method, DatasetExpression operand, Positioned position) throws VtlScriptException {
        super(position);
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

    private Map<Component, ResolvableExpression> createExpressionMap(Method method, DatasetExpression operand, Class<?> parameterType) throws VtlScriptException {
        // TODO: test with function that changes the type.
        Map<Component, ResolvableExpression> parametersMap = new LinkedHashMap<>();
        for (Component component : operand.getDataStructure().values()) {
            if (!component.isMeasure() || !parameterType.isAssignableFrom(component.getType())) {
                continue;
            }
            List<ResolvableExpression> parameters = List.of(new ComponentExpression(component, operand));
            parametersMap.put(component, new FunctionExpression(method, parameters, this));
        }
        return Map.copyOf(parametersMap);
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
