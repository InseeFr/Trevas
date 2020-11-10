package fr.insee.vtl.engine.processors;

import fr.insee.vtl.model.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InMemoryProcessingEngine implements ProcessingEngine {

    @Override
    public DatasetExpression executeCalc(DatasetExpression expression, Map<String, ResolvableExpression> expressions,
                                         Map<String, Dataset.Role> roles) {

        // Copy the structure and mutate based on the expressions.
        var newStructure = new Structured.DataStructure(expression.getDataStructure());
        for (String columnName : expressions.keySet()) {
            newStructure.put(columnName, new Dataset.Component(
                    columnName,
                    expressions.get(columnName).getType(),
                    roles.get(columnName))
            );
        }

        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                var dataset = expression.resolve(context);
                List<List<Object>> result = dataset.getDataPoints().stream().map(dataPoint -> {
                    var newDataPoint = new DataPoint(newStructure, dataPoint);
                    for (String columnName : expressions.keySet()) {
                        newDataPoint.set(columnName, expressions.get(columnName).resolve(dataPoint));
                    }
                    return newDataPoint;
                }).collect(Collectors.toList());
                return new InMemoryDataset(result, newStructure);
            }

            @Override
            public DataStructure getDataStructure() {
                return newStructure;
            }
        };

    }

    @Override
    public DatasetExpression executeFilter(DatasetExpression expression, ResolvableExpression filter) {
        return new DatasetExpression() {

            @Override
            public DataStructure getDataStructure() {
                return expression.getDataStructure();
            }

            @Override
            public Dataset resolve(Map<String, Object> context) {
                Dataset resolve = expression.resolve(context);
                List<List<Object>> result = resolve.getDataPoints().stream()
                        .filter(map -> (Boolean) filter.resolve(map))
                        .collect(Collectors.toList());
                return new InMemoryDataset(result, getDataStructure());
            }

        };
    }

    @Override
    public DatasetExpression executeRename(DatasetExpression expression, Map<String, String> fromTo) {
        var structure = expression.getDataStructure().values().stream()
                .map(component ->
                        !fromTo.containsKey(component.getName())
                                ? component
                                : new Dataset.Component(
                                fromTo.get(component.getName()),
                                component.getType(),
                                component.getRole())
                ).collect(Collectors.toList());
        Structured.DataStructure renamedStructure = new Structured.DataStructure(structure);
        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                var result = expression.resolve(context).getDataPoints().stream()
                        .map(dataPoint -> {
                            var newDataPoint = new DataPoint(renamedStructure, dataPoint);
                            for (String fromName : fromTo.keySet()) {
                                var toName = fromTo.get(fromName);
                                newDataPoint.set(toName, dataPoint.get(fromName));
                            }
                            return newDataPoint;
                        }).collect(Collectors.toList());
                return new InMemoryDataset(result, getDataStructure());
            }

            @Override
            public DataStructure getDataStructure() {
                return renamedStructure;
            }
        };
    }

    @Override
    public DatasetExpression executeProject(DatasetExpression expression, List<String> columnNames) {

        var structure = expression.getDataStructure().values().stream()
                .filter(component -> columnNames.contains(component.getName()))
                .collect(Collectors.toList());
        var newStructure = new Structured.DataStructure(structure);

        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                var columnNames = getColumnNames();
                List<List<Object>> result = expression.resolve(context).getDataPoints().stream()
                        .map(data -> {
                            var projectedDataPoint = new DataPoint(newStructure);
                            for (String column : columnNames) {
                                projectedDataPoint.set(column, data.get(column));
                            }
                            return projectedDataPoint;
                        }).collect(Collectors.toList());
                return new InMemoryDataset(result, getDataStructure());
            }

            @Override
            public DataStructure getDataStructure() {
                return newStructure;
            }
        };
    }
}
