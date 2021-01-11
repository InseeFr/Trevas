package fr.insee.vtl.engine.processors;

import fr.insee.vtl.engine.utils.MapCollector;
import fr.insee.vtl.model.*;

import javax.script.ScriptEngine;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    @Override
    public DatasetExpression executeUnion(List<DatasetExpression> datasets) {
        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                Stream<DataPoint> stream = Stream.empty();
                for (DatasetExpression datasetExpression : datasets) {
                    var dataset = datasetExpression.resolve(context);
                    stream = Stream.concat(stream, dataset.getDataPoints().stream());
                }
                List<DataPoint> data = stream.distinct().collect(Collectors.toList());
                return new InMemoryDataset(data, getDataStructure());
            }

            @Override
            public DataStructure getDataStructure() {
                return (datasets.get(0)).getDataStructure();
            }
        };
    }

    @Override
    public DatasetExpression executeAggr(DatasetExpression expression, Structured.DataStructure structure,
                                         Map<String, AggregationExpression> collectorMap,
                                         Function<Structured.DataPoint, Map<String, Object>> keyExtractor) {
        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {

                List<DataPoint> data = expression.resolve(Map.of()).getDataPoints();
                MapCollector collector = new MapCollector(structure, collectorMap);
                List<DataPoint> collect = data.stream()
                        .collect(Collectors.groupingBy(keyExtractor, collector))
                        .entrySet().stream()
                        .map(e -> {
                            DataPoint dataPoint = e.getValue();
                            Map<String, Object> identifiers = e.getKey();
                            for (Map.Entry<String, Object> identifierElement : identifiers.entrySet()) {
                                dataPoint.set(identifierElement.getKey(), identifierElement.getValue());
                            }
                            return dataPoint;
                        }).collect(Collectors.toList());

                return new InMemoryDataset(collect, structure);
            }

            @Override
            public DataStructure getDataStructure() {
                return structure;
            }
        };
    }

    @Override
    public DatasetExpression executeLeftJoin(Map<String, DatasetExpression> datasets, List<Structured.Component> components) {
        var iterator = datasets.values().iterator();
        var leftMost = iterator.next();
        while (iterator.hasNext()) {
            leftMost = handleLeftJoin(components, leftMost, iterator.next());
        }
        return leftMost;
    }

    private DatasetExpression handleLeftJoin(List<Structured.Component> identifiers, DatasetExpression left, DatasetExpression right) {
        // Create common structure
        List<Structured.Component> components = new ArrayList<>(identifiers);
        for (Structured.Component component : left.getDataStructure().values()) {
            if (!identifiers.contains(component)) {
                components.add(component);
            }
        }
        for (Structured.Component component : right.getDataStructure().values()) {
            if (!identifiers.contains(component)) {
                components.add(component);
            }
        }

        var structure = new Structured.DataStructure(components);

        // Predicate for the join.
        Comparator<Structured.DataPoint> predicate = (dl, dr) -> {
            for (Structured.Component identifier : identifiers) {
                if (!Objects.equals(dl.get(identifier.getName()), dr.get(identifier.getName()))) {
                    return -1;
                }
            }
            return 0;
        };

        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                var leftPoints = left.resolve(context).getDataPoints();
                var rightPoints = right.resolve(context).getDataPoints();
                List<DataPoint> result = new ArrayList<>();
                for (DataPoint leftPoint : leftPoints) {
                    List<DataPoint> matches = new ArrayList<>();
                    for (DataPoint rightPoint : rightPoints) {
                        // Check equality
                        if (predicate.compare(leftPoint, rightPoint) == 0) {
                            matches.add(rightPoint);
                        }
                    }

                    // Create merge datapoint.
                    var mergedPoint = new DataPoint(structure);
                    for (String leftColumn : left.getDataStructure().keySet()) {
                        mergedPoint.set(leftColumn, leftPoint.get(leftColumn));
                    }

                    if (matches.isEmpty()) {
                        result.add(mergedPoint);
                    } else {
                        for (DataPoint match : matches) {
                            var matchPoint = new DataPoint(structure, mergedPoint);
                            for (String rightColumn : right.getDataStructure().keySet()) {
                                matchPoint.set(rightColumn, match.get(rightColumn));
                            }
                            result.add(matchPoint);
                        }
                    }
                }
                return new InMemoryDataset(result, structure);
            }

            @Override
            public DataStructure getDataStructure() {
                return structure;
            }
        };
    }

    public static class Factory implements ProcessingEngineFactory {

        @Override
        public String getName() {
            return "memory";
        }

        @Override
        public ProcessingEngine getProcessingEngine(ScriptEngine engine) {
            return new InMemoryProcessingEngine();
        }
    }
}
