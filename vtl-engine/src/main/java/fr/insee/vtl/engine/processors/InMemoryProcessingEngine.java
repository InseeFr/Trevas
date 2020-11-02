package fr.insee.vtl.engine.processors;

import fr.insee.vtl.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InMemoryProcessingEngine implements ProcessingEngine {

    @Override
    public DatasetExpression executeCalc(DatasetExpression expression, Map<String, ResolvableExpression> expressions,
                                         Map<String, Dataset.Role> roles) {

        var structure = new ArrayList<>(expression.getDataStructure().values());

        for (String columnName : expressions.keySet()) {
            // We construct a new structure
            structure.add(new Dataset.Component(columnName, expressions.get(columnName).getType(),
                    roles.get(columnName)));
        }

        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                var dataset = expression.resolve(context);
                var columns = getColumnNames();
                List<List<Object>> result = dataset.getDataAsMap().stream().map(map -> {
                    var newMap = new HashMap<>(map);
                    for (String columnName : expressions.keySet()) {
                        newMap.put(columnName, expressions.get(columnName).resolve(newMap));
                    }
                    return newMap;
                }).map(map -> Dataset.mapToRowMajor(map, columns)).collect(Collectors.toList());
                return new InMemoryDataset(result, structure);
            }

            @Override
            public DataStructure getDataStructure() {
                return new DataStructure(structure);
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
                List<String> columns = resolve.getColumnNames();
                List<List<Object>> result = resolve.getDataAsMap().stream()
                        .filter(map -> (Boolean) filter.resolve(map))
                        .map(map -> Dataset.mapToRowMajor(map, columns))
                        .collect(Collectors.toList());
                return new InMemoryDataset(result, getDataStructure());
            }

        };
    }

    @Override
    public DatasetExpression executeRename(DatasetExpression expression, Map<String, String> fromTo) {
        var structure = expression.getDataStructure().values().stream().map(component -> {
            return !fromTo.containsKey(component.getName()) ?
                    component :
                    new Dataset.Component(fromTo.get(component.getName()), component.getType(), component.getRole());
        }).collect(Collectors.toList());

        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                var result = expression.resolve(context).getDataAsMap().stream()
                        .map(map -> {
                            var newMap = new HashMap<>(map);
                            for (String fromName : fromTo.keySet()) {
                                newMap.remove(fromName);
                            }
                            for (String fromName : fromTo.keySet()) {
                                var toName = fromTo.get(fromName);
                                newMap.put(toName, map.get(fromName));
                            }
                            return newMap;
                        }).map(map -> Dataset.mapToRowMajor(map, getColumnNames()))
                        .collect(Collectors.toList());
                return new InMemoryDataset(result, getDataStructure());
            }

            @Override
            public DataStructure getDataStructure() {
                return new DataStructure(structure);
            }
        };
    }

    @Override
    public DatasetExpression executeProject(DatasetExpression expression, List<String> columnNames) {

        var structure = expression.getDataStructure().values().stream()
                .filter(component -> columnNames.contains(component.getName()))
                .collect(Collectors.toList());

        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                var columnNames = getColumnNames();
                List<List<Object>> result = expression.resolve(context).getDataAsMap().stream()
                        .map(data -> data.entrySet().stream().filter(entry -> columnNames.contains(entry.getKey()))
                                .collect(HashMap<String, Object>::new, (acc, entry) -> acc.put(entry.getKey(), entry.getValue()), HashMap::putAll))
                        .map(map -> Dataset.mapToRowMajor(map, getColumnNames())).collect(Collectors.toList());
                return new InMemoryDataset(result, getDataStructure());
            }

            @Override
            public DataStructure getDataStructure() {
                return new DataStructure(structure);
            }
        };
    }
}
