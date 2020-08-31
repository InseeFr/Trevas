package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.*;
import java.util.stream.Collectors;

public class ClauseVisitor extends VtlBaseVisitor<DatasetExpression> {

    private final DatasetExpression datasetExpression;
    private final ExpressionVisitor componentExpressionVisitor;

    public ClauseVisitor(DatasetExpression datasetExpression) {
        this.datasetExpression = Objects.requireNonNull(datasetExpression);
        // Here we "switch" to the dataset context.
        Map<String, Object> componentMap = datasetExpression.getDataStructure().stream()
                .collect(Collectors.toMap(Dataset.Component::getName, component -> component));
        this.componentExpressionVisitor = new ExpressionVisitor(componentMap);
    }

    private String getName(VtlParser.ComponentIDContext context) {
        // TODO: Should be an expression so we can handle membership better and use the exceptions
        //  for undefined var etc.
        return context.getText();
    }

    @Override
    public DatasetExpression visitKeepOrDropClause(VtlParser.KeepOrDropClauseContext ctx) {

        // Normalize to keep operation.
        var keep = ctx.op.getType() == VtlParser.KEEP;
        var componentNames = ctx.componentID().stream().map(this::getName).collect(Collectors.toSet());
        var structure = datasetExpression.getDataStructure().stream()
                .filter(component -> keep == componentNames.contains(component.getName()))
                .collect(Collectors.toList());

        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                var columnNames = getColumnNames();
                List<List<Object>> result = datasetExpression.resolve(context).getDataAsMap().stream()
                        .map(data -> {
                            return data.entrySet().stream().filter(entry -> columnNames.contains(entry.getKey()))
                                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                        }).map(map -> Dataset.mapToRowMajor(map, getColumnNames())).collect(Collectors.toList());
                return new InMemoryDataset(result, getDataStructure());
            }

            @Override
            public List<Dataset.Component> getDataStructure() {
                return structure;
            }
        };
    }

    @Override
    public DatasetExpression visitCalcClause(VtlParser.CalcClauseContext ctx) {

        var structure = new ArrayList<>(datasetExpression.getDataStructure());
        var expressions = new HashMap<String, ResolvableExpression>();
        for (VtlParser.CalcClauseItemContext calcCtx : ctx.calcClauseItem()) {


            var columnName = getName(calcCtx.componentID());
            ResolvableExpression calc = componentExpressionVisitor.visit(calcCtx);

            // We construct a new structure
            // TODO: Handle role. Ie: Optional.ofNullable(calcCtx.componentRole());
            structure.add(new Dataset.Component(columnName, calc.getType(), Dataset.Role.MEASURE));

            expressions.put(columnName, calc);
        }

        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                var dataset = datasetExpression.resolve(context);
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
            public List<Dataset.Component> getDataStructure() {
                return structure;
            }
        };

    }

    @Override
    public DatasetExpression visitFilterClause(VtlParser.FilterClauseContext ctx) {
        ResolvableExpression filter = componentExpressionVisitor.visit(ctx.expr());

        return new DatasetExpression() {

            @Override
            public List<Dataset.Component> getDataStructure() {
                return datasetExpression.getDataStructure();
            }

            @Override
            public Dataset resolve(Map<String, Object> context) {
                Dataset resolve = datasetExpression.resolve(context);
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
    public DatasetExpression visitRenameClause(VtlParser.RenameClauseContext ctx) {
        Map<String, String> fromTo = new LinkedHashMap<>();
        for (VtlParser.RenameClauseItemContext renameCtx : ctx.renameClauseItem()) {
            fromTo.put(getName(renameCtx.fromName), getName(renameCtx.toName));
        }

        var structure = datasetExpression.getDataStructure().stream().map(component -> {
            return !fromTo.containsKey(component.getName()) ?
                    component :
                    new Dataset.Component(fromTo.get(component.getName()), component.getType(), component.getRole());
        }).collect(Collectors.toList());

        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                var result = datasetExpression.resolve(context).getDataAsMap().stream()
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
            public List<Dataset.Component> getDataStructure() {
                return structure;
            }
        };
    }
}
