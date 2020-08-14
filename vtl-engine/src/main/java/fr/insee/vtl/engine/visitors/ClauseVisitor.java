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

    @Override
    public DatasetExpression visitCalcClause(VtlParser.CalcClauseContext ctx) {

        var structure = new ArrayList<>(datasetExpression.getDataStructure());
        var expressions = new HashMap<String, ResolvableExpression>();
        for (VtlParser.CalcClauseItemContext calcCtx : ctx.calcClauseItem()) {

            // TODO: Should be an expression so we can handle membership better.
            var columnName = calcCtx.componentID().getText();
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
}
