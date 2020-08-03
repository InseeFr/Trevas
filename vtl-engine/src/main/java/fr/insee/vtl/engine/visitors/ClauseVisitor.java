package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.visitors.component.ComponentExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ClauseVisitor extends VtlBaseVisitor<DatasetExpression> {

    private final DatasetExpression datasetExpression;
    private final ComponentExpressionVisitor componentExpressionVisitor;

    public ClauseVisitor(DatasetExpression datasetExpression) {
        this.datasetExpression = Objects.requireNonNull(datasetExpression);
        this.componentExpressionVisitor = new ComponentExpressionVisitor(datasetExpression);
    }

    @Override
    public DatasetExpression visitFilterClause(VtlParser.FilterClauseContext ctx) {
        ResolvableExpression filter = componentExpressionVisitor.visit(ctx.exprComponent());

        return new DatasetExpression() {

            @Override
            public List<Dataset.Component> getDataStructure() {
                return datasetExpression.getDataStructure();
            }

            @Override
            public Dataset resolve(Map<String, Object> context) {
                Dataset resolve = datasetExpression.resolve(context);
                List<String> columns = resolve.getColumns();
                List<List<Object>> result = resolve.getDataAsMap().stream()
                        .filter(map -> (Boolean) filter.resolve(map))
                        .map(map -> Dataset.mapToRowMajor(map, columns))
                        .collect(Collectors.toList());
                return new InMemoryDataset(result, getDataStructure());
            }
        };
    }
}
