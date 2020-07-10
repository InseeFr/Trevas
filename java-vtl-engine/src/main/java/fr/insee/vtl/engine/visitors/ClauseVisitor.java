package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.visitors.component.ComponentExpressionVisitor;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
            public Dataset resolve(Map<String, Object> context) {
                Dataset resolve = datasetExpression.resolve(context);
                List<Map<String, Object>> result = resolve.stream()
                        .filter(map -> {
                            return (Boolean) filter.resolve(map);
                        }).collect(Collectors.toList());

                Map<String, Role> roles = datasetExpression.getColumns().stream()
                        .collect(Collectors.toMap(col -> col, datasetExpression::getRole));

                Map<String, Class<?>> types = datasetExpression.getColumns().stream()
                        .collect(Collectors.toMap(col -> col, datasetExpression::getType));

                return new InMemoryDataset(result, types, roles);
            }

            @Override
            public Set<String> getColumns() {
                return datasetExpression.getColumns();
            }

            @Override
            public Class<?> getType(String col) {
                return datasetExpression.getType(col);
            }

            @Override
            public Role getRole(String col) {
                return datasetExpression.getRole(col);
            }

            @Override
            public int getIndex(String col) {
                return datasetExpression.getIndex(col);
            }
        };
    }
}
