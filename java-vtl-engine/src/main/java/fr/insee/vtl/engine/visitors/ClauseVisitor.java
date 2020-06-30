package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.visitors.component.ComponentExpressionVisitor;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.DatasetWrapper;
import fr.insee.vtl.model.InMemoryDatasetWrapper;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;
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
            public DatasetWrapper resolve(ScriptContext context) {
                return resolve(context.getBindings(ScriptContext.ENGINE_SCOPE));
            }

            @Override
            public DatasetWrapper resolve(Map<String, Object> context) {
                DatasetWrapper resolve = datasetExpression.resolve(context);


                List<Map<String, Object>> result = resolve.stream()
                        .filter(map -> {
                            return (Boolean) filter.resolve(map);
                        }).collect(Collectors.toList());

                return new InMemoryDatasetWrapper(result);

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
            public int getIndex(String col) {
                return datasetExpression.getIndex(col);
            }
        };
    }
}
