package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.exceptions.VtlScriptException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static fr.insee.vtl.engine.utils.TypeChecking.assertNumber;

public class ClauseVisitor extends VtlBaseVisitor<DatasetExpression> {

    private final DatasetExpression datasetExpression;
    private final ExpressionVisitor componentExpressionVisitor;

    private final ProcessingEngine processingEngine;

    public ClauseVisitor(DatasetExpression datasetExpression, ProcessingEngine processingEngine) {
        this.datasetExpression = Objects.requireNonNull(datasetExpression);
        // Here we "switch" to the dataset context.
        Map<String, Object> componentMap = datasetExpression.getDataStructure().stream()
                .collect(Collectors.toMap(Dataset.Component::getName, component -> component));
        this.componentExpressionVisitor = new ExpressionVisitor(componentMap, processingEngine);
        this.processingEngine = Objects.requireNonNull(processingEngine);
    }

    private static String getName(VtlParser.ComponentIDContext context) {
        // TODO: Should be an expression so we can handle membership better and use the exceptions
        //  for undefined var etc.
        return context.getText();
    }

    @Override
    public DatasetExpression visitKeepOrDropClause(VtlParser.KeepOrDropClauseContext ctx) {
        // Normalize to keep operation.
        var keep = ctx.op.getType() == VtlParser.KEEP;
        var names = ctx.componentID().stream().map(ClauseVisitor::getName)
                .collect(Collectors.toSet());
        List<String> columnNames = datasetExpression.getDataStructure().stream().map(Dataset.Component::getName)
                .filter(name -> keep == names.contains(name))
                .collect(Collectors.toList());

        return processingEngine.executeProject(datasetExpression, columnNames);
    }

    @Override
    public DatasetExpression visitCalcClause(VtlParser.CalcClauseContext ctx) {

        var expressions = new LinkedHashMap<String, ResolvableExpression>();
        for (VtlParser.CalcClauseItemContext calcCtx : ctx.calcClauseItem()) {
            var columnName = calcCtx.componentID().getText();
            ResolvableExpression calc = componentExpressionVisitor.visit(calcCtx);
            expressions.put(columnName, calc);
        }

        return processingEngine.executeCalc(datasetExpression, expressions);
    }

    @Override
    public DatasetExpression visitFilterClause(VtlParser.FilterClauseContext ctx) {
        ResolvableExpression filter = componentExpressionVisitor.visit(ctx.expr());
        return processingEngine.executeFilter(datasetExpression, filter);
    }

    @Override
    public DatasetExpression visitRenameClause(VtlParser.RenameClauseContext ctx) {
        Map<String, String> fromTo = new LinkedHashMap<>();
        for (VtlParser.RenameClauseItemContext renameCtx : ctx.renameClauseItem()) {
            fromTo.put(getName(renameCtx.fromName), getName(renameCtx.toName));
        }
        return processingEngine.executeRename(datasetExpression, fromTo);
    }

    @Override
    public DatasetExpression visitAggrClause(VtlParser.AggrClauseContext ctx) {

        // Get a set of columns we are grouping by.
        var groupByCtx = ctx.groupingClause();
        Set<String> groupBy = Set.of();
        if (groupByCtx instanceof VtlParser.GroupByOrExceptContext) {
            groupBy = ((VtlParser.GroupByOrExceptContext) groupByCtx).componentID()
                    .stream().map(ClauseVisitor::getName).collect(Collectors.toSet());
        }

        // Create a keyExtractor with the columns we group by.
        // TODO: Extract.
        Set<String> finalGroupBy = groupBy;
        Function<Map<String, Object>, Map<String, Object>> keyExtractor = map -> {
            return map.entrySet().stream().filter(entry -> finalGroupBy.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        };

        // Create a
        Map<String, AggregationExpression> aggregationExpressions = new LinkedHashMap<>();
        for (VtlParser.AggrFunctionClauseContext functionCtx : ctx.aggregateClause().aggrFunctionClause()) {
            String name = getName(functionCtx.componentID());
            var groupFunctionCtx = (VtlParser.AggrDatasetContext) functionCtx.aggrOperatorsGrouping();
            var expression = componentExpressionVisitor.visit(groupFunctionCtx.expr());
            if (groupFunctionCtx.SUM() != null) {
                aggregationExpressions.put(name, new SumExpression(assertNumber(expression, groupFunctionCtx.expr())));
            } else {
                throw new VtlRuntimeException(new VtlScriptException("not implemented", groupFunctionCtx));
            }
        }

        // Compute the new data structure.
        Map<String, Dataset.Component> newStructure = new LinkedHashMap<>();
        for (Dataset.Component component : datasetExpression.getDataStructure()) {
            if (groupBy.contains(component.getName())) {
                newStructure.put(component.getName(), component);
            }
        }
        for (Map.Entry<String, AggregationExpression> entry : aggregationExpressions.entrySet()) {
            newStructure.put(entry.getKey(), new Dataset.Component(
                    entry.getKey(),
                    entry.getValue().getType(),
                    Dataset.Role.MEASURE)
            );
        }

        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {

                List<Map<String, Object>> data = datasetExpression.resolve(Map.of()).getDataAsMap();
                AggregationCollector collector = new AggregationCollector(aggregationExpressions);
                Map<Map<String, Object>, Map<String, Object>> collect =
                        data.stream().collect(Collectors.groupingBy(keyExtractor, collector));

                return new InMemoryDataset(collect.entrySet().stream().map(e -> {
                    e.getValue().putAll(e.getKey());
                    return Dataset.mapToRowMajor(e.getValue(), newStructure.keySet());
                }).collect(Collectors.toList()), newStructure.values());
            }

            @Override
            public List<Dataset.Component> getDataStructure() {
                return new ArrayList<>(newStructure.values());
            }
        };
    }

    static class AggregationCollector implements Collector<Map<String, Object>, Map<String, Object>, Map<String, Object>> {

        private final Map<String, AggregationExpression> expressions;

        AggregationCollector(Map<String, AggregationExpression> expressions) {
            this.expressions = Objects.requireNonNull(expressions);
        }

        @Override
        public Supplier<Map<String, Object>> supplier() {
            // Initialize all the aggregation expressions.
            return () -> {
                HashMap<String, Object> map = new HashMap<>();
                for (Map.Entry<String, AggregationExpression> entry : expressions.entrySet()) {
                    map.put(entry.getKey(), entry.getValue().init());
                }
                return map;
            };
        }

        @Override
        public BiConsumer<Map<String, Object>, Map<String, Object>> accumulator() {
            return (map, group) -> {
                for (Map.Entry<String, AggregationExpression> entry : expressions.entrySet()) {
                    String column = entry.getKey();
                    AggregationExpression aggregation = entry.getValue();
                    Object value = aggregation.getExpression().resolve(group);
                    aggregation.accept(map.get(column), value);
                }
            };
        }


        @Override
        public BinaryOperator<Map<String, Object>> combiner() {
            // Combine all aggregations.
            return (map, map2) -> {
                for (Map.Entry<String, AggregationExpression> entry : expressions.entrySet()) {
                    String column = entry.getKey();
                    AggregationExpression aggregation = entry.getValue();
                    map.put(column, aggregation.combine(map.get(column), map2.get(column)));
                }
                return map;
            };
        }

        @Override
        public Function<Map<String, Object>, Map<String, Object>> finisher() {
            return map -> {
                for (Map.Entry<String, AggregationExpression> entry : expressions.entrySet()) {
                    String column = entry.getKey();
                    AggregationExpression aggregation = entry.getValue();
                    Object group = map.remove(column);
                    map.put(column, aggregation.finish(group));
                }
                return map;
            };
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Set.of(Characteristics.UNORDERED);
        }
    }
}
