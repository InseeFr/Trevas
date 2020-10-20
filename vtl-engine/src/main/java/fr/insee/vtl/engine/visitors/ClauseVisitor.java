package fr.insee.vtl.engine.visitors;

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

        Set<String> groupBy = Set.of("country");
        ResolvableExpression expression = LongExpression.of(map -> {
            return (Long) map.get("age");
        });

        Function<Map<String, Object>, Map<String, Object>> keyExtractor = map -> {
            var key = new LinkedHashMap<String, Object>();
            for (String column : groupBy) {
                key.put(column, map.get(column));
            }
            return key;
        };

        List<Map<String, Object>> data = datasetExpression.resolve(Map.of()).getDataAsMap();
        AggregationCollector collector = new AggregationCollector(Map.of(
                "avgAge", new AggregationExpression<List<Object>, Object, Object>() {

                    @Override
                    public Class<?> getType() {
                        return expression.getType();
                    }

                    @Override
                    public Object resolve(Map<String, Object> context) {
                        return expression.resolve(context);
                    }

                    @Override
                    public List<Object> init() {
                        return new ArrayList<>();
                    }

                    @Override
                    public void accept(List<Object> objects, Object o) {
                        objects.add(o);
                    }

                    @Override
                    public List<Object> combine(List<Object> c1, List<Object> c2) {
                        c1.addAll(c2);
                        return c1;
                    }

                    @Override
                    public Object finish(List<Object> objects) {
                        return objects.toString();
                    }
                }
        ));
        Map<Map<String, Object>, Map<String, Object>> collect =
                data.stream().collect(Collectors.groupingBy(keyExtractor, collector));

        return null;
    }

    interface AggregationExpression<C, T, V> extends ResolvableExpression {

        C init();

        void accept(C c, T o);

        C combine(C c1, C c2);

        V finish(C c);

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
                    Object value = aggregation.resolve(group);
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
