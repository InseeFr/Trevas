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

public class ClauseVisitor extends VtlBaseVisitor<DatasetExpression> {

    private final DatasetExpression datasetExpression;
    private final ExpressionVisitor componentExpressionVisitor;

    private final ProcessingEngine processingEngine;

    public ClauseVisitor(DatasetExpression datasetExpression, ProcessingEngine processingEngine) {
        this.datasetExpression = Objects.requireNonNull(datasetExpression);
        // Here we "switch" to the dataset context.
        Map<String, Object> componentMap = datasetExpression.getDataStructure().values().stream()
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
        List<String> columnNames = datasetExpression.getDataStructure().values().stream().map(Dataset.Component::getName)
                .filter(name -> keep == names.contains(name))
                .collect(Collectors.toList());

        return processingEngine.executeProject(datasetExpression, columnNames);
    }

    @Override
    public DatasetExpression visitCalcClause(VtlParser.CalcClauseContext ctx) {

        var expressions = new LinkedHashMap<String, ResolvableExpression>();
        var roles = new LinkedHashMap<String, Dataset.Role>();
        for (VtlParser.CalcClauseItemContext calcCtx : ctx.calcClauseItem()) {
            var columnName = calcCtx.componentID().getText();
            var columnRole = calcCtx.componentRole() == null ? Dataset.Role.MEASURE
                    : Dataset.Role.valueOf(calcCtx.componentRole().getText().toUpperCase());
            ResolvableExpression calc = componentExpressionVisitor.visit(calcCtx);
            expressions.put(columnName, calc);
            roles.put(columnName, columnRole);
        }

        return processingEngine.executeCalc(datasetExpression, expressions, roles);
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
        Function<Structured.DataPoint, Map<String, Object>> keyExtractor = dataPoint -> {
            return new Structured.DataPointMap(dataPoint).entrySet().stream().filter(entry -> finalGroupBy.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        };

        // Create a map of collectors.
        Map<String, AggregationExpression> collectorMap = new LinkedHashMap<>();
        for (VtlParser.AggrFunctionClauseContext functionCtx : ctx.aggregateClause().aggrFunctionClause()) {
            String name = getName(functionCtx.componentID());
            var groupFunctionCtx = (VtlParser.AggrDatasetContext) functionCtx.aggrOperatorsGrouping();
            var expression = componentExpressionVisitor.visit(groupFunctionCtx.expr());
            if (groupFunctionCtx.SUM() != null) {
                collectorMap.put(name, AggregationExpression.sum(expression));
            } else if (groupFunctionCtx.AVG() != null) {
                collectorMap.put(name, AggregationExpression.avg(expression));
            } else if (groupFunctionCtx.COUNT() != null) {
                collectorMap.put(name, AggregationExpression.count());
            } else if (groupFunctionCtx.MAX() != null) {
                throw new VtlRuntimeException(new VtlScriptException("not implemented", groupFunctionCtx));
            } else if (groupFunctionCtx.MIN() != null) {
                throw new VtlRuntimeException(new VtlScriptException("not implemented", groupFunctionCtx));
            } else if (groupFunctionCtx.MEDIAN() != null) {
                throw new VtlRuntimeException(new VtlScriptException("not implemented", groupFunctionCtx));
            } else if (groupFunctionCtx.STDDEV_POP() != null) {
                throw new VtlRuntimeException(new VtlScriptException("not implemented", groupFunctionCtx));
            } else if (groupFunctionCtx.STDDEV_SAMP() != null) {
                throw new VtlRuntimeException(new VtlScriptException("not implemented", groupFunctionCtx));
            } else if (groupFunctionCtx.VAR_POP() != null) {
                throw new VtlRuntimeException(new VtlScriptException("not implemented", groupFunctionCtx));
            } else if (groupFunctionCtx.VAR_SAMP() != null) {
                throw new VtlRuntimeException(new VtlScriptException("not implemented", groupFunctionCtx));
            } else {
                throw new VtlRuntimeException(new VtlScriptException("not implemented", groupFunctionCtx));
            }
        }

        // Compute the new data structure.
        Map<String, Dataset.Component> newStructure = new LinkedHashMap<>();
        for (Dataset.Component component : datasetExpression.getDataStructure().values()) {
            if (groupBy.contains(component.getName())) {
                newStructure.put(component.getName(), component);
            }
        }
        for (Map.Entry<String, AggregationExpression> entry : collectorMap.entrySet()) {
            newStructure.put(entry.getKey(), new Dataset.Component(
                    entry.getKey(),
                    entry.getValue().getType(),
                    Dataset.Role.MEASURE)
            );
        }

        Structured.DataStructure structure = new Structured.DataStructure(newStructure.values());
        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {

                List<DataPoint> data = datasetExpression.resolve(Map.of()).getDataPoints();
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

    /**
     * Collector that uses a map of collectors.
     */
    static class MapCollector implements Collector<Structured.DataPoint, Structured.DataPoint, Structured.DataPoint> {

        private final Structured.DataStructure structure;
        private final Map<String, Supplier<Object>> supplierMap = new HashMap<>();
        private final Map<String, BiConsumer<Object, Structured.DataPoint>> accumulatorMap = new HashMap<>();
        private final Map<String, BinaryOperator<Object>> combinerMap = new HashMap<>();
        private final Map<String, Function<Object, Object>> finisherMap = new HashMap<>();

        public MapCollector(Structured.DataStructure structure, Map<String, ? extends Collector<Structured.DataPoint, Object, Object>> collectorMap) {
            this.structure = Objects.requireNonNull(structure);
            if (!structure.keySet().containsAll(collectorMap.keySet())) {
                throw new IllegalArgumentException("inconsistent collector map");
            }
            for (Map.Entry<String, ? extends Collector<Structured.DataPoint, Object, Object>> entry : collectorMap.entrySet()) {
                supplierMap.put(entry.getKey(), entry.getValue().supplier());
                accumulatorMap.put(entry.getKey(), entry.getValue().accumulator());
                combinerMap.put(entry.getKey(), entry.getValue().combiner());
                finisherMap.put(entry.getKey(), entry.getValue().finisher());
            }
        }

        @Override
        public Supplier<Structured.DataPoint> supplier() {
            return () -> {
                Structured.DataPoint dataPoint = new Structured.DataPoint(structure);
                for (Map.Entry<String, Supplier<Object>> entry : supplierMap.entrySet()) {
                    String column = entry.getKey();
                    dataPoint.set(column, entry.getValue().get());
                }
                return dataPoint;
            };
        }

        @Override
        public BiConsumer<Structured.DataPoint, Structured.DataPoint> accumulator() {
            return (map, context) -> {
                for (Map.Entry<String, BiConsumer<Object, Structured.DataPoint>> entry : accumulatorMap.entrySet()) {
                    String column = entry.getKey();
                    Object accumulatorValue = map.get(column);
                    entry.getValue().accept(accumulatorValue, context);
                }
            };
        }

        @Override
        public BinaryOperator<Structured.DataPoint> combiner() {
            return (map, map2) -> {
                for (Map.Entry<String, BinaryOperator<Object>> entry : combinerMap.entrySet()) {
                    String column = entry.getKey();
                    Object newValue = entry.getValue().apply(map.get(column), map2.get(column));
                    map.set(column, newValue);
                }
                return map;
            };
        }

        @Override
        public Function<Structured.DataPoint, Structured.DataPoint> finisher() {
            return map -> {
                for (Map.Entry<String, Function<Object, Object>> entry : finisherMap.entrySet()) {
                    String column = entry.getKey();
                    map.set(column, entry.getValue().apply(map.get(column)));
                }
                return map;
            };
        }

        @Override
        public Set<Characteristics> characteristics() {
            // TODO: Think about this.
            return Set.of(Characteristics.UNORDERED);
        }
    }
}
