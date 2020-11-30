package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.exceptions.VtlScriptException;
import fr.insee.vtl.engine.utils.MapCollector;
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

}
