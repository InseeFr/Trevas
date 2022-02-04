package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.exceptions.VtlScriptException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static fr.insee.vtl.engine.utils.TypeChecking.assertNumber;

/**
 * <code>ClauseVisitor</code> is the visitor for VTL clause expressions (component filter, aggr, drop, etc.).
 */
public class ClauseVisitor extends VtlBaseVisitor<DatasetExpression> {

    private final DatasetExpression datasetExpression;
    private final ExpressionVisitor componentExpressionVisitor;

    private final ProcessingEngine processingEngine;

    /**
     * Constructor taking a dataset expression and a processing engine.
     *
     * @param datasetExpression The dataset expression containing the clause expression.
     * @param processingEngine  The processing engine for dataset expressions.
     */
    public ClauseVisitor(DatasetExpression datasetExpression, ProcessingEngine processingEngine) {
        this.datasetExpression = Objects.requireNonNull(datasetExpression);
        // Here we "switch" to the dataset context.
        Map<String, Object> componentMap = datasetExpression.getDataStructure().values().stream()
                .collect(Collectors.toMap(Dataset.Component::getName, component -> component));
        this.componentExpressionVisitor = new ExpressionVisitor(componentMap, processingEngine);
        this.processingEngine = Objects.requireNonNull(processingEngine);
    }

    private static String getName(VtlParser.ComponentIDContext context) {
        // TODO: Should be an expression so we can handle membership better
        //  and use the exceptions for undefined var etc.
        //        res := ds1[calc test := m1 * ds1#m2 + m3]
        //        res := ds1#m1 -> dataset with only m1.
        //        res := ceil(ds1#m1)
        //        res := ceil(ds1)
        String text = context.getText();
        if (text.startsWith("'") && text.endsWith("'")) {
            text = text.substring(1, text.length() - 1);
        }
        return text;
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
        var expressionStrings = new LinkedHashMap<String, String>();
        var roles = new LinkedHashMap<String, Dataset.Role>();
        for (VtlParser.CalcClauseItemContext calcCtx : ctx.calcClauseItem()) {
            var columnName = getName(calcCtx.componentID());
            var columnRole = calcCtx.componentRole() == null
                    ? Dataset.Role.MEASURE
                    : Dataset.Role.valueOf(calcCtx.componentRole().getText().toUpperCase());
            ResolvableExpression calc = componentExpressionVisitor.visit(calcCtx);
            expressions.put(columnName, calc);
            expressionStrings.put(columnName, getSource(calcCtx.expr()));
            roles.put(columnName, columnRole);
        }

        return processingEngine.executeCalc(datasetExpression, expressions, roles, expressionStrings);
    }

    static String getSource(ParserRuleContext ctx) {
        var stream = ctx.getStart().getInputStream();
        return stream.getText(new Interval(
                ctx.getStart().getStartIndex(),
                ctx.getStop().getStopIndex()
        ));
    }

    @Override
    public DatasetExpression visitFilterClause(VtlParser.FilterClauseContext ctx) {
        BooleanExpression filter = (BooleanExpression) componentExpressionVisitor.visit(ctx.expr());
        return processingEngine.executeFilter(datasetExpression, filter, getSource(ctx.expr()));
    }

    @Override
    public DatasetExpression visitRenameClause(VtlParser.RenameClauseContext ctx) {
        Map<String, String> fromTo = new LinkedHashMap<>();
        Set<String> renamed = new HashSet<>();
        for (VtlParser.RenameClauseItemContext renameCtx : ctx.renameClauseItem()) {
            var toNameString = getName(renameCtx.toName);
            var fromNameString = getName(renameCtx.fromName);
            if (!renamed.add(toNameString)) {
                throw new VtlRuntimeException(new InvalidArgumentException(
                        String.format("duplicate column: %s", toNameString), renameCtx
                ));
            }
            fromTo.put(fromNameString, toNameString);
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
        // TODO: Refactor to its own class. Possibly using the Datapoint/DatapointMap as
        //  return types to improve performance.
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
                var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
                collectorMap.put(name, AggregationExpression.sum(numberExpression));
            } else if (groupFunctionCtx.AVG() != null) {
                var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
                collectorMap.put(name, AggregationExpression.avg(numberExpression));
            } else if (groupFunctionCtx.COUNT() != null) {
                collectorMap.put(name, AggregationExpression.count());
            } else if (groupFunctionCtx.MAX() != null) {
                var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
                collectorMap.put(name, AggregationExpression.max(numberExpression));
            } else if (groupFunctionCtx.MIN() != null) {
                var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
                collectorMap.put(name, AggregationExpression.min(numberExpression));
            } else if (groupFunctionCtx.MEDIAN() != null) {
                var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
                collectorMap.put(name, AggregationExpression.median(numberExpression));
            } else if (groupFunctionCtx.STDDEV_POP() != null) {
                var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
                collectorMap.put(name, AggregationExpression.stdDevPop(numberExpression));
            } else if (groupFunctionCtx.STDDEV_SAMP() != null) {
                var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
                collectorMap.put(name, AggregationExpression.stdDevSamp(numberExpression));
            } else if (groupFunctionCtx.VAR_POP() != null) {
                var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
                collectorMap.put(name, AggregationExpression.varPop(numberExpression));
            } else if (groupFunctionCtx.VAR_SAMP() != null) {
                var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
                collectorMap.put(name, AggregationExpression.varSamp(numberExpression));
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

        return processingEngine.executeAggr(datasetExpression, structure, collectorMap, keyExtractor);
    }

}
