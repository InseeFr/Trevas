package fr.insee.vtl.engine.semantics.aggregation;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.semantics.attribute.ComponentRoles;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlParser;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** Executes {@code [ aggr … group by … ]} clauses. */
public final class AggrClauseExecutor {

  private AggrClauseExecutor() {}

  public static DatasetExpression execute(
      DatasetExpression input,
      VtlParser.AggrClauseContext ctx,
      ExpressionVisitor componentExpressionVisitor,
      ProcessingEngine processingEngine) {

    var aggregationsWithExpressions =
        ctx.aggregateClause().aggrFunctionClause().stream()
            .filter(agg -> agg.aggrOperatorsGrouping() instanceof VtlParser.AggrDatasetContext)
            .toList();

    Map<String, ResolvableExpression> expressions =
        aggregationsWithExpressions.stream()
            .collect(
                Collectors.toMap(
                    agg -> VtlParseTrees.componentName(agg.componentID()),
                    agg ->
                        componentExpressionVisitor.visit(
                            ((VtlParser.AggrDatasetContext) agg.aggrOperatorsGrouping()).expr()),
                    (a, b) -> b,
                    LinkedHashMap::new));

    Map<String, Dataset.Role> roles =
        aggregationsWithExpressions.stream()
            .collect(
                Collectors.toMap(
                    agg -> VtlParseTrees.componentName(agg.componentID()),
                    agg ->
                        agg.componentRole() == null
                            ? Dataset.Role.MEASURE
                            : ComponentRoles.fromParser(agg.componentRole()),
                    (a, b) -> b,
                    LinkedHashMap::new));

    Map<String, String> expressionStrings =
        aggregationsWithExpressions.stream()
            .collect(
                Collectors.toMap(
                    agg -> VtlParseTrees.componentName(agg.componentID()),
                    agg -> VtlParseTrees.sourceText(agg.aggrOperatorsGrouping()),
                    (a, b) -> b,
                    LinkedHashMap::new));

    DatasetExpression normalizedDataset =
        processingEngine.executeCalc(input, expressions, roles, expressionStrings);

    GroupingPlan grouping =
        GroupingResolver.resolve(
            normalizedDataset, ctx.groupingClause(), componentExpressionVisitor, processingEngine);

    Map<String, AggregationExpression> measureCollectors =
        AggregationCollectors.fromAggrClause(
            ctx, grouping.dataset().getDataStructure(), fromContext(ctx));

    Positioned position = fromContext(ctx);
    HavingClauseApplier.Plan havingPlan =
        HavingClauseApplier.plan(
            ctx.havingClause(),
            grouping.dataset().getDataStructure(),
            measureCollectors.keySet(),
            position);

    DatasetExpression groupedInput =
        HavingClauseApplier.materializeTemporaryColumns(
            grouping.dataset(), havingPlan, position, processingEngine);

    AggregationViralPropagation viralPropagation =
        grouping.groupByKeys().isEmpty()
            ? AggregationViralPropagation.INVOCATION_GLOBAL
            : AggregationViralPropagation.AGGR_CLAUSE_GROUPED;
    AggregationPlan.Prepared plan =
        AggregationPlan.prepare(
            grouping.dataset().getDataStructure(),
            grouping.groupByKeys(),
            measureCollectors,
            viralPropagation);

    Map<String, AggregationExpression> allCollectors = new LinkedHashMap<>(plan.collectors());
    allCollectors.putAll(havingPlan.extraCollectors());

    DatasetExpression aggregated =
        processingEngine.executeAggr(groupedInput, grouping.groupByKeys(), allCollectors);

    return HavingClauseApplier.apply(
        aggregated,
        plan.structure(),
        ctx.havingClause(),
        havingPlan,
        componentExpressionVisitor,
        processingEngine);
  }
}
