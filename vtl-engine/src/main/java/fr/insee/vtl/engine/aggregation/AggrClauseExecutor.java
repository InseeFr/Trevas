package fr.insee.vtl.engine.aggregation;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
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
                            : Dataset.Role.valueOf(agg.componentRole().getText().toUpperCase()),
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

    Structured.DataStructure normalizedStructure = grouping.dataset().getDataStructure();
    Map<String, AggregationExpression> collectorMap = new LinkedHashMap<>();

    for (VtlParser.AggrFunctionClauseContext functionCtx :
        ctx.aggregateClause().aggrFunctionClause()) {
      String alias = VtlParseTrees.componentName(functionCtx.componentID());
      if (normalizedStructure.containsKey(alias)) {
        Structured.Component normalizedComponent = normalizedStructure.get(alias);
        collectorMap.put(
            alias,
            AggregationExpressionFactory.fromAggrDataset(
                (VtlParser.AggrDatasetContext) functionCtx.aggrOperatorsGrouping(),
                columnReference(fromContext(ctx), alias, normalizedComponent.getType())));
      } else {
        collectorMap.put(alias, AggregationExpressionFactory.countRows());
      }
    }

    return processingEngine.executeAggr(grouping.dataset(), grouping.groupByKeys(), collectorMap);
  }

  private static ResolvableExpression columnReference(
      Positioned position, String alias, Class<?> type) {
    return new ResolvableExpression(position) {
      @Override
      public Object resolve(Map<String, Object> context) {
        return context.get(alias);
      }

      @Override
      public Class<?> getType() {
        return type;
      }
    };
  }
}
