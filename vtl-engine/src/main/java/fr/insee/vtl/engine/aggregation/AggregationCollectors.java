package fr.insee.vtl.engine.aggregation;

import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlParser;
import java.util.LinkedHashMap;
import java.util.Map;

/** Shared collector map creation for aggregate invocation and aggr clauses. */
public final class AggregationCollectors {

  private AggregationCollectors() {}

  public static Map<String, AggregationExpression> fromAggrDatasetInvocation(
      VtlParser.AggrDatasetContext ctx, DatasetExpression dataset, Positioned position) {
    if (ctx.COUNT() != null) {
      return Map.of(AggregationNames.COUNT_MEASURE, AggregationExpressionFactory.countRows());
    }

    Map<String, AggregationExpression> collectors = new LinkedHashMap<>();
    for (Structured.Component measure : dataset.getDataStructure().getMeasures()) {
      String name = measure.getName();
      collectors.put(name, forAlias(ctx, name, measure.getType(), position));
    }
    return collectors;
  }

  public static Map<String, AggregationExpression> fromAggrClause(
      VtlParser.AggrClauseContext ctx,
      Structured.DataStructure normalizedStructure,
      Positioned position) {
    Map<String, AggregationExpression> collectorMap = new LinkedHashMap<>();
    for (VtlParser.AggrFunctionClauseContext functionCtx :
        ctx.aggregateClause().aggrFunctionClause()) {
      String alias = VtlParseTrees.componentName(functionCtx.componentID());
      if (normalizedStructure.containsKey(alias)) {
        Structured.Component normalizedComponent = normalizedStructure.get(alias);
        collectorMap.put(
            alias,
            forAlias(
                (VtlParser.AggrDatasetContext) functionCtx.aggrOperatorsGrouping(),
                alias,
                normalizedComponent.getType(),
                position));
      } else {
        collectorMap.put(alias, AggregationExpressionFactory.countRows());
      }
    }
    return collectorMap;
  }

  private static AggregationExpression forAlias(
      VtlParser.AggrDatasetContext ctx, String alias, Class<?> type, Positioned position) {
    return AggregationExpressionFactory.fromAggrDataset(
        ctx, AggregationColumnReferences.columnReference(position, alias, type));
  }
}
