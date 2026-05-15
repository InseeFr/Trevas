package fr.insee.vtl.engine.aggregation;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlParser;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Executes dataset-level aggregate invocation ({@code sum(DS group by …)}, etc.) per VTL 2.1: the
 * operator is applied to each measure component of the input dataset within the resolved groups.
 */
public final class DatasetAggregateInvocationExecutor {

  private static final String COUNT_RESULT = "count";

  private DatasetAggregateInvocationExecutor() {}

  public static DatasetExpression executeSumLike(
      DatasetExpression input,
      VtlParser.AggrDatasetContext ctx,
      ExpressionVisitor expressionVisitor,
      ProcessingEngine processingEngine) {

    GroupingPlan grouping =
        GroupingResolver.resolve(input, ctx.groupingClause(), expressionVisitor, processingEngine);

    Map<String, AggregationExpression> collectorMap = new LinkedHashMap<>();
    for (Structured.Component measure : grouping.dataset().getDataStructure().getMeasures()) {
      String name = measure.getName();
      collectorMap.put(
          name,
          AggregationExpressionFactory.fromAggrDataset(
              ctx, columnReference(fromContext(ctx), name, measure.getType())));
    }

    if (collectorMap.isEmpty()) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "aggregate invocation requires at least one measure in the dataset",
              fromContext(ctx)));
    }

    return processingEngine.executeAggr(grouping.dataset(), grouping.groupByKeys(), collectorMap);
  }

  public static DatasetExpression executeCount(
      DatasetExpression input,
      VtlParser.GroupingClauseContext groupingClause,
      ExpressionVisitor expressionVisitor,
      ProcessingEngine processingEngine) {

    GroupingPlan grouping =
        GroupingResolver.resolve(input, groupingClause, expressionVisitor, processingEngine);

    Map<String, AggregationExpression> collectorMap =
        Map.of(COUNT_RESULT, AggregationExpressionFactory.countRows());

    return processingEngine.executeAggr(grouping.dataset(), grouping.groupByKeys(), collectorMap);
  }

  private static ResolvableExpression columnReference(
      Positioned position, String columnName, Class<?> type) {
    return new ResolvableExpression(position) {
      @Override
      public Object resolve(Map<String, Object> context) {
        return context.get(columnName);
      }

      @Override
      public Class<?> getType() {
        return type;
      }
    };
  }
}
