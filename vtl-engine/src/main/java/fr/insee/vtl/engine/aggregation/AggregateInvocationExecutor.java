package fr.insee.vtl.engine.aggregation;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlParser;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Executes aggregate invocation ({@code sum(DS group by …)}, {@code avg(DS)}, {@code count(DS group
 * by …)}, etc.).
 */
public final class AggregateInvocationExecutor {

  private AggregateInvocationExecutor() {}

  public static DatasetExpression executeAggrDataset(
      VtlParser.AggrDatasetContext ctx,
      ExpressionVisitor expressionVisitor,
      ProcessingEngine processingEngine) {

    DatasetExpression input =
        AggregateOperandResolver.requireDataset(expressionVisitor.visit(ctx.expr()), ctx);

    GroupingPlan grouping =
        GroupingResolver.resolve(input, ctx.groupingClause(), expressionVisitor, processingEngine);

    Map<String, AggregationExpression> collectors =
        buildCollectors(ctx, grouping.dataset(), fromContext(ctx));

    if (collectors.isEmpty()) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "aggregate invocation requires at least one measure in the dataset",
              fromContext(ctx)));
    }

    fr.insee.vtl.model.AggregationViralPropagation viralPropagation =
        grouping.groupByKeys().isEmpty()
            ? fr.insee.vtl.model.AggregationViralPropagation.INVOCATION_GLOBAL
            : fr.insee.vtl.model.AggregationViralPropagation.INVOCATION_GROUPED;
    DatasetExpression result =
        processingEngine.executeAggr(
            grouping.dataset(), grouping.groupByKeys(), collectors, viralPropagation);

    return HavingClauseApplier.apply(
        result, ctx.havingClause(), expressionVisitor, processingEngine);
  }

  private static Map<String, AggregationExpression> buildCollectors(
      VtlParser.AggrDatasetContext ctx, DatasetExpression dataset, Positioned position) {

    if (ctx.COUNT() != null) {
      return Map.of(AggregationNames.COUNT_MEASURE, AggregationExpressionFactory.countRows());
    }

    Map<String, AggregationExpression> collectors = new LinkedHashMap<>();
    for (Structured.Component measure : dataset.getDataStructure().getMeasures()) {
      String name = measure.getName();
      collectors.put(
          name,
          AggregationExpressionFactory.fromAggrDataset(
              ctx, AggregationColumnReferences.columnReference(position, name, measure.getType())));
    }
    return collectors;
  }
}
