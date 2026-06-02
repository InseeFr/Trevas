package fr.insee.vtl.engine.aggregation;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.exceptions.VtlRuntimeException;
import fr.insee.vtl.parser.VtlParser;
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
        AggregationCollectors.fromAggrDatasetInvocation(ctx, grouping.dataset(), fromContext(ctx));

    if (collectors.isEmpty()) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "aggregate invocation requires at least one measure in the dataset",
              fromContext(ctx)));
    }

    DatasetExpression result =
        processingEngine.executeAggr(grouping.dataset(), grouping.groupByKeys(), collectors);

    return HavingClauseApplier.apply(
        result, ctx.havingClause(), expressionVisitor, processingEngine);
  }
}
