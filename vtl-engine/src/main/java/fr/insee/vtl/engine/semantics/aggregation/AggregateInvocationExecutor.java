package fr.insee.vtl.engine.semantics.aggregation;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.assertDatasetExpression;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
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

    DatasetExpression input = assertDatasetExpression(expressionVisitor.visit(ctx.expr()), ctx);

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

    Positioned position = fromContext(ctx);
    HavingClauseApplier.Plan havingPlan =
        HavingClauseApplier.plan(
            ctx.havingClause(),
            grouping.dataset().getDataStructure(),
            collectors.keySet(),
            position);

    DatasetExpression groupedInput =
        HavingClauseApplier.materializeTemporaryColumns(
            grouping.dataset(), havingPlan, position, processingEngine);

    AggregationViralPropagation viralPropagation =
        grouping.groupByKeys().isEmpty()
            ? AggregationViralPropagation.INVOCATION_GLOBAL
            : AggregationViralPropagation.INVOCATION_GROUPED;
    AggregationPlan.Prepared plan =
        AggregationPlan.prepare(
            grouping.dataset().getDataStructure(),
            grouping.groupByKeys(),
            collectors,
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
        expressionVisitor,
        processingEngine);
  }
}
