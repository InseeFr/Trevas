package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.semantics.aggregation.AggregateInvocationExecutor;
import fr.insee.vtl.engine.semantics.aggregation.AggregationColumnReferences;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.Objects;

/**
 * Visitor dispatch for aggregate invocations; orchestration in {@link AggregateInvocationExecutor}.
 */
public class AggregateFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor expressionVisitor;
  private final ProcessingEngine processingEngine;

  public AggregateFunctionsVisitor(
      ExpressionVisitor expressionVisitor, ProcessingEngine processingEngine) {
    this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
    this.processingEngine = Objects.requireNonNull(processingEngine);
  }

  @Override
  public ResolvableExpression visitAggregateFunctions(VtlParser.AggregateFunctionsContext ctx) {
    return visit(ctx.aggrOperatorsGrouping());
  }

  @Override
  public DatasetExpression visitAggrDataset(VtlParser.AggrDatasetContext ctx) {
    return AggregateInvocationExecutor.executeAggrDataset(ctx, expressionVisitor, processingEngine);
  }

  @Override
  public ResolvableExpression visitCountAggr(VtlParser.CountAggrContext ctx) {
    return AggregationColumnReferences.countMeasure(fromContext(ctx));
  }
}
