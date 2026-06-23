package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.semantics.analytic.AnalyticExecutor;
import fr.insee.vtl.engine.semantics.analytic.MultiMeasureAnalyticExecutor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.Objects;

/** Visitor dispatch for analytic expressions; orchestration lives in {@link AnalyticExecutor}. */
public class AnalyticFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor expressionVisitor;
  private final ProcessingEngine processingEngine;

  public AnalyticFunctionsVisitor(
      ExpressionVisitor expressionVisitor, ProcessingEngine processingEngine) {
    this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
    this.processingEngine = Objects.requireNonNull(processingEngine);
  }

  @Override
  public ResolvableExpression visitRatioToReportAn(VtlParser.RatioToReportAnContext ctx) {
    var datasetExpression = (DatasetExpression) expressionVisitor.visit(ctx.expr());
    return MultiMeasureAnalyticExecutor.execute(
        processingEngine,
        datasetExpression,
        ctx.expr().getText(),
        ctx.op.getText(),
        (mono, targetColumnName) ->
            AnalyticExecutor.executeRatioToReport(ctx, processingEngine, mono, targetColumnName));
  }

  @Override
  public ResolvableExpression visitLagOrLeadAn(VtlParser.LagOrLeadAnContext ctx) {
    var datasetExpression = (DatasetExpression) expressionVisitor.visit(ctx.expr());
    return MultiMeasureAnalyticExecutor.execute(
        processingEngine,
        datasetExpression,
        ctx.expr().getText(),
        ctx.op.getText(),
        (mono, targetColumnName) ->
            AnalyticExecutor.executeLagLead(ctx, processingEngine, mono, targetColumnName));
  }

  @Override
  public DatasetExpression visitAnSimpleFunction(VtlParser.AnSimpleFunctionContext ctx) {
    var datasetExpression = (DatasetExpression) expressionVisitor.visit(ctx.expr());
    return MultiMeasureAnalyticExecutor.execute(
        processingEngine,
        datasetExpression,
        ctx.expr().getText(),
        ctx.op.getText(),
        (mono, targetColumnName) ->
            AnalyticExecutor.executeSimple(ctx, processingEngine, mono, targetColumnName));
  }
}
