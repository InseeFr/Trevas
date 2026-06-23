package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.antlr.runtime.ParserRuleContext;
import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.semantics.aggregation.VtlParseTrees;
import fr.insee.vtl.engine.semantics.clause.ClauseExecutor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Parse-tree dispatch for dataset clauses; orchestration lives in {@link ClauseExecutor}. */
public class ClauseVisitor extends VtlBaseVisitor<DatasetExpression> {

  private final DatasetExpression datasetExpression;
  private final ExpressionVisitor componentExpressionVisitor;
  private final ProcessingEngine processingEngine;

  public ClauseVisitor(
      DatasetExpression datasetExpression,
      ProcessingEngine processingEngine,
      VtlScriptEngine engine) {
    this.datasetExpression = Objects.requireNonNull(datasetExpression);
    Map<String, Object> componentMap =
        datasetExpression.getDataStructure().values().stream()
            .collect(Collectors.toMap(c -> c.getName(), component -> component));
    this.componentExpressionVisitor = new ExpressionVisitor(componentMap, processingEngine, engine);
    this.processingEngine = Objects.requireNonNull(processingEngine);
  }

  public static String getName(VtlParser.ComponentIDContext context) {
    return VtlParseTrees.componentName(context);
  }

  static String getSource(ParserRuleContext ctx) {
    return VtlParseTrees.sourceText(ctx);
  }

  @Override
  public DatasetExpression visitKeepOrDropClause(VtlParser.KeepOrDropClauseContext ctx) {
    return ClauseExecutor.keepOrDrop(datasetExpression, ctx, processingEngine);
  }

  @Override
  public DatasetExpression visitCalcClause(VtlParser.CalcClauseContext ctx) {
    return ClauseExecutor.calc(
        datasetExpression, ctx, componentExpressionVisitor, processingEngine);
  }

  @Override
  public DatasetExpression visitFilterClause(VtlParser.FilterClauseContext ctx) {
    return ClauseExecutor.filter(
        datasetExpression, ctx, componentExpressionVisitor, processingEngine);
  }

  @Override
  public DatasetExpression visitRenameClause(VtlParser.RenameClauseContext ctx) {
    return ClauseExecutor.rename(datasetExpression, ctx, processingEngine);
  }

  @Override
  public DatasetExpression visitSubspaceClause(VtlParser.SubspaceClauseContext ctx) {
    return ClauseExecutor.subspace(
        datasetExpression, ctx, componentExpressionVisitor, processingEngine);
  }

  @Override
  public DatasetExpression visitAggrClause(VtlParser.AggrClauseContext ctx) {
    return ClauseExecutor.aggr(
        datasetExpression, ctx, componentExpressionVisitor, processingEngine);
  }

  @Override
  public DatasetExpression visitPivotOrUnpivotClause(VtlParser.PivotOrUnpivotClauseContext ctx) {
    return ClauseExecutor.pivot(datasetExpression, ctx, processingEngine);
  }
}
