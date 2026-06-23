package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.semantics.join.JoinExecutor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.Objects;

/** Visitor dispatch for join expressions; orchestration lives in {@link JoinExecutor}. */
public class JoinFunctionsVisitor extends VtlBaseVisitor<DatasetExpression> {

  private final ExpressionVisitor expressionVisitor;
  private final ProcessingEngine processingEngine;

  public JoinFunctionsVisitor(
      ExpressionVisitor expressionVisitor, ProcessingEngine processingEngine) {
    this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
    this.processingEngine = Objects.requireNonNull(processingEngine);
  }

  @Override
  public DatasetExpression visitJoinExpr(VtlParser.JoinExprContext ctx) {
    if (ctx.LEFT_JOIN() != null) {
      return JoinExecutor.leftJoin(ctx, expressionVisitor, processingEngine);
    }
    if (ctx.INNER_JOIN() != null) {
      return JoinExecutor.innerJoin(ctx, expressionVisitor, processingEngine);
    }
    if (ctx.FULL_JOIN() != null) {
      return JoinExecutor.fullJoin(ctx, expressionVisitor, processingEngine);
    }
    if (ctx.CROSS_JOIN() != null) {
      return JoinExecutor.crossJoin(ctx, expressionVisitor, processingEngine);
    }
    throw new UnsupportedOperationException("unknown join type");
  }
}
