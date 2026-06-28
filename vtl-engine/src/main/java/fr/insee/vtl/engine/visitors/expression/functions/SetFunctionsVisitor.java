package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;

import fr.insee.vtl.engine.semantics.set.UnionExecutor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Visitor dispatch for set functions; orchestration lives in {@link UnionExecutor}. */
public class SetFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor expressionVisitor;
  private final ProcessingEngine processingEngine;

  public SetFunctionsVisitor(
      ExpressionVisitor expressionVisitor, ProcessingEngine processingEngine) {
    this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
    this.processingEngine = Objects.requireNonNull(processingEngine);
  }

  @Override
  public ResolvableExpression visitUnionAtom(VtlParser.UnionAtomContext ctx) {
    List<DatasetExpression> datasets = new ArrayList<>();
    for (VtlParser.ExprContext expr : ctx.expr()) {
      datasets.add(
          (DatasetExpression)
              assertTypeExpression(expressionVisitor.visit(expr), Dataset.class, expr));
    }
    return UnionExecutor.union(processingEngine, datasets, ctx);
  }
}
