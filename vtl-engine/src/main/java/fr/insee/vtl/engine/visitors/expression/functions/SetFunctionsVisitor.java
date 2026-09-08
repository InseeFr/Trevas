package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;

import fr.insee.vtl.engine.semantics.set.SetOperatorsExecutor;
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

/** Visitor dispatch for set functions; orchestration lives in {@link SetOperatorsExecutor}. */
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
    return SetOperatorsExecutor.union(processingEngine, datasetOperands(ctx.expr()), ctx);
  }

  @Override
  public ResolvableExpression visitIntersectAtom(VtlParser.IntersectAtomContext ctx) {
    return SetOperatorsExecutor.intersect(processingEngine, datasetOperands(ctx.expr()), ctx);
  }

  @Override
  public ResolvableExpression visitSetOrSYmDiffAtom(VtlParser.SetOrSYmDiffAtomContext ctx) {
    DatasetExpression left =
        (DatasetExpression)
            assertTypeExpression(expressionVisitor.visit(ctx.left), Dataset.class, ctx.left);
    DatasetExpression right =
        (DatasetExpression)
            assertTypeExpression(expressionVisitor.visit(ctx.right), Dataset.class, ctx.right);
    return SetOperatorsExecutor.setOrSymDiff(processingEngine, left, right, ctx);
  }

  private List<DatasetExpression> datasetOperands(List<VtlParser.ExprContext> exprs) {
    List<DatasetExpression> datasets = new ArrayList<>(exprs.size());
    for (VtlParser.ExprContext expr : exprs) {
      datasets.add(
          (DatasetExpression)
              assertTypeExpression(expressionVisitor.visit(expr), Dataset.class, expr));
    }
    return datasets;
  }
}
