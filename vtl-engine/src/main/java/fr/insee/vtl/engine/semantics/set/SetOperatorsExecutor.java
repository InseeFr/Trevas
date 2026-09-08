package fr.insee.vtl.engine.semantics.set;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.antlr.runtime.RuleContext;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlParser;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Orchestration for VTL set operators ({@code union}, {@code intersect}, {@code setdiff}, {@code
 * symdiff}).
 */
public final class SetOperatorsExecutor {

  private SetOperatorsExecutor() {}

  public static DatasetExpression union(
      ProcessingEngine engine, List<DatasetExpression> datasets, VtlParser.UnionAtomContext ctx) {
    assertCompatibleStructures(datasets, ctx.expr(), ctx);
    List<String> dedupeOn = identifierNames(datasets.get(0));
    return engine.executeUnion(datasets, dedupeOn);
  }

  public static DatasetExpression intersect(
      ProcessingEngine engine,
      List<DatasetExpression> datasets,
      VtlParser.IntersectAtomContext ctx) {
    assertCompatibleStructures(datasets, ctx.expr(), ctx);
    return engine.executeIntersect(datasets, identifierNames(datasets.get(0)));
  }

  public static DatasetExpression setOrSymDiff(
      ProcessingEngine engine,
      DatasetExpression left,
      DatasetExpression right,
      VtlParser.SetOrSYmDiffAtomContext ctx) {
    assertCompatibleStructures(List.of(left, right), List.of(ctx.left, ctx.right), ctx);
    List<String> ids = identifierNames(left);
    if (ctx.op.getType() == VtlParser.SETDIFF) {
      return engine.executeSetDiff(left, right, ids);
    }
    return engine.executeSymDiff(left, right, ids);
  }

  private static List<String> identifierNames(DatasetExpression dataset) {
    return dataset.getIdentifiers().stream().map(Structured.Component::getName).toList();
  }

  private static void assertCompatibleStructures(
      List<DatasetExpression> datasets, List<VtlParser.ExprContext> exprs, RuleContext position) {
    Structured.DataStructure structure = null;
    for (int i = 0; i < datasets.size(); i++) {
      DatasetExpression dataset = datasets.get(i);
      if (structure == null) {
        structure = dataset.getDataStructure();
      } else if (!structure.equals(dataset.getDataStructure())) {
        VtlParser.ExprContext expr = exprs.get(i);
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "dataset structure of %s is incompatible with %s"
                    .formatted(
                        expr.getText(),
                        exprs.stream().map(RuleContext::getText).collect(Collectors.joining(", "))),
                fromContext(position)));
      }
    }
  }
}
