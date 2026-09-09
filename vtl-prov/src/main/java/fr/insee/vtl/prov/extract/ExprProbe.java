package fr.insee.vtl.prov.extract;

import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Single AST probe for scalar-vs-dataset classification (replaces multiple anonymous visitors in
 * {@link ProvenanceVisitor}).
 */
final class ExprProbe {

  private ExprProbe() {}

  record Findings(
      boolean eval, boolean aggregateOrAnalytic, boolean datasetUdo, Set<String> varIds) {}

  static Findings probe(VtlParser.ExprContext expr, Predicate<String> isDatasetUdo) {
    boolean[] eval = {false};
    boolean[] aggrAn = {false};
    boolean[] datasetUdo = {false};
    Set<String> varIds = new LinkedHashSet<>();
    new VtlBaseVisitor<Void>() {
      @Override
      public Void visitEvalAtom(VtlParser.EvalAtomContext ctx) {
        eval[0] = true;
        return null;
      }

      @Override
      public Void visitAggregateFunctions(VtlParser.AggregateFunctionsContext ctx) {
        aggrAn[0] = true;
        return null;
      }

      @Override
      public Void visitAnalyticFunctions(VtlParser.AnalyticFunctionsContext ctx) {
        aggrAn[0] = true;
        return null;
      }

      @Override
      public Void visitCallDataset(VtlParser.CallDatasetContext ctx) {
        if (isDatasetUdo.test(ctx.operatorID().getText())) {
          datasetUdo[0] = true;
        }
        for (VtlParser.ParameterContext parameter : ctx.parameter()) {
          if (parameter.varID() != null) {
            varIds.add(parameter.varID().getText());
          }
        }
        return null;
      }

      @Override
      public Void visitVarIdExpr(VtlParser.VarIdExprContext ctx) {
        varIds.add(ctx.varID().getText());
        return null;
      }
    }.visit(expr);
    return new Findings(eval[0], aggrAn[0], datasetUdo[0], Set.copyOf(varIds));
  }

  /**
   * Pure scalar expression (constants / prior scalars only): no eval, no dataset UDO, no dataset
   * names.
   */
  static boolean isPureScalar(Findings findings, Predicate<String> isDatasetName) {
    if (findings.eval() || findings.datasetUdo()) {
      return false;
    }
    for (String name : findings.varIds()) {
      if (isDatasetName.test(name)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Assignment RHS should be treated as scalar (IR {@code kind=scalar}), not a dataset producer.
   */
  static boolean isScalarAssignment(Findings findings, Predicate<String> isDatasetName) {
    if (findings.eval() || findings.datasetUdo() || findings.aggregateOrAnalytic()) {
      return false;
    }
    return isPureScalar(findings, isDatasetName);
  }
}
