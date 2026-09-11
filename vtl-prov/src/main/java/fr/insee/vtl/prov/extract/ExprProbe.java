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

  /**
   * @param datasetSyntax join / set / clause / membership / validation / hierarchy / time-series /
   *     exists_in — structural dataset producers independent of bound names
   */
  record Findings(
      boolean eval,
      boolean aggregateOrAnalytic,
      boolean datasetUdo,
      boolean datasetSyntax,
      Set<String> varIds) {}

  /**
   * @param isDatasetUdo true for known dataset UDOs and for unknown/registered calls (treated as
   *     external dataset producers). False only for known scalar {@code define operator}s.
   */
  static Findings probe(VtlParser.ExprContext expr, Predicate<String> isDatasetUdo) {
    boolean[] eval = {false};
    boolean[] aggrAn = {false};
    boolean[] datasetUdo = {false};
    boolean[] datasetSyntax = {false};
    Set<String> varIds = new LinkedHashSet<>();
    new VtlBaseVisitor<Void>() {
      @Override
      public Void visitEvalAtom(VtlParser.EvalAtomContext ctx) {
        eval[0] = true;
        return visitChildren(ctx);
      }

      @Override
      public Void visitAggregateFunctions(VtlParser.AggregateFunctionsContext ctx) {
        aggrAn[0] = true;
        return visitChildren(ctx);
      }

      @Override
      public Void visitAnalyticFunctions(VtlParser.AnalyticFunctionsContext ctx) {
        aggrAn[0] = true;
        return visitChildren(ctx);
      }

      @Override
      public Void visitClauseExpr(VtlParser.ClauseExprContext ctx) {
        datasetSyntax[0] = true;
        return visitChildren(ctx);
      }

      @Override
      public Void visitMembershipExpr(VtlParser.MembershipExprContext ctx) {
        datasetSyntax[0] = true;
        return visitChildren(ctx);
      }

      @Override
      public Void visitJoinFunctions(VtlParser.JoinFunctionsContext ctx) {
        datasetSyntax[0] = true;
        return visitChildren(ctx);
      }

      @Override
      public Void visitSetFunctions(VtlParser.SetFunctionsContext ctx) {
        datasetSyntax[0] = true;
        return visitChildren(ctx);
      }

      @Override
      public Void visitValidationFunctions(VtlParser.ValidationFunctionsContext ctx) {
        datasetSyntax[0] = true;
        return visitChildren(ctx);
      }

      @Override
      public Void visitHierarchyFunctions(VtlParser.HierarchyFunctionsContext ctx) {
        datasetSyntax[0] = true;
        return visitChildren(ctx);
      }

      @Override
      public Void visitExistInAtom(VtlParser.ExistInAtomContext ctx) {
        datasetSyntax[0] = true;
        return visitChildren(ctx);
      }

      @Override
      public Void visitFlowAtom(VtlParser.FlowAtomContext ctx) {
        datasetSyntax[0] = true;
        return visitChildren(ctx);
      }

      @Override
      public Void visitFillTimeAtom(VtlParser.FillTimeAtomContext ctx) {
        datasetSyntax[0] = true;
        return visitChildren(ctx);
      }

      @Override
      public Void visitTimeShiftAtom(VtlParser.TimeShiftAtomContext ctx) {
        datasetSyntax[0] = true;
        return visitChildren(ctx);
      }

      @Override
      public Void visitTimeAggAtom(VtlParser.TimeAggAtomContext ctx) {
        if (ctx.op != null && ctx.op.expr() != null) {
          datasetSyntax[0] = true;
        }
        return visitChildren(ctx);
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
    return new Findings(eval[0], aggrAn[0], datasetUdo[0], datasetSyntax[0], Set.copyOf(varIds));
  }

  /** True when the expression is (or embeds) a dataset producer, not a pure scalar. */
  static boolean looksLikeDatasetProducer(Findings findings, Predicate<String> isDatasetName) {
    if (findings.eval()
        || findings.datasetUdo()
        || findings.aggregateOrAnalytic()
        || findings.datasetSyntax()) {
      return true;
    }
    for (String name : findings.varIds()) {
      if (isDatasetName.test(name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Pure scalar expression (constants / prior scalars only): no dataset producer syntax, no
   * external/registered call, no aggregate/analytic, no dataset names.
   */
  static boolean isPureScalar(Findings findings, Predicate<String> isDatasetName) {
    return !looksLikeDatasetProducer(findings, isDatasetName);
  }

  /**
   * Assignment RHS should be treated as scalar (IR {@code kind=scalar}), not a dataset producer.
   */
  static boolean isScalarAssignment(Findings findings, Predicate<String> isDatasetName) {
    return isPureScalar(findings, isDatasetName);
  }
}
