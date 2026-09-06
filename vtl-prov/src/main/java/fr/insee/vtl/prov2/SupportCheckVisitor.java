package fr.insee.vtl.prov2;

import fr.insee.vtl.antlr.runtime.tree.RuleNode;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.List;

/**
 * Grammar-only support gate: throws {@code unsupported: …} before the structure oracle runs, so the
 * corpus backlog stays explicit even when the engine cannot eval the script.
 *
 * <p>Registers {@code define operator} / datapoint ruleset names into the shared {@link
 * ScriptSymbols}. Mirrors {@link ProvenanceVisitor} coverage. Message vocabulary (stable for
 * harness / ops): {@code define}, {@code scalar}, {@code arithmetic}, {@code clause}, {@code
 * calc}, {@code aggr}, {@code join}, {@code set}, {@code functions} (catch-all for other function
 * families), {@code check}.
 */
class SupportCheckVisitor extends VtlBaseVisitor<Void> {

  protected final ScriptSymbols symbols;

  SupportCheckVisitor(ScriptSymbols symbols) {
    this.symbols = symbols;
  }

  @Override
  public Void visitChildren(RuleNode node) {
    throw unsupported(node.getClass().getSimpleName());
  }

  @Override
  public Void visitStart(VtlParser.StartContext ctx) {
    for (VtlParser.StatementContext statement : ctx.statement()) {
      visit(statement);
    }
    return null;
  }

  @Override
  public Void visitTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
    return visit(ctx.expr());
  }

  @Override
  public Void visitPersistAssignment(VtlParser.PersistAssignmentContext ctx) {
    return visit(ctx.expr());
  }

  @Override
  public Void visitDefineExpression(VtlParser.DefineExpressionContext ctx) {
    return visit(ctx.defOperators());
  }

  @Override
  public Void visitDefDatapointRuleset(VtlParser.DefDatapointRulesetContext ctx) {
    if (ctx.rulesetSignature().VARIABLE() == null) {
      throw unsupported("define");
    }
    List<String> variables = new ArrayList<>();
    for (VtlParser.SignatureContext signature : ctx.rulesetSignature().signature()) {
      if (signature.alias() != null) {
        throw unsupported("define");
      }
      variables.add(signature.varID().getText());
    }
    symbols.putDatapointRuleset(ctx.rulesetID().getText(), variables);
    return null;
  }

  @Override
  public Void visitDefOperator(VtlParser.DefOperatorContext ctx) {
    List<String> params = new ArrayList<>();
    for (VtlParser.ParameterItemContext item : ctx.parameterItem()) {
      params.add(item.varID().getText());
    }
    symbols.putUserOperator(ctx.operatorID().getText(), params, ctx.expr());
    return null;
  }

  @Override
  public Void visitDefHierarchical(VtlParser.DefHierarchicalContext ctx) {
    if (ctx.hierRuleSignature().VARIABLE() == null) {
      throw unsupported("define");
    }
    symbols.addHierarchicalRuleset(ctx.rulesetID().getText());
    return null;
  }

  @Override
  public Void visitParenthesisExpr(VtlParser.ParenthesisExprContext ctx) {
    return visit(ctx.expr());
  }

  @Override
  public Void visitVarIdExpr(VtlParser.VarIdExprContext ctx) {
    return null;
  }

  @Override
  public Void visitArithmeticExpr(VtlParser.ArithmeticExprContext ctx) {
    leafOperand(ctx.left);
    leafOperand(ctx.right);
    return null;
  }

  @Override
  public Void visitArithmeticExprOrConcat(VtlParser.ArithmeticExprOrConcatContext ctx) {
    leafOperand(ctx.left);
    leafOperand(ctx.right);
    return null;
  }

  @Override
  public Void visitUnaryExpr(VtlParser.UnaryExprContext ctx) {
    throw unsupported("arithmetic");
  }

  @Override
  public Void visitClauseExpr(VtlParser.ClauseExprContext ctx) {
    requireDatasetOrClause(ctx.expr());
    VtlParser.DatasetClauseContext clause = ctx.datasetClause();
    if (clause.calcClause() != null) {
      clause.calcClause().calcClauseItem().forEach(item -> calcRhs(item.expr()));
      return null;
    }
    if (clause.filterClause() != null) {
      requireScalarPredicate(clause.filterClause().expr());
      return null;
    }
    if (clause.aggrClause() != null) {
      if (clause.aggrClause().havingClause() != null) {
        requireScalarPredicate(clause.aggrClause().havingClause().expr());
      }
      return null;
    }
    if (clause.customPivotClause() != null) {
      return null;
    }
    if (clause.subspaceClause() != null
        || clause.keepOrDropClause() != null
        || clause.renameClause() != null) {
      return null;
    }
    if (clause.pivotOrUnpivotClause() != null) {
      return null;
    }
    throw unsupported("clause");
  }

  @Override
  public Void visitFunctionsExpression(VtlParser.FunctionsExpressionContext ctx) {
    if (ctx.functions() instanceof VtlParser.JoinFunctionsContext join) {
      return visit(join);
    }
    if (ctx.functions() instanceof VtlParser.SetFunctionsContext set) {
      return visit(set);
    }
    if (ctx.functions() instanceof VtlParser.ValidationFunctionsContext validation) {
      return visit(validation);
    }
    if (ctx.functions() instanceof VtlParser.NumericFunctionsContext numeric) {
      return visit(numeric);
    }
    if (ctx.functions() instanceof VtlParser.HierarchyFunctionsContext hierarchy) {
      return visit(hierarchy);
    }
    if (ctx.functions() instanceof VtlParser.TimeFunctionsContext time) {
      return visit(time);
    }
    if (ctx.functions() instanceof VtlParser.ComparisonFunctionsContext comparison) {
      if (comparison.comparisonOperators() instanceof VtlParser.ExistInAtomContext existIn) {
        return visit(existIn);
      }
      throw unsupported("functions");
    }
    if (ctx.functions() instanceof VtlParser.GenericFunctionsContext generic) {
      if (generic.genericOperators() instanceof VtlParser.EvalAtomContext eval) {
        return visit(eval);
      }
      throw unsupported("functions");
    }
    throw unsupported("functions");
  }

  @Override
  public Void visitExistInAtom(VtlParser.ExistInAtomContext ctx) {
    requireDatasetVarId(ctx.left, "functions");
    requireDatasetVarId(ctx.right, "functions");
    return null;
  }

  @Override
  public Void visitEvalAtom(VtlParser.EvalAtomContext ctx) {
    // Black-box: at least one dataset varID argument (constants alone unsupported).
    if (ctx.varID().isEmpty()) {
      throw unsupported("functions");
    }
    return null;
  }

  @Override
  public Void visitHierarchyFunctions(VtlParser.HierarchyFunctionsContext ctx) {
    return visit(ctx.hierarchyOperators());
  }

  @Override
  public Void visitHierarchyOperators(VtlParser.HierarchyOperatorsContext ctx) {
    requireDatasetVarId(ctx.op, "functions");
    if (!symbols.isHierarchicalRuleset(ctx.hrName.getText())) {
      throw unsupported("functions");
    }
    return null;
  }

  @Override
  public Void visitTimeFunctions(VtlParser.TimeFunctionsContext ctx) {
    VtlParser.TimeOperatorsContext op = ctx.timeOperators();
    if (op instanceof VtlParser.FlowAtomContext
        || op instanceof VtlParser.FillTimeAtomContext
        || op instanceof VtlParser.TimeShiftAtomContext
        || op instanceof VtlParser.TimeAggAtomContext) {
      return visit(op);
    }
    // Scalar time ops (getyear, datediff, …) are calc-only, not dataset producers.
    throw unsupported("functions");
  }

  @Override
  public Void visitFlowAtom(VtlParser.FlowAtomContext ctx) {
    requireDatasetVarId(ctx.expr(), "functions");
    return null;
  }

  @Override
  public Void visitFillTimeAtom(VtlParser.FillTimeAtomContext ctx) {
    requireDatasetVarId(ctx.expr(), "functions");
    return null;
  }

  @Override
  public Void visitTimeShiftAtom(VtlParser.TimeShiftAtomContext ctx) {
    requireDatasetVarId(ctx.expr(), "functions");
    return null;
  }

  @Override
  public Void visitTimeAggAtom(VtlParser.TimeAggAtomContext ctx) {
    // Dataset form uses optionalExpr as the operand when present.
    if (ctx.op == null || ctx.op.expr() == null) {
      throw unsupported("functions");
    }
    requireDatasetVarId(ctx.op.expr(), "functions");
    return null;
  }

  @Override
  public Void visitNumericFunctions(VtlParser.NumericFunctionsContext ctx) {
    return visit(ctx.numericOperators());
  }

  @Override
  public Void visitUnaryNumeric(VtlParser.UnaryNumericContext ctx) {
    requireDatasetVarId(ctx.expr(), "functions");
    return null;
  }

  @Override
  public Void visitUnaryWithOptionalNumeric(VtlParser.UnaryWithOptionalNumericContext ctx) {
    requireDatasetVarId(ctx.expr(), "functions");
    return null;
  }

  @Override
  public Void visitBinaryNumeric(VtlParser.BinaryNumericContext ctx) {
    throw unsupported("functions");
  }

  @Override
  public Void visitMembershipExpr(VtlParser.MembershipExprContext ctx) {
    requireDatasetOrClause(ctx.expr());
    return null;
  }

  @Override
  public Void visitValidationFunctions(VtlParser.ValidationFunctionsContext ctx) {
    return visit(ctx.validationOperators());
  }

  @Override
  public Void visitValidateDPruleset(VtlParser.ValidateDPrulesetContext ctx) {
    if (ctx.componentID() != null && !ctx.componentID().isEmpty()) {
      throw unsupported("check");
    }
    requireDatasetVarId(ctx.op, "check");
    return null;
  }

  @Override
  public Void visitValidateHRruleset(VtlParser.ValidateHRrulesetContext ctx) {
    throw unsupported("check");
  }

  @Override
  public Void visitValidationSimple(VtlParser.ValidationSimpleContext ctx) {
    requireDatasetVarId(ctx.expr(), "check");
    if (ctx.imbalanceExpr() != null) {
      requireDatasetVarId(ctx.imbalanceExpr().expr(), "check");
    }
    return null;
  }

  @Override
  public Void visitJoinFunctions(VtlParser.JoinFunctionsContext ctx) {
    return visit(ctx.joinOperators());
  }

  @Override
  public Void visitJoinExpr(VtlParser.JoinExprContext ctx) {
    for (VtlParser.JoinClauseItemContext item : joinItems(ctx)) {
      if (item.AS() != null) {
        throw unsupported("join");
      }
      requireDatasetVarId(item.expr(), "join");
    }
    checkJoinBody(ctx.joinBody());
    return null;
  }

  @Override
  public Void visitSetFunctions(VtlParser.SetFunctionsContext ctx) {
    return visit(ctx.setOperators());
  }

  @Override
  public Void visitUnionAtom(VtlParser.UnionAtomContext ctx) {
    ctx.expr().forEach(e -> requireDatasetVarId(e, "set"));
    return null;
  }

  @Override
  public Void visitIntersectAtom(VtlParser.IntersectAtomContext ctx) {
    ctx.expr().forEach(e -> requireDatasetVarId(e, "set"));
    return null;
  }

  @Override
  public Void visitSetOrSYmDiffAtom(VtlParser.SetOrSYmDiffAtomContext ctx) {
    requireDatasetVarId(ctx.left, "set");
    requireDatasetVarId(ctx.right, "set");
    return null;
  }

  @Override
  public Void visitConstantExpr(VtlParser.ConstantExprContext ctx) {
    throw unsupported("scalar");
  }

  /** Dataset name only (no nested clause / expression operands yet). */
  private void requireDatasetVarId(VtlParser.ExprContext expr, String what) {
    if (!(unwrap(expr) instanceof VtlParser.VarIdExprContext)) {
      throw unsupported(what);
    }
  }

  static List<VtlParser.JoinClauseItemContext> joinItems(VtlParser.JoinExprContext ctx) {
    if (ctx.joinClause() != null) {
      return ctx.joinClause().joinClauseItem();
    }
    return ctx.joinClauseWithoutUsing().joinClauseItem();
  }

  private void checkJoinBody(VtlParser.JoinBodyContext body) {
    if (body == null) {
      return;
    }
    if (body.filterClause() != null) {
      requireScalarPredicate(body.filterClause().expr());
    }
    if (body.calcClause() != null) {
      body.calcClause().calcClauseItem().forEach(item -> calcRhs(item.expr()));
    }
    if (body.joinApplyClause() != null) {
      requireScalarPredicate(body.joinApplyClause().expr());
    }
    if (body.aggrClause() != null && body.aggrClause().havingClause() != null) {
      requireScalarPredicate(body.aggrClause().havingClause().expr());
    }
  }

  /** Dataset name or scalar literal; nested ops deferred. */
  private void leafOperand(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = unwrap(expr);
    if (current instanceof VtlParser.VarIdExprContext
        || current instanceof VtlParser.ConstantExprContext) {
      return;
    }
    throw unsupported("arithmetic");
  }

  /** Dataset name, nested clause chain ({@code ds[…][…]}), or join frame ({@code join(…)[…]}). */
  private void requireDatasetOrClause(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = unwrap(expr);
    if (current instanceof VtlParser.VarIdExprContext) {
      return;
    }
    if (current instanceof VtlParser.ClauseExprContext clause) {
      visit(clause);
      return;
    }
    // Clauses after a join: {@code inner_join(…)[calc…][drop…]} (BPE).
    if (current instanceof VtlParser.FunctionsExpressionContext functions
        && functions.functions() instanceof VtlParser.JoinFunctionsContext) {
      visit(functions);
      return;
    }
    throw unsupported("clause");
  }

  /**
   * Scalar expression allow-list for calc / filter / sub / aggr args (Wave A). Rejects nested
   * dataset clauses and dataset-level producers; walks everything else so {@code cast}, {@code
   * if}, string/numeric/time scalars, comparisons, etc. are covered without per-op PendingOps.
   */
  private void calcRhs(VtlParser.ExprContext expr) {
    requireScalarExpr(expr);
  }

  /** Filter / sub predicates share the scalar allow-list (nested clauses still {@code clause}). */
  private void requireScalarPredicate(VtlParser.ExprContext expr) {
    requireScalarExpr(expr);
  }

  private void requireScalarExpr(VtlParser.ExprContext expr) {
    new VtlBaseVisitor<Void>() {
      @Override
      public Void visitClauseExpr(VtlParser.ClauseExprContext ctx) {
        throw unsupported("clause");
      }

      @Override
      public Void visitMembershipExpr(VtlParser.MembershipExprContext ctx) {
        throw unsupported("calc");
      }

      @Override
      public Void visitFunctionsExpression(VtlParser.FunctionsExpressionContext ctx) {
        var functions = ctx.functions();
        if (functions instanceof VtlParser.JoinFunctionsContext
            || functions instanceof VtlParser.SetFunctionsContext
            || functions instanceof VtlParser.ValidationFunctionsContext
            || functions instanceof VtlParser.HierarchyFunctionsContext
            || functions instanceof VtlParser.DistanceFunctionsContext) {
          throw unsupported("calc");
        }
        // AggregateFunctions allowed in having / scalar contexts (e.g. sum(m1) > 0).
        if (functions instanceof VtlParser.AggregateFunctionsContext) {
          return visitChildren(ctx);
        }
        if (functions instanceof VtlParser.TimeFunctionsContext time) {
          VtlParser.TimeOperatorsContext op = time.timeOperators();
          if (op instanceof VtlParser.FillTimeAtomContext
              || op instanceof VtlParser.FlowAtomContext
              || op instanceof VtlParser.TimeShiftAtomContext
              || op instanceof VtlParser.TimeAggAtomContext) {
            throw unsupported("calc");
          }
          return visitChildren(ctx);
        }
        if (functions instanceof VtlParser.ComparisonFunctionsContext comparison) {
          if (comparison.comparisonOperators() instanceof VtlParser.ExistInAtomContext) {
            throw unsupported("calc");
          }
          return visitChildren(ctx);
        }
        if (functions instanceof VtlParser.GenericFunctionsContext generic) {
          VtlParser.GenericOperatorsContext op = generic.genericOperators();
          if (op instanceof VtlParser.EvalAtomContext) {
            throw unsupported("calc");
          }
          if (op instanceof VtlParser.CallDatasetContext call) {
            requireKnownUdoCall(call);
            return null;
          }
          // castExprDataset and other generic scalars
          return visitChildren(ctx);
        }
        // string / numeric / conditional / analytic
        return visitChildren(ctx);
      }
    }.visit(expr);
  }

  /** Scalar UDO call in calc: known operator, args are varId or constant only. */
  private void requireKnownUdoCall(VtlParser.CallDatasetContext call) {
    if (!symbols.isUserOperator(call.operatorID().getText())) {
      throw unsupported("calc");
    }
    for (VtlParser.ParameterContext parameter : call.parameter()) {
      if (parameter.OPTIONAL() != null) {
        throw unsupported("calc");
      }
    }
  }

  static VtlParser.ExprContext unwrap(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = expr;
    while (current instanceof VtlParser.ParenthesisExprContext parenthesis) {
      current = parenthesis.expr();
    }
    return current;
  }

  static UnsupportedOperationException unsupported(String what) {
    return new UnsupportedOperationException("unsupported: " + what);
  }
}
