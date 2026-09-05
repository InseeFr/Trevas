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
    // Black-box: register the name; do not walk the body.
    symbols.addUserOperator(ctx.operatorID().getText());
    return null;
  }

  @Override
  public Void visitDefHierarchical(VtlParser.DefHierarchicalContext ctx) {
    throw unsupported("define");
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
      // having / group except|all: Phase 2 PR-27 — fail loud until covered.
      if (clause.aggrClause().havingClause() != null) {
        throw unsupported("aggr");
      }
      return null;
    }
    if (clause.customPivotClause() != null) {
      throw unsupported("clause");
    }
    if (clause.subspaceClause() != null
        || clause.keepOrDropClause() != null
        || clause.renameClause() != null) {
      return null;
    }
    if (clause.pivotOrUnpivotClause() != null) {
      if (clause.pivotOrUnpivotClause().op.getType() == VtlParser.UNPIVOT) {
        throw unsupported("clause");
      }
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
    throw unsupported("functions");
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
    throw unsupported("check");
  }

  @Override
  public Void visitJoinFunctions(VtlParser.JoinFunctionsContext ctx) {
    return visit(ctx.joinOperators());
  }

  @Override
  public Void visitJoinExpr(VtlParser.JoinExprContext ctx) {
    requireEmptyJoinBody(ctx.joinBody());
    for (VtlParser.JoinClauseItemContext item : joinItems(ctx)) {
      if (item.AS() != null) {
        throw unsupported("join");
      }
      requireDatasetVarId(item.expr(), "join");
    }
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

  static void requireEmptyJoinBody(VtlParser.JoinBodyContext body) {
    if (body == null) {
      return;
    }
    if (body.filterClause() != null
        || body.calcClause() != null
        || body.joinApplyClause() != null
        || body.aggrClause() != null
        || body.keepOrDropClause() != null
        || body.renameClause() != null) {
      throw unsupported("join");
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

  /** Dataset name, or a nested clause chain ({@code ds[…][…]}). */
  private void requireDatasetOrClause(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = unwrap(expr);
    if (current instanceof VtlParser.VarIdExprContext) {
      return;
    }
    if (current instanceof VtlParser.ClauseExprContext clause) {
      visit(clause);
      return;
    }
    throw unsupported("clause");
  }

  /** Component-level calc RHS (not dataset-level arithmetic). */
  private void calcRhs(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = unwrap(expr);
    if (current instanceof VtlParser.VarIdExprContext
        || current instanceof VtlParser.ConstantExprContext) {
      return;
    }
    if (current instanceof VtlParser.ArithmeticExprContext arithmetic) {
      calcRhs(arithmetic.left);
      calcRhs(arithmetic.right);
      return;
    }
    if (current instanceof VtlParser.ArithmeticExprOrConcatContext arithmetic) {
      calcRhs(arithmetic.left);
      calcRhs(arithmetic.right);
      return;
    }
    if (current instanceof VtlParser.FunctionsExpressionContext functions
        && functions.functions() instanceof VtlParser.AnalyticFunctionsContext) {
      return;
    }
    if (current instanceof VtlParser.FunctionsExpressionContext functions
        && functions.functions() instanceof VtlParser.GenericFunctionsContext generic
        && generic.genericOperators() instanceof VtlParser.CallDatasetContext call) {
      requireKnownUdoCall(call);
      return;
    }
    throw unsupported("calc");
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

  /** Filter predicates may use functions/comparisons; reject nested dataset clauses only. */
  private void requireScalarPredicate(VtlParser.ExprContext expr) {
    new VtlBaseVisitor<Void>() {
      @Override
      public Void visitClauseExpr(VtlParser.ClauseExprContext ctx) {
        throw unsupported("clause");
      }
    }.visit(expr);
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
