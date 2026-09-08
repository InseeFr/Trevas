package fr.insee.vtl.prov.extract;

import fr.insee.vtl.antlr.runtime.tree.RuleNode;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Grammar-only support gate: throws {@code unsupported: …} before the structure oracle runs, so the
 * corpus backlog stays explicit even when the engine cannot eval the script.
 *
 * <p>Registers {@code define operator} / datapoint ruleset names into the shared {@link
 * ScriptSymbols}. Mirrors {@link ProvenanceVisitor} coverage. Message vocabulary (stable for
 * harness / ops): {@code define}, {@code scalar}, {@code arithmetic}, {@code clause}, {@code calc},
 * {@code aggr}, {@code join}, {@code set}, {@code functions} (catch-all for other function
 * families), {@code check}.
 *
 * <p><b>Dataset operands:</b> wherever a dataset is required, {@link #requireDatasetOperand}
 * rejects constants and otherwise {@code visit}s the expression — nested producers are validated by
 * their own visit methods; extraction materializes them as {@code #s…} anonymes.
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
    Set<String> datasetParams = new LinkedHashSet<>();
    for (VtlParser.ParameterItemContext item : ctx.parameterItem()) {
      String name = item.varID().getText();
      params.add(name);
      if (item.inputParameterType().datasetType() != null) {
        datasetParams.add(name);
      }
    }
    symbols.putUserOperator(
        ctx.operatorID().getText(),
        params,
        datasetParams,
        returnsDataset(ctx, datasetParams),
        ctx.expr());
    return null;
  }

  /**
   * Dataset producer when {@code RETURNS dataset} is declared, or when the body is clearly a
   * dataset expression (clause / membership / dataset formal).
   */
  private static boolean returnsDataset(
      VtlParser.DefOperatorContext ctx, Set<String> datasetParams) {
    if (ctx.outputParameterType() != null && ctx.outputParameterType().datasetType() != null) {
      return true;
    }
    VtlParser.ExprContext body = unwrap(ctx.expr());
    if (body instanceof VtlParser.ClauseExprContext
        || body instanceof VtlParser.MembershipExprContext) {
      return true;
    }
    if (body instanceof VtlParser.VarIdExprContext varId
        && datasetParams.contains(varId.varID().getText())) {
      return true;
    }
    return false;
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
  public Void visitComparisonExpr(VtlParser.ComparisonExprContext ctx) {
    leafOperand(ctx.left);
    leafOperand(ctx.right);
    return null;
  }

  @Override
  public Void visitBooleanExpr(VtlParser.BooleanExprContext ctx) {
    leafOperand(ctx.left);
    leafOperand(ctx.right);
    return null;
  }

  @Override
  public Void visitUnaryExpr(VtlParser.UnaryExprContext ctx) {
    // Dataset-level +/−/not: operand may itself be a nested producer.
    requireDatasetOperand(ctx.right, "arithmetic");
    return null;
  }

  @Override
  public Void visitIfExpr(VtlParser.IfExprContext ctx) {
    // Dataset-level if: each branch may be a nested producer or a scalar literal.
    leafOperand(ctx.conditionalExpr);
    leafOperand(ctx.thenExpr);
    leafOperand(ctx.elseExpr);
    return null;
  }

  @Override
  public Void visitClauseExpr(VtlParser.ClauseExprContext ctx) {
    // Left of […] is any dataset producer (name, nested clause, set, join, arith, …).
    requireDatasetOperand(ctx.expr(), "clause");
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
      if (generic.genericOperators() instanceof VtlParser.CallDatasetContext call) {
        return visit(call);
      }
      throw unsupported("functions");
    }
    throw unsupported("functions");
  }

  /**
   * Dataset-returning UDO call as a producer ({@code res := scale_by(ds, 3)}). Scalar UDOs stay
   * calc-only via {@link #requireKnownUdoCall}.
   */
  @Override
  public Void visitCallDataset(VtlParser.CallDatasetContext ctx) {
    requireDatasetUdoCall(ctx);
    return null;
  }

  @Override
  public Void visitExistInAtom(VtlParser.ExistInAtomContext ctx) {
    requireDatasetOperand(ctx.left, "functions");
    requireDatasetOperand(ctx.right, "functions");
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
    requireDatasetOperand(ctx.op, "functions");
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
    requireDatasetOperand(ctx.expr(), "functions");
    return null;
  }

  @Override
  public Void visitFillTimeAtom(VtlParser.FillTimeAtomContext ctx) {
    requireDatasetOperand(ctx.expr(), "functions");
    return null;
  }

  @Override
  public Void visitTimeShiftAtom(VtlParser.TimeShiftAtomContext ctx) {
    requireDatasetOperand(ctx.expr(), "functions");
    return null;
  }

  @Override
  public Void visitTimeAggAtom(VtlParser.TimeAggAtomContext ctx) {
    // Dataset form uses optionalExpr as the operand when present.
    if (ctx.op == null || ctx.op.expr() == null) {
      throw unsupported("functions");
    }
    requireDatasetOperand(ctx.op.expr(), "functions");
    return null;
  }

  @Override
  public Void visitNumericFunctions(VtlParser.NumericFunctionsContext ctx) {
    return visit(ctx.numericOperators());
  }

  @Override
  public Void visitUnaryNumeric(VtlParser.UnaryNumericContext ctx) {
    requireDatasetOperand(ctx.expr(), "functions");
    return null;
  }

  @Override
  public Void visitUnaryWithOptionalNumeric(VtlParser.UnaryWithOptionalNumericContext ctx) {
    requireDatasetOperand(ctx.expr(), "functions");
    return null;
  }

  @Override
  public Void visitBinaryNumeric(VtlParser.BinaryNumericContext ctx) {
    throw unsupported("functions");
  }

  @Override
  public Void visitMembershipExpr(VtlParser.MembershipExprContext ctx) {
    requireDatasetOperand(ctx.expr(), "clause");
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
    requireDatasetOperand(ctx.op, "check");
    return null;
  }

  @Override
  public Void visitValidateHRruleset(VtlParser.ValidateHRrulesetContext ctx) {
    throw unsupported("check");
  }

  @Override
  public Void visitValidationSimple(VtlParser.ValidationSimpleContext ctx) {
    requireDatasetOperand(ctx.expr(), "check");
    if (ctx.imbalanceExpr() != null) {
      requireDatasetOperand(ctx.imbalanceExpr().expr(), "check");
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
      requireDatasetOperand(item.expr(), "join");
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
    ctx.expr().forEach(e -> requireDatasetOperand(e, "set"));
    return null;
  }

  @Override
  public Void visitIntersectAtom(VtlParser.IntersectAtomContext ctx) {
    ctx.expr().forEach(e -> requireDatasetOperand(e, "set"));
    return null;
  }

  @Override
  public Void visitSetOrSYmDiffAtom(VtlParser.SetOrSYmDiffAtomContext ctx) {
    requireDatasetOperand(ctx.left, "set");
    requireDatasetOperand(ctx.right, "set");
    return null;
  }

  @Override
  public Void visitConstantExpr(VtlParser.ConstantExprContext ctx) {
    throw unsupported("scalar");
  }

  /**
   * Anywhere a dataset is required: reject constants, otherwise visit the expression. Nested
   * producers (clause / set / join / arith / UDO / …) are validated by their own {@code visit*}
   * methods; extraction materializes non-identity results as {@code #s…} anonymes.
   */
  private void requireDatasetOperand(VtlParser.ExprContext expr, String what) {
    VtlParser.ExprContext current = unwrap(expr);
    if (current instanceof VtlParser.ConstantExprContext) {
      throw unsupported(what);
    }
    visit(current);
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

  /** Arithmetic leaf: scalar literal OK, otherwise a dataset producer. */
  private void leafOperand(VtlParser.ExprContext expr) {
    if (unwrap(expr) instanceof VtlParser.ConstantExprContext) {
      return;
    }
    requireDatasetOperand(expr, "arithmetic");
  }

  /**
   * Scalar expression allow-list for calc / filter / sub / aggr args (Wave A). Rejects nested
   * dataset clauses and dataset-level producers; walks everything else so {@code cast}, {@code if},
   * string/numeric/time scalars, comparisons, etc. are covered without per-op PendingOps.
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

  /** Scalar UDO call in calc: known scalar operator, args are varId or constant only. */
  private void requireKnownUdoCall(VtlParser.CallDatasetContext call) {
    ScriptSymbols.UserOperator udo = symbols.userOperator(call.operatorID().getText());
    if (udo == null || udo.returnsDataset()) {
      throw unsupported("calc");
    }
    for (VtlParser.ParameterContext parameter : call.parameter()) {
      if (parameter.OPTIONAL() != null) {
        throw unsupported("calc");
      }
    }
  }

  /** Dataset UDO call as producer: known returns-dataset operator; args varID/constant only. */
  private void requireDatasetUdoCall(VtlParser.CallDatasetContext call) {
    ScriptSymbols.UserOperator udo = symbols.userOperator(call.operatorID().getText());
    if (udo == null || !udo.returnsDataset()) {
      throw unsupported("functions");
    }
    List<VtlParser.ParameterContext> args = call.parameter();
    for (int i = 0; i < udo.params().size(); i++) {
      if (i >= args.size()) {
        break;
      }
      VtlParser.ParameterContext arg = args.get(i);
      if (arg.OPTIONAL() != null) {
        throw unsupported("functions");
      }
      String formal = udo.params().get(i);
      if (udo.datasetParams().contains(formal)) {
        if (arg.varID() == null) {
          throw unsupported("functions");
        }
      } else if (arg.varID() == null && arg.constant() == null) {
        throw unsupported("functions");
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
