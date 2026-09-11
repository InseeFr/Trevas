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
 * {@code aggr}, {@code join}, {@code set}, {@code functions} (remaining gaps: bare {@code count()},
 * {@code rank(over …)} without a dataset — intentional fail-loud, not dataset producers alone —,
 * unknown UDO, …), {@code check}. Bare constants are allowed ({@code x := 1}); dataset contexts
 * still reject them via {@link #requireDatasetOperand}. Pivot without table {@code $input} rows
 * stays {@code unsupported: clause} (data-dependent).
 *
 * <p><b>Dataset operands:</b> wherever a dataset is required, {@link #requireDatasetOperand}
 * rejects constants and otherwise {@code visit}s the expression — nested producers are validated by
 * their own visit methods; extraction materializes them as {@code #s…} anonymes. Component-wise
 * scalar functions (§5.13) use {@link #leafOperand} so pure-scalar assignments remain valid.
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
    if (ctx.functions() instanceof VtlParser.StringFunctionsContext string) {
      return visit(string);
    }
    if (ctx.functions() instanceof VtlParser.ConditionalFunctionsContext conditional) {
      return visit(conditional);
    }
    if (ctx.functions() instanceof VtlParser.DistanceFunctionsContext distance) {
      return visit(distance);
    }
    if (ctx.functions() instanceof VtlParser.HierarchyFunctionsContext hierarchy) {
      return visit(hierarchy);
    }
    if (ctx.functions() instanceof VtlParser.TimeFunctionsContext time) {
      return visit(time);
    }
    if (ctx.functions() instanceof VtlParser.ComparisonFunctionsContext comparison) {
      return visit(comparison);
    }
    if (ctx.functions() instanceof VtlParser.GenericFunctionsContext generic) {
      if (generic.genericOperators() instanceof VtlParser.EvalAtomContext eval) {
        return visit(eval);
      }
      if (generic.genericOperators() instanceof VtlParser.CallDatasetContext call) {
        return visit(call);
      }
      if (generic.genericOperators() instanceof VtlParser.CastExprDatasetContext cast) {
        return visit(cast);
      }
      throw unsupported("functions");
    }
    if (ctx.functions() instanceof VtlParser.AggregateFunctionsContext aggregate) {
      return visit(aggregate);
    }
    if (ctx.functions() instanceof VtlParser.AnalyticFunctionsContext analytic) {
      return visit(analytic);
    }
    throw unsupported("functions");
  }

  /**
   * Dataset-returning UDO call as a producer ({@code res := scale_by(ds, 3)}). Scalar UDOs as
   * statement RHS ({@code y := add1(1)}) are allowed here; inside calc they go through {@link
   * #requireKnownUdoCall}.
   *
   * <p>Unknown operator names (Java {@code registerMethod} / global natives not declared with
   * {@code define operator}) are treated as external black-box producers — args must be {@code
   * varID} or constant only (same surface as {@code eval}).
   */
  @Override
  public Void visitCallDataset(VtlParser.CallDatasetContext ctx) {
    ScriptSymbols.UserOperator udo = symbols.userOperator(ctx.operatorID().getText());
    if (udo != null && !udo.returnsDataset()) {
      for (VtlParser.ParameterContext parameter : ctx.parameter()) {
        if (parameter.OPTIONAL() != null) {
          throw unsupported("functions");
        }
      }
      return null;
    }
    if (udo != null) {
      requireDatasetUdoCall(ctx);
      return null;
    }
    requireExternalCallArgs(ctx);
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
    // Constants alone OK (black-box empty structure); varIDs must be dataset names when present.
    return null;
  }

  @Override
  public Void visitAggregateFunctions(VtlParser.AggregateFunctionsContext ctx) {
    return visit(ctx.aggrOperatorsGrouping());
  }

  @Override
  public Void visitAggrDataset(VtlParser.AggrDatasetContext ctx) {
    requireDatasetOperand(ctx.expr(), "functions");
    if (ctx.havingClause() != null) {
      requireScalarPredicate(ctx.havingClause().expr());
    }
    return null;
  }

  @Override
  public Void visitCountAggr(VtlParser.CountAggrContext ctx) {
    // Wave G PR-53: bare count() is only valid inside an aggr clause / analytic window with a
    // dataset operand — never a stand-alone dataset producer. Keep fail-loud.
    throw unsupported("functions");
  }

  @Override
  public Void visitAnalyticFunctions(VtlParser.AnalyticFunctionsContext ctx) {
    return visit(ctx.anFunction());
  }

  @Override
  public Void visitAnSimpleFunction(VtlParser.AnSimpleFunctionContext ctx) {
    requireAnalyticOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitLagOrLeadAn(VtlParser.LagOrLeadAnContext ctx) {
    requireAnalyticOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitRatioToReportAn(VtlParser.RatioToReportAnContext ctx) {
    requireAnalyticOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitRankAn(VtlParser.RankAnContext ctx) {
    // Wave G PR-54: rank(over …) has no dataset operand — only valid inside calc / with a dataset
    // analytic form. Keep fail-loud.
    throw unsupported("functions");
  }

  /** Analytic window needs a dataset-valued expr (name, membership, nested producer). */
  private void requireAnalyticOperand(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = unwrap(expr);
    if (current instanceof VtlParser.ConstantExprContext) {
      throw unsupported("functions");
    }
    visit(current);
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
    return visit(ctx.timeOperators());
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
    // Dataset form uses optionalExpr as the operand when present; bare time_agg("A") is scalar.
    if (ctx.op == null || ctx.op.expr() == null) {
      return null;
    }
    requireDatasetOperand(ctx.op.expr(), "functions");
    return null;
  }

  /** Date/time scalars as dataset producers (§5.13) or pure scalars (assignment). */
  @Override
  public Void visitPeriodAtom(VtlParser.PeriodAtomContext ctx) {
    if (ctx.expr() != null) {
      leafOperand(ctx.expr());
    }
    return null;
  }

  @Override
  public Void visitCurrentDateAtom(VtlParser.CurrentDateAtomContext ctx) {
    return null;
  }

  @Override
  public Void visitDateDiffAtom(VtlParser.DateDiffAtomContext ctx) {
    leafOperand(ctx.dateFrom);
    leafOperand(ctx.dateTo);
    return null;
  }

  @Override
  public Void visitDateAddAtom(VtlParser.DateAddAtomContext ctx) {
    leafOperand(ctx.op);
    leafOperand(ctx.shiftNumber);
    leafOperand(ctx.periodInd);
    return null;
  }

  @Override
  public Void visitYearAtom(VtlParser.YearAtomContext ctx) {
    leafOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitMonthAtom(VtlParser.MonthAtomContext ctx) {
    leafOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitDayOfMonthAtom(VtlParser.DayOfMonthAtomContext ctx) {
    leafOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitDayOfYearAtom(VtlParser.DayOfYearAtomContext ctx) {
    leafOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitDayToYearAtom(VtlParser.DayToYearAtomContext ctx) {
    leafOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitDayToMonthAtom(VtlParser.DayToMonthAtomContext ctx) {
    leafOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitYearTodayAtom(VtlParser.YearTodayAtomContext ctx) {
    leafOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitMonthTodayAtom(VtlParser.MonthTodayAtomContext ctx) {
    leafOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitNumericFunctions(VtlParser.NumericFunctionsContext ctx) {
    return visit(ctx.numericOperators());
  }

  @Override
  public Void visitStringFunctions(VtlParser.StringFunctionsContext ctx) {
    return visit(ctx.stringOperators());
  }

  @Override
  public Void visitConditionalFunctions(VtlParser.ConditionalFunctionsContext ctx) {
    return visit(ctx.conditionalOperators());
  }

  @Override
  public Void visitDistanceFunctions(VtlParser.DistanceFunctionsContext ctx) {
    return visit(ctx.distanceOperators());
  }

  @Override
  public Void visitComparisonFunctions(VtlParser.ComparisonFunctionsContext ctx) {
    return visit(ctx.comparisonOperators());
  }

  @Override
  public Void visitUnaryNumeric(VtlParser.UnaryNumericContext ctx) {
    leafOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitUnaryWithOptionalNumeric(VtlParser.UnaryWithOptionalNumericContext ctx) {
    leafOperand(ctx.expr());
    if (ctx.optionalExpr() != null && ctx.optionalExpr().expr() != null) {
      leafOperand(ctx.optionalExpr().expr());
    }
    return null;
  }

  @Override
  public Void visitBinaryNumeric(VtlParser.BinaryNumericContext ctx) {
    leafOperand(ctx.left);
    leafOperand(ctx.right);
    return null;
  }

  @Override
  public Void visitUnaryStringFunction(VtlParser.UnaryStringFunctionContext ctx) {
    leafOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitSubstrAtom(VtlParser.SubstrAtomContext ctx) {
    leafOperand(ctx.expr());
    if (ctx.startParameter != null && ctx.startParameter.expr() != null) {
      leafOperand(ctx.startParameter.expr());
    }
    if (ctx.endParameter != null && ctx.endParameter.expr() != null) {
      leafOperand(ctx.endParameter.expr());
    }
    return null;
  }

  @Override
  public Void visitReplaceAtom(VtlParser.ReplaceAtomContext ctx) {
    leafOperand(ctx.expr(0));
    leafOperand(ctx.param);
    if (ctx.optionalExpr() != null && ctx.optionalExpr().expr() != null) {
      leafOperand(ctx.optionalExpr().expr());
    }
    return null;
  }

  @Override
  public Void visitInstrAtom(VtlParser.InstrAtomContext ctx) {
    leafOperand(ctx.expr(0));
    leafOperand(ctx.pattern);
    if (ctx.startParameter != null && ctx.startParameter.expr() != null) {
      leafOperand(ctx.startParameter.expr());
    }
    if (ctx.occurrenceParameter != null && ctx.occurrenceParameter.expr() != null) {
      leafOperand(ctx.occurrenceParameter.expr());
    }
    return null;
  }

  @Override
  public Void visitNvlAtom(VtlParser.NvlAtomContext ctx) {
    leafOperand(ctx.left);
    leafOperand(ctx.right);
    return null;
  }

  @Override
  public Void visitBetweenAtom(VtlParser.BetweenAtomContext ctx) {
    leafOperand(ctx.op);
    leafOperand(ctx.from_);
    leafOperand(ctx.to_);
    return null;
  }

  @Override
  public Void visitCharsetMatchAtom(VtlParser.CharsetMatchAtomContext ctx) {
    leafOperand(ctx.op);
    leafOperand(ctx.pattern);
    return null;
  }

  @Override
  public Void visitIsNullAtom(VtlParser.IsNullAtomContext ctx) {
    leafOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitLevenshteinAtom(VtlParser.LevenshteinAtomContext ctx) {
    leafOperand(ctx.left);
    leafOperand(ctx.right);
    return null;
  }

  @Override
  public Void visitCastExprDataset(VtlParser.CastExprDatasetContext ctx) {
    leafOperand(ctx.expr());
    return null;
  }

  @Override
  public Void visitInNotInExpr(VtlParser.InNotInExprContext ctx) {
    leafOperand(ctx.left);
    return null;
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
    if (!symbols.isHierarchicalRuleset(ctx.hrName.getText())) {
      throw unsupported("check");
    }
    requireDatasetOperand(ctx.op, "check");
    return null;
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
      // {@code AS} aliases rename the join-body binding; provenance still uses item.expr().
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

  /**
   * Constants are valid in scalar contexts ({@code x := 1}, calc RHS, …). Dataset contexts reject
   * them via {@link #requireDatasetOperand} / {@link #leafOperand} before a bare constant is
   * visited as a producer.
   */
  @Override
  public Void visitConstantExpr(VtlParser.ConstantExprContext ctx) {
    return null;
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
        // Join-body / calc qualifier {@code ds#comp}: left must be a simple name (dataset or
        // join alias), not a nested producer. Lineage collects the component only.
        VtlParser.ExprContext left = unwrap(ctx.expr());
        if (!(left instanceof VtlParser.VarIdExprContext)) {
          throw unsupported("calc");
        }
        return null;
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

  /**
   * Registered / unknown call ({@code loadCSV("…")}, …): only {@code varID} / constant args — no
   * nested {@code _} optional.
   */
  private void requireExternalCallArgs(VtlParser.CallDatasetContext call) {
    for (VtlParser.ParameterContext parameter : call.parameter()) {
      if (parameter.OPTIONAL() != null) {
        throw unsupported("functions");
      }
      if (parameter.varID() == null && parameter.constant() == null) {
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
