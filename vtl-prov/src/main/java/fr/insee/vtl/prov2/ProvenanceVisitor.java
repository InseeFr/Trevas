package fr.insee.vtl.prov2;

import fr.insee.vtl.antlr.runtime.CharStream;
import fr.insee.vtl.antlr.runtime.ParserRuleContext;
import fr.insee.vtl.antlr.runtime.Token;
import fr.insee.vtl.antlr.runtime.misc.Interval;
import fr.insee.vtl.engine.utils.DefaultMeasureNames;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import fr.insee.vtl.prov.utils.VTLTypes;
import fr.insee.vtl.prov2.PendingOp.Aggr;
import fr.insee.vtl.prov2.PendingOp.Apply;
import fr.insee.vtl.prov2.PendingOp.Arithmetic;
import fr.insee.vtl.prov2.PendingOp.Calc;
import fr.insee.vtl.prov2.PendingOp.Check;
import fr.insee.vtl.prov2.PendingOp.CheckDatapoint;
import fr.insee.vtl.prov2.PendingOp.Drop;
import fr.insee.vtl.prov2.PendingOp.ExistsIn;
import fr.insee.vtl.prov2.PendingOp.Filter;
import fr.insee.vtl.prov2.PendingOp.Identity;
import fr.insee.vtl.prov2.PendingOp.Join;
import fr.insee.vtl.prov2.PendingOp.Keep;
import fr.insee.vtl.prov2.PendingOp.Membership;
import fr.insee.vtl.prov2.PendingOp.PassThrough;
import fr.insee.vtl.prov2.PendingOp.Pivot;
import fr.insee.vtl.prov2.PendingOp.Rename;
import fr.insee.vtl.prov2.PendingOp.SetOp;
import fr.insee.vtl.prov2.PendingOp.Sub;
import fr.insee.vtl.prov2.PendingOp.Unpivot;
import fr.insee.vtl.testutils.InputDataset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Grammar-driven provenance walk ({@code VtlBaseVisitor<Void>}). Extends {@link
 * SupportCheckVisitor} for the shared {@code unsupported: …} surface; mutates a shared {@link
 * ProvGraph}.
 *
 * <p>{@code T = Void}: the graph is the artifact. After visiting an expression, {@link #pending}
 * holds a {@link PendingOp}. Nested clauses materialize anonymous intermediates ({@code
 * #s{stmt}.{seq}}) when the left expression is already a non-identity op. Structure and edges are
 * delegated to {@link StructureDeriver} / {@link EdgeLinker}; defines are registered once in {@link
 * ScriptSymbols} during support-check.
 */
final class ProvenanceVisitor extends SupportCheckVisitor {

  private final ProvGraph graph;
  private final StructureOracle oracle;
  private final ScriptSymbols symbols;
  private final StructureDeriver deriver;
  private final EdgeLinker linker;

  /** Versioned dataset id → binding rows (for data-dependent ops such as pivot). */
  private final Map<String, InputDataset> bindingsWithRows = new LinkedHashMap<>();

  private final Map<String, String> versions = new LinkedHashMap<>();

  /** Scalar name → versioned id ({@code x@1}); separate from dataset {@link #versions}. */
  private final Map<String, String> scalarVersions = new LinkedHashMap<>();

  private final Map<String, DataStructure> structures = new LinkedHashMap<>();
  private int stmtIndex;
  private int exprSeq;
  private int anonSeq;

  /** When true, assignment structure comes from {@link StructureDeriver} only (join apply, …). */
  private boolean forceDerive;

  /** Outcome of the last visited expression; never null after a successful expr visit. */
  private PendingOp pending;

  ProvenanceVisitor(
      ProvGraph graph, StructureOracle oracle, List<InputDataset> inputs, ScriptSymbols symbols) {
    super(symbols);
    this.graph = graph;
    this.oracle = oracle;
    this.symbols = symbols;
    this.deriver = new StructureDeriver(structures::get);
    this.linker = new EdgeLinker(graph, structures::get);
    for (InputDataset input : inputs) {
      String id = input.name() + "@0";
      versions.put(input.name(), id);
      if (!input.rows().isEmpty()) {
        bindingsWithRows.put(id, input);
      }
      addDataset(id, oracle.requireDataset(input.name()), null, false);
    }
  }

  @Override
  public Void visitTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
    return assign(ctx.varID().getText(), ctx.expr());
  }

  @Override
  public Void visitPersistAssignment(VtlParser.PersistAssignmentContext ctx) {
    return assign(ctx.varID().getText(), ctx.expr());
  }

  @Override
  public Void visitDefineExpression(VtlParser.DefineExpressionContext ctx) {
    return visit(ctx.defOperators());
  }

  @Override
  public Void visitDefDatapointRuleset(VtlParser.DefDatapointRulesetContext ctx) {
    // Support-check already validated and registered the signature in ScriptSymbols.
    stmtIndex++;
    return null;
  }

  @Override
  public Void visitDefOperator(VtlParser.DefOperatorContext ctx) {
    // Support-check already registered the operator name in ScriptSymbols.
    stmtIndex++;
    return null;
  }

  @Override
  public Void visitDefHierarchical(VtlParser.DefHierarchicalContext ctx) {
    // Support-check already registered the ruleset name in ScriptSymbols.
    stmtIndex++;
    return null;
  }

  @Override
  public Void visitHierarchyOperators(VtlParser.HierarchyOperatorsContext ctx) {
    String srcId = datasetOperand(ctx.op);
    if (srcId == null) {
      throw unsupported("functions");
    }
    String ruleset = ctx.hrName.getText();
    if (!symbols.isHierarchicalRuleset(ruleset)) {
      throw new IllegalStateException("unknown hierarchical ruleset " + ruleset);
    }
    pending = new PassThrough(srcId, "hierarchy", ruleset);
    return null;
  }

  @Override
  public Void visitFlowAtom(VtlParser.FlowAtomContext ctx) {
    return unaryPassThrough(ctx.expr(), ctx.op.getText());
  }

  @Override
  public Void visitFillTimeAtom(VtlParser.FillTimeAtomContext ctx) {
    return unaryPassThrough(ctx.expr(), "fill_time_series");
  }

  @Override
  public Void visitTimeShiftAtom(VtlParser.TimeShiftAtomContext ctx) {
    return unaryPassThrough(ctx.expr(), "timeshift");
  }

  @Override
  public Void visitTimeAggAtom(VtlParser.TimeAggAtomContext ctx) {
    if (ctx.op == null || ctx.op.expr() == null) {
      throw unsupported("functions");
    }
    return unaryPassThrough(ctx.op.expr(), "time_agg");
  }

  private Void unaryPassThrough(VtlParser.ExprContext expr, String op) {
    String srcId = datasetOperand(expr);
    if (srcId == null) {
      throw unsupported("functions");
    }
    pending = new PassThrough(srcId, op);
    return null;
  }

  @Override
  public Void visitExistInAtom(VtlParser.ExistInAtomContext ctx) {
    String leftId = datasetOperand(ctx.left);
    String rightId = datasetOperand(ctx.right);
    if (leftId == null || rightId == null) {
      throw unsupported("functions");
    }
    pending = new ExistsIn(leftId, rightId);
    return null;
  }

  @Override
  public Void visitEvalAtom(VtlParser.EvalAtomContext ctx) {
    List<String> operands = new ArrayList<>();
    for (VtlParser.VarIDContext varId : ctx.varID()) {
      String id = versions.get(varId.getText());
      if (id == null || !structures.containsKey(id)) {
        throw unsupported("functions");
      }
      operands.add(id);
    }
    if (operands.isEmpty()) {
      throw unsupported("functions");
    }
    // Single operand: structure copy; several: component-wise like arithmetic.
    pending =
        operands.size() == 1
            ? new PassThrough(operands.get(0), "eval")
            : new Arithmetic("eval", List.copyOf(operands));
    return null;
  }

  @Override
  public Void visitVarIdExpr(VtlParser.VarIdExprContext ctx) {
    String name = ctx.varID().getText();
    String id = versions.get(name);
    if (id == null) {
      throw new IllegalStateException("unknown dataset " + name);
    }
    pending = new Identity(id);
    return null;
  }

  @Override
  public Void visitArithmeticExpr(VtlParser.ArithmeticExprContext ctx) {
    return binaryArithmetic(ctx.left, ctx.right, ctx.op);
  }

  @Override
  public Void visitArithmeticExprOrConcat(VtlParser.ArithmeticExprOrConcatContext ctx) {
    return binaryArithmetic(ctx.left, ctx.right, ctx.op);
  }

  @Override
  public Void visitJoinExpr(VtlParser.JoinExprContext ctx) {
    List<String> operands = new ArrayList<>();
    for (VtlParser.JoinClauseItemContext item : joinItems(ctx)) {
      if (item.AS() != null) {
        throw unsupported("join");
      }
      String operandId = datasetOperand(item.expr());
      if (operandId == null) {
        throw unsupported("join");
      }
      operands.add(operandId);
    }
    if (operands.size() < 2) {
      throw unsupported("join");
    }
    pending = new Join(ctx.joinKeyword.getText(), List.copyOf(operands));
    applyJoinBody(ctx);
    return null;
  }

  @Override
  public Void visitMembershipExpr(VtlParser.MembershipExprContext ctx) {
    visit(ctx.expr());
    ensureMaterialized();
    pending = new Membership(pending.focusId(), ctx.simpleComponentId().getText());
    return null;
  }

  @Override
  public Void visitUnaryNumeric(VtlParser.UnaryNumericContext ctx) {
    String srcId = datasetOperand(ctx.expr());
    if (srcId == null) {
      throw unsupported("functions");
    }
    pending = new Arithmetic(ctx.op.getText(), List.of(srcId));
    return null;
  }

  @Override
  public Void visitUnaryWithOptionalNumeric(VtlParser.UnaryWithOptionalNumericContext ctx) {
    String srcId = datasetOperand(ctx.expr());
    if (srcId == null) {
      throw unsupported("functions");
    }
    pending = new Arithmetic(ctx.op.getText(), List.of(srcId));
    return null;
  }

  @Override
  public Void visitUnionAtom(VtlParser.UnionAtomContext ctx) {
    return multiDatasetOp("union", ctx.expr());
  }

  @Override
  public Void visitIntersectAtom(VtlParser.IntersectAtomContext ctx) {
    return multiDatasetOp("intersect", ctx.expr());
  }

  @Override
  public Void visitSetOrSYmDiffAtom(VtlParser.SetOrSYmDiffAtomContext ctx) {
    return multiDatasetOp(ctx.op.getText(), List.of(ctx.left, ctx.right));
  }

  @Override
  public Void visitValidateDPruleset(VtlParser.ValidateDPrulesetContext ctx) {
    if (ctx.componentID() != null && !ctx.componentID().isEmpty()) {
      throw unsupported("check");
    }
    String srcId = datasetOperand(ctx.op);
    if (srcId == null) {
      throw unsupported("check");
    }
    String ruleset = ctx.dpName.getText();
    List<String> validated = symbols.datapointVariables(ruleset);
    if (validated == null) {
      throw new IllegalStateException("unknown datapoint ruleset " + ruleset);
    }
    pending = new CheckDatapoint(srcId, ruleset, validated);
    return null;
  }

  @Override
  public Void visitValidationSimple(VtlParser.ValidationSimpleContext ctx) {
    String srcId = datasetOperand(ctx.expr());
    if (srcId == null) {
      throw unsupported("check");
    }
    String imbalanceId = null;
    if (ctx.imbalanceExpr() != null) {
      imbalanceId = datasetOperand(ctx.imbalanceExpr().expr());
      if (imbalanceId == null) {
        throw unsupported("check");
      }
    }
    pending = new Check(srcId, imbalanceId);
    return null;
  }

  private Void multiDatasetOp(String op, List<? extends VtlParser.ExprContext> exprs) {
    List<String> operands = new ArrayList<>(exprs.size());
    for (VtlParser.ExprContext expr : exprs) {
      String operandId = datasetOperand(expr);
      if (operandId == null) {
        throw unsupported("set");
      }
      operands.add(operandId);
    }
    if (operands.size() < 2) {
      throw unsupported("set");
    }
    pending = new SetOp(op, List.copyOf(operands));
    return null;
  }

  @Override
  public Void visitClauseExpr(VtlParser.ClauseExprContext ctx) {
    visit(ctx.expr());
    // Left was itself a clause/op: emit anonymous intermediate before this clause.
    ensureMaterialized();
    String srcId = pending.focusId();
    VtlParser.DatasetClauseContext clause = ctx.datasetClause();
    if (clause.calcClause() != null) {
      return applyCalc(srcId, clause.calcClause());
    }
    if (clause.filterClause() != null) {
      return applyFilter(srcId, clause.filterClause());
    }
    if (clause.subspaceClause() != null) {
      return applySub(srcId, clause.subspaceClause());
    }
    if (clause.keepOrDropClause() != null) {
      return applyKeepOrDrop(srcId, clause.keepOrDropClause());
    }
    if (clause.renameClause() != null) {
      return applyRename(srcId, clause.renameClause());
    }
    if (clause.aggrClause() != null) {
      return applyAggr(srcId, clause.aggrClause());
    }
    if (clause.pivotOrUnpivotClause() != null) {
      return applyPivot(srcId, clause.pivotOrUnpivotClause());
    }
    if (clause.customPivotClause() != null) {
      return applyCustomPivot(srcId, clause.customPivotClause());
    }
    throw unsupported("clause");
  }

  private void applyJoinBody(VtlParser.JoinExprContext ctx) {
    VtlParser.JoinBodyContext body = ctx.joinBody();
    if (body == null
        || (body.filterClause() == null
            && body.calcClause() == null
            && body.joinApplyClause() == null
            && body.aggrClause() == null
            && body.keepOrDropClause() == null
            && body.renameClause() == null)) {
      return;
    }
    // Engine ignores join body — structure must come from PendingOp derive.
    forceDerive = true;
    String joinSrc = joinSourceFragment(ctx);
    materializeAnonymous(joinSrc);
    if (body.filterClause() != null) {
      applyFilter(pending.focusId(), body.filterClause());
    }
    if (body.calcClause() != null) {
      ensureMaterialized();
      applyCalc(pending.focusId(), body.calcClause());
    } else if (body.joinApplyClause() != null) {
      ensureMaterialized();
      applyJoinApply(pending.focusId(), body.joinApplyClause());
    } else if (body.aggrClause() != null) {
      ensureMaterialized();
      applyAggr(pending.focusId(), body.aggrClause());
    }
    if (body.keepOrDropClause() != null) {
      ensureMaterialized();
      applyKeepOrDrop(pending.focusId(), body.keepOrDropClause());
    }
    if (body.renameClause() != null) {
      ensureMaterialized();
      applyRename(pending.focusId(), body.renameClause());
    }
  }

  private String joinSourceFragment(VtlParser.JoinExprContext ctx) {
    StringBuilder sb = new StringBuilder();
    sb.append(ctx.joinKeyword.getText()).append("(");
    List<VtlParser.JoinClauseItemContext> items = joinItems(ctx);
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(text(items.get(i).expr()));
    }
    if (ctx.joinClause() != null && ctx.joinClause().USING() != null) {
      sb.append(" using ");
      List<VtlParser.ComponentIDContext> keys = ctx.joinClause().componentID();
      for (int i = 0; i < keys.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(keys.get(i).getText());
      }
    }
    sb.append(")");
    return sb.toString();
  }

  private Void applyJoinApply(String srcId, VtlParser.JoinApplyClauseContext apply) {
    VtlParser.ExprContext rhs = apply.expr();
    String exprId = nextExprId();
    Set<String> refs = componentRefs(rhs);
    addExpression(exprId, text(rhs), srcId, refs, Set.of(), null);
    Class<?> type = inferCalcType(rhs, requireStructure(srcId), refs);
    String measureName = DefaultMeasureNames.forType(type);
    pending = new Apply(srcId, exprId, measureName, type);
    return null;
  }

  private Void applyPivot(String srcId, VtlParser.PivotOrUnpivotClauseContext pivot) {
    String idComponent = pivot.id_.getText();
    String measureComponent = pivot.mea.getText();
    if (pivot.op.getType() == VtlParser.UNPIVOT) {
      pending = new Unpivot(srcId, idComponent, measureComponent);
      return null;
    }
    List<String> pivoted = distinctPivotValues(srcId, idComponent);
    if (pivoted.isEmpty()) {
      throw unsupported("clause");
    }
    pending = new Pivot(srcId, idComponent, measureComponent, pivoted, "pivot");
    return null;
  }

  private Void applyCustomPivot(String srcId, VtlParser.CustomPivotClauseContext custom) {
    String idComponent = custom.id_.getText();
    String measureComponent = custom.mea.getText();
    List<String> pivoted = new ArrayList<>();
    for (VtlParser.ConstantContext constant : custom.constant()) {
      pivoted.add(stripConstant(constant.getText()));
    }
    if (pivoted.isEmpty()) {
      throw unsupported("clause");
    }
    pending = new Pivot(srcId, idComponent, measureComponent, pivoted, "customPivot");
    return null;
  }

  private static String stripConstant(String raw) {
    if (raw.length() >= 2
        && ((raw.startsWith("\"") && raw.endsWith("\""))
            || (raw.startsWith("'") && raw.endsWith("'")))) {
      return raw.substring(1, raw.length() - 1);
    }
    return raw;
  }

  /**
   * Distinct values of the pivot identifier, in first-seen order. Requires {@code $input} rows on
   * the binding that produced {@code srcId} (engine pivot is unimplemented in-memory).
   */
  private List<String> distinctPivotValues(String srcId, String idComponent) {
    InputDataset input = bindingsWithRows.get(srcId);
    if (input == null) {
      throw unsupported("clause");
    }
    int col = -1;
    for (int i = 0; i < input.columns().size(); i++) {
      if (input.columns().get(i).name().equals(idComponent)) {
        col = i;
        break;
      }
    }
    if (col < 0) {
      throw unsupported("clause");
    }
    LinkedHashSet<String> values = new LinkedHashSet<>();
    for (List<String> row : input.rows()) {
      values.add(row.get(col));
    }
    return List.copyOf(values);
  }

  private Void applyCalc(String srcId, VtlParser.CalcClauseContext calc) {
    DataStructure src = requireStructure(srcId);
    Map<String, String> calcExprs = new LinkedHashMap<>();
    Map<String, Class<?>> calcTypes = new LinkedHashMap<>();
    for (VtlParser.CalcClauseItemContext item : calc.calcClauseItem()) {
      String component = item.componentID().getText();
      VtlParser.ExprContext rhs = item.expr();
      String exprId = nextExprId();
      Set<String> valueRefs = expressionValueRefs(rhs);
      Set<String> conditionRefs = analyticConditionRefs(rhs);
      addExpression(exprId, text(rhs), srcId, valueRefs, conditionRefs, null);
      calcExprs.put(component, exprId);
      calcTypes.put(component, inferCalcType(rhs, src, valueRefs));
    }
    pending = new Calc(srcId, Map.copyOf(calcExprs), Map.copyOf(calcTypes));
    return null;
  }

  private Void applyFilter(String srcId, VtlParser.FilterClauseContext filter) {
    VtlParser.ExprContext predicate = filter.expr();
    String exprId = nextExprId();
    addExpression(exprId, text(predicate), srcId, componentRefs(predicate), Set.of(), null);
    pending = new Filter(srcId, List.of(exprId));
    return null;
  }

  private Void applySub(String srcId, VtlParser.SubspaceClauseContext sub) {
    List<String> conditionIds = new ArrayList<>();
    for (VtlParser.SubspaceClauseItemContext item : sub.subspaceClauseItem()) {
      String exprId = nextExprId();
      addExpression(
          exprId, text(item), srcId, Set.of(item.componentID().getText()), Set.of(), null);
      conditionIds.add(exprId);
    }
    pending = new Sub(srcId, List.copyOf(conditionIds));
    return null;
  }

  private Void applyKeepOrDrop(String srcId, VtlParser.KeepOrDropClauseContext keepOrDrop) {
    List<String> columns =
        keepOrDrop.componentID().stream().map(c -> c.getText()).collect(Collectors.toList());
    if (keepOrDrop.op.getType() == VtlParser.KEEP) {
      pending = new Keep(srcId, List.copyOf(columns));
    } else {
      pending = new Drop(srcId, List.copyOf(columns));
    }
    return null;
  }

  private Void applyRename(String srcId, VtlParser.RenameClauseContext rename) {
    Map<String, String> renames = new LinkedHashMap<>();
    for (VtlParser.RenameClauseItemContext item : rename.renameClauseItem()) {
      renames.put(item.toName.getText(), item.fromName.getText());
    }
    pending = new Rename(srcId, Map.copyOf(renames));
    return null;
  }

  private Void applyAggr(String srcId, VtlParser.AggrClauseContext aggr) {
    DataStructure src = requireStructure(srcId);
    Map<String, String> aggrExprs = new LinkedHashMap<>();
    Map<String, Class<?>> aggrTypes = new LinkedHashMap<>();
    for (VtlParser.AggrFunctionClauseContext item : aggr.aggregateClause().aggrFunctionClause()) {
      String component = item.componentID().getText();
      VtlParser.AggrOperatorsGroupingContext op = item.aggrOperatorsGrouping();
      String exprId = nextExprId();
      Set<String> refs;
      if (op instanceof VtlParser.AggrDatasetContext datasetAggr) {
        refs = componentRefs(datasetAggr.expr());
      } else if (op instanceof VtlParser.CountAggrContext) {
        refs = Set.of();
      } else {
        throw unsupported("aggr");
      }
      addExpression(exprId, text(op), srcId, refs, Set.of(), null);
      aggrExprs.put(component, exprId);
      aggrTypes.put(component, inferCalcType(null, src, refs));
    }
    List<String> havingIds = new ArrayList<>();
    if (aggr.havingClause() != null) {
      VtlParser.ExprContext having = aggr.havingClause().expr();
      String havingId = nextExprId();
      addExpression(havingId, text(having), srcId, componentRefs(having), Set.of(), null);
      havingIds.add(havingId);
    }
    pending =
        new Aggr(
            srcId,
            Map.copyOf(aggrExprs),
            Map.copyOf(aggrTypes),
            groupByColumns(aggr.groupingClause()),
            List.copyOf(havingIds));
    return null;
  }

  private List<String> groupByColumns(VtlParser.GroupingClauseContext grouping) {
    if (grouping == null) {
      return List.of();
    }
    if (grouping instanceof VtlParser.GroupByOrExceptContext groupByOrExcept) {
      if (groupByOrExcept.op.getType() != VtlParser.BY) {
        throw unsupported("aggr");
      }
      return groupByOrExcept.componentID().stream()
          .map(c -> c.getText())
          .collect(Collectors.toList());
    }
    throw unsupported("aggr");
  }

  private Void binaryArithmetic(VtlParser.ExprContext left, VtlParser.ExprContext right, Token op) {
    List<String> operands = new ArrayList<>(2);
    String leftId = datasetOperand(left);
    if (leftId != null) {
      operands.add(leftId);
    }
    String rightId = datasetOperand(right);
    if (rightId != null) {
      operands.add(rightId);
    }
    if (operands.isEmpty()) {
      throw unsupported("scalar");
    }
    pending = new Arithmetic(op.getText(), List.copyOf(operands));
    return null;
  }

  /** {@code null} if the operand is a scalar literal (not a provenance node). */
  private String datasetOperand(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = unwrap(expr);
    if (current instanceof VtlParser.VarIdExprContext) {
      visit(current);
      return ((Identity) pending).datasetId();
    }
    if (current instanceof VtlParser.ConstantExprContext) {
      return null;
    }
    throw unsupported("arithmetic");
  }

  private Void assign(String out, VtlParser.ExprContext expr) {
    stmtIndex++;
    exprSeq = 0;
    anonSeq = 0;
    forceDerive = false;
    if (isScalarAssignment(expr)) {
      return assignScalar(out, expr);
    }
    visit(expr);
    requirePending();
    String outId = out + "@" + stmtIndex;
    DataStructure outStructure = structureForAssignment(out);
    addDataset(outId, outStructure, text(expr), false);
    linker.link(pending, outId, outStructure);
    versions.put(out, outId);
    pending = new Identity(outId);
    return null;
  }

  /**
   * RHS is a scalar expression (no dataset producer / dataset operand): emit {@code kind=scalar}
   * plus a reference-level expression node (§5.36).
   */
  private Void assignScalar(String out, VtlParser.ExprContext expr) {
    String exprId = nextExprId();
    Map<String, String> exprAttrs = new LinkedHashMap<>();
    exprAttrs.put("kind", "expression");
    exprAttrs.put("src", text(expr));
    graph.addVertex(exprId, exprAttrs);
    for (String name : varIdNames(expr)) {
      String scalarId = scalarVersions.get(name);
      if (scalarId == null) {
        throw new IllegalStateException("unknown scalar " + name);
      }
      graph.addEdge(exprId, scalarId, Map.of());
    }
    Class<?> type = inferTypeFromAst(unwrap(expr));
    if (type == null) {
      type = inferScalarTypeFromRefs(expr);
    }
    if (type == null) {
      type = Long.class;
    }
    String outId = out + "@" + stmtIndex;
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put("kind", "scalar");
    attrs.put("type", VTLTypes.getVtlType(type));
    graph.addVertex(outId, attrs);
    graph.addEdge(outId, exprId, Map.of());
    scalarVersions.put(out, outId);
    pending = null;
    return null;
  }

  /**
   * Scalar when the RHS never touches a dataset: no {@code eval(…)} (varIDs, not {@code
   * VarIdExpr}), and every {@code VarId} is either a literal-free reference to a prior scalar or
   * absent. Dataset producers always name a dataset {@code VarId} (or {@code eval}).
   */
  private boolean isScalarAssignment(VtlParser.ExprContext expr) {
    if (containsEval(expr)) {
      return false;
    }
    for (String name : varIdNames(expr)) {
      if (isDatasetName(name)) {
        return false;
      }
    }
    return true;
  }

  private static boolean containsEval(VtlParser.ExprContext expr) {
    boolean[] found = {false};
    new VtlBaseVisitor<Void>() {
      @Override
      public Void visitEvalAtom(VtlParser.EvalAtomContext ctx) {
        found[0] = true;
        return null;
      }
    }.visit(expr);
    return found[0];
  }

  private boolean isDatasetName(String name) {
    String id = versions.get(name);
    return id != null && structures.containsKey(id);
  }

  private Class<?> inferScalarTypeFromRefs(VtlParser.ExprContext expr) {
    Class<?> result = null;
    for (String name : varIdNames(expr)) {
      String id = scalarVersions.get(name);
      if (id == null) {
        continue;
      }
      Map<String, String> attrs = graph.vertices().get(id);
      if (attrs == null || attrs.get("type") == null) {
        continue;
      }
      Class<?> t = VtlJavaTypes.javaType(attrs.get("type"));
      if (result == null) {
        result = t;
      } else if (!result.equals(t)
          && Number.class.isAssignableFrom(result)
          && Number.class.isAssignableFrom(t)) {
        result = Double.class;
      }
    }
    return result;
  }

  private static Set<String> varIdNames(VtlParser.ExprContext expr) {
    Set<String> names = new LinkedHashSet<>();
    new VtlBaseVisitor<Void>() {
      @Override
      public Void visitVarIdExpr(VtlParser.VarIdExprContext ctx) {
        names.add(ctx.varID().getText());
        return null;
      }
    }.visit(expr);
    return names;
  }

  /**
   * Emits {@code #s{stmt}.{seq}} for a pending clause that is not the assignment LHS — the next
   * clause in the chain uses it as source.
   */
  private void ensureMaterialized() {
    requirePending();
    if (!(pending instanceof Identity)) {
      materializeAnonymous();
    }
  }

  private void materializeAnonymous() {
    materializeAnonymous(null);
  }

  private void materializeAnonymous(String src) {
    requirePending();
    anonSeq++;
    String anonId = "#s" + stmtIndex + "." + anonSeq;
    DataStructure structure = deriver.derive(pending);
    addDataset(anonId, structure, src, true);
    linker.link(pending, anonId, structure);
    pending = new Identity(anonId);
  }

  /**
   * Named LHS: engine binding if present, else derive from {@link #pending}. Never mix both for one
   * dataset (stable goldens when the engine later implements an op). Join bodies force derive
   * because the engine ignores them.
   */
  private DataStructure structureForAssignment(String out) {
    if (!forceDerive && oracle.hasDataset(out)) {
      return oracle.requireDataset(out);
    }
    return deriver.derive(pending);
  }

  private static Class<?> inferCalcType(
      VtlParser.ExprContext rhs, DataStructure src, Set<String> refs) {
    if (rhs != null) {
      Class<?> fromAst = inferTypeFromAst(unwrap(rhs));
      if (fromAst != null) {
        return fromAst;
      }
    }
    Class<?> result = null;
    for (String ref : refs) {
      Component component = src.get(ref);
      if (component == null) {
        continue;
      }
      if (result == null) {
        result = component.getType();
      } else if (!result.equals(component.getType())
          && Number.class.isAssignableFrom(result)
          && Number.class.isAssignableFrom(component.getType())) {
        result = Double.class;
      }
    }
    return result != null ? result : Long.class;
  }

  /**
   * Best-effort Java type from scalar AST when the structure oracle cannot eval (engine gap). Used
   * only on the pure-derive path.
   */
  private static Class<?> inferTypeFromAst(VtlParser.ExprContext expr) {
    if (expr instanceof VtlParser.ComparisonExprContext
        || expr instanceof VtlParser.BooleanExprContext
        || expr instanceof VtlParser.InNotInExprContext) {
      return Boolean.class;
    }
    if (expr instanceof VtlParser.FunctionsExpressionContext functions) {
      if (functions.functions() instanceof VtlParser.ComparisonFunctionsContext) {
        return Boolean.class;
      }
      if (functions.functions() instanceof VtlParser.TimeFunctionsContext time) {
        VtlParser.TimeOperatorsContext op = time.timeOperators();
        if (op instanceof VtlParser.YearAtomContext
            || op instanceof VtlParser.MonthAtomContext
            || op instanceof VtlParser.DayOfMonthAtomContext
            || op instanceof VtlParser.DayOfYearAtomContext) {
          return Long.class;
        }
        if (op instanceof VtlParser.CurrentDateAtomContext) {
          return java.time.Instant.class;
        }
      }
      if (functions.functions() instanceof VtlParser.GenericFunctionsContext generic
          && generic.genericOperators() instanceof VtlParser.CastExprDatasetContext cast) {
        if (cast.basicScalarType() != null) {
          String t = cast.basicScalarType().getText().toUpperCase();
          return switch (t) {
            case "STRING" -> String.class;
            case "INTEGER", "INT" -> Long.class;
            case "NUMBER", "FLOAT" -> Double.class;
            case "BOOLEAN", "BOOL" -> Boolean.class;
            case "DATE" -> java.time.Instant.class;
            default -> null;
          };
        }
      }
    }
    if (expr instanceof VtlParser.IfExprContext ifExpr) {
      Class<?> thenType = inferTypeFromAst(unwrap(ifExpr.thenExpr));
      if (thenType != null) {
        return thenType;
      }
      return inferTypeFromAst(unwrap(ifExpr.elseExpr));
    }
    return null;
  }

  private void addExpression(
      String exprId,
      String src,
      String datasetId,
      Set<String> valueRefs,
      Set<String> conditionRefs,
      String valueEdgeOp) {
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put("kind", "expression");
    attrs.put("src", src);
    graph.addVertex(exprId, attrs);
    Map<String, String> valueEdge = valueEdgeOp == null ? Map.of() : Map.of("op", valueEdgeOp);
    for (String ref : valueRefs) {
      graph.addEdge(exprId, datasetId + "." + ref, valueEdge);
    }
    Map<String, String> condition = Map.of("role", "condition");
    for (String ref : conditionRefs) {
      graph.addEdge(exprId, datasetId + "." + ref, condition);
    }
  }

  /**
   * Component names feeding a scalar expression. Pure UDO calls are inlined: walk the operator body
   * and map formal parameters to call-site {@code varID} arguments (PR-39). Otherwise collect every
   * {@code VarId} under the AST (reference-level).
   */
  private Set<String> expressionValueRefs(VtlParser.ExprContext expr) {
    VtlParser.CallDatasetContext call = asUdoCall(expr);
    if (call != null) {
      return udoInlinedRefs(call);
    }
    return componentRefs(expr);
  }

  private VtlParser.CallDatasetContext asUdoCall(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = unwrap(expr);
    if (!(current instanceof VtlParser.FunctionsExpressionContext functions)) {
      return null;
    }
    if (!(functions.functions() instanceof VtlParser.GenericFunctionsContext generic)) {
      return null;
    }
    if (!(generic.genericOperators() instanceof VtlParser.CallDatasetContext call)) {
      return null;
    }
    return symbols.isUserOperator(call.operatorID().getText()) ? call : null;
  }

  /**
   * Substitute call args into the registered body: only parameters that appear in the body
   * contribute, and only when the corresponding argument is a {@code varID}.
   */
  private Set<String> udoInlinedRefs(VtlParser.CallDatasetContext call) {
    ScriptSymbols.UserOperator udo = symbols.userOperator(call.operatorID().getText());
    if (udo == null) {
      throw new IllegalStateException("unknown user operator " + call.operatorID().getText());
    }
    List<VtlParser.ParameterContext> args = call.parameter();
    Map<String, String> paramToArg = new LinkedHashMap<>();
    for (int i = 0; i < udo.params().size() && i < args.size(); i++) {
      VtlParser.ParameterContext arg = args.get(i);
      if (arg.varID() != null) {
        paramToArg.put(udo.params().get(i), arg.varID().getText());
      }
    }
    Set<String> refs = new LinkedHashSet<>();
    for (String bodyRef : componentRefs(udo.body())) {
      String mapped = paramToArg.get(bodyRef);
      if (mapped != null) {
        refs.add(mapped);
      }
    }
    return refs;
  }

  /** Partition / order-by keys of analytic windows — condition inputs, not value operands. */
  private static Set<String> analyticConditionRefs(VtlParser.ExprContext expr) {
    Set<String> refs = new LinkedHashSet<>();
    new VtlBaseVisitor<Void>() {
      @Override
      public Void visitAnSimpleFunction(VtlParser.AnSimpleFunctionContext ctx) {
        addPartitionOrder(ctx.partition, ctx.orderBy, refs);
        return visit(ctx.expr());
      }

      @Override
      public Void visitLagOrLeadAn(VtlParser.LagOrLeadAnContext ctx) {
        addPartitionOrder(ctx.partition, ctx.orderBy, refs);
        return visit(ctx.expr());
      }

      @Override
      public Void visitRatioToReportAn(VtlParser.RatioToReportAnContext ctx) {
        addPartitionOrder(ctx.partition, null, refs);
        return visit(ctx.expr());
      }

      @Override
      public Void visitRankAn(VtlParser.RankAnContext ctx) {
        addPartitionOrder(ctx.partition, ctx.orderBy, refs);
        return null;
      }
    }.visit(expr);
    return refs;
  }

  private static void addPartitionOrder(
      VtlParser.PartitionByClauseContext partition,
      VtlParser.OrderByClauseContext orderBy,
      Set<String> refs) {
    if (partition != null) {
      for (VtlParser.ComponentIDContext component : partition.componentID()) {
        refs.add(component.getText());
      }
    }
    if (orderBy != null) {
      for (VtlParser.OrderByItemContext item : orderBy.orderByItem()) {
        refs.add(item.componentID().getText());
      }
    }
  }

  private void addDataset(String id, DataStructure structure, String src, boolean anon) {
    if (graph.vertices().containsKey(id)) {
      return;
    }
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put("kind", "dataset");
    if (src != null) {
      attrs.put("src", src);
    }
    if (anon) {
      attrs.put("anon", "true");
    }
    graph.addVertex(id, attrs);
    structures.put(id, structure);
    for (Component component : structure.values()) {
      Map<String, String> variable = new LinkedHashMap<>();
      variable.put("kind", "variable");
      variable.put("dataset", id);
      variable.put("role", component.getRole().name());
      variable.put("type", VTLTypes.getVtlType(component.getType()));
      graph.addVertex(id + "." + component.getName(), variable);
    }
  }

  private DataStructure requireStructure(String datasetId) {
    DataStructure structure = structures.get(datasetId);
    if (structure == null) {
      throw new IllegalStateException("unknown structure for " + datasetId);
    }
    return structure;
  }

  private void requirePending() {
    if (pending == null) {
      throw new IllegalStateException("no pending expression result");
    }
  }

  private String nextExprId() {
    exprSeq++;
    return "e" + stmtIndex + "." + exprSeq;
  }

  /** Component names referenced in a scalar expression (not dataset bindings). */
  private static Set<String> componentRefs(VtlParser.ExprContext expr) {
    Set<String> refs = new LinkedHashSet<>();
    new VtlBaseVisitor<Void>() {
      @Override
      public Void visitVarIdExpr(VtlParser.VarIdExprContext ctx) {
        refs.add(ctx.varID().getText());
        return null;
      }
    }.visit(expr);
    return refs;
  }

  private static String text(ParserRuleContext ctx) {
    CharStream input = ctx.getStart().getInputStream();
    return input.getText(Interval.of(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex()));
  }
}
