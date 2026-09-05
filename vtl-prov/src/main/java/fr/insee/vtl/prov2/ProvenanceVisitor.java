package fr.insee.vtl.prov2;

import fr.insee.vtl.antlr.runtime.CharStream;
import fr.insee.vtl.antlr.runtime.ParserRuleContext;
import fr.insee.vtl.antlr.runtime.Token;
import fr.insee.vtl.antlr.runtime.misc.Interval;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import fr.insee.vtl.prov.utils.VTLTypes;
import fr.insee.vtl.prov2.PendingOp.Aggr;
import fr.insee.vtl.prov2.PendingOp.Arithmetic;
import fr.insee.vtl.prov2.PendingOp.Calc;
import fr.insee.vtl.prov2.PendingOp.Drop;
import fr.insee.vtl.prov2.PendingOp.Filter;
import fr.insee.vtl.prov2.PendingOp.Identity;
import fr.insee.vtl.prov2.PendingOp.Join;
import fr.insee.vtl.prov2.PendingOp.Keep;
import fr.insee.vtl.prov2.PendingOp.Rename;
import fr.insee.vtl.prov2.PendingOp.SetOp;
import fr.insee.vtl.prov2.PendingOp.Sub;
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
 * holds a {@link PendingOp} describing what the enclosing assignment (or anonymous materialization)
 * will emit: {@link Identity} for a bare dataset ref, or a typed operator carrying its operands and
 * clause payload. Nested clauses materialize anonymous intermediates ({@code #s{stmt}.{seq}}) when
 * the left expression is already a non-identity op.
 */
final class ProvenanceVisitor extends SupportCheckVisitor {

  private final ProvGraph graph;
  private final StructureOracle oracle;
  private final Map<String, String> versions = new LinkedHashMap<>();
  private final Map<String, DataStructure> structures = new LinkedHashMap<>();
  private int stmtIndex;
  private int exprSeq;
  private int anonSeq;

  /** Outcome of the last visited expression; never null after a successful expr visit. */
  private PendingOp pending;

  ProvenanceVisitor(ProvGraph graph, StructureOracle oracle, List<InputDataset> inputs) {
    this.graph = graph;
    this.oracle = oracle;
    for (InputDataset input : inputs) {
      String id = input.name() + "@0";
      versions.put(input.name(), id);
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
    requireEmptyJoinBody(ctx.joinBody());
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
    requirePending();
    // Left was itself a clause/op: emit anonymous intermediate before this clause.
    if (!(pending instanceof Identity)) {
      materializeAnonymous();
    }
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
    throw unsupported("clause");
  }

  private Void applyCalc(String srcId, VtlParser.CalcClauseContext calc) {
    DataStructure src = requireStructure(srcId);
    Map<String, String> calcExprs = new LinkedHashMap<>();
    Map<String, Class<?>> calcTypes = new LinkedHashMap<>();
    for (VtlParser.CalcClauseItemContext item : calc.calcClauseItem()) {
      String component = item.componentID().getText();
      VtlParser.ExprContext rhs = item.expr();
      String exprId = nextExprId();
      Set<String> valueRefs = componentRefs(rhs);
      Set<String> conditionRefs = analyticConditionRefs(rhs);
      addExpression(exprId, text(rhs), srcId, valueRefs, conditionRefs);
      calcExprs.put(component, exprId);
      calcTypes.put(component, inferCalcType(src, valueRefs));
    }
    pending = new Calc(srcId, Map.copyOf(calcExprs), Map.copyOf(calcTypes));
    return null;
  }

  private Void applyFilter(String srcId, VtlParser.FilterClauseContext filter) {
    VtlParser.ExprContext predicate = filter.expr();
    String exprId = nextExprId();
    addExpression(exprId, text(predicate), srcId, componentRefs(predicate), Set.of());
    pending = new Filter(srcId, List.of(exprId));
    return null;
  }

  private Void applySub(String srcId, VtlParser.SubspaceClauseContext sub) {
    List<String> conditionIds = new ArrayList<>();
    for (VtlParser.SubspaceClauseItemContext item : sub.subspaceClauseItem()) {
      String exprId = nextExprId();
      addExpression(exprId, text(item), srcId, Set.of(item.componentID().getText()), Set.of());
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
      addExpression(exprId, text(op), srcId, refs, Set.of());
      aggrExprs.put(component, exprId);
      aggrTypes.put(component, inferCalcType(src, refs));
    }
    pending =
        new Aggr(
            srcId,
            Map.copyOf(aggrExprs),
            Map.copyOf(aggrTypes),
            groupByColumns(aggr.groupingClause()));
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
    visit(expr);
    requirePending();
    String outId = out + "@" + stmtIndex;
    DataStructure outStructure = structureForAssignment(out);
    addDataset(outId, outStructure, text(expr), false);
    linkPending(outId, outStructure);
    versions.put(out, outId);
    pending = new Identity(outId);
    return null;
  }

  /**
   * Emits {@code #s{stmt}.{seq}} for a pending clause that is not the assignment LHS — the next
   * clause in the chain uses it as source.
   */
  private void materializeAnonymous() {
    requirePending();
    anonSeq++;
    String anonId = "#s" + stmtIndex + "." + anonSeq;
    DataStructure structure = deriveStructure(pending);
    addDataset(anonId, structure, null, true);
    linkPending(anonId, structure);
    pending = new Identity(anonId);
  }

  /**
   * Named LHS: engine binding if present, else derive from {@link #pending}. Never mix both for one
   * dataset (stable goldens when the engine later implements an op).
   */
  private DataStructure structureForAssignment(String out) {
    if (oracle.hasDataset(out)) {
      return oracle.requireDataset(out);
    }
    return deriveStructure(pending);
  }

  private DataStructure deriveStructure(PendingOp op) {
    if (op instanceof Identity id) {
      return new DataStructure(requireStructure(id.datasetId()));
    }
    if (op instanceof Arithmetic arithmetic) {
      return new DataStructure(requireStructure(arithmetic.operandIds().get(0)));
    }
    if (op instanceof Calc calc) {
      return deriveCalcStructure(requireStructure(calc.srcId()), calc.types());
    }
    if (op instanceof Aggr aggr) {
      return deriveAggrStructure(requireStructure(aggr.srcId()), aggr.types(), aggr.groupBy());
    }
    if (op instanceof Filter filter) {
      return new DataStructure(requireStructure(filter.srcId()));
    }
    if (op instanceof Sub sub) {
      return new DataStructure(requireStructure(sub.srcId()));
    }
    if (op instanceof Keep keep) {
      return deriveKeepStructure(requireStructure(keep.srcId()), keep.columns());
    }
    if (op instanceof Drop drop) {
      return deriveDropStructure(requireStructure(drop.srcId()), drop.columns());
    }
    if (op instanceof Rename rename) {
      return deriveRenameStructure(requireStructure(rename.srcId()), rename.renameFrom());
    }
    if (op instanceof Join join) {
      return deriveJoinStructure(join.operandIds());
    }
    if (op instanceof SetOp setOp) {
      return new DataStructure(requireStructure(setOp.operandIds().get(0)));
    }
    throw new IllegalStateException("unhandled pending op " + op.getClass().getName());
  }

  private DataStructure deriveJoinStructure(List<String> operandIds) {
    List<Component> components = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();
    for (String operandId : operandIds) {
      for (Component component : requireStructure(operandId).componentsInOrder()) {
        if (seen.add(component.getName())) {
          components.add(new Component(component));
        }
      }
    }
    return new DataStructure(components);
  }

  private static DataStructure deriveAggrStructure(
      DataStructure src, Map<String, Class<?>> aggrTypes, List<String> groupBy) {
    List<Component> components = new ArrayList<>();
    for (String key : groupBy) {
      Component component = src.get(key);
      if (component == null) {
        throw new IllegalStateException("unknown group-by component " + key);
      }
      components.add(
          new Component(component.getName(), component.getType(), Dataset.Role.IDENTIFIER));
    }
    for (Map.Entry<String, Class<?>> entry : aggrTypes.entrySet()) {
      components.add(new Component(entry.getKey(), entry.getValue(), Dataset.Role.MEASURE));
    }
    return new DataStructure(components);
  }

  private static DataStructure deriveCalcStructure(
      DataStructure src, Map<String, Class<?>> calcTypes) {
    List<Component> components = new ArrayList<>(src.componentsInOrder());
    for (Map.Entry<String, Class<?>> entry : calcTypes.entrySet()) {
      String name = entry.getKey();
      Class<?> type = entry.getValue();
      int existing = -1;
      for (int i = 0; i < components.size(); i++) {
        if (components.get(i).getName().equals(name)) {
          existing = i;
          break;
        }
      }
      Component component = new Component(name, type, Dataset.Role.MEASURE);
      if (existing >= 0) {
        components.set(existing, component);
      } else {
        components.add(component);
      }
    }
    return new DataStructure(components);
  }

  private static DataStructure deriveKeepStructure(DataStructure src, List<String> columns) {
    List<Component> kept = new ArrayList<>();
    for (Component component : src.componentsInOrder()) {
      if (component.isIdentifier() || columns.contains(component.getName())) {
        kept.add(component);
      }
    }
    return new DataStructure(kept);
  }

  private static DataStructure deriveDropStructure(DataStructure src, List<String> columns) {
    Set<String> dropped = new LinkedHashSet<>(columns);
    List<Component> kept = new ArrayList<>();
    for (Component component : src.componentsInOrder()) {
      if (component.isIdentifier() || !dropped.contains(component.getName())) {
        kept.add(component);
      }
    }
    return new DataStructure(kept);
  }

  private static DataStructure deriveRenameStructure(
      DataStructure src, Map<String, String> renameFrom) {
    Map<String, String> fromTo = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : renameFrom.entrySet()) {
      fromTo.put(entry.getValue(), entry.getKey());
    }
    List<Component> renamed = new ArrayList<>();
    for (Component component : src.componentsInOrder()) {
      String name = fromTo.getOrDefault(component.getName(), component.getName());
      renamed.add(new Component(name, component.getType(), component.getRole()));
    }
    return new DataStructure(renamed);
  }

  private static Class<?> inferCalcType(DataStructure src, Set<String> refs) {
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

  private void linkPending(String outId, DataStructure outStructure) {
    requirePending();
    if (pending instanceof Identity id) {
      linkComponentWise(outId, outStructure, List.of(id.datasetId()), "assign");
      return;
    }
    if (pending instanceof Arithmetic arithmetic) {
      linkComponentWise(outId, outStructure, arithmetic.operandIds(), arithmetic.op());
      return;
    }
    if (pending instanceof Calc calc) {
      linkMappedExprs(outId, outStructure, calc.srcId(), calc.exprs(), "calc");
      return;
    }
    if (pending instanceof Aggr aggr) {
      linkMappedExprs(outId, outStructure, aggr.srcId(), aggr.exprs(), "aggr");
      return;
    }
    if (pending instanceof Filter filter) {
      linkConditionClause(
          outId, outStructure, filter.srcId(), filter.conditionExprIds(), "filter");
      return;
    }
    if (pending instanceof Sub sub) {
      linkConditionClause(outId, outStructure, sub.srcId(), sub.conditionExprIds(), "sub");
      return;
    }
    if (pending instanceof Keep keep) {
      linkPassThroughAll(outId, outStructure, keep.srcId(), "keep");
      return;
    }
    if (pending instanceof Drop drop) {
      linkPassThroughAll(outId, outStructure, drop.srcId(), "drop");
      return;
    }
    if (pending instanceof Rename rename) {
      linkRename(outId, outStructure, rename.srcId(), rename.renameFrom());
      return;
    }
    if (pending instanceof Join join) {
      linkJoin(outId, outStructure, join.operandIds(), join.op());
      return;
    }
    if (pending instanceof SetOp setOp) {
      if ("setdiff".equals(setOp.op())) {
        linkSetDiff(outId, outStructure, setOp.operandIds().get(0), setOp.operandIds().get(1));
      } else {
        linkComponentWise(outId, outStructure, setOp.operandIds(), setOp.op());
      }
      return;
    }
    throw new IllegalStateException("unhandled pending op " + pending.getClass().getName());
  }

  private void linkPassThroughAll(
      String outId, DataStructure outStructure, String srcId, String op) {
    Map<String, String> edge = opEdge(op);
    graph.addEdge(outId, srcId, edge);
    linkPassThrough(outId, outStructure, srcId, edge);
  }

  private void linkSetDiff(
      String outId, DataStructure outStructure, String leftId, String rightId) {
    Map<String, String> edge = opEdge("setdiff");
    Map<String, String> condition = new LinkedHashMap<>(edge);
    condition.put("role", "condition");
    graph.addEdge(outId, leftId, edge);
    graph.addEdge(outId, rightId, condition);
    linkPassThrough(outId, outStructure, leftId, edge);
  }

  private void linkJoin(
      String outId, DataStructure outStructure, List<String> operandIds, String op) {
    Map<String, String> edge = opEdge(op);
    for (String operandId : operandIds) {
      graph.addEdge(outId, operandId, edge);
    }
    for (Component component : outStructure.values()) {
      String name = component.getName();
      String outVar = outId + "." + name;
      for (String operandId : operandIds) {
        if (requireStructure(operandId).containsKey(name)) {
          graph.addEdge(outVar, operandId + "." + name, edge);
        }
      }
    }
  }

  private void linkMappedExprs(
      String outId,
      DataStructure outStructure,
      String srcId,
      Map<String, String> mappedExprs,
      String op) {
    Map<String, String> edge = opEdge(op);
    graph.addEdge(outId, srcId, edge);
    for (Component component : outStructure.values()) {
      String outVar = outId + "." + component.getName();
      String exprId = mappedExprs.get(component.getName());
      if (exprId != null) {
        graph.addEdge(outVar, exprId, edge);
      } else {
        graph.addEdge(outVar, srcId + "." + component.getName(), edge);
      }
    }
  }

  private void linkConditionClause(
      String outId,
      DataStructure outStructure,
      String srcId,
      List<String> conditionExprIds,
      String op) {
    Map<String, String> edge = opEdge(op);
    Map<String, String> condition = new LinkedHashMap<>(edge);
    condition.put("role", "condition");
    graph.addEdge(outId, srcId, edge);
    for (String exprId : conditionExprIds) {
      graph.addEdge(outId, exprId, condition);
    }
    linkPassThrough(outId, outStructure, srcId, edge);
  }

  private void linkRename(
      String outId, DataStructure outStructure, String srcId, Map<String, String> renameFrom) {
    Map<String, String> edge = opEdge("rename");
    graph.addEdge(outId, srcId, edge);
    for (Component component : outStructure.values()) {
      String srcComponent = renameFrom.getOrDefault(component.getName(), component.getName());
      graph.addEdge(outId + "." + component.getName(), srcId + "." + srcComponent, edge);
    }
  }

  private void linkComponentWise(
      String outId, DataStructure outStructure, List<String> operandIds, String op) {
    Map<String, String> edge = opEdge(op);
    for (String operandId : operandIds) {
      graph.addEdge(outId, operandId, edge);
    }
    for (Component component : outStructure.values()) {
      String outVar = outId + "." + component.getName();
      for (String operandId : operandIds) {
        graph.addEdge(outVar, operandId + "." + component.getName(), edge);
      }
    }
  }

  private void linkPassThrough(
      String outId, DataStructure outStructure, String srcId, Map<String, String> edge) {
    for (Component component : outStructure.values()) {
      graph.addEdge(outId + "." + component.getName(), srcId + "." + component.getName(), edge);
    }
  }

  private void addExpression(
      String exprId,
      String src,
      String datasetId,
      Set<String> valueRefs,
      Set<String> conditionRefs) {
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put("kind", "expression");
    attrs.put("src", src);
    graph.addVertex(exprId, attrs);
    for (String ref : valueRefs) {
      graph.addEdge(exprId, datasetId + "." + ref, Map.of());
    }
    Map<String, String> condition = Map.of("role", "condition");
    for (String ref : conditionRefs) {
      graph.addEdge(exprId, datasetId + "." + ref, condition);
    }
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

  private static Map<String, String> opEdge(String op) {
    return Map.of("op", op);
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
