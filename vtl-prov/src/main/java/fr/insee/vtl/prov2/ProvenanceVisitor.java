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
 * <p>{@code T = Void}: the graph is the artifact. After visiting an expression, run state describes
 * what the enclosing assignment materializes: identity ({@code lastOp == null}), component-wise ops
 * ({@code +}, {@code *}, {@code keep}, …), or clause ops ({@code calc}, {@code filter}, {@code
 * sub}, {@code rename}, {@code aggr}). Nested clauses materialize anonymous intermediates ({@code
 * #s{stmt}.{seq}}) before the next clause applies.
 */
final class ProvenanceVisitor extends SupportCheckVisitor {

  private final ProvGraph graph;
  private final StructureOracle oracle;
  private final Map<String, String> versions = new LinkedHashMap<>();
  private final Map<String, DataStructure> structures = new LinkedHashMap<>();
  private int stmtIndex;
  private int exprSeq;
  private int anonSeq;

  private String lastResultId;
  private String lastOp;
  private List<String> lastOperandIds = List.of();
  private Map<String, String> lastCalcExprs = Map.of();
  private Map<String, Class<?>> lastCalcTypes = Map.of();
  private Map<String, String> lastRenameFrom = Map.of();
  private List<String> lastKeepDropColumns = List.of();
  private List<String> lastConditionExprIds = List.of();

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
    clearExprState();
    lastResultId = id;
    lastOperandIds = List.of(id);
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
  public Void visitClauseExpr(VtlParser.ClauseExprContext ctx) {
    visit(ctx.expr());
    if (lastResultId == null) {
      throw unsupported("clause");
    }
    // Left was itself a clause: emit anonymous intermediate before this clause.
    if (lastOp != null) {
      materializeAnonymous();
    }
    String srcId = lastResultId;
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
      Set<String> refs = componentRefs(rhs);
      addExpression(exprId, text(rhs), srcId, refs);
      calcExprs.put(component, exprId);
      calcTypes.put(component, inferCalcType(src, refs));
    }
    return finishUnaryOp(
        "calc",
        srcId,
        Map.copyOf(calcExprs),
        Map.copyOf(calcTypes),
        Map.of(),
        List.of(),
        List.of());
  }

  private Void applyFilter(String srcId, VtlParser.FilterClauseContext filter) {
    VtlParser.ExprContext predicate = filter.expr();
    String exprId = nextExprId();
    addExpression(exprId, text(predicate), srcId, componentRefs(predicate));
    return finishUnaryOp("filter", srcId, Map.of(), Map.of(), Map.of(), List.of(), List.of(exprId));
  }

  private Void applySub(String srcId, VtlParser.SubspaceClauseContext sub) {
    List<String> conditionIds = new ArrayList<>();
    for (VtlParser.SubspaceClauseItemContext item : sub.subspaceClauseItem()) {
      String exprId = nextExprId();
      addExpression(exprId, text(item), srcId, Set.of(item.componentID().getText()));
      conditionIds.add(exprId);
    }
    return finishUnaryOp(
        "sub", srcId, Map.of(), Map.of(), Map.of(), List.of(), List.copyOf(conditionIds));
  }

  private Void applyKeepOrDrop(String srcId, VtlParser.KeepOrDropClauseContext keepOrDrop) {
    List<String> columns =
        keepOrDrop.componentID().stream().map(c -> c.getText()).collect(Collectors.toList());
    return finishUnaryOp(
        keepOrDrop.op.getText(),
        srcId,
        Map.of(),
        Map.of(),
        Map.of(),
        List.copyOf(columns),
        List.of());
  }

  private Void applyRename(String srcId, VtlParser.RenameClauseContext rename) {
    Map<String, String> renames = new LinkedHashMap<>();
    for (VtlParser.RenameClauseItemContext item : rename.renameClauseItem()) {
      renames.put(item.toName.getText(), item.fromName.getText());
    }
    return finishUnaryOp(
        "rename", srcId, Map.of(), Map.of(), Map.copyOf(renames), List.of(), List.of());
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
        throw unsupported("clause");
      }
      addExpression(exprId, text(op), srcId, refs);
      aggrExprs.put(component, exprId);
      aggrTypes.put(component, inferCalcType(src, refs));
    }
    return finishUnaryOp(
        "aggr",
        srcId,
        Map.copyOf(aggrExprs),
        Map.copyOf(aggrTypes),
        Map.of(),
        groupByColumns(aggr.groupingClause()),
        List.of());
  }

  private List<String> groupByColumns(VtlParser.GroupingClauseContext grouping) {
    if (grouping == null) {
      return List.of();
    }
    if (grouping instanceof VtlParser.GroupByOrExceptContext groupByOrExcept) {
      if (groupByOrExcept.op.getType() != VtlParser.BY) {
        throw unsupported("clause");
      }
      return groupByOrExcept.componentID().stream()
          .map(c -> c.getText())
          .collect(Collectors.toList());
    }
    throw unsupported("clause");
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
    clearExprState();
    lastOp = op.getText();
    lastOperandIds = List.copyOf(operands);
    return null;
  }

  /** {@code null} if the operand is a scalar literal (not a provenance node). */
  private String datasetOperand(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = unwrap(expr);
    if (current instanceof VtlParser.VarIdExprContext) {
      visit(current);
      return lastResultId;
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
    String outId = out + "@" + stmtIndex;
    DataStructure outStructure = oracle.requireDataset(out);
    addDataset(outId, outStructure, text(expr), false);
    linkAssignment(outId, outStructure);
    versions.put(out, outId);
    clearExprState();
    lastResultId = outId;
    lastOperandIds = List.of(outId);
    return null;
  }

  /**
   * Emits {@code #s{stmt}.{seq}} for a pending clause that is not the assignment LHS — the next
   * clause in the chain uses it as source.
   */
  private void materializeAnonymous() {
    anonSeq++;
    String anonId = "#s" + stmtIndex + "." + anonSeq;
    DataStructure structure = structureForPendingOp();
    addDataset(anonId, structure, null, true);
    linkAssignment(anonId, structure);
    clearExprState();
    lastResultId = anonId;
    lastOperandIds = List.of(anonId);
  }

  private DataStructure structureForPendingOp() {
    DataStructure src = requireStructure(lastResultId);
    return switch (lastOp) {
      case "filter", "sub" -> new DataStructure(src);
      case "calc" -> deriveCalcStructure(src, lastCalcTypes);
      case "aggr" -> deriveAggrStructure(src, lastCalcTypes, lastKeepDropColumns);
      case "keep" -> deriveKeepStructure(src, lastKeepDropColumns);
      case "drop" -> deriveDropStructure(src, lastKeepDropColumns);
      case "rename" -> deriveRenameStructure(src, lastRenameFrom);
      default -> throw unsupported("clause");
    };
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

  private void linkAssignment(String outId, DataStructure outStructure) {
    if (lastOp == null) {
      if (lastResultId == null) {
        throw unsupported("scalar");
      }
      linkComponentWise(outId, outStructure, List.of(lastResultId), "assign");
      return;
    }
    switch (lastOp) {
      case "calc", "aggr" ->
          linkMappedExprs(outId, outStructure, lastResultId, lastCalcExprs, lastOp);
      case "filter", "sub" ->
          linkConditionClause(outId, outStructure, lastResultId, lastConditionExprIds, lastOp);
      case "rename" -> linkRename(outId, outStructure, lastResultId, lastRenameFrom);
      default -> linkComponentWise(outId, outStructure, lastOperandIds, lastOp);
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

  private void addExpression(String exprId, String src, String datasetId, Set<String> refs) {
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put("kind", "expression");
    attrs.put("src", src);
    graph.addVertex(exprId, attrs);
    for (String ref : refs) {
      graph.addEdge(exprId, datasetId + "." + ref, Map.of());
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

  private Void finishUnaryOp(
      String op,
      String srcId,
      Map<String, String> calcExprs,
      Map<String, Class<?>> calcTypes,
      Map<String, String> renameFrom,
      List<String> keepDropColumns,
      List<String> conditionExprIds) {
    lastOp = op;
    lastResultId = srcId;
    lastOperandIds = List.of(srcId);
    lastCalcExprs = calcExprs;
    lastCalcTypes = calcTypes;
    lastRenameFrom = renameFrom;
    lastKeepDropColumns = keepDropColumns;
    lastConditionExprIds = conditionExprIds;
    return null;
  }

  private void clearExprState() {
    lastOp = null;
    lastCalcExprs = Map.of();
    lastCalcTypes = Map.of();
    lastRenameFrom = Map.of();
    lastKeepDropColumns = List.of();
    lastConditionExprIds = List.of();
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
