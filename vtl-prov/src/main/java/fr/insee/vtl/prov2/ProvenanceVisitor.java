package fr.insee.vtl.prov2;

import fr.insee.vtl.antlr.runtime.CharStream;
import fr.insee.vtl.antlr.runtime.ParserRuleContext;
import fr.insee.vtl.antlr.runtime.Token;
import fr.insee.vtl.antlr.runtime.misc.Interval;
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

/**
 * Grammar-driven provenance walk ({@code VtlBaseVisitor<Void>}). Extends {@link
 * SupportCheckVisitor} for the shared {@code unsupported: …} surface; mutates a shared {@link
 * ProvGraph}.
 *
 * <p>{@code T = Void}: the graph is the artifact; parse {@code ctx} plus run state carry what each
 * visit needs. After visiting an expression: {@code lastOp == null} means identity ({@code
 * lastResultId} is the source dataset); otherwise {@code lastOp} + operands / calc / condition
 * exprs describe a dataset op whose result node is created by the enclosing assignment.
 */
final class ProvenanceVisitor extends SupportCheckVisitor {

  private final ProvGraph graph;
  private final StructureOracle oracle;
  private final Map<String, String> versions = new LinkedHashMap<>();
  private int stmtIndex;
  private int exprSeq;

  /** Dataset id produced by the last varId (identity) expression. */
  private String lastResultId;

  /** Operator of the last dataset expression ({@code +}, {@code calc}, {@code filter}, …). */
  private String lastOp;

  /** Dataset operand ids of the last component-wise expression (literals omitted). */
  private List<String> lastOperandIds = List.of();

  /** Calc outputs: component name → expression node id. Empty unless {@code lastOp} is calc. */
  private Map<String, String> lastCalcExprs = Map.of();

  /** Condition expression node ids ({@code filter}/{@code sub}). */
  private List<String> lastConditionExprIds = List.of();

  ProvenanceVisitor(ProvGraph graph, StructureOracle oracle, List<InputDataset> inputs) {
    this.graph = graph;
    this.oracle = oracle;
    for (InputDataset input : inputs) {
      String id = input.name() + "@0";
      versions.put(input.name(), id);
      addDataset(id, oracle.requireDataset(input.name()), null);
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
    if (lastOp != null || lastResultId == null) {
      throw unsupported("clause");
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
    throw unsupported("clause");
  }

  private Void applyCalc(String srcId, VtlParser.CalcClauseContext calc) {
    Map<String, String> calcExprs = new LinkedHashMap<>();
    for (VtlParser.CalcClauseItemContext item : calc.calcClauseItem()) {
      String component = item.componentID().getText();
      VtlParser.ExprContext rhs = item.expr();
      String exprId = nextExprId();
      addExpression(exprId, text(rhs), srcId, componentRefs(rhs));
      calcExprs.put(component, exprId);
    }
    lastOp = "calc";
    lastResultId = srcId;
    lastOperandIds = List.of(srcId);
    lastCalcExprs = Map.copyOf(calcExprs);
    lastConditionExprIds = List.of();
    return null;
  }

  private Void applyFilter(String srcId, VtlParser.FilterClauseContext filter) {
    VtlParser.ExprContext predicate = filter.expr();
    String exprId = nextExprId();
    addExpression(exprId, text(predicate), srcId, componentRefs(predicate));
    lastOp = "filter";
    lastResultId = srcId;
    lastOperandIds = List.of(srcId);
    lastCalcExprs = Map.of();
    lastConditionExprIds = List.of(exprId);
    return null;
  }

  private Void applySub(String srcId, VtlParser.SubspaceClauseContext sub) {
    List<String> conditionIds = new ArrayList<>();
    for (VtlParser.SubspaceClauseItemContext item : sub.subspaceClauseItem()) {
      String exprId = nextExprId();
      String component = item.componentID().getText();
      addExpression(exprId, text(item), srcId, Set.of(component));
      conditionIds.add(exprId);
    }
    lastOp = "sub";
    lastResultId = srcId;
    lastOperandIds = List.of(srcId);
    lastCalcExprs = Map.of();
    lastConditionExprIds = List.copyOf(conditionIds);
    return null;
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
    lastOperandIds = List.copyOf(operands);
    lastOp = op.getText();
    lastResultId = null;
    lastCalcExprs = Map.of();
    lastConditionExprIds = List.of();
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
    visit(expr);
    String outId = out + "@" + stmtIndex;
    DataStructure outStructure = oracle.requireDataset(out);
    addDataset(outId, outStructure, text(expr));

    if (lastOp == null) {
      if (lastResultId == null) {
        throw unsupported("scalar");
      }
      linkComponentWise(outId, outStructure, List.of(lastResultId), "assign");
    } else if ("calc".equals(lastOp)) {
      linkCalc(outId, outStructure, lastResultId, lastCalcExprs);
    } else if ("filter".equals(lastOp) || "sub".equals(lastOp)) {
      linkConditionClause(outId, outStructure, lastResultId, lastConditionExprIds, lastOp);
    } else {
      linkComponentWise(outId, outStructure, lastOperandIds, lastOp);
    }

    versions.put(out, outId);
    clearExprState();
    lastResultId = outId;
    lastOperandIds = List.of(outId);
    return null;
  }

  private void linkCalc(
      String outId, DataStructure outStructure, String srcId, Map<String, String> calcExprs) {
    Map<String, String> edge = Map.of("op", "calc");
    graph.addEdge(outId, srcId, edge);
    for (Component component : outStructure.values()) {
      String outVar = outId + "." + component.getName();
      String exprId = calcExprs.get(component.getName());
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
    Map<String, String> edge = Map.of("op", op);
    Map<String, String> condition = new LinkedHashMap<>();
    condition.put("op", op);
    condition.put("role", "condition");
    graph.addEdge(outId, srcId, edge);
    for (String exprId : conditionExprIds) {
      graph.addEdge(outId, exprId, condition);
    }
    for (Component component : outStructure.values()) {
      graph.addEdge(outId + "." + component.getName(), srcId + "." + component.getName(), edge);
    }
  }

  private void linkComponentWise(
      String outId, DataStructure outStructure, List<String> operandIds, String op) {
    Map<String, String> edge = Map.of("op", op);
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

  private void addExpression(String exprId, String src, String datasetId, Set<String> refs) {
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put("kind", "expression");
    attrs.put("src", src);
    graph.addVertex(exprId, attrs);
    for (String ref : refs) {
      graph.addEdge(exprId, datasetId + "." + ref, Map.of());
    }
  }

  private String nextExprId() {
    exprSeq++;
    return "e" + stmtIndex + "." + exprSeq;
  }

  private void clearExprState() {
    lastOp = null;
    lastCalcExprs = Map.of();
    lastConditionExprIds = List.of();
  }

  private void addDataset(String id, DataStructure structure, String src) {
    if (graph.vertices().containsKey(id)) {
      return;
    }
    Map<String, String> attrs = new LinkedHashMap<>();
    attrs.put("kind", "dataset");
    if (src != null) {
      attrs.put("src", src);
    }
    graph.addVertex(id, attrs);
    for (Component component : structure.values()) {
      Map<String, String> variable = new LinkedHashMap<>();
      variable.put("kind", "variable");
      variable.put("dataset", id);
      variable.put("role", component.getRole().name());
      variable.put("type", VTLTypes.getVtlType(component.getType()));
      graph.addVertex(id + "." + component.getName(), variable);
    }
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
