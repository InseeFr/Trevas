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
 * lastResultId} is the source dataset); otherwise {@code lastOp} + operands / calc items describe a
 * dataset op whose result node is created by the enclosing assignment.
 */
final class ProvenanceVisitor extends SupportCheckVisitor {

  private final ProvGraph graph;
  private final StructureOracle oracle;
  private final Map<String, String> versions = new LinkedHashMap<>();
  private int stmtIndex;

  /** Dataset id produced by the last varId (identity) expression. */
  private String lastResultId;

  /** Operator of the last dataset expression ({@code +}, {@code *}, {@code calc}, …), or null. */
  private String lastOp;

  /** Dataset operand ids of the last component-wise expression (literals omitted). */
  private List<String> lastOperandIds = List.of();

  /** Calc outputs: component name → expression node id. Empty unless {@code lastOp} is calc. */
  private Map<String, String> lastCalcExprs = Map.of();

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
    lastResultId = id;
    lastOp = null;
    lastOperandIds = List.of(id);
    lastCalcExprs = Map.of();
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
    VtlParser.CalcClauseContext calc = ctx.datasetClause().calcClause();
    if (calc != null) {
      return applyCalc(srcId, calc);
    }
    throw unsupported("clause");
  }

  private Void applyCalc(String srcId, VtlParser.CalcClauseContext calc) {
    Map<String, String> calcExprs = new LinkedHashMap<>();
    int exprSeq = 0;
    for (VtlParser.CalcClauseItemContext item : calc.calcClauseItem()) {
      exprSeq++;
      String component = item.componentID().getText();
      VtlParser.ExprContext rhs = item.expr();
      String exprId = "e" + stmtIndex + "." + exprSeq;
      Map<String, String> attrs = new LinkedHashMap<>();
      attrs.put("kind", "expression");
      attrs.put("src", text(rhs));
      graph.addVertex(exprId, attrs);
      for (String ref : componentRefs(rhs)) {
        graph.addEdge(exprId, srcId + "." + ref, Map.of());
      }
      calcExprs.put(component, exprId);
    }
    lastOp = "calc";
    lastResultId = srcId;
    lastOperandIds = List.of(srcId);
    lastCalcExprs = Map.copyOf(calcExprs);
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
    } else {
      linkComponentWise(outId, outStructure, lastOperandIds, lastOp);
    }

    versions.put(out, outId);
    lastResultId = outId;
    lastOp = null;
    lastOperandIds = List.of(outId);
    lastCalcExprs = Map.of();
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

  /** Component names referenced in a calc RHS (not dataset bindings). */
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
