package fr.insee.vtl.prov2;

import fr.insee.vtl.antlr.runtime.CharStream;
import fr.insee.vtl.antlr.runtime.CharStreams;
import fr.insee.vtl.antlr.runtime.CommonTokenStream;
import fr.insee.vtl.antlr.runtime.ParserRuleContext;
import fr.insee.vtl.antlr.runtime.misc.Interval;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import fr.insee.vtl.prov.utils.VTLTypes;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * PR-2 extractor: walk top-level assignments, support identity {@code ds2 := ds1} / {@code ds2 <-
 * ds1}, throw {@code unsupported: …} on everything else.
 */
public final class StatementWalkExtractor implements ProvenanceExtractor {

  @Override
  public ProvGraph extract(String script, List<InputDataset> inputs) {
    VtlParser.StartContext start = parse(script);
    List<IdentityAssign> assigns = collectIdentityAssigns(start);
    StructureOracle oracle = StructureOracle.run(script, inputs);
    return emit(assigns, inputs, oracle);
  }

  private static List<IdentityAssign> collectIdentityAssigns(VtlParser.StartContext start) {
    List<IdentityAssign> assigns = new ArrayList<>();
    for (VtlParser.StatementContext statement : start.statement()) {
      if (statement instanceof VtlParser.TemporaryAssignmentContext temporary) {
        assigns.add(requireIdentity(temporary.varID(), temporary.expr()));
      } else if (statement instanceof VtlParser.PersistAssignmentContext persist) {
        assigns.add(requireIdentity(persist.varID(), persist.expr()));
      } else {
        throw unsupported("define");
      }
    }
    return assigns;
  }

  private static IdentityAssign requireIdentity(
      VtlParser.VarIDContext varId, VtlParser.ExprContext expr) {
    String source = asVarId(expr);
    if (source == null) {
      throw unsupported(describe(expr));
    }
    return new IdentityAssign(varId.getText(), source, text(expr));
  }

  private static String asVarId(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = expr;
    while (current instanceof VtlParser.ParenthesisExprContext parenthesis) {
      current = parenthesis.expr();
    }
    if (current instanceof VtlParser.VarIdExprContext varId) {
      return varId.varID().getText();
    }
    return null;
  }

  private static ProvGraph emit(
      List<IdentityAssign> assigns, List<InputDataset> inputs, StructureOracle oracle) {
    ProvGraph graph = new ProvGraph();
    Map<String, String> versions = new LinkedHashMap<>();
    for (InputDataset input : inputs) {
      versions.put(input.name(), input.name() + "@0");
    }
    int stmtIndex = 0;
    for (IdentityAssign assign : assigns) {
      stmtIndex++;
      String srcId = versions.get(assign.src());
      if (srcId == null) {
        throw new IllegalStateException("unknown dataset " + assign.src());
      }
      String outId = assign.out() + "@" + stmtIndex;
      DataStructure srcStructure = oracle.requireDataset(assign.src());
      DataStructure outStructure = oracle.requireDataset(assign.out());
      addDataset(graph, srcId, srcStructure, null);
      addDataset(graph, outId, outStructure, assign.srcText());
      graph.addEdge(outId, srcId, Map.of("op", "assign"));
      for (Component component : outStructure.values()) {
        graph.addEdge(
            outId + "." + component.getName(),
            srcId + "." + component.getName(),
            Map.of("op", "assign"));
      }
      versions.put(assign.out(), outId);
    }
    return graph;
  }

  private static void addDataset(ProvGraph graph, String id, DataStructure structure, String src) {
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

  private static VtlParser.StartContext parse(String script) {
    VtlLexer lexer = new VtlLexer(CharStreams.fromString(script));
    return new VtlParser(new CommonTokenStream(lexer)).start();
  }

  private static String describe(VtlParser.ExprContext expr) {
    if (expr instanceof VtlParser.ArithmeticExprContext
        || expr instanceof VtlParser.ArithmeticExprOrConcatContext
        || expr instanceof VtlParser.UnaryExprContext) {
      return "arithmetic";
    }
    if (expr instanceof VtlParser.ClauseExprContext) {
      return "clause";
    }
    if (expr instanceof VtlParser.FunctionsExpressionContext) {
      return "functions";
    }
    if (expr instanceof VtlParser.ConstantExprContext) {
      return "scalar";
    }
    return text(expr);
  }

  private static String text(ParserRuleContext ctx) {
    CharStream input = ctx.getStart().getInputStream();
    return input.getText(Interval.of(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex()));
  }

  private static UnsupportedOperationException unsupported(String what) {
    return new UnsupportedOperationException("unsupported: " + what);
  }

  private record IdentityAssign(String out, String src, String srcText) {}
}
