package fr.insee.vtl.engine.utils.dag;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.util.*;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.jgrapht.Graph;
import org.jgrapht.alg.cycle.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;

/** Helper Class to build a DAG from VTL */
public class DAGBuilder {

  private final Graph<DAGStatement, DefaultEdge> graph;
  private final VtlParser.StartContext startContext;

  public DAGBuilder(List<DAGStatement> statements, VtlParser.StartContext startContext) {
    graph = buildDAG(statements);
    this.startContext = startContext;
  }

  /**
   * Builds a DAG from a list of VTL statements
   *
   * @param DAGStatements List of DAGStatements created from VTL statements
   */
  private static Graph<DAGStatement, DefaultEdge> buildDAG(List<DAGStatement> DAGStatements) {
    // Init graph
    Graph<DAGStatement, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
    for (DAGStatement stmt : DAGStatements) {
      graph.addVertex(stmt);
    }

    // Find dependencies and add edges
    for (DAGStatement stmt1 : DAGStatements) {
      for (DAGStatement stmt2 : DAGStatements) {
        if (!stmt1.equals(stmt2) && dependsOn(stmt2, stmt1)) {
          graph.addEdge(stmt1, stmt2);
        }
      }
    }
    return graph;
  }

  private static boolean dependsOn(DAGStatement stmt2, DAGStatement stmt1) {
    // Check if stmt2 consumes data from stmt1
    String produced = stmt1.produces();
    return stmt2.consumes().contains(produced);
  }

  public List<DAGStatement> topologicalSortedStatements() throws VtlScriptException {
    Optional<VtlScriptException> cycleError = checkForCycles();
    if (cycleError.isPresent()) {
      throw cycleError.get();
    }
    final List<DAGStatement> topologicalSorted = new ArrayList<>();
    new TopologicalOrderIterator<>(graph).forEachRemaining(topologicalSorted::add);
    return topologicalSorted;
  }

  private Optional<VtlScriptException> checkForCycles() {
    CycleDetector<DAGStatement, DefaultEdge> cycleDetector = new CycleDetector<>(graph);
    Set<DAGStatement> statementsInCycle = cycleDetector.findCycles();
    if (statementsInCycle.isEmpty()) {
      return Optional.empty();
    }
    String dagStatementsInCycle =
        statementsInCycle.stream()
            .map(d -> parseTreeToText(startContext.getChild(d.unsortedIndex())))
            .collect(Collectors.joining(";", "", ""));
    return Optional.of(
        new VtlScriptException(
            "Cycle detected in Script. The following statements form at least one cycle: "
                + dagStatementsInCycle,
            VtlScriptEngine.fromContext(startContext)));
  }

  private String parseTreeToText(ParseTree child) {
    StringBuilder result = new StringBuilder();
    if (child instanceof TerminalNode) {
      result.append(child.getText());
      result.append(" ");
    } else if (child instanceof RuleNode && child.getChildCount() != 0) {
      for (int i = 0; i < child.getChildCount(); ++i) {
        result.append(parseTreeToText(child.getChild(i)));
      }
    }
    return result.toString();
  }
}
