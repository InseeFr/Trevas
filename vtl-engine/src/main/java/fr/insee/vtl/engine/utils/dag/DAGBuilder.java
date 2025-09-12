package fr.insee.vtl.engine.utils.dag;

import fr.insee.vtl.model.exceptions.VtlMultiErrorScriptException;
import fr.insee.vtl.model.exceptions.VtlMultiStatementScriptException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;
import org.jgrapht.Graph;
import org.jgrapht.alg.connectivity.GabowStrongConnectivityInspector;
import org.jgrapht.alg.interfaces.StrongConnectivityAlgorithm;
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
   * @param dagStatements List of DAGStatements created from VTL statements
   */
  private static Graph<DAGStatement, DefaultEdge> buildDAG(List<DAGStatement> dagStatements) {
    // Init graph
    Graph<DAGStatement, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
    for (DAGStatement stmt : dagStatements) {
      graph.addVertex(stmt);
    }

    // Find dependencies and add edges
    for (DAGStatement stmt1 : dagStatements) {
      for (DAGStatement stmt2 : dagStatements) {
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

  /**
   * Sorts the DAGStatements according to the topological dependency order
   *
   * @return DAGStatements sorted according to the topological dependency order
   * @throws VtlScriptException when the script contains a cycle
   */
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
    StrongConnectivityAlgorithm<DAGStatement, DefaultEdge> inspector =
        new GabowStrongConnectivityInspector<>(graph);
    List<Set<DAGStatement>> stronglyConnectedSets = inspector.stronglyConnectedSets();

    List<Set<DAGStatement>> cycles =
        stronglyConnectedSets.stream().filter(s -> s.size() > 1 || hasSelfLoop(s)).toList();

    if (cycles.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(buildVTLScriptExceptionForCycles(cycles));
  }

  private VtlScriptException buildVTLScriptExceptionForCycles(List<Set<DAGStatement>> cycles) {
    List<VtlMultiStatementScriptException> cycleExceptions =
        cycles.stream()
            .map(
                cycle ->
                    DAGStatement.buildMultiStatementExceptionUsingTheLastDAGStatementAsMainPosition(
                        "assignment creates a cycle: " + buildAssignmentChain(cycle),
                        cycle,
                        startContext))
            .toList();

    return VtlMultiErrorScriptException.usingTheFirstMainPositionExceptionAsCause(cycleExceptions);
  }

  private String buildAssignmentChain(Set<DAGStatement> cycle) {
    // Collect all produced variable names in this cycle
    Set<String> producedVars =
        cycle.stream().map(DAGStatement::produces).collect(Collectors.toSet());

    // Pick a stable start
    String startVar =
        producedVars.stream()
            .sorted()
            .findFirst()
            .orElseThrow(() -> new AssertionError("Cycle contains out of at least two statements"));

    StringBuilder sb = new StringBuilder();
    String current = startVar;

    do {
      sb.append(current).append(" <- ");

      // Find the unique statement that produces 'current'
      String finalCurrent = current;
      DAGStatement producer =
          cycle.stream()
              .filter(stmt -> stmt.produces().equals(finalCurrent))
              .reduce(
                  (a, b) -> {
                    throw new AssertionError(
                        "Multiple producers of "
                            + finalCurrent
                            + " cannot occur here, this is already validated before");
                  })
              .orElseThrow(
                  () ->
                      new AssertionError(
                          "A cycle is always closed, there must be a consumer for  "
                              + finalCurrent));

      // Choose the next consumed variable that stays inside the cycle
      current =
          producer.consumes().stream()
              .filter(producedVars::contains)
              .findFirst()
              .orElseThrow(
                  () ->
                      new AssertionError(
                          "Broken cycle at "
                              + finalCurrent
                              + ": no consumed var stays inside cycle"));
    } while (!current.equals(startVar));

    // close the loop
    sb.append(startVar);
    return "[" + sb + "]";
  }

  private boolean hasSelfLoop(Set<DAGStatement> set) {
    if (set.size() != 1) return false;
    DAGStatement v = set.iterator().next();
    return graph.containsEdge(v, v);
  }
}
