package fr.insee.vtl.engine.utils.dag;

import java.util.*;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;

/** Helper Class to build a DAG from VTL */
public class DAGBuilder {

    private final Graph<DAGStatement, DefaultEdge> graph;

    public DAGBuilder(List<DAGStatement> statements) {
        graph = buildDAG(statements);
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

    public List<DAGStatement> topologicalSortedStatements() {
        final List<DAGStatement> topologicalSorted = new ArrayList<>();
        new TopologicalOrderIterator<>(graph).forEachRemaining(topologicalSorted::add);
        return topologicalSorted;
    }
}
