package fr.insee.vtl.engine;

import fr.insee.vtl.engine.utils.dag.DAGBuilder;
import fr.insee.vtl.engine.utils.dag.DAGStatement;
import fr.insee.vtl.engine.visitors.DAGBuildingVisitor;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.ParserRuleContext;

/** Class for reordering the VTL script based on its DAG */
public class DAGScriptReordering {

  /**
   * Method to reorder the VTL script according to the variables dependency order as defined by the
   * VTL standard.
   *
   * @param start Root of parsetree
   * @return reordered VTL
   */
  public static VtlParser.StartContext reorderScript(final VtlParser.StartContext start)
      throws VtlScriptException {
    DAGBuildingVisitor visitor = new DAGBuildingVisitor();
    List<DAGStatement> unsortedStatements = visitor.visit(start);

    // Create DAG & topological sort
    DAGBuilder dagBuilder = new DAGBuilder(unsortedStatements, start);
    List<DAGStatement> sortedStatements = dagBuilder.topologicalSortedStatements();

    VtlParser.StartContext startReordered =
        new VtlParser.StartContext((ParserRuleContext) start.getRuleContext(), start.invokingState);

    // Build a set of unsorted indices that need reordering
    Set<Integer> sortedIndices =
        sortedStatements.stream().map(DAGStatement::unsortedIndex).collect(Collectors.toSet());

    int sortedIndex = 0;
    for (int i = 0; i < start.getChildCount(); i++) {
      if (sortedIndices.contains(i)) {
        DAGStatement stmt = sortedStatements.get(sortedIndex++);
        startReordered.addAnyChild(start.getChild(stmt.unsortedIndex()));
      } else {
        startReordered.addAnyChild(start.getChild(i));
      }
    }
    return startReordered;
  }
}
