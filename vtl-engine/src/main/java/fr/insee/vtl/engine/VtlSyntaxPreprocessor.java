package fr.insee.vtl.engine;

import fr.insee.vtl.engine.utils.dag.DAGBuilder;
import fr.insee.vtl.engine.utils.dag.DAGStatement;
import fr.insee.vtl.engine.visitors.DAGBuildingVisitor;
import fr.insee.vtl.model.exceptions.VtlMultiErrorScriptException;
import fr.insee.vtl.model.exceptions.VtlMultiStatementScriptException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.util.*;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.ParserRuleContext;

/**
 * Class for preprocessing the VTL script for resolving script errors and reordering statements
 * based on the variables dependency order
 */
public class VtlSyntaxPreprocessor {

  private final VtlParser.StartContext startContext;
  private final List<DAGStatement> unsortedStatements;

  public VtlSyntaxPreprocessor(VtlParser.StartContext startContext) {
    this.startContext = startContext;
    DAGBuildingVisitor visitor = new DAGBuildingVisitor();
    this.unsortedStatements = visitor.visit(startContext);
  }

  /**
   * Method to check for multiple assignments of variables and reorder the VTL script according to
   * the variables dependency order as defined by the VTL standard.
   *
   * @return reordered VTL
   * @throws VtlScriptException when variables are assigned multiple times
   */
  public VtlParser.StartContext checkForMultipleAssignmentsAndReorderScript()
      throws VtlScriptException {
    checkForMultipleAssignments();

    // Create DAG & topological sort
    DAGBuilder dagBuilder = new DAGBuilder(unsortedStatements, startContext);
    List<DAGStatement> sortedStatements = dagBuilder.topologicalSortedStatements();

    VtlParser.StartContext startReordered =
        new VtlParser.StartContext(
            (ParserRuleContext) startContext.getRuleContext(), startContext.invokingState);

    // Build a set of unsorted indices that need reordering
    Set<Integer> unsortedIndices =
        sortedStatements.stream().map(DAGStatement::unsortedIndex).collect(Collectors.toSet());

    int sortedIndex = 0;
    for (int i = 0; i < startContext.getChildCount(); i++) {
      if (unsortedIndices.contains(i)) {
        DAGStatement stmt = sortedStatements.get(sortedIndex++);
        startReordered.addAnyChild(startContext.getChild(stmt.unsortedIndex()));
      } else {
        startReordered.addAnyChild(startContext.getChild(i));
      }
    }
    return startReordered;
  }

  /**
   * Method to check for multiple assignments of variables.
   *
   * @throws VtlScriptException when variables are assigned multiple times.
   */
  public void checkForMultipleAssignments() throws VtlScriptException {
    Map<String, List<DAGStatement>> groupedByProducedVar =
        unsortedStatements.stream().collect(Collectors.groupingBy(DAGStatement::produces));

    List<VtlMultiStatementScriptException> multiProducedExceptions =
        groupedByProducedVar.entrySet().stream()
            .filter(produced -> produced.getValue().size() > 1)
            .map(
                multiProduced ->
                    DAGStatement.buildMultiStatementExceptionUsingTheLastDAGStatementAsMainPosition(
                        "Dataset " + multiProduced.getKey() + " has already been assigned",
                        multiProduced.getValue(),
                        startContext))
            .toList();

    if (!multiProducedExceptions.isEmpty()) {
      throw VtlMultiErrorScriptException.usingTheFirstMainPositionExceptionAsCause(
          multiProducedExceptions);
    }
  }
}
