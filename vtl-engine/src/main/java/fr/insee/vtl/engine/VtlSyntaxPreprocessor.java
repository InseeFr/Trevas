package fr.insee.vtl.engine;

import fr.insee.vtl.engine.utils.dag.DAGBuilder;
import fr.insee.vtl.engine.utils.dag.DAGStatement;
import fr.insee.vtl.engine.visitors.DAGBuildingVisitor;
import fr.insee.vtl.model.exceptions.VtlMultiErrorScriptException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.antlr.v4.runtime.ParserRuleContext;

/**
 * Class for preprocessing the VTL script for resolving script errors and reordering statements
 * based on the variables dependency order
 */
public class VtlSyntaxPreprocessor {

  private final VtlParser.StartContext startContext;
  private final Set<String> bindingVarIds;
  private final List<DAGStatement> unsortedStatements;

  public VtlSyntaxPreprocessor(VtlParser.StartContext startContext, Set<String> bindingVarIds) {
    this.startContext = startContext;
    this.bindingVarIds = bindingVarIds;
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
    List<DAGStatement> bindingPseudoStatements =
        bindingVarIds.stream()
            .map(
                bindingVarId ->
                    new DAGStatement(
                        DAGStatement.PSEUDO_BINDING_POSITION,
                        new DAGStatement.Identifier(
                            DAGStatement.Identifier.Type.VARIABLE, bindingVarId),
                        Set.of()))
            .toList();
    Map<DAGStatement.Identifier, List<DAGStatement>> groupedByProducedIdentifier =
        Stream.concat(bindingPseudoStatements.stream(), unsortedStatements.stream())
            .collect(Collectors.groupingBy(DAGStatement::produces));

    List<VtlScriptException> multiProducedExceptions =
        groupedByProducedIdentifier.entrySet().stream()
            .filter(produced -> produced.getValue().size() > 1)
            .map(
                multiProduced ->
                    buildScriptExceptionFromMultipleAssignment(
                        multiProduced.getKey(), multiProduced.getValue()))
            .toList();

    if (!multiProducedExceptions.isEmpty()) {
      throw VtlMultiErrorScriptException.of(
          multiProducedExceptions.toArray(new VtlScriptException[] {}));
    }
  }

  private VtlScriptException buildScriptExceptionFromMultipleAssignment(
      DAGStatement.Identifier identifier, List<DAGStatement> statements) {
    final List<DAGStatement> statementsWithoutBinding =
        statements.stream()
            .filter(statement -> statement.unsortedIndex() != DAGStatement.PSEUDO_BINDING_POSITION)
            .toList();

    if (statementsWithoutBinding.size() == 1) {
      return new VtlScriptException(
          "Dataset "
              + identifier.name()
              + " is part of the bindings and therefore cannot be assigned",
          statementsWithoutBinding.get(0).getPosition(startContext));
    }

    return DAGStatement.buildMultiStatementExceptionUsingTheLastDAGStatementAsMainPosition(
        "Dataset "
            + identifier.name()
            + " has already been assigned"
            + (statements.size() == statementsWithoutBinding.size()
                ? ""
                : " and is part of the bindings and therefore cannot be assigned"),
        statementsWithoutBinding,
        startContext);
  }
}
