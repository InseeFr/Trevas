package fr.insee.vtl.engine.utils.dag;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlMultiStatementScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Representation of a VTL Statement
 *
 * @param unsortedIndex index of the original parsed statement without resorting
 * @param produces Produced data
 * @param consumes Consumed data
 */
public record DAGStatement(int unsortedIndex, String produces, Set<String> consumes) {

  public static VtlMultiStatementScriptException
      buildMultiStatementExceptionUsingTheLastDAGStatementAsMainPosition(
          final String message,
          final Collection<DAGStatement> dagStatements,
          VtlParser.StartContext startContext) {
    // The last statement according to the order in the original script is defined as main Position
    final DAGStatement lastDagStatement =
        dagStatements.stream()
            .max(Comparator.comparingInt(DAGStatement::unsortedIndex))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "A VtlMultiStatementScriptException must contain of at least two statements"));

    Set<DAGStatement> rest = new HashSet<>(dagStatements);
    rest.remove(lastDagStatement);

    Set<Positioned> restPositions =
        rest.stream().map(element -> element.getPosition(startContext)).collect(Collectors.toSet());

    return new VtlMultiStatementScriptException(
        message, lastDagStatement.getPosition(startContext), restPositions);
  }

  @Override
  public String toString() {
    return "Statement{"
        + "unsortedIndex='"
        + unsortedIndex
        + '\''
        + ", produces="
        + produces
        + ", consumes="
        + consumes
        + '}';
  }

  public Positioned getPosition(final VtlParser.StartContext startContext) {
    return VtlScriptEngine.fromContext(startContext.getChild(unsortedIndex));
  }
}
