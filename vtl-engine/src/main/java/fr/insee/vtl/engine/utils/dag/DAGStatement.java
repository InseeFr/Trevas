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
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * Representation of a VTL Statement
 *
 * @param unsortedIndex index of the original parsed statement without resorting
 * @param produces Produced data
 * @param consumes Consumed data
 */
public record DAGStatement(int unsortedIndex, Identifier produces, Set<Identifier> consumes) {

  public static int PSEUDO_BINDING_POSITION = Integer.MIN_VALUE;

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

  public static DAGStatement of(
      Identifier.Type outIdentifierType,
      TerminalNode outIdentifierNode,
      Set<Identifier> inIdentifiers,
      ParserRuleContext node) {
    Identifier rulesetOutIdentifier =
        new Identifier(outIdentifierType, outIdentifierNode.getSymbol().getText());
    final int statementIndex = getParentStatementIndex(node);
    return new DAGStatement(statementIndex, rulesetOutIdentifier, inIdentifiers);
  }

  private static int getParentStatementIndex(final RuleNode node) {
    final ParseTree parent = node.getParent();
    for (int i = 0; i < parent.getChildCount(); ++i) {
      final ParseTree child = parent.getChild(i);
      if (child == node) {
        return i;
      }
    }
    throw new AssertionError("Statement must always be part of the its parent node");
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

  public record Identifier(Type identifierType, String name) {
    public enum Type {
      VARIABLE,
      OPERATOR,
      RULESET_HIERARCHICAL,
      RULESET_DATAPOINT
    }
  }
}
