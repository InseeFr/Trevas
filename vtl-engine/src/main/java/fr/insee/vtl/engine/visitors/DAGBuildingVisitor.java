package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.utils.dag.DAGStatement;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;

/** <code>DagbuildingVisitor</code> is the visitor for creating a DAG from VTL statements. */
public class DAGBuildingVisitor extends VtlBaseVisitor<List<DAGStatement>> {

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
  public List<DAGStatement> visitChildren(RuleNode node) {
    throw new AssertionError(
        "DAGBuildingVisitor only supports StartContext, Assignments and DefineExpressions");
  }

  @Override // explicit call to super, as visiting children is only supported for the start node
  // (only top level statements can be reordered)
  public List<DAGStatement> visitStart(VtlParser.StartContext node) {
    return super.visitChildren(node);
  }

  // Extract statements that can be reordered
  @Override
  public List<DAGStatement> visitTemporaryAssignment(VtlParser.TemporaryAssignmentContext node) {
    return visitAnyAssignment(node, node.varID(), node.expr());
  }

  // Extract statements that can be reordered
  @Override
  public List<DAGStatement> visitPersistAssignment(VtlParser.PersistAssignmentContext node) {
    return visitAnyAssignment(node, node.varID(), node.expr());
  }

  // Ignore for reordering
  @Override
  public List<DAGStatement> visitDefineExpression(VtlParser.DefineExpressionContext ctx) {
    return defaultResult();
  }

  private List<DAGStatement> visitAnyAssignment(
      VtlParser.StatementContext node,
      VtlParser.VarIDContext varIdCtx,
      VtlParser.ExprContext expr) {
    String assignmentOutVarId = varIdCtx.getText();
    Set<String> assignmentInVarIds = new VarIdsExtractingVisitor().visit(expr);

    final int statementIndex = getParentStatementIndex(node);
    return List.of(new DAGStatement(statementIndex, assignmentOutVarId, assignmentInVarIds));
  }

  @Override
  protected List<DAGStatement> aggregateResult(
      List<DAGStatement> aggregate, List<DAGStatement> nextResult) {
    return Stream.concat(aggregate.stream(), nextResult.stream()).toList();
  }

  @Override
  protected List<DAGStatement> defaultResult() {
    return List.of();
  }

  /**
   * <code>VarIDsExtractingVisitor</code> is the visitor for extracting the used VarIds from VTL
   * statements.
   */
  private static class VarIdsExtractingVisitor extends VtlBaseVisitor<Set<String>> {

    @Override
    public Set<String> visitVarID(VtlParser.VarIDContext node) {
      final Set<String> thisResult = Set.of(node.getText());
      final Set<String> subResult = this.visitChildren(node);
      return aggregateResult(thisResult, subResult);
    }

    @Override
    protected Set<String> aggregateResult(Set<String> aggregate, Set<String> nextResult) {
      return Stream.concat(aggregate.stream(), nextResult.stream()).collect(Collectors.toSet());
    }

    @Override
    protected Set<String> defaultResult() {
      return Set.of();
    }
  }
}
