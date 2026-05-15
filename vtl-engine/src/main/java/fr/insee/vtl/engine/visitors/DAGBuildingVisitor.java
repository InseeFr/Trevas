package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.antlr.runtime.RuleContext;
import fr.insee.vtl.antlr.runtime.Token;
import fr.insee.vtl.antlr.runtime.tree.RuleNode;
import fr.insee.vtl.antlr.runtime.tree.TerminalNode;
import fr.insee.vtl.engine.utils.dag.DAGStatement;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** <code>DagbuildingVisitor</code> is the visitor for creating a DAG from VTL statements. */
public class DAGBuildingVisitor extends VtlBaseVisitor<List<DAGStatement>> {

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
    VtlParser.VarIDContext varIdCtx = node.varID();
    VtlParser.ExprContext expr = node.expr();
    return List.of(
        DAGStatement.of(
            DAGStatement.Identifier.Type.VARIABLE,
            varIdCtx.IDENTIFIER(),
            new IdentifierExtractingVisitor().visit(expr),
            node));
  }

  // Extract statements that can be reordered
  @Override
  public List<DAGStatement> visitPersistAssignment(VtlParser.PersistAssignmentContext node) {
    VtlParser.VarIDContext varIdCtx = node.varID();
    VtlParser.ExprContext expr = node.expr();
    return List.of(
        DAGStatement.of(
            DAGStatement.Identifier.Type.VARIABLE,
            varIdCtx.IDENTIFIER(),
            new IdentifierExtractingVisitor().visit(expr),
            node));
  }

  // Extract statements that can be reordered
  // Concrete define expression is child of this statement
  @Override
  public List<DAGStatement> visitDefineExpression(VtlParser.DefineExpressionContext node) {
    return super.visitChildren(node);
  }

  // Extract statements that can be reordered
  @Override
  public List<DAGStatement> visitDefOperator(VtlParser.DefOperatorContext node) {
    final VtlParser.OperatorIDContext operatorId = node.operatorID();
    final List<VtlParser.ParameterItemContext> parameters = node.parameterItem();
    final Set<String> ignoreInnerScopedVarIdentifiers =
        parameters.stream()
            .map(VtlParser.ParameterItemContext::varID)
            .map(VtlParser.VarIDContext::IDENTIFIER)
            .map(TerminalNode::getSymbol)
            .map(Token::getText)
            .collect(Collectors.toSet());
    final VtlParser.ExprContext expr = node.expr();
    return List.of(
        DAGStatement.of(
            DAGStatement.Identifier.Type.OPERATOR,
            operatorId.IDENTIFIER(),
            new IdentifierExtractingVisitor(ignoreInnerScopedVarIdentifiers).visit(expr),
            // Define statements have a DefineExpressionContext-Parent, so we need to reorder the
            // parent
            node.getParent()));
  }

  // Extract statements that can be reordered
  @Override
  public List<DAGStatement> visitDefDatapointRuleset(VtlParser.DefDatapointRulesetContext node) {
    final VtlParser.RulesetIDContext rulesetId = node.rulesetID();
    final VtlParser.RulesetSignatureContext signature = node.rulesetSignature();
    final VtlParser.RuleClauseDatapointContext ruleClause = node.ruleClauseDatapoint();
    final Set<String> ignoreInnerScopedVarIdentifiers =
        signature.signature().stream()
            .map(
                signatureVar ->
                    signatureVar.alias() != null
                        ? signatureVar.alias().IDENTIFIER()
                        : signatureVar.varID().IDENTIFIER())
            .map(TerminalNode::getSymbol)
            .map(Token::getText)
            .collect(Collectors.toSet());
    return List.of(
        DAGStatement.of(
            DAGStatement.Identifier.Type.RULESET_DATAPOINT,
            rulesetId.IDENTIFIER(),
            new IdentifierExtractingVisitor(ignoreInnerScopedVarIdentifiers).visit(ruleClause),
            // Define statements have a DefineExpressionContext-Parent, so we need to reorder the
            // parent
            node.getParent()));
  }

  // Extract statements that can be reordered
  @Override
  public List<DAGStatement> visitDefHierarchical(VtlParser.DefHierarchicalContext node) {
    final VtlParser.RulesetIDContext rulesetId = node.rulesetID();
    final VtlParser.HierRuleSignatureContext signature = node.hierRuleSignature();
    final VtlParser.RuleClauseHierarchicalContext ruleClause = node.ruleClauseHierarchical();
    final Set<String> ignoreInnerScopedVarIdentifiers =
        Optional.ofNullable(signature.valueDomainSignature())
            .map(VtlParser.ValueDomainSignatureContext::signature)
            .stream()
            .flatMap(Collection::stream)
            .map(
                signatureVar ->
                    signatureVar.alias() != null
                        ? signatureVar.alias().IDENTIFIER()
                        : signatureVar.varID().IDENTIFIER())
            .map(TerminalNode::getSymbol)
            .map(Token::getText)
            .collect(Collectors.toSet());
    return List.of(
        DAGStatement.of(
            DAGStatement.Identifier.Type.RULESET_HIERARCHICAL,
            rulesetId.IDENTIFIER(),
            new IdentifierExtractingVisitor(ignoreInnerScopedVarIdentifiers).visit(ruleClause),
            // Define statements have a DefineExpressionContext-Parent, so we need to reorder the
            // parent
            node.getParent()));
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
  static class IdentifierExtractingVisitor extends VtlBaseVisitor<Set<DAGStatement.Identifier>> {

    private final Set<String> ignoreInnerScopedVarIdentifiers;
    private int componentContextDepth = 0;

    public IdentifierExtractingVisitor() {
      this(Set.of());
    }

    public IdentifierExtractingVisitor(Set<String> ignoreInnerScopedVarIdentifiers) {
      super();
      this.ignoreInnerScopedVarIdentifiers = ignoreInnerScopedVarIdentifiers;
    }

    private Set<DAGStatement.Identifier> enterRestrictedContext(RuleContext ctx) {
      componentContextDepth++;
      Set<DAGStatement.Identifier> result = super.visitChildren(ctx);
      componentContextDepth--;
      return result;
    }

    @Override
    public Set<DAGStatement.Identifier> visitVarID(VtlParser.VarIDContext node) {
      final var currentVarIdentifier = node.IDENTIFIER().getSymbol().getText();

      // If we are inside a component context (depth > 0), we ignore this identifier
      // because it is a component name, not a dataset dependency, according to the unadjusted VTL
      // syntax
      if (componentContextDepth > 0) {
        return Set.of();
      }

      return ignoreInnerScopedVarIdentifiers.contains(currentVarIdentifier)
          ? Set.of()
          : Set.of(
              new DAGStatement.Identifier(
                  DAGStatement.Identifier.Type.VARIABLE, currentVarIdentifier));
    }

    // Workaround for https://github.com/InseeFr/Trevas/issues/457, as long the open points in here
    // are not clarified and https://github.com/InseeFr/Trevas/issues/355 is not implemented
    // If we are inside a component context (depth > 0), we ignore this identifier
    // because it is a component name, not a dataset dependency, according to the unadjusted VTL
    // syntax
    @Override
    public Set<DAGStatement.Identifier> visitFilterClause(VtlParser.FilterClauseContext ctx) {
      return enterRestrictedContext(ctx);
    }

    @Override
    public Set<DAGStatement.Identifier> visitCalcClauseItem(VtlParser.CalcClauseItemContext ctx) {
      return enterRestrictedContext(ctx);
    }

    @Override
    public Set<DAGStatement.Identifier> visitHavingClause(VtlParser.HavingClauseContext ctx) {
      return enterRestrictedContext(ctx);
    }

    @Override
    public Set<DAGStatement.Identifier> visitJoinApplyClause(VtlParser.JoinApplyClauseContext ctx) {
      return enterRestrictedContext(ctx);
    }

    @Override
    public Set<DAGStatement.Identifier> visitAggrFunctionClause(
        VtlParser.AggrFunctionClauseContext ctx) {
      return enterRestrictedContext(ctx);
    }

    // Aggregate & Analytic Functions (First arg is Dataset, rest are components)
    @Override
    public Set<DAGStatement.Identifier> visitAggrDataset(VtlParser.AggrDatasetContext ctx) {
      Set<DAGStatement.Identifier> datasetRef = visit(ctx.expr());
      return aggregateResult(datasetRef, enterRestrictedContext(ctx));
    }

    @Override
    public Set<DAGStatement.Identifier> visitAnSimpleFunction(
        VtlParser.AnSimpleFunctionContext ctx) {
      Set<DAGStatement.Identifier> datasetRef = visit(ctx.expr());
      return aggregateResult(datasetRef, enterRestrictedContext(ctx));
    }

    @Override
    public Set<DAGStatement.Identifier> visitLagOrLeadAn(VtlParser.LagOrLeadAnContext ctx) {
      Set<DAGStatement.Identifier> datasetRef = visit(ctx.expr());
      return aggregateResult(datasetRef, enterRestrictedContext(ctx));
    }

    @Override
    public Set<DAGStatement.Identifier> visitRatioToReportAn(VtlParser.RatioToReportAnContext ctx) {
      Set<DAGStatement.Identifier> datasetRef = visit(ctx.expr());
      return aggregateResult(datasetRef, enterRestrictedContext(ctx));
    }

    @Override
    public Set<DAGStatement.Identifier> visitRankAn(VtlParser.RankAnContext ctx) {

      return enterRestrictedContext(ctx);
    }

    @Override
    public Set<DAGStatement.Identifier> visitMembershipExpr(VtlParser.MembershipExprContext ctx) {
      // Only visit the dataset (left side), ignore the component (right side)
      return visit(ctx.expr());
    }

    @Override
    public Set<DAGStatement.Identifier> visitValidateDPruleset(
        VtlParser.ValidateDPrulesetContext node) {
      Set<DAGStatement.Identifier> rulesetRef =
          Set.of(
              new DAGStatement.Identifier(
                  DAGStatement.Identifier.Type.RULESET_DATAPOINT,
                  node.IDENTIFIER().getSymbol().getText()));
      return aggregateResult(rulesetRef, visit(node.expr()));
    }

    @Override
    public Set<DAGStatement.Identifier> visitValidateHRruleset(
        VtlParser.ValidateHRrulesetContext node) {
      Set<DAGStatement.Identifier> rulesetRef =
          Set.of(
              new DAGStatement.Identifier(
                  DAGStatement.Identifier.Type.RULESET_HIERARCHICAL,
                  node.IDENTIFIER().getSymbol().getText()));
      return aggregateResult(rulesetRef, visit(node.expr()));
    }

    @Override
    public Set<DAGStatement.Identifier> visitOperatorID(VtlParser.OperatorIDContext node) {
      return Set.of(
          new DAGStatement.Identifier(
              DAGStatement.Identifier.Type.OPERATOR, node.IDENTIFIER().getSymbol().getText()));
    }

    @Override
    protected Set<DAGStatement.Identifier> aggregateResult(
        Set<DAGStatement.Identifier> aggregate, Set<DAGStatement.Identifier> nextResult) {
      return Stream.concat(aggregate.stream(), nextResult.stream()).collect(Collectors.toSet());
    }

    @Override
    protected Set<DAGStatement.Identifier> defaultResult() {
      return Set.of();
    }
  }
}
