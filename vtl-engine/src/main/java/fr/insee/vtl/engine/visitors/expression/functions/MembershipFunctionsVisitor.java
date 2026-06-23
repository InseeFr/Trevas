package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.semantics.membership.MembershipExecutor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.Objects;

/**
 * Visitor dispatch for membership ({@code #}); orchestration lives in {@link MembershipExecutor}.
 */
public class MembershipFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor expressionVisitor;
  private final ProcessingEngine processingEngine;

  public MembershipFunctionsVisitor(
      ExpressionVisitor expressionVisitor, ProcessingEngine processingEngine) {
    this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
    this.processingEngine = Objects.requireNonNull(processingEngine);
  }

  @Override
  public ResolvableExpression visitMembershipExpr(VtlParser.MembershipExprContext ctx) {
    try {
      DatasetExpression ds =
          (DatasetExpression)
              assertTypeExpression(expressionVisitor.visit(ctx.expr()), Dataset.class, ctx.expr());
      String componentName = ctx.simpleComponentId().getText();
      if (!ds.getDataStructure().containsKey(componentName)) {
        throw new VtlScriptException(
            "column %s not found in %s".formatted(componentName, ctx.expr().getText()),
            fromContext(ctx));
      }
      return MembershipExecutor.execute(processingEngine, ds, componentName);
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }
}
