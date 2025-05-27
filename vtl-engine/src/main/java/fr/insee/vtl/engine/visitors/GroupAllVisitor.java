package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

public class GroupAllVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor expressionVisitor;

  public GroupAllVisitor(ExpressionVisitor expressionVisitor) {
    this.expressionVisitor = expressionVisitor;
  }

  @Override
  public ResolvableExpression visitGroupAll(VtlParser.GroupAllContext ctx) {
    return expressionVisitor.visit(ctx.expr());
  }
}
