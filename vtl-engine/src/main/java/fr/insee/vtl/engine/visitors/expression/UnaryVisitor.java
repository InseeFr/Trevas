package fr.insee.vtl.engine.visitors.expression;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.List;
import java.util.Objects;

/** Dispatch for unary expressions (plus, minus, not). */
public class UnaryVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor exprVisitor;
  private final GenericFunctionsVisitor genericFunctionsVisitor;

  public UnaryVisitor(
      ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
    exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.genericFunctionsVisitor = genericFunctionsVisitor;
  }

  @Override
  public ResolvableExpression visitUnaryExpr(VtlParser.UnaryExprContext ctx) {
    try {
      var pos = fromContext(ctx);
      var parameters = List.of(exprVisitor.visit(ctx.right));
      return switch (ctx.op.getType()) {
        case VtlParser.PLUS -> genericFunctionsVisitor.invokeFunction("plus", parameters, pos);
        case VtlParser.MINUS -> genericFunctionsVisitor.invokeFunction("minus", parameters, pos);
        case VtlParser.NOT -> genericFunctionsVisitor.invokeFunction("not", parameters, pos);
        default -> throw new UnsupportedOperationException("unknown operator " + ctx);
      };
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }
}
