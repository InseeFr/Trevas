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

/** Dispatch for multiplication or division expressions. */
public class ArithmeticVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor exprVisitor;
  private final GenericFunctionsVisitor genericFunctionsVisitor;

  public ArithmeticVisitor(
      ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
    exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
  }

  @Override
  public ResolvableExpression visitArithmeticExpr(VtlParser.ArithmeticExprContext ctx) {
    try {
      List<ResolvableExpression> parameters =
          List.of(exprVisitor.visit(ctx.left), exprVisitor.visit(ctx.right));
      return switch (ctx.op.getType()) {
        case VtlParser.MUL ->
            genericFunctionsVisitor.invokeFunction("multiplication", parameters, fromContext(ctx));
        case VtlParser.DIV ->
            genericFunctionsVisitor.invokeFunction("division", parameters, fromContext(ctx));
        default -> throw new UnsupportedOperationException("unknown operator " + ctx);
      };
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }
}
