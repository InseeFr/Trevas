package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.List;
import java.util.Objects;

/** Dispatch for numeric function parse-tree nodes. */
public class NumericFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor exprVisitor;
  private final GenericFunctionsVisitor genericFunctionsVisitor;

  private final String UNKNOWN_OPERATOR = "unknown operator ";

  public NumericFunctionsVisitor(
      ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
    this.exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
  }

  @Override
  public ResolvableExpression visitUnaryNumeric(VtlParser.UnaryNumericContext ctx) {
    try {
      VtlParser.ExprContext expr = ctx.expr();
      List<ResolvableExpression> parameter = List.of(exprVisitor.visit(expr));
      return switch (ctx.op.getType()) {
        case VtlParser.CEIL ->
            genericFunctionsVisitor.invokeFunction("ceil", parameter, fromContext(ctx));
        case VtlParser.FLOOR ->
            genericFunctionsVisitor.invokeFunction("floor", parameter, fromContext(ctx));
        case VtlParser.ABS ->
            genericFunctionsVisitor.invokeFunction("abs", parameter, fromContext(ctx));
        case VtlParser.EXP ->
            genericFunctionsVisitor.invokeFunction("exp", parameter, fromContext(ctx));
        case VtlParser.LN ->
            genericFunctionsVisitor.invokeFunction("ln", parameter, fromContext(ctx));
        case VtlParser.SQRT ->
            genericFunctionsVisitor.invokeFunction("sqrt", parameter, fromContext(ctx));
        default -> throw new UnsupportedOperationException(UNKNOWN_OPERATOR + ctx);
      };
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  @Override
  public ResolvableExpression visitUnaryWithOptionalNumeric(
      VtlParser.UnaryWithOptionalNumericContext ctx) {
    try {
      var pos = fromContext(ctx);
      List<ResolvableExpression> parameters =
          List.of(
              exprVisitor.visit(ctx.expr()),
              ctx.optionalExpr() == null
                  ? ResolvableExpression.withType(Long.class).withPosition(pos).using(c -> 0L)
                  : exprVisitor.visit(ctx.optionalExpr()));
      return switch (ctx.op.getType()) {
        case VtlParser.ROUND ->
            genericFunctionsVisitor.invokeFunction("round", parameters, fromContext(ctx));
        case VtlParser.TRUNC ->
            genericFunctionsVisitor.invokeFunction("trunc", parameters, fromContext(ctx));
        default -> throw new UnsupportedOperationException(UNKNOWN_OPERATOR + ctx);
      };
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  @Override
  public ResolvableExpression visitBinaryNumeric(VtlParser.BinaryNumericContext ctx) {
    try {
      List<ResolvableExpression> parameters =
          List.of(exprVisitor.visit(ctx.left), exprVisitor.visit(ctx.right));
      return switch (ctx.op.getType()) {
        case VtlParser.MOD ->
            genericFunctionsVisitor.invokeFunction("mod", parameters, fromContext(ctx));
        case VtlParser.POWER ->
            genericFunctionsVisitor.invokeFunction("power", parameters, fromContext(ctx));
        case VtlParser.RANDOM ->
            genericFunctionsVisitor.invokeFunction("random", parameters, fromContext(ctx));
        case VtlParser.LOG ->
            genericFunctionsVisitor.invokeFunction("log", parameters, fromContext(ctx));
        default -> throw new UnsupportedOperationException(UNKNOWN_OPERATOR + ctx);
      };
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }
}
