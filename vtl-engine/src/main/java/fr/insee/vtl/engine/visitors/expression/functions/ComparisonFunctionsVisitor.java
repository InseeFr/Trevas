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

/** Dispatch for comparison function parse-tree nodes. */
public class ComparisonFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor exprVisitor;
  private final GenericFunctionsVisitor genericFunctionsVisitor;

  public ComparisonFunctionsVisitor(
      ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
    exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
  }

  @Override
  public ResolvableExpression visitBetweenAtom(VtlParser.BetweenAtomContext ctx) {
    try {
      List<ResolvableExpression> parameters =
          List.of(
              exprVisitor.visit(ctx.op), exprVisitor.visit(ctx.from_), exprVisitor.visit(ctx.to_));
      return genericFunctionsVisitor.invokeFunction("between", parameters, fromContext(ctx));
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  @Override
  public ResolvableExpression visitCharsetMatchAtom(VtlParser.CharsetMatchAtomContext ctx) {
    try {
      List<ResolvableExpression> parameters =
          List.of(exprVisitor.visit(ctx.op), exprVisitor.visit(ctx.pattern));
      return genericFunctionsVisitor.invokeFunction("charsetMatch", parameters, fromContext(ctx));
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  @Override
  public ResolvableExpression visitIsNullAtom(VtlParser.IsNullAtomContext ctx) {
    try {
      List<ResolvableExpression> parameters = List.of(exprVisitor.visit(ctx.expr()));
      return genericFunctionsVisitor.invokeFunction("isNull", parameters, fromContext(ctx));
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }
}
