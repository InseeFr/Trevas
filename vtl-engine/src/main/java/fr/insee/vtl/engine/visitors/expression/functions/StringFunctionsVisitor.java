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

/** Dispatch for string function parse-tree nodes. */
public class StringFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor exprVisitor;
  private final GenericFunctionsVisitor genericFunctionsVisitor;

  public StringFunctionsVisitor(
      ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
    this.exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
  }

  @Override
  public ResolvableExpression visitUnaryStringFunction(VtlParser.UnaryStringFunctionContext ctx) {
    try {
      var pos = fromContext(ctx);
      var parameters = List.of(exprVisitor.visit(ctx.expr()));
      return switch (ctx.op.getType()) {
        case VtlParser.TRIM -> genericFunctionsVisitor.invokeFunction("trim", parameters, pos);
        case VtlParser.LTRIM -> genericFunctionsVisitor.invokeFunction("ltrim", parameters, pos);
        case VtlParser.RTRIM -> genericFunctionsVisitor.invokeFunction("rtrim", parameters, pos);
        case VtlParser.UCASE -> genericFunctionsVisitor.invokeFunction("ucase", parameters, pos);
        case VtlParser.LCASE -> genericFunctionsVisitor.invokeFunction("lcase", parameters, pos);
        case VtlParser.LEN -> genericFunctionsVisitor.invokeFunction("len", parameters, pos);
        default -> throw new UnsupportedOperationException("unknown operator " + ctx.op.getText());
      };
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  @Override
  public ResolvableExpression visitSubstrAtom(VtlParser.SubstrAtomContext ctx) {
    try {
      var pos = fromContext(ctx);
      ResolvableExpression expr =
          ctx.expr() == null
              ? ResolvableExpression.withType(String.class).withPosition(pos).using(c -> null)
              : exprVisitor.visit(ctx.expr());
      ResolvableExpression start =
          ctx.startParameter == null
              ? ResolvableExpression.withType(Long.class).withPosition(pos).using(c -> null)
              : exprVisitor.visit(ctx.startParameter);
      ResolvableExpression len =
          ctx.endParameter == null
              ? ResolvableExpression.withType(Long.class).withPosition(pos).using(c -> null)
              : exprVisitor.visit(ctx.endParameter);
      List<ResolvableExpression> parameters = List.of(expr, start, len);
      return genericFunctionsVisitor.invokeFunction("substr", parameters, fromContext(ctx));
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  @Override
  public ResolvableExpression visitReplaceAtom(VtlParser.ReplaceAtomContext ctx) {
    try {
      var pos = fromContext(ctx);
      ResolvableExpression expr =
          ctx.expr(0) == null
              ? ResolvableExpression.withType(String.class).withPosition(pos).using(c -> null)
              : exprVisitor.visit(ctx.expr(0));
      ResolvableExpression param =
          ctx.param == null
              ? ResolvableExpression.withType(String.class).withPosition(pos).using(c -> null)
              : exprVisitor.visit(ctx.param);
      ResolvableExpression optionalExpr =
          ctx.optionalExpr() == null
              ? ResolvableExpression.withType(String.class).withPosition(pos).using(c -> null)
              : exprVisitor.visit(ctx.optionalExpr());
      List<ResolvableExpression> parameters = List.of(expr, param, optionalExpr);

      return genericFunctionsVisitor.invokeFunction("replace", parameters, fromContext(ctx));
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  @Override
  public ResolvableExpression visitInstrAtom(VtlParser.InstrAtomContext ctx) {
    try {
      var pos = fromContext(ctx);
      ResolvableExpression expr =
          ctx.expr(0) == null
              ? ResolvableExpression.withType(String.class).withPosition(pos).using(c -> null)
              : exprVisitor.visit(ctx.expr(0));
      ResolvableExpression pattern =
          ctx.pattern == null
              ? ResolvableExpression.withType(String.class).withPosition(pos).using(c -> null)
              : exprVisitor.visit(ctx.pattern);
      ResolvableExpression start =
          ctx.startParameter == null
              ? ResolvableExpression.withType(Long.class).withPosition(pos).using(c -> null)
              : exprVisitor.visit(ctx.startParameter);
      ResolvableExpression occurence =
          ctx.occurrenceParameter == null
              ? ResolvableExpression.withType(Long.class).withPosition(pos).using(c -> null)
              : exprVisitor.visit(ctx.occurrenceParameter);
      List<ResolvableExpression> parameters = List.of(expr, pattern, start, occurence);

      return genericFunctionsVisitor.invokeFunction("instr", parameters, fromContext(ctx));
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }
}
