package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <code>ComparisonFunctionsVisitor</code> is the base visitor for expressions involving comparison
 * functions.
 */
public class ComparisonFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor exprVisitor;
  private final GenericFunctionsVisitor genericFunctionsVisitor;

  public static Boolean between(Number operand, Number from, Number to) {
    // TODO: handle other types (dates?)
    if (operand == null || from == null || to == null) {
      return null;
    }
    BigDecimal operandValue =
        operand instanceof Long
            ? BigDecimal.valueOf(operand.longValue())
            : BigDecimal.valueOf(operand.doubleValue());
    BigDecimal fromValue =
        from instanceof Long
            ? BigDecimal.valueOf(from.longValue())
            : BigDecimal.valueOf(from.doubleValue());
    BigDecimal toValue =
        to instanceof Long
            ? BigDecimal.valueOf(to.longValue())
            : BigDecimal.valueOf(to.doubleValue());
    return operandValue.compareTo(fromValue) >= 0 && operandValue.compareTo(toValue) <= 0;
  }

  public static Boolean charsetMatch(String operandValue, String patternValue) {
    if (operandValue == null || patternValue == null) {
      return null;
    }
    Pattern pattern = Pattern.compile(patternValue);
    Matcher matcher = pattern.matcher(operandValue);
    return matcher.matches();
  }

  public static Boolean isNull(Object obj) {
    if (obj == null) {
      return Boolean.TRUE;
    }
    return Boolean.FALSE;
  }

  /**
   * Constructor taking an expression visitor.
   *
   * @param expressionVisitor The visitor for the enclosing expression.
   */
  public ComparisonFunctionsVisitor(
      ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
    exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
  }

  /**
   * Visits a 'between' expression with scalar operand and delimiters.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to a boolean (<code>true</code> if the
   *     operand is between the delimiters).
   */
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

  /**
   * Visits a pattern matching expression with string operand and regular expression.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to a boolean (<code>true</code> if the
   *     operand matches the pattern).
   */
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

  /**
   * Visits a null testing expression with scalar operand.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to a boolean (<code>true</code> if the
   *     operand is null).
   */
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
