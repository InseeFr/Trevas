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
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

/**
 * <code>ComparisonFunctionsVisitor</code> is the base visitor for expressions involving string
 * functions.
 */
public class StringFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  static final Pattern LTRIM = Pattern.compile("^\\s+");
  static final Pattern RTRIM = Pattern.compile("\\s+$");

  private final ExpressionVisitor exprVisitor;
  private final GenericFunctionsVisitor genericFunctionsVisitor;

  /**
   * Constructor taking an expression visitor.
   *
   * @param expressionVisitor The visitor for the enclosing expression.
   */
  public StringFunctionsVisitor(
      ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
    this.exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
  }

  public static String trim(String value) {
    if (value == null) {
      return null;
    }
    return value.trim();
  }

  public static String ltrim(String value) {
    if (value == null) {
      return null;
    }
    return LTRIM.matcher(value).replaceAll("");
  }

  public static String rtrim(String value) {
    if (value == null) {
      return null;
    }
    return RTRIM.matcher(value).replaceAll("");
  }

  public static String ucase(String value) {
    if (value == null) {
      return null;
    }
    return value.toUpperCase();
  }

  public static String lcase(String value) {
    if (value == null) {
      return null;
    }
    return value.toLowerCase();
  }

  public static Long len(String value) {
    if (value == null) {
      return null;
    }
    return (long) value.length();
  }

  public static String substr(String value, Long start, Long len) {
    if (value == null) {
      return null;
    }
    if (start == null) {
      start = 1L;
    }
    if (len == null) {
      len = Long.valueOf(value.length());
    }
    if (start > value.length()) {
      return "";
    }
    if (start != 0) {
      start = start - 1;
    }

    var end = start + len;
    if (end > value.length()) {
      return value.substring(Math.toIntExact(start));
    }
    return value.substring(Math.toIntExact(start), Math.toIntExact(end));
  }

  public static String replace(String value, String pattern, String replacement) {
    if (value == null || pattern == null) {
      return null;
    }
    if (replacement == null) {
      replacement = "";
    }
    return value.replaceAll(pattern, replacement);
  }

  public static Long instr(String v, String v2, Long start, Long occurence) {
    if (v == null || v2 == null) {
      return null;
    }
    if (start == null) {
      start = 0L;
    }
    if (occurence == null) {
      occurence = 1L;
    }
    return StringUtils.ordinalIndexOf(v.substring(start.intValue()), v2, occurence.intValue()) + 1L;
  }

  /**
   * Visits expressions corresponding to unary string functions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the string function on
   *     the operand.
   */
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

  /**
   * Visits expressions corresponding to the substring function on a string operand.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the substring function
   *     on the operand.
   */
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

  /**
   * Visits expressions corresponding to the replace function on a string operand.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the replace function on
   *     the operand.
   */
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

  /**
   * Visits expressions corresponding to the pattern location function on a string operand.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the pattern location
   *     function on the operand.
   */
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
