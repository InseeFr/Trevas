package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 * <code>NumericFunctionsVisitor</code> is the visitor for expressions involving numeric functions.
 */
public class NumericFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor exprVisitor;
  private final GenericFunctionsVisitor genericFunctionsVisitor;

  private final String UNKNOWN_OPERATOR = "unknown operator ";

  /**
   * Constructor taking a scripting context.
   *
   * @param expressionVisitor The expression visitor.
   * @param genericFunctionsVisitor
   */
  public NumericFunctionsVisitor(
      ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
    this.exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
  }

  public static Long ceil(Number value) {
    if (value == null) {
      return null;
    }
    return (long) Math.ceil(value.doubleValue());
  }

  public static Long floor(Number value) {
    if (value == null) {
      return null;
    }
    return (long) Math.floor(value.doubleValue());
  }

  public static Double abs(Number value) {
    if (value == null) {
      return null;
    }
    return Math.abs(value.doubleValue());
  }

  public static Double exp(Number value) {
    if (value == null) {
      return null;
    }
    return Math.exp(value.doubleValue());
  }

  public static Double ln(Number value) {
    if (value == null) {
      return null;
    }
    return Math.log(value.doubleValue());
  }

  public static Double sqrt(Number value) {
    if (value == null) {
      return null;
    }
    if (value.doubleValue() < 0) {
      throw new IllegalArgumentException("operand has to be 0 or positive");
    }
    return Math.sqrt(value.doubleValue());
  }

  public static Double round(Number value, Long decimal) {
    if (decimal == null) {
      decimal = 0L;
    }
    if (value == null) {
      return null;
    }
    BigDecimal bd = new BigDecimal(Double.toString(value.doubleValue()));
    bd = bd.setScale(decimal.intValue(), RoundingMode.HALF_UP);
    return bd.doubleValue();
  }

  public static Double trunc(Number value, Long decimal) {
    if (decimal == null) {
      decimal = 0L;
    }
    if (value == null) {
      return null;
    }
    BigDecimal bd = new BigDecimal(Double.toString(value.doubleValue()));
    bd = bd.setScale(decimal.intValue(), RoundingMode.DOWN);
    return bd.doubleValue();
  }

  public static Double mod(Number left, Number right) {
    if (left == null || right == null) {
      return null;
    }
    if (right.doubleValue() == 0) {
      return left.doubleValue();
    }
    return (left.doubleValue() % right.doubleValue()) * (right.doubleValue() < 0 ? -1 : 1);
  }

  public static Double power(Number left, Number right) {
    if (left == null || right == null) {
      return null;
    }
    return Math.pow(left.doubleValue(), right.doubleValue());
  }

  public static Double random(Long left, Long right) {
    if (left == null || right == null) {
      return null;
    }
    Double res = null;
    Random random = new Random(left);
    for (int i = 0; i < right; i++) {
      res = random.nextDouble();
    }
    return res;
  }

  public static Double log(Number operand, Number base) {
    if (operand == null || base == null) {
      return null;
    }
    if (operand.doubleValue() <= 0) throw new IllegalArgumentException("operand must be positive");
    if (base.doubleValue() < 1)
      throw new IllegalArgumentException("base must be greater or equal than 1");
    return Math.log(operand.doubleValue()) / Math.log(base.doubleValue());
  }

  /**
   * Visits a 'unaryNumeric' expressi on.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to a double.
   */
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

  /**
   * Visits a 'unaryWithOptionalNumeric' expression.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to a double).
   */
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

  /**
   * Visits a 'binaryNumeric' expression.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to a double.
   */
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
