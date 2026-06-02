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

/**
 * <code>ArithmeticExprOrConcatVisitor</code> is the base visitor for plus, minus or concatenation
 * expressions.
 */
public class ArithmeticExprOrConcatVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor exprVisitor;
  private final GenericFunctionsVisitor genericFunctionsVisitor;

  /**
   * Constructor taking an expression visitor.
   *
   * @param expressionVisitor The visitor for the enclosing expression.
   * @param genericFunctionsVisitor
   */
  public ArithmeticExprOrConcatVisitor(
      ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
    exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
  }

  public static Long addition(Long valueA, Long valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA + valueB;
  }

  public static Double addition(Long valueA, Double valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA + valueB;
  }

  public static Double addition(Double valueA, Long valueB) {
    return addition(valueB, valueA);
  }

  public static Double addition(Double valueA, Double valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA + valueB;
  }

  public static Long subtraction(Long valueA, Long valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA - valueB;
  }

  public static Double subtraction(Long valueA, Double valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA - valueB;
  }

  public static Double subtraction(Double valueA, Long valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA - valueB.doubleValue();
  }

  public static Double subtraction(Double valueA, Double valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA - valueB;
  }

  public static String concat(String valueA, String valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA + valueB;
  }

  /**
   * Visits expressions with plus, minus or concatenation operators.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the plus, minus or
   *     concatenation operation.
   */
  @Override
  public ResolvableExpression visitArithmeticExprOrConcat(
      VtlParser.ArithmeticExprOrConcatContext ctx) {
    try {
      var pos = fromContext(ctx);
      var parameters = List.of(exprVisitor.visit(ctx.left), exprVisitor.visit(ctx.right));
      return switch (ctx.op.getType()) {
        case VtlParser.PLUS -> genericFunctionsVisitor.invokeFunction("addition", parameters, pos);
        case VtlParser.MINUS ->
            genericFunctionsVisitor.invokeFunction("subtraction", parameters, pos);
        case VtlParser.CONCAT -> genericFunctionsVisitor.invokeFunction("concat", parameters, pos);
        default -> throw new UnsupportedOperationException("unknown operator " + ctx);
      };
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }
}
