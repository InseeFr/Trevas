package fr.insee.vtl.engine.visitors.expression;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.hasSameTypeOrNull;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.*;
import java.util.stream.Collectors;

/** <code>IfVisitor</code> is the base visitor for if-then-else expressions. */
public class ConditionalVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor exprVisitor;

  private final GenericFunctionsVisitor genericFunctionsVisitor;

  /**
   * Constructor taking an expression visitor.
   *
   * @param expressionVisitor The visitor for the enclosing expression.
   * @param genericFunctionsVisitor
   */
  public ConditionalVisitor(
      ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
    this.exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
  }

  public static Long ifThenElse(Boolean condition, Long thenExpr, Long elseExpr) {
    if (condition == null) {
      return null;
    }
    return condition ? thenExpr : elseExpr;
  }

  public static Double ifThenElse(Boolean condition, Double thenExpr, Double elseExpr) {
    if (condition == null) {
      return null;
    }
    return condition ? thenExpr : elseExpr;
  }

  public static String ifThenElse(Boolean condition, String thenExpr, String elseExpr) {
    if (condition == null) {
      return null;
    }
    return condition ? thenExpr : elseExpr;
  }

  public static Boolean ifThenElse(Boolean condition, Boolean thenExpr, Boolean elseExpr) {
    if (condition == null) {
      return null;
    }
    return condition ? thenExpr : elseExpr;
  }

  public static Long nvl(Long value, Long defaultValue) {
    return value == null ? defaultValue : value;
  }

  public static Double nvl(Double value, Double defaultValue) {
    return value == null ? defaultValue : value;
  }

  public static Double nvl(Double value, Long defaultValue) {
    return value == null ? defaultValue.doubleValue() : value;
  }

  public static Double nvl(Long value, Double defaultValue) {
    return value == null ? defaultValue : value.doubleValue();
  }

  public static String nvl(String value, String defaultValue) {
    return value == null ? defaultValue : value;
  }

  public static Boolean nvl(Boolean value, Boolean defaultValue) {
    return value == null ? defaultValue : value;
  }

  /**
   * Visits if-then-else expressions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the if or else clause resolution
   *     depending on the condition resolution.
   */
  @Override
  public ResolvableExpression visitIfExpr(VtlParser.IfExprContext ctx) {
    try {
      var conditionalExpr = exprVisitor.visit(ctx.conditionalExpr);
      var thenExpression = exprVisitor.visit(ctx.thenExpr);
      var elseExpression = exprVisitor.visit(ctx.elseExpr);
      Positioned position = fromContext(ctx);
      ResolvableExpression expression =
          genericFunctionsVisitor.invokeFunction(
              "ifThenElse", List.of(conditionalExpr, thenExpression, elseExpression), position);
      Class<?> actualType = thenExpression.getType();
      return new CastExpression(position, expression, actualType);
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  /**
   * Visits case expressions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the case resolution depending on the
   *     condition resolution.
   */
  @Override
  public ResolvableExpression visitCaseExpr(VtlParser.CaseExprContext ctx) {
    try {
      Positioned pos = fromContext(ctx);
      List<VtlParser.ExprContext> exprs = ctx.expr();
      List<VtlParser.ExprContext> whenExprs = new ArrayList<>();
      List<VtlParser.ExprContext> thenExprs = new ArrayList<>();
      for (int i = 0; i < exprs.size() - 1; i = i + 2) {
        whenExprs.add(exprs.get(i));
        thenExprs.add(exprs.get(i + 1));
      }
      List<ResolvableExpression> whenExpressions =
          whenExprs.stream().map(exprVisitor::visit).collect(Collectors.toList());
      List<ResolvableExpression> thenExpressions =
          thenExprs.stream().map(exprVisitor::visit).collect(Collectors.toList());
      ResolvableExpression elseExpression = exprVisitor.visit(exprs.get(exprs.size() - 1));
      List<ResolvableExpression> forTypeCheck = (new ArrayList<>(thenExpressions));
      forTypeCheck.add(elseExpression);
      // TODO: handle better the default element position
      if (!hasSameTypeOrNull(forTypeCheck)) {
        try {
          throw new InvalidTypeException(
              forTypeCheck.get(0).getClass(), Boolean.class, fromContext(ctx.expr(0)));
        } catch (InvalidTypeException e) {
          throw new RuntimeException(e);
        }
      }

      Class<?> outputType = elseExpression.getType();
      return new CastExpression(
          pos,
          caseToIfIt(
              whenExpressions.listIterator(), thenExpressions.listIterator(), elseExpression),
          outputType);
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  private ResolvableExpression caseToIfIt(
      ListIterator<ResolvableExpression> whenExpr,
      ListIterator<ResolvableExpression> thenExpr,
      ResolvableExpression elseExpression)
      throws VtlScriptException {
    if (!whenExpr.hasNext() || !thenExpr.hasNext()) {
      return elseExpression;
    }

    ResolvableExpression nextWhen = whenExpr.next();

    return genericFunctionsVisitor.invokeFunction(
        "ifThenElse",
        List.of(nextWhen, thenExpr.next(), caseToIfIt(whenExpr, thenExpr, elseExpression)),
        nextWhen);
  }

  /**
   * Visits nvl expressions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the null value clause.
   */
  @Override
  public ResolvableExpression visitNvlAtom(VtlParser.NvlAtomContext ctx) {
    try {
      ResolvableExpression expression = exprVisitor.visit(ctx.left);
      ResolvableExpression defaultExpression = exprVisitor.visit(ctx.right);

      Positioned position = fromContext(ctx);
      return genericFunctionsVisitor.invokeFunction(
          "nvl", List.of(expression, defaultExpression), position);
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  static class CastExpression extends ResolvableExpression {
    private final Class<?> type;
    private final ResolvableExpression expression;

    CastExpression(Positioned pos, ResolvableExpression expression, Class<?> type) {
      super(pos);
      this.type = type;
      this.expression = expression;
    }

    @Override
    public Object resolve(Map<String, Object> context) {
      return type.cast(expression.resolve(context));
    }

    @Override
    public Class<?> getType() {
      return type;
    }
  }
}
