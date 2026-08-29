package fr.insee.vtl.engine.visitors.expression;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.hasSameTypeOrNull;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.ConstantExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.*;
import java.util.stream.Collectors;

/** Dispatch for if-then-else and nvl expressions. */
public class ConditionalVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor exprVisitor;
  private final GenericFunctionsVisitor genericFunctionsVisitor;

  public ConditionalVisitor(
      ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
    this.exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
  }

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
    ResolvableExpression caseCondition =
        genericFunctionsVisitor.invokeFunction(
            "nvl", List.of(nextWhen, new ConstantExpression(false, nextWhen)), nextWhen);

    return genericFunctionsVisitor.invokeFunction(
        "ifThenElse",
        List.of(caseCondition, thenExpr.next(), caseToIfIt(whenExpr, thenExpr, elseExpression)),
        caseCondition);
  }

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
