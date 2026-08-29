package fr.insee.vtl.engine.visitors.expression;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.antlr.runtime.Token;
import fr.insee.vtl.antlr.runtime.tree.TerminalNode;
import fr.insee.vtl.engine.exceptions.ConflictingTypesException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.ListExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.TypedExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.*;
import java.util.stream.Collectors;

/** Dispatch for comparison, 'element of' and list expressions. */
public class ComparisonVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private static final String unknownOperator = "unknown operator ";
  private final ExpressionVisitor exprVisitor;
  private final GenericFunctionsVisitor genericFunctionsVisitor;

  public ComparisonVisitor(
      ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
    exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.genericFunctionsVisitor = genericFunctionsVisitor;
  }

  @Override
  public ResolvableExpression visitComparisonExpr(VtlParser.ComparisonExprContext ctx) {
    try {
      Token type = ((TerminalNode) ctx.op.getChild(0)).getSymbol();
      var leftExpression = exprVisitor.visit(ctx.left);
      var rightExpression = exprVisitor.visit(ctx.right);
      List<ResolvableExpression> parameters = List.of(leftExpression, rightExpression);
      if (!TypeChecking.hasSameTypeOrNumberOrNull(parameters)) {
        var types = List.of(leftExpression.getType(), rightExpression.getType());
        throw new ConflictingTypesException(types, fromContext(ctx));
      }
      if (parameters.stream().map(TypedExpression::getType).anyMatch(Object.class::equals)) {
        return ResolvableExpression.withType(Boolean.class)
            .withPosition(fromContext(ctx))
            .using(c -> null);
      }
      return switch (type.getType()) {
        case VtlParser.EQ ->
            genericFunctionsVisitor.invokeFunction("isEqual", parameters, fromContext(ctx));
        case VtlParser.NEQ ->
            genericFunctionsVisitor.invokeFunction("isNotEqual", parameters, fromContext(ctx));
        case VtlParser.LT ->
            genericFunctionsVisitor.invokeFunction("isLessThan", parameters, fromContext(ctx));
        case VtlParser.MT ->
            genericFunctionsVisitor.invokeFunction("isGreaterThan", parameters, fromContext(ctx));
        case VtlParser.LE ->
            genericFunctionsVisitor.invokeFunction(
                "isLessThanOrEqual", parameters, fromContext(ctx));
        case VtlParser.ME ->
            genericFunctionsVisitor.invokeFunction(
                "isGreaterThanOrEqual", parameters, fromContext(ctx));
        default -> throw new UnsupportedOperationException(unknownOperator + ctx);
      };
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  @Override
  public ResolvableExpression visitInNotInExpr(VtlParser.InNotInExprContext ctx) {
    try {
      List<ResolvableExpression> parameters =
          List.of(exprVisitor.visit(ctx.left), visit(ctx.lists()));
      Positioned pos = fromContext(ctx);

      return switch (ctx.op.getType()) {
        case VtlParser.IN -> genericFunctionsVisitor.invokeFunction("in", parameters, pos);
        case VtlParser.NOT_IN -> genericFunctionsVisitor.invokeFunction("notIn", parameters, pos);
        default -> throw new IllegalStateException("Unexpected value: " + ctx.op.getType());
      };
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  @Override
  public ResolvableExpression visitLists(VtlParser.ListsContext ctx) {

    List<ResolvableExpression> listExpressions =
        ctx.constant().stream().map(exprVisitor::visitConstant).collect(Collectors.toList());

    Set<Class<?>> types =
        listExpressions.stream().map(TypedExpression::getType).collect(Collectors.toSet());

    var pos = fromContext(ctx);

    if (types.size() > 1) {
      throw new VtlRuntimeException(new ConflictingTypesException(types, pos));
    }

    Class<?> type = types.iterator().next();

    List<Object> values =
        listExpressions.stream()
            .map(expression -> expression.resolve(Map.of()))
            .collect(Collectors.toList());

    return ListExpression.withContainedType(values, type, pos);
  }
}
