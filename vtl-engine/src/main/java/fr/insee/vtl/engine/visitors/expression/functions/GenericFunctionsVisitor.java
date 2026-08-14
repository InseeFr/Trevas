package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.antlr.runtime.Token;
import fr.insee.vtl.antlr.runtime.tree.TerminalNode;
import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.FunctionNotFoundException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.expressions.CastExpression;
import fr.insee.vtl.engine.semantics.functions.DatasetScalarFunctionExecutor;
import fr.insee.vtl.engine.semantics.udo.UdoDefinition;
import fr.insee.vtl.engine.semantics.udo.UdoInvokeExecutor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

/** Visitor for cast expressions and generic scalar function dispatch. */
public class GenericFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final VtlScriptEngine engine;
  private final ExpressionVisitor exprVisitor;
  private final Map<String, Object> context;

  public GenericFunctionsVisitor(
      ExpressionVisitor expressionVisitor, VtlScriptEngine engine, Map<String, Object> context) {
    this.engine = Objects.requireNonNull(engine);
    exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.context = Objects.requireNonNull(context);
  }

  private static Class<?> getOutputClass(Integer basicScalarType, String basicScalarText) {
    return switch (basicScalarType) {
      case VtlParser.STRING -> String.class;
      case VtlParser.INTEGER -> Long.class;
      case VtlParser.NUMBER -> Double.class;
      case VtlParser.BOOLEAN -> Boolean.class;
      case VtlParser.DATE -> Instant.class;
      case VtlParser.DURATION -> PeriodDuration.class;
      case VtlParser.TIME_PERIOD -> Interval.class;
      default ->
          throw new UnsupportedOperationException(
              "basic scalar type " + basicScalarText + " unsupported");
    };
  }

  public ResolvableExpression invokeFunction(
      String funcName, List<ResolvableExpression> parameters, Positioned position)
      throws VtlScriptException {
    try {
      return DatasetScalarFunctionExecutor.invoke(engine, funcName, parameters, position);
    } catch (NoSuchMethodException e) {
      throw new VtlRuntimeException(new FunctionNotFoundException(e.getMessage(), position));
    }
  }

  @Override
  public ResolvableExpression visitCallDataset(VtlParser.CallDatasetContext ctx) {
    try {
      String name = ctx.operatorID().getText();
      Object binding = context.get(name);
      if (binding instanceof UdoDefinition udo) {
        return UdoInvokeExecutor.invoke(udo, ctx, exprVisitor, engine, fromContext(ctx));
      }
      List<ResolvableExpression> parameters =
          ctx.parameter().stream().map(exprVisitor::visit).collect(Collectors.toList());
      return invokeFunction(name, parameters, fromContext(ctx));
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  @Override
  public ResolvableExpression visitCastExprDataset(VtlParser.CastExprDatasetContext ctx) {
    ResolvableExpression expression = exprVisitor.visit(ctx.expr());
    TerminalNode maskNode = ctx.STRING_CONSTANT();
    String mask =
        maskNode == null
            ? null
            : maskNode.getText().replace("\"", "").replace("YYYY", "yyyy").replace("DD", "dd");
    Token symbol = ((TerminalNode) ctx.basicScalarType().getChild(0)).getSymbol();
    Integer basicScalarType = symbol.getType();
    String basicScalarText = symbol.getText();

    Class<?> outputClass = getOutputClass(basicScalarType, basicScalarText);

    if (Object.class.equals(expression.getType())) {
      return ResolvableExpression.withType(outputClass)
          .withPosition(fromContext(ctx))
          .using(c -> null);
    }
    try {
      return new CastExpression(fromContext(ctx), expression, mask, outputClass);
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }
}
