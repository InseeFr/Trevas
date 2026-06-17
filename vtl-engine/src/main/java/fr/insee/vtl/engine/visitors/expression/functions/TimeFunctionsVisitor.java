package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.antlr.runtime.tree.ParseTree;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.engine.time.TimeSeriesConversionExecutor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import org.threeten.extra.Interval;

/** Visitor for time function expressions (dispatches to {@link TimeSeriesConversionExecutor}). */
public class TimeFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final GenericFunctionsVisitor genericFunctionsVisitor;
  private final ExpressionVisitor expressionVisitor;
  private final ProcessingEngine processingEngine;

  public TimeFunctionsVisitor(
      GenericFunctionsVisitor genericFunctionsVisitor,
      ExpressionVisitor expressionVisitor,
      ProcessingEngine processingEngine) {
    this.genericFunctionsVisitor = genericFunctionsVisitor;
    this.expressionVisitor = expressionVisitor;
    this.processingEngine = processingEngine;
  }

  @Override
  public ResolvableExpression visitCurrentDateAtom(VtlParser.CurrentDateAtomContext ctx) {
    return new ConstantExpression(Instant.now(), fromContext(ctx));
  }

  @Override
  public ResolvableExpression visitFlowAtom(VtlParser.FlowAtomContext ctx) {
    if (ctx.FLOW_TO_STOCK() != null) {
      return flowToStock(ctx);
    } else if (ctx.STOCK_TO_FLOW() != null) {
      return stockToFlows(ctx);
    }
    throw new UnsupportedOperationException("unknown op token " + ctx.op);
  }

  private ResolvableExpression stockToFlows(VtlParser.FlowAtomContext ctx) {
    try {
      Positioned position = fromContext(ctx);
      ResolvableExpression operand = expressionVisitor.visit(ctx.expr());
      if (!(operand instanceof DatasetExpression ds)) {
        throw new InvalidArgumentException("flow to stock only supports datasets", position);
      }
      return TimeSeriesConversionExecutor.stockToFlow(
          processingEngine, genericFunctionsVisitor, ds, extractTimeComponent(ctx, ds), position);
    } catch (VtlScriptException iae) {
      throw new VtlRuntimeException(iae);
    }
  }

  private ResolvableExpression flowToStock(VtlParser.FlowAtomContext ctx) {
    try {
      Positioned position = fromContext(ctx);
      ResolvableExpression operand = expressionVisitor.visit(ctx.expr());
      if (!(operand instanceof DatasetExpression ds)) {
        throw new InvalidArgumentException("flow to stock only supports datasets", position);
      }
      return TimeSeriesConversionExecutor.flowToStock(
          processingEngine, ds, extractTimeComponent(ctx, ds));
    } catch (VtlScriptException iae) {
      throw new VtlRuntimeException(iae);
    }
  }

  @Override
  public ResolvableExpression visitTimeShiftAtom(VtlParser.TimeShiftAtomContext ctx) {
    try {
      ResolvableExpression operand = expressionVisitor.visit(ctx.expr());
      ConstantExpression n =
          new ConstantExpression(
              Long.parseLong(ctx.signedInteger().getText()), fromContext(ctx.signedInteger()));

      if (!(operand instanceof DatasetExpression ds)) {
        return genericFunctionsVisitor.invokeFunction(
            "timeshift", List.of(operand, n), fromContext(ctx));
      }

      var time = extractTimeComponent(ctx, ds);
      var compExpr =
          genericFunctionsVisitor.invokeFunction(
              "timeshift",
              List.of(new ComponentExpression(time, fromContext(ctx)), n),
              fromContext(ctx));
      return processingEngine.executeCalc(
          ds, Map.of(time.getName(), compExpr), Map.of(time.getName(), time.getRole()), Map.of());
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  private static Structured.Component extractTimeComponent(ParseTree ctx, DatasetExpression ds)
      throws InvalidArgumentException {
    return ds.getIdentifiers().stream()
        .filter(
            component ->
                component.getType().equals(Interval.class)
                    || component.getType().equals(Instant.class)
                    || component.getType().equals(ZonedDateTime.class)
                    || component.getType().equals(OffsetDateTime.class))
        .findFirst()
        .orElseThrow(
            () ->
                new InvalidArgumentException(
                    "no time column in " + ctx.getText(), fromContext(ctx)));
  }
}
