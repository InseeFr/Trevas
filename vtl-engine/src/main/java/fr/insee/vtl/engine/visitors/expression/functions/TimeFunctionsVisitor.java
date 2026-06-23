package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.semantics.time.TimeExecutor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

/** Visitor dispatch for time expressions; orchestration lives in {@link TimeExecutor}. */
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
    return TimeExecutor.currentDate(ctx);
  }

  @Override
  public ResolvableExpression visitFlowAtom(VtlParser.FlowAtomContext ctx) {
    try {
      if (ctx.FLOW_TO_STOCK() != null) {
        return TimeExecutor.flowToStock(ctx, expressionVisitor, processingEngine);
      }
      if (ctx.STOCK_TO_FLOW() != null) {
        return TimeExecutor.stockToFlow(
            ctx, expressionVisitor, processingEngine, genericFunctionsVisitor);
      }
      throw new UnsupportedOperationException("unknown op token " + ctx.op);
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  @Override
  public ResolvableExpression visitTimeShiftAtom(VtlParser.TimeShiftAtomContext ctx) {
    try {
      return TimeExecutor.timeShift(
          ctx, expressionVisitor, processingEngine, genericFunctionsVisitor);
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }
}
