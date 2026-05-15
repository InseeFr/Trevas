package fr.insee.vtl.engine.aggregation;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.assertBasicScalarType;
import static fr.insee.vtl.engine.utils.TypeChecking.assertNumber;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;

/** Maps {@link VtlParser.AggrDatasetContext} operators to {@link AggregationExpression}. */
public final class AggregationExpressionFactory {

  private AggregationExpressionFactory() {}

  public static AggregationExpression fromAggrDataset(
      VtlParser.AggrDatasetContext groupFunctionCtx, ResolvableExpression expression) {
    if (groupFunctionCtx.SUM() != null) {
      return AggregationExpression.sum(assertNumber(expression, groupFunctionCtx.expr()));
    }
    if (groupFunctionCtx.AVG() != null) {
      return AggregationExpression.avg(assertNumber(expression, groupFunctionCtx.expr()));
    }
    if (groupFunctionCtx.COUNT() != null) {
      return AggregationExpression.count();
    }
    if (groupFunctionCtx.MAX() != null) {
      return AggregationExpression.max(assertBasicScalarType(expression, groupFunctionCtx.expr()));
    }
    if (groupFunctionCtx.MIN() != null) {
      return AggregationExpression.min(assertBasicScalarType(expression, groupFunctionCtx.expr()));
    }
    if (groupFunctionCtx.MEDIAN() != null) {
      return AggregationExpression.median(assertNumber(expression, groupFunctionCtx.expr()));
    }
    if (groupFunctionCtx.STDDEV_POP() != null) {
      return AggregationExpression.stdDevPop(assertNumber(expression, groupFunctionCtx.expr()));
    }
    if (groupFunctionCtx.STDDEV_SAMP() != null) {
      return AggregationExpression.stdDevSamp(assertNumber(expression, groupFunctionCtx.expr()));
    }
    if (groupFunctionCtx.VAR_POP() != null) {
      return AggregationExpression.varPop(assertNumber(expression, groupFunctionCtx.expr()));
    }
    if (groupFunctionCtx.VAR_SAMP() != null) {
      return AggregationExpression.varSamp(assertNumber(expression, groupFunctionCtx.expr()));
    }
    throw new VtlRuntimeException(
        new VtlScriptException("not implemented", fromContext(groupFunctionCtx)));
  }

  public static AggregationExpression countRows() {
    return AggregationExpression.count();
  }
}
