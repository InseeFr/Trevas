package fr.insee.vtl.engine.aggregation;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.antlr.runtime.ParserRuleContext;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ResolvableExpression;

/** Resolves the operand of an aggregate invocation to a {@link DatasetExpression}. */
public final class AggregateOperandResolver {

  private AggregateOperandResolver() {}

  public static DatasetExpression requireDataset(
      ResolvableExpression expression, ParserRuleContext ctx) {
    if (expression instanceof DatasetExpression datasetExpression) {
      return datasetExpression;
    }
    throw new VtlRuntimeException(
        new InvalidArgumentException(
            "aggregate invocation operand must be a dataset", fromContext(ctx)));
  }
}
