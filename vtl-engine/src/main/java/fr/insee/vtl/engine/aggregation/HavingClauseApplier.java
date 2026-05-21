package fr.insee.vtl.engine.aggregation;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlParser;

/** Applies {@code having} after an aggregate invocation. */
public final class HavingClauseApplier {

  private HavingClauseApplier() {}

  public static DatasetExpression apply(
      DatasetExpression aggregated,
      VtlParser.HavingClauseContext havingClause,
      ExpressionVisitor expressionVisitor,
      ProcessingEngine processingEngine) {
    if (havingClause == null) {
      return aggregated;
    }
    ResolvableExpression filter = expressionVisitor.visit(havingClause.expr());
    return processingEngine.executeFilter(aggregated, filter, havingClause.getText());
  }
}
