package fr.insee.vtl.engine.aggregation;

import fr.insee.vtl.engine.DatasetResults;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Structured.DataStructure;

/** Attaches VTL aggregation semantics (structure, roles) to a mechanical engine result. */
public final class AggregationResults {

  private AggregationResults() {}

  public static DatasetExpression withStructure(
      DatasetExpression aggregated, DataStructure structure) {
    return DatasetResults.withStructure(aggregated, structure);
  }
}
