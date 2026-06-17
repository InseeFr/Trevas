package fr.insee.vtl.engine.aggregation;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.Map;
import java.util.Objects;

/** Attaches VTL aggregation semantics (structure, roles) to a mechanical engine result. */
public final class AggregationResults {

  private AggregationResults() {}

  public static DatasetExpression withStructure(
      DatasetExpression aggregated, DataStructure structure) {
    Objects.requireNonNull(structure);
    return new DatasetExpression(aggregated) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        return aggregated.resolve(context).withDataStructure(structure);
      }

      @Override
      public DataStructure getDataStructure() {
        return structure;
      }
    };
  }
}
