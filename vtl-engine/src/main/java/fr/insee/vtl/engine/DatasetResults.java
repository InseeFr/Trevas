package fr.insee.vtl.engine;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.Map;
import java.util.Objects;

/** Attaches VTL structure metadata (roles, column layout) to a mechanical engine result. */
public final class DatasetResults {

  private DatasetResults() {}

  public static DatasetExpression withStructure(DatasetExpression result, DataStructure structure) {
    Objects.requireNonNull(structure);
    return new DatasetExpression(result) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        return result.resolve(context).withDataStructure(structure);
      }

      @Override
      public DataStructure getDataStructure() {
        return structure;
      }
    };
  }
}
