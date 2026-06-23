package fr.insee.vtl.model;

import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Mechanical output layout for {@link ProcessingEngine#executeAggr} (no VTL role semantics). */
public final class AggregationOutputStructure {

  private AggregationOutputStructure() {}

  /**
   * Columns produced by grouping and aggregation: {@code groupBy} keys from the input, then
   * collector output columns (types from expressions, placeholder {@link Dataset.Role#MEASURE}).
   */
  public static DataStructure mechanical(
      DataStructure input, List<String> groupBy, Map<String, AggregationExpression> collectors) {
    Map<String, Component> columns = new LinkedHashMap<>();
    for (String key : groupBy) {
      Component component = input.get(key);
      if (component != null) {
        columns.put(key, new Component(component));
      }
    }
    for (Map.Entry<String, AggregationExpression> entry : collectors.entrySet()) {
      String name = entry.getKey();
      if (!columns.containsKey(name)) {
        columns.put(name, new Component(name, entry.getValue().getType(), Dataset.Role.MEASURE));
      }
    }
    return new DataStructure(columns.values());
  }
}
