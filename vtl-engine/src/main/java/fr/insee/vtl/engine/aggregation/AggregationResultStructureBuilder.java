package fr.insee.vtl.engine.aggregation;

import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Builds the output {@link Structured.DataStructure} of an aggregate operation. */
public final class AggregationResultStructureBuilder {

  private AggregationResultStructureBuilder() {}

  public static Structured.DataStructure build(
      Structured.DataStructure input,
      List<String> groupByKeys,
      Map<String, AggregationExpression> collectors) {
    boolean globalAggregation = groupByKeys.isEmpty();
    Map<String, Structured.Component> columns = new LinkedHashMap<>();

    for (String key : groupByKeys) {
      Structured.Component id = input.get(key);
      if (id != null) {
        columns.put(key, new Structured.Component(id));
      }
    }

    for (Map.Entry<String, AggregationExpression> entry : collectors.entrySet()) {
      Dataset.Role role = globalAggregation ? Dataset.Role.IDENTIFIER : Dataset.Role.MEASURE;
      columns.put(
          entry.getKey(),
          new Structured.Component(entry.getKey(), entry.getValue().getType(), role));
    }

    for (Structured.Component component : input.values()) {
      if (isPreservedAttribute(component) && !columns.containsKey(component.getName())) {
        columns.put(component.getName(), new Structured.Component(component));
      }
    }

    return new Structured.DataStructure(columns.values());
  }

  private static boolean isPreservedAttribute(Structured.Component component) {
    return component.isViralAttribute();
  }
}
