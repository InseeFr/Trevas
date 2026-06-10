package fr.insee.vtl.engine.aggregation;

import fr.insee.vtl.engine.attribute.ViralAttributeAggregationRules;
import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.AggregationViralPropagation;
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
    AggregationViralPropagation propagation =
        groupByKeys.isEmpty()
            ? AggregationViralPropagation.INVOCATION_GLOBAL
            : AggregationViralPropagation.INVOCATION_GROUPED;
    return build(input, groupByKeys, collectors, propagation);
  }

  public static Structured.DataStructure build(
      Structured.DataStructure input,
      List<String> groupByKeys,
      Map<String, AggregationExpression> collectors,
      AggregationViralPropagation viralPropagation) {
    boolean globalAggregation = groupByKeys.isEmpty();
    Map<String, Structured.Component> columns = new LinkedHashMap<>();

    for (String key : groupByKeys) {
      Structured.Component id = input.get(key);
      if (id != null) {
        columns.put(key, new Structured.Component(id));
      }
    }

    for (Map.Entry<String, AggregationExpression> entry : collectors.entrySet()) {
      String name = entry.getKey();
      Structured.Component source = input.get(name);
      Dataset.Role role =
          globalAggregation
              ? Dataset.Role.IDENTIFIER
              : (source != null ? source.getRole() : Dataset.Role.MEASURE);
      columns.put(name, new Structured.Component(name, entry.getValue().getType(), role));
    }

    if (viralPropagation.propagatesViralAttributes()) {
      for (Structured.Component component : input.values()) {
        if (component.isViralAttribute() && !columns.containsKey(component.getName())) {
          columns.put(
              component.getName(),
              ViralAttributeAggregationRules.asPropagatedComponent(component, viralPropagation));
        }
      }
    }

    return new Structured.DataStructure(columns.values());
  }
}
