package fr.insee.vtl.engine.attribute;

import fr.insee.vtl.model.AggregationViralPropagation;
import fr.insee.vtl.model.Structured.Component;

/** Maps propagated viral components to the output role required by the aggregation context. */
public final class ViralAttributeAggregationRules {

  private ViralAttributeAggregationRules() {}

  public static Component asPropagatedComponent(
      Component viral, AggregationViralPropagation propagation) {
    return new Component(
        viral.getName(),
        viral.getType(),
        propagation.propagatedViralRole(),
        viral.getNullable(),
        viral.getValuedomain());
  }
}
