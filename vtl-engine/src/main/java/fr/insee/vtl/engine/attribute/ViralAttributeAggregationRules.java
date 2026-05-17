package fr.insee.vtl.engine.attribute;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured.Component;
import java.util.List;

/** Structure rules for viral attributes in {@code aggr} / aggregate invocation. */
public final class ViralAttributeAggregationRules {

  private ViralAttributeAggregationRules() {}

  /** Propagated virals are kept only on grouped aggregation, not on global {@code avg(DS)} etc. */
  public static boolean preservePropagatedVirals(List<String> groupByKeys) {
    return groupByKeys != null && !groupByKeys.isEmpty();
  }

  /**
   * Auto-propagated viral columns use role {@link Dataset.Role#ATTRIBUTE}. Explicit {@code aggr
   * viral attribute} keeps {@link Dataset.Role#VIRALATTRIBUTE} via the collector path.
   */
  public static Component asPropagatedAttribute(Component viral) {
    return new Component(
        viral.getName(),
        viral.getType(),
        Dataset.Role.ATTRIBUTE,
        viral.getNullable(),
        viral.getValuedomain());
  }
}
