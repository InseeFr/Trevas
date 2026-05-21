package fr.insee.vtl.engine.attribute;

import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.AggregationViralPropagation;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Collectors for viral attributes during {@code group by} aggregation.
 *
 * <p>Delegates to {@link AggregationExpression#min(ResolvableExpression)} so in-memory and Spark
 * paths share the same comparator semantics as {@link AttributePropagationAlgorithm}.
 *
 * <p>See package {@linkplain fr.insee.vtl.engine.attribute} documentation for a warning on
 * revisiting this propagation rule if the spec or language evolves.
 */
public final class ViralAttributeCollectors {

  private static final Positioned DEFAULT_POSITION =
      () -> new Positioned.Position("viral-attribute", 0, 0, 0, 0);

  private ViralAttributeCollectors() {}

  /** In-memory / {@link AggregationExpression} collector for one viral component. */
  public static AggregationExpression grouped(Structured.Component viralComponent) {
    return AggregationExpression.min(columnExpression(viralComponent));
  }

  private static ResolvableExpression columnExpression(Structured.Component component) {
    String name = component.getName();
    Class<?> type = component.getType();
    if (String.class.equals(type)) {
      return ResolvableExpression.withType(String.class)
          .withPosition(DEFAULT_POSITION)
          .using(ctx -> (String) ctx.get(name));
    }
    if (Long.class.equals(type)) {
      return ResolvableExpression.withType(Long.class)
          .withPosition(DEFAULT_POSITION)
          .using(ctx -> (Long) ctx.get(name));
    }
    if (Double.class.equals(type)) {
      return ResolvableExpression.withType(Double.class)
          .withPosition(DEFAULT_POSITION)
          .using(ctx -> (Double) ctx.get(name));
    }
    if (Boolean.class.equals(type)) {
      return ResolvableExpression.withType(Boolean.class)
          .withPosition(DEFAULT_POSITION)
          .using(ctx -> (Boolean) ctx.get(name));
    }
    if (java.time.Instant.class.equals(type)) {
      return ResolvableExpression.withType(java.time.Instant.class)
          .withPosition(DEFAULT_POSITION)
          .using(ctx -> (java.time.Instant) ctx.get(name));
    }
    throw new IllegalArgumentException(
        "unsupported viral attribute type for grouped aggregation: " + type.getName());
  }

  /**
   * Merges measure aggregations with viral-attribute aggregations present in the output structure.
   */
  public static Map<String, AggregationExpression> mergeMeasureCollectors(
      Structured.DataStructure input,
      Structured.DataStructure output,
      Map<String, AggregationExpression> measureCollectors) {
    return mergeMeasureCollectors(
        input, output, measureCollectors, AggregationViralPropagation.INVOCATION_GROUPED);
  }

  public static Map<String, AggregationExpression> mergeMeasureCollectors(
      Structured.DataStructure input,
      Structured.DataStructure output,
      Map<String, AggregationExpression> measureCollectors,
      AggregationViralPropagation viralPropagation) {
    Map<String, AggregationExpression> merged = new LinkedHashMap<>(measureCollectors);
    if (!viralPropagation.propagatesViralAttributes()) {
      return merged;
    }
    for (Structured.Component viral : input.getViralAttributes()) {
      String name = viral.getName();
      if (output.containsKey(name) && !merged.containsKey(name)) {
        merged.put(name, grouped(viral));
      }
    }
    return merged;
  }
}
