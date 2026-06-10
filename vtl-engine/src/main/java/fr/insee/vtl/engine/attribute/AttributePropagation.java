package fr.insee.vtl.engine.attribute;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * VTL 2.1 attribute propagation — structure rules (values handled separately).
 *
 * <p>Non-viral {@link Dataset.Role#ATTRIBUTE} components are not propagated unless explicitly
 * produced by {@code calc}, {@code keep}, or {@code aggr}.
 */
public final class AttributePropagation {

  private AttributePropagation() {}

  /**
   * Merges operand structures for a multi-operand result: identifiers and measures are taken from
   * the first occurrence across operands; only {@link Dataset.Role#VIRALATTRIBUTE} components are
   * unioned.
   */
  public static Structured.DataStructure mergeStructure(Structured.DataStructure... operands) {
    if (operands == null || operands.length == 0) {
      throw new IllegalArgumentException("at least one operand structure is required");
    }
    Map<String, Structured.Component> columns = new LinkedHashMap<>();
    for (Structured.DataStructure operand : operands) {
      for (Structured.Component component : operand.values()) {
        if (component.isIdentifier() || component.isMeasure()) {
          columns.putIfAbsent(component.getName(), new Structured.Component(component));
        }
      }
    }
    for (Structured.DataStructure operand : operands) {
      for (Structured.Component component : operand.values()) {
        if (component.isViralAttribute()) {
          columns.put(component.getName(), asViralAttribute(component));
        }
      }
    }
    return new Structured.DataStructure(columns.values());
  }

  /**
   * Unary transformation structure: identifiers unchanged; output measures as given; only viral
   * attributes from the input are retained.
   */
  public static Structured.DataStructure unaryStructure(
      Structured.DataStructure input, Map<String, Class<?>> outputMeasures) {
    Map<String, Structured.Component> columns = new LinkedHashMap<>();
    for (Structured.Component component : input.values()) {
      if (component.isIdentifier()) {
        columns.put(component.getName(), new Structured.Component(component));
      }
    }
    for (Map.Entry<String, Class<?>> entry : outputMeasures.entrySet()) {
      columns.put(
          entry.getKey(),
          new Structured.Component(entry.getKey(), entry.getValue(), Dataset.Role.MEASURE));
    }
    for (Structured.Component component : input.values()) {
      if (component.isViralAttribute()) {
        columns.putIfAbsent(component.getName(), asViralAttribute(component));
      }
    }
    return new Structured.DataStructure(columns.values());
  }

  /** Returns viral attribute names from a structure (stable order). */
  public static Set<String> viralAttributeNames(Structured.DataStructure structure) {
    return structure.values().stream()
        .filter(Structured.Component::isViralAttribute)
        .map(Structured.Component::getName)
        .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
  }

  private static Structured.Component asViralAttribute(Structured.Component component) {
    if (component.isViralAttribute()) {
      return new Structured.Component(component);
    }
    return new Structured.Component(
        component.getName(),
        component.getType(),
        Dataset.Role.VIRALATTRIBUTE,
        component.getNullable(),
        component.getValuedomain());
  }
}
