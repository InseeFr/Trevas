package fr.insee.vtl.engine.semantics.attribute;

import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.ArrayList;
import java.util.List;

/** Column lists for unary dataset transforms (filter, generic functions, etc.). */
public final class UnaryAttributePropagation {

  private UnaryAttributePropagation() {}

  /**
   * Column list for a mono-measure pass: identifiers, one measure, then viral attributes (stable
   * order).
   */
  public static List<String> columnsForMonoMeasureOperation(
      DataStructure source, String measureColumn) {
    List<String> columns = new ArrayList<>();
    for (Component component : source.getIdentifiers()) {
      columns.add(component.getName());
    }
    columns.add(measureColumn);
    columns.addAll(AttributePropagation.viralAttributeNames(source));
    return columns;
  }

  /**
   * Projection after a dataset function: identifiers, named output measure(s), viral attributes.
   * Does not include join scratch columns ({@code arg*}).
   */
  public static List<String> columnsForUnaryOutput(
      DataStructure current, List<String> outputMeasureColumns) {
    List<String> columns = new ArrayList<>();
    for (Component component : current.getIdentifiers()) {
      columns.add(component.getName());
    }
    columns.addAll(outputMeasureColumns);
    columns.addAll(AttributePropagation.viralAttributeNames(current));
    return columns;
  }
}
