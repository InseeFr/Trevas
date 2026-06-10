package fr.insee.vtl.engine.attribute;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataPoint;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Keeps viral attributes on unary dataset transforms (filter, generic functions, etc.). */
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

  /**
   * Rebuilds rows so viral attribute values match {@code sourceDataset} (unchanged per identifier
   * key). Used when an intermediate step projected measures only.
   */
  public static DatasetExpression reattachViralAttributes(
      DatasetExpression sourceDataset,
      DatasetExpression transformed,
      Map<String, Class<?>> outputMeasuresByName) {

    ViralAttributeReattachPlan plan =
        ViralAttributeReattachPlan.unary(sourceDataset, outputMeasuresByName);
    DataStructure targetStructure = plan.targetStructure();
    List<String> identifierNames = plan.identifierNames();
    Set<String> viralNames = plan.viralNames();

    if (!plan.hasVirals()) {
      return withStructure(transformed, targetStructure);
    }

    return new DatasetExpression(transformed) {
      @Override
      public DataStructure getDataStructure() {
        return targetStructure;
      }

      @Override
      public Dataset resolve(Map<String, Object> context) {
        Map<List<Object>, DataPoint> sourceByKey =
            indexByIdentifiers(
                sourceDataset.resolve(context).getDataPoints(), plan.identifierNames());

        List<DataPoint> rows =
            transformed.resolve(context).getDataPoints().stream()
                .map(
                    row -> {
                      DataPoint out = new DataPoint(targetStructure, row);
                      DataPoint source = sourceByKey.get(keyValues(row, identifierNames));
                      if (source != null) {
                        for (String viral : viralNames) {
                          out.set(viral, source.get(viral));
                        }
                      }
                      return out;
                    })
                .collect(Collectors.toList());
        return InMemoryDataset.ofDataPoints(rows, targetStructure);
      }
    };
  }

  private static DatasetExpression withStructure(
      DatasetExpression expression, DataStructure structure) {
    return new DatasetExpression(expression) {
      @Override
      public DataStructure getDataStructure() {
        return structure;
      }

      @Override
      public Dataset resolve(Map<String, Object> context) {
        List<DataPoint> rows =
            expression.resolve(context).getDataPoints().stream()
                .map(row -> new DataPoint(structure, row))
                .collect(Collectors.toList());
        return InMemoryDataset.ofDataPoints(rows, structure);
      }
    };
  }

  private static Map<List<Object>, DataPoint> indexByIdentifiers(
      List<DataPoint> rows, List<String> identifierNames) {
    Map<List<Object>, DataPoint> index = new LinkedHashMap<>();
    for (DataPoint row : rows) {
      index.put(keyValues(row, identifierNames), row);
    }
    return index;
  }

  private static List<Object> keyValues(DataPoint row, List<String> identifierNames) {
    return identifierNames.stream().map(row::get).collect(Collectors.toList());
  }
}
