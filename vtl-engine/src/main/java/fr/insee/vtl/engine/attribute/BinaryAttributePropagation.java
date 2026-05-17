package fr.insee.vtl.engine.attribute;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataPoint;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Viral attribute values after binary dataset operations and joins. */
public final class BinaryAttributePropagation {

  private BinaryAttributePropagation() {}

  /**
   * Merges two viral values for one matched join row (same rule as grouped {@code min}, nulls
   * first).
   */
  public static Object mergeViralValues(Object left, Object right, Class<?> type) {
    return AttributePropagationAlgorithm.propagateBinaryValue(left, right, type);
  }

  /**
   * Copies right-hand columns onto a join row; homonym {@link Dataset.Role#VIRALATTRIBUTE}
   * components use {@link #mergeViralValues}.
   */
  public static void applyRightColumns(
      DataPoint target,
      DataStructure targetStructure,
      DataPoint leftRow,
      DataStructure leftStructure,
      DataPoint rightRow,
      DataStructure rightStructure) {
    for (Component component : rightStructure.componentsInOrder()) {
      String name = component.getName();
      if (!targetStructure.containsKey(name)) {
        continue;
      }
      Object rightValue = rightRow.get(name);
      Component leftComponent = leftStructure.get(name);
      if (leftComponent != null
          && leftComponent.isViralAttribute()
          && component.isViralAttribute()) {
        target.set(name, mergeViralValues(leftRow.get(name), rightValue, component.getType()));
      } else {
        target.set(name, rightValue);
      }
    }
  }

  /**
   * Restores viral columns on a transformed dataset from one or more source operands (binary
   * propagation when several sources expose the same viral name).
   */
  public static DatasetExpression reattachViralAttributes(
      List<DatasetExpression> sources,
      DatasetExpression transformed,
      Map<String, Class<?>> outputMeasuresByName) {

    if (sources.isEmpty()) {
      throw new IllegalArgumentException("at least one source dataset is required");
    }
    if (sources.size() == 1) {
      return UnaryAttributePropagation.reattachViralAttributes(
          sources.get(0), transformed, outputMeasuresByName);
    }

    ViralAttributeReattachPlan plan =
        ViralAttributeReattachPlan.binary(sources, outputMeasuresByName);
    DataStructure targetStructure = plan.targetStructure();
    Set<String> viralNames = plan.viralNames();
    if (!plan.hasVirals()) {
      return UnaryAttributePropagation.reattachViralAttributes(
          sources.get(0), transformed, outputMeasuresByName);
    }

    List<String> identifierNames = plan.identifierNames();
    List<SourceIndex> indexes =
        sources.stream().map(source -> new SourceIndex(source, identifierNames)).toList();

    return new DatasetExpression(transformed) {
      @Override
      public DataStructure getDataStructure() {
        return targetStructure;
      }

      @Override
      public Dataset resolve(Map<String, Object> context) {
        List<DataPoint> rows =
            transformed.resolve(context).getDataPoints().stream()
                .map(
                    row -> {
                      DataPoint out = new DataPoint(targetStructure, row);
                      List<Object> key = keyValues(row, identifierNames);
                      for (String viral : viralNames) {
                        out.set(viral, mergedViralValue(indexes, key, viral, context));
                      }
                      return out;
                    })
                .collect(Collectors.toList());
        return InMemoryDataset.ofDataPoints(rows, targetStructure);
      }
    };
  }

  private static Object mergedViralValue(
      List<SourceIndex> indexes, List<Object> key, String viral, Map<String, Object> context) {
    Class<?> type = null;
    Object merged = null;
    for (SourceIndex index : indexes) {
      DataPoint sourceRow = index.rowsByKey.get(key);
      if (sourceRow == null || !index.structure.containsKey(viral)) {
        continue;
      }
      Component component = index.structure.get(viral);
      if (!component.isViralAttribute()) {
        continue;
      }
      type = component.getType();
      Object value = sourceRow.get(viral);
      merged = merged == null ? value : mergeViralValues(merged, value, type);
    }
    return merged;
  }

  private static List<Object> keyValues(DataPoint row, List<String> identifierNames) {
    return identifierNames.stream().map(row::get).collect(Collectors.toList());
  }

  private static final class SourceIndex {
    private final DataStructure structure;
    private final Map<List<Object>, DataPoint> rowsByKey;

    private SourceIndex(DatasetExpression source, List<String> identifierNames) {
      structure = source.getDataStructure();
      rowsByKey = new LinkedHashMap<>();
      for (DataPoint row : source.resolve(Map.of()).getDataPoints()) {
        rowsByKey.put(keyValues(row, identifierNames), row);
      }
    }
  }
}
