package fr.insee.vtl.engine.join;

import static fr.insee.vtl.model.Structured.Component;

import fr.insee.vtl.engine.attribute.AttributePropagationAlgorithm;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured.DataPoint;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Projects a join virtual dataset onto bare column names in VTL output order. */
public final class JoinProjection {

  private JoinProjection() {}

  public static DatasetExpression project(
      DatasetExpression dataset, List<String> outputColumnNames) {
    DataStructure source = dataset.getDataStructure();
    DataStructure targetStructure = buildTargetStructure(source, outputColumnNames);
    return new DatasetExpression(dataset) {
      @Override
      public fr.insee.vtl.model.Dataset resolve(Map<String, Object> context) {
        List<DataPoint> rows =
            dataset.resolve(context).getDataPoints().stream()
                .map(row -> projectRow(targetStructure, source, row, outputColumnNames))
                .toList();
        return InMemoryDataset.ofDataPoints(rows, targetStructure);
      }

      @Override
      public DataStructure getDataStructure() {
        return targetStructure;
      }
    };
  }

  static DataStructure buildTargetStructure(DataStructure source, List<String> outputColumnNames) {
    List<Component> components = new ArrayList<>(outputColumnNames.size());
    for (String bareName : outputColumnNames) {
      String sourceColumn = resolveSourceColumn(source, bareName);
      Component src = source.get(sourceColumn);
      components.add(new Component(bareName, src.getType(), src.getRole(), src.getNullable()));
    }
    return new DataStructure(components);
  }

  static DataPoint projectRow(
      DataStructure target, DataStructure source, DataPoint row, List<String> outputColumnNames) {
    DataPoint projected = new DataPoint(target);
    for (String bareName : outputColumnNames) {
      projected.set(bareName, resolveProjectedValue(source, row, bareName));
    }
    return projected;
  }

  static Object resolveProjectedValue(DataStructure source, DataPoint row, String bareName) {
    List<Component> viralSources =
        matchingComponents(source, bareName).stream().filter(Component::isViralAttribute).toList();
    if (viralSources.size() <= 1) {
      return row.get(resolveSourceColumn(source, bareName));
    }
    Object merged = null;
    Class<?> type = viralSources.get(0).getType();
    for (Component component : viralSources) {
      Object value = row.get(component.getName());
      merged =
          merged == null
              ? value
              : AttributePropagationAlgorithm.propagateBinaryValue(merged, value, type);
    }
    return merged;
  }

  private static List<Component> matchingComponents(DataStructure source, String bareName) {
    List<Component> matches = new ArrayList<>();
    for (Component component : source.componentsInOrder()) {
      if (stripJoinAlias(component.getName()).equals(bareName)) {
        matches.add(component);
      }
    }
    return matches;
  }

  /** Prefer {@code alias#name} over bare {@code name} when both exist after a join rename. */
  static String resolveSourceColumn(DataStructure source, String bareName) {
    String bare = null;
    String aliased = null;
    for (Component component : source.componentsInOrder()) {
      String name = component.getName();
      if (!stripJoinAlias(name).equals(bareName)) {
        continue;
      }
      if (name.contains("#")) {
        aliased = name;
      } else if (name.equals(bareName)) {
        bare = name;
      }
    }
    if (aliased != null) {
      return aliased;
    }
    if (bare != null) {
      return bare;
    }
    return bareName;
  }

  public static String stripJoinAlias(String columnName) {
    return columnName.substring(columnName.lastIndexOf('#') + 1);
  }
}
