package fr.insee.vtl.engine.join;

import fr.insee.vtl.engine.attribute.BinaryAttributePropagation;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataPoint;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * In-memory equi-join execution: index the right operand by join keys, then probe from the left
 * (O(n + m) per binary join instead of nested loops).
 */
public final class InMemoryJoinExecutor {

  private InMemoryJoinExecutor() {}

  public static List<DataPoint> innerJoin(
      DataStructure structure,
      DataStructure leftStructure,
      DataStructure rightStructure,
      List<String> leftColumnNames,
      List<String> joinKeyColumns,
      List<DataPoint> leftPoints,
      List<DataPoint> rightPoints) {
    Map<List<Object>, List<DataPoint>> rightIndex = indexByJoinKey(rightPoints, joinKeyColumns);
    List<DataPoint> result = new ArrayList<>();
    for (DataPoint leftPoint : leftPoints) {
      List<DataPoint> matches = rightIndex.get(joinKey(leftPoint, joinKeyColumns));
      if (matches == null || matches.isEmpty()) {
        continue;
      }
      appendMergedRows(
          result, structure, leftStructure, rightStructure, leftColumnNames, leftPoint, matches);
    }
    return result;
  }

  public static List<DataPoint> leftJoin(
      DataStructure structure,
      DataStructure leftStructure,
      DataStructure rightStructure,
      List<String> leftColumnNames,
      List<String> joinKeyColumns,
      List<DataPoint> leftPoints,
      List<DataPoint> rightPoints) {
    Map<List<Object>, List<DataPoint>> rightIndex = indexByJoinKey(rightPoints, joinKeyColumns);
    List<DataPoint> result = new ArrayList<>();
    for (DataPoint leftPoint : leftPoints) {
      List<DataPoint> matches = rightIndex.get(joinKey(leftPoint, joinKeyColumns));
      if (matches == null || matches.isEmpty()) {
        result.add(copyLeftOnly(structure, leftColumnNames, leftPoint));
      } else {
        appendMergedRows(
            result, structure, leftStructure, rightStructure, leftColumnNames, leftPoint, matches);
      }
    }
    return result;
  }

  public static List<DataPoint> crossJoin(
      DataStructure structure,
      DataStructure leftStructure,
      DataStructure rightStructure,
      List<String> leftColumnNames,
      List<DataPoint> leftPoints,
      List<DataPoint> rightPoints) {
    List<DataPoint> result = new ArrayList<>(leftPoints.size() * Math.max(1, rightPoints.size()));
    for (DataPoint leftPoint : leftPoints) {
      for (DataPoint rightPoint : rightPoints) {
        var mergedPoint = new DataPoint(structure);
        for (String leftColumn : leftColumnNames) {
          mergedPoint.set(leftColumn, leftPoint.get(leftColumn));
        }
        BinaryAttributePropagation.applyRightColumns(
            mergedPoint, structure, leftPoint, leftStructure, rightPoint, rightStructure);
        result.add(mergedPoint);
      }
    }
    return result;
  }

  public static DataStructure commonStructure(
      List<Component> identifiers, DatasetExpression left, DatasetExpression right) {
    return JoinStructureBuilder.build(
        identifiers, left.getDataStructure(), right.getDataStructure());
  }

  public static List<String> joinKeyColumnNames(List<Component> identifiers) {
    return identifiers.stream().map(Component::getName).toList();
  }

  private static Map<List<Object>, List<DataPoint>> indexByJoinKey(
      List<DataPoint> points, List<String> joinKeyColumns) {
    Map<List<Object>, List<DataPoint>> index = new HashMap<>();
    for (DataPoint point : points) {
      index.computeIfAbsent(joinKey(point, joinKeyColumns), k -> new ArrayList<>()).add(point);
    }
    return index;
  }

  private static List<Object> joinKey(DataPoint point, List<String> joinKeyColumns) {
    List<Object> key = new ArrayList<>(joinKeyColumns.size());
    for (String column : joinKeyColumns) {
      key.add(point.get(column));
    }
    return key;
  }

  private static DataPoint copyLeftOnly(
      DataStructure structure, List<String> leftColumnNames, DataPoint leftPoint) {
    var mergedPoint = new DataPoint(structure);
    for (String leftColumn : leftColumnNames) {
      mergedPoint.set(leftColumn, leftPoint.get(leftColumn));
    }
    return mergedPoint;
  }

  private static void appendMergedRows(
      List<DataPoint> result,
      DataStructure structure,
      DataStructure leftStructure,
      DataStructure rightStructure,
      List<String> leftColumnNames,
      DataPoint leftPoint,
      List<DataPoint> matches) {
    var mergedPoint = new DataPoint(structure);
    for (String leftColumn : leftColumnNames) {
      mergedPoint.set(leftColumn, leftPoint.get(leftColumn));
    }
    for (DataPoint match : matches) {
      var matchPoint = new DataPoint(structure, (DataPoint) mergedPoint);
      BinaryAttributePropagation.applyRightColumns(
          matchPoint, structure, leftPoint, leftStructure, match, rightStructure);
      result.add(matchPoint);
    }
  }

  /** Package-visible for tests: same equality semantics as the previous join comparator. */
  static boolean joinKeysEqual(DataPoint left, DataPoint right, List<String> joinKeyColumns) {
    for (String column : joinKeyColumns) {
      if (!Objects.equals(left.get(column), right.get(column))) {
        return false;
      }
    }
    return true;
  }
}
