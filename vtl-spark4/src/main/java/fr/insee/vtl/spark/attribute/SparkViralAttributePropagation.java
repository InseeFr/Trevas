package fr.insee.vtl.spark.attribute;

import static fr.insee.vtl.engine.join.JoinProjection.buildTargetStructure;
import static fr.insee.vtl.engine.join.JoinProjection.resolveSourceColumn;
import static fr.insee.vtl.engine.join.JoinProjection.stripJoinAlias;
import static scala.collection.JavaConverters.iterableAsScalaIterable;

import fr.insee.vtl.engine.attribute.ViralAttributeReattachPlan;
import fr.insee.vtl.engine.attribute.ViralColumnMergePlan;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import fr.insee.vtl.spark.SparkDataset;
import fr.insee.vtl.spark.SparkDatasetExpression;
import fr.insee.vtl.spark.SparkUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

/** Spark-native viral attribute propagation (join projection and reattach). */
public final class SparkViralAttributePropagation {

  private SparkViralAttributePropagation() {}

  public static DatasetExpression joinProjection(
      SparkDatasetExpression expression, List<String> outputColumnNames) {
    SparkDataset dataset = expression.resolve(Map.of());
    DataStructure source = dataset.getDataStructure();
    DataStructure target = buildTargetStructure(source, outputColumnNames);

    List<Column> columns = new ArrayList<>(outputColumnNames.size());
    for (String bareName : outputColumnNames) {
      columns.add(projectColumn(source, bareName).as(bareName));
    }

    org.apache.spark.sql.Dataset<Row> projected = selectColumns(dataset.getSparkDataset(), columns);
    return new SparkDatasetExpression(
        new SparkDataset(projected, rolesFromStructure(target)), expression);
  }

  public static DatasetExpression reattachUnary(
      DatasetExpression source,
      SparkDatasetExpression transformed,
      Map<String, Class<?>> outputMeasuresByName) {
    ViralAttributeReattachPlan plan =
        ViralAttributeReattachPlan.unary(source, outputMeasuresByName);
    if (!plan.hasVirals()) {
      return withStructure(transformed, plan.targetStructure());
    }

    org.apache.spark.sql.Dataset<Row> base = transformed.resolve(Map.of()).getSparkDataset();
    org.apache.spark.sql.Dataset<Row> sourceRows = asSpark(source).getSparkDataset();
    List<String> ids = plan.identifierNames();

    for (String viral : plan.viralNames()) {
      String scratch = "__viral_src_" + viral;
      List<Column> selectCols = new ArrayList<>();
      for (String id : ids) {
        selectCols.add(SparkUtils.safeCol(id));
      }
      selectCols.add(SparkUtils.safeCol(viral).as(scratch));
      org.apache.spark.sql.Dataset<Row> side = selectColumns(sourceRows, selectCols);
      base = base.join(side, iterableAsScalaIterable(ids).toSeq(), "left");
      if (Arrays.asList(base.columns()).contains(viral)) {
        base = base.drop(viral);
      }
      base = base.withColumn(viral, SparkUtils.safeCol(scratch)).drop(scratch);
    }

    return new SparkDatasetExpression(
        new SparkDataset(selectStructure(base, plan.targetStructure()), plan.targetStructure()),
        transformed);
  }

  public static DatasetExpression reattachBinary(
      List<DatasetExpression> sources,
      SparkDatasetExpression transformed,
      Map<String, Class<?>> outputMeasuresByName) {
    ViralAttributeReattachPlan plan =
        ViralAttributeReattachPlan.binary(sources, outputMeasuresByName);
    if (!plan.hasVirals()) {
      return reattachUnary(sources.get(0), transformed, outputMeasuresByName);
    }

    org.apache.spark.sql.Dataset<Row> base = transformed.resolve(Map.of()).getSparkDataset();
    List<String> ids = plan.identifierNames();

    for (String viral : plan.viralNames()) {
      Class<?> type = plan.viralType(viral);
      Column merged = null;
      List<String> scratches = new ArrayList<>();
      int index = 0;
      for (DatasetExpression source : sources) {
        Component component = source.getDataStructure().get(viral);
        if (component == null || !component.isViralAttribute()) {
          continue;
        }
        String scratch = "__viral_bin_" + viral + "_" + index++;
        scratches.add(scratch);
        List<Column> selectCols = new ArrayList<>();
        for (String id : ids) {
          selectCols.add(SparkUtils.safeCol(id));
        }
        selectCols.add(SparkUtils.safeCol(viral).as(scratch));
        org.apache.spark.sql.Dataset<Row> side =
            selectColumns(asSpark(source).getSparkDataset(), selectCols);
        base = base.join(side, iterableAsScalaIterable(ids).toSeq(), "left");
        Column part = SparkUtils.safeCol(scratch);
        merged = merged == null ? part : SparkViralColumnExpressions.merge(merged, part, type);
      }
      if (merged != null) {
        if (Arrays.asList(base.columns()).contains(viral)) {
          base = base.drop(viral);
        }
        base = base.withColumn(viral, merged);
        for (String scratch : scratches) {
          base = base.drop(scratch);
        }
      }
    }

    return new SparkDatasetExpression(
        new SparkDataset(selectStructure(base, plan.targetStructure()), plan.targetStructure()),
        transformed);
  }

  public static org.apache.spark.sql.Dataset<Row> collapseHomonymViralColumns(
      org.apache.spark.sql.Dataset<Row> joined, DataStructure structure) {
    Set<String> viralBareNames = new LinkedHashSet<>();
    Map<String, Class<?>> viralTypes = new LinkedHashMap<>();
    for (Component component : structure.values()) {
      if (!component.isViralAttribute()) {
        continue;
      }
      String bare = stripJoinAlias(component.getName());
      viralBareNames.add(bare);
      viralTypes.putIfAbsent(bare, component.getType());
    }
    if (viralBareNames.isEmpty()) {
      return joined;
    }

    Set<String> consumed = new LinkedHashSet<>();
    Map<String, Column> merges = new LinkedHashMap<>();
    for (String bare : viralBareNames) {
      List<String> physical = findPhysicalViralColumns(joined, bare);
      if (physical.size() <= 1) {
        continue;
      }
      merges.put(bare, mergedViralColumn(joined, bare, viralTypes.get(bare)));
      consumed.addAll(physical);
    }
    if (merges.isEmpty()) {
      return joined;
    }

    List<Column> projections = new ArrayList<>();
    for (String col : joined.columns()) {
      if (isConsumedColumn(consumed, col)) {
        continue;
      }
      projections.add(SparkUtils.safeCol(col));
    }
    for (Map.Entry<String, Column> entry : merges.entrySet()) {
      projections.add(entry.getValue().as(entry.getKey()));
    }
    return selectColumns(joined, projections);
  }

  public static Map<String, fr.insee.vtl.model.Dataset.Role> rolesFromStructure(
      DataStructure structure) {
    return structure.values().stream()
        .collect(
            Collectors.toMap(
                Component::getName, Component::getRole, (a, b) -> a, LinkedHashMap::new));
  }

  private static Column mergedViralColumn(
      org.apache.spark.sql.Dataset<Row> joined, String bareName, Class<?> type) {
    List<String> physical = findPhysicalViralColumns(joined, bareName);
    if (physical.isEmpty()) {
      throw new IllegalStateException(
          "join result missing homonym viral column "
              + bareName
              + " in "
              + List.of(joined.columns()));
    }
    Column merged = null;
    for (String column : physical) {
      Column value = SparkUtils.safeCol(column);
      merged = merged == null ? value : SparkViralColumnExpressions.merge(merged, value, type);
    }
    return merged;
  }

  /** Resolves a logical column for {@code select}, merging homonym viral columns when needed. */
  public static Column resolveProjectColumn(
      org.apache.spark.sql.Dataset<Row> joined, DataStructure structure, String logicalName) {
    Component component = structure.get(logicalName);
    if (component != null && component.isViralAttribute()) {
      List<String> physical = findPhysicalViralColumns(joined, logicalName);
      if (physical.size() > 1) {
        return mergedViralColumn(joined, logicalName, component.getType()).as(logicalName);
      }
    }
    List<Component> viralSources = ViralColumnMergePlan.viralSources(structure, logicalName);
    if (viralSources.size() > 1) {
      Class<?> type = viralSources.get(0).getType();
      Column merged = null;
      for (Component viralSource : viralSources) {
        Column value = SparkUtils.safeCol(viralSource.getName());
        merged = merged == null ? value : SparkViralColumnExpressions.merge(merged, value, type);
      }
      return merged.as(logicalName);
    }
    return SparkUtils.safeCol(resolveSourceColumn(structure, logicalName)).as(logicalName);
  }

  private static List<String> findPhysicalViralColumns(
      org.apache.spark.sql.Dataset<Row> joined, String bareName) {
    LinkedHashSet<String> matches = new LinkedHashSet<>();
    for (StructField field : joined.schema().fields()) {
      String name = field.name();
      if (matchesLogicalName(name, bareName)) {
        matches.add(name);
      }
    }
    if (matches.size() <= 1) {
      for (String col : joined.columns()) {
        if (matchesLogicalName(col, bareName)) {
          matches.add(col);
        }
      }
    }
    return new ArrayList<>(matches);
  }

  private static boolean isConsumedColumn(Set<String> consumed, String column) {
    String normalized = normalizedColumnName(column);
    for (String candidate : consumed) {
      if (normalizedColumnName(candidate).equals(normalized)) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesLogicalName(String physicalColumn, String logicalName) {
    String physical = normalizedColumnName(physicalColumn);
    if (physical.equals(logicalName)) {
      return true;
    }
    if (stripJoinAlias(physical).equals(logicalName)) {
      return true;
    }
    int dot = physical.lastIndexOf('.');
    return dot >= 0 && physical.substring(dot + 1).equals(logicalName);
  }

  private static String normalizedColumnName(String physicalColumn) {
    return physicalColumn.replace("`", "");
  }

  private static Column projectColumn(DataStructure source, String bareName) {
    List<Component> viralSources = ViralColumnMergePlan.viralSources(source, bareName);
    if (viralSources.size() <= 1) {
      return SparkUtils.safeCol(resolveSourceColumn(source, bareName));
    }
    Class<?> type = viralSources.get(0).getType();
    Column merged = null;
    for (Component component : viralSources) {
      Column value = SparkUtils.safeCol(component.getName());
      merged = merged == null ? value : SparkViralColumnExpressions.merge(merged, value, type);
    }
    return merged;
  }

  private static org.apache.spark.sql.Dataset<Row> selectStructure(
      org.apache.spark.sql.Dataset<Row> dataset, DataStructure structure) {
    List<Column> columns =
        structure.componentsInOrder().stream().map(c -> SparkUtils.safeCol(c.getName())).toList();
    return selectColumns(dataset, columns);
  }

  private static org.apache.spark.sql.Dataset<Row> selectColumns(
      org.apache.spark.sql.Dataset<Row> dataset, List<Column> columns) {
    return dataset.select(columns.toArray(Column[]::new));
  }

  private static SparkDataset asSpark(DatasetExpression expression) {
    if (expression instanceof SparkDatasetExpression sparkExpression) {
      return sparkExpression.resolve(Map.of());
    }
    throw new IllegalArgumentException(
        "expected SparkDatasetExpression, got " + expression.getClass().getName());
  }

  private static DatasetExpression withStructure(
      SparkDatasetExpression expression, DataStructure structure) {
    SparkDataset dataset = expression.resolve(Map.of());
    return new SparkDatasetExpression(
        new SparkDataset(dataset.getSparkDataset(), rolesFromStructure(structure)), expression);
  }
}
