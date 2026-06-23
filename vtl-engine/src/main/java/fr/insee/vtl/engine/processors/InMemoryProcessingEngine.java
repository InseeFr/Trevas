package fr.insee.vtl.engine.processors;

import static fr.insee.vtl.model.Structured.Component;
import static fr.insee.vtl.model.Structured.DataStructure;
import static java.util.stream.Collectors.toList;

import fr.insee.vtl.engine.semantics.join.InMemoryJoinExecutor;
import fr.insee.vtl.engine.utils.KeyExtractor;
import fr.insee.vtl.engine.utils.MapCollector;
import fr.insee.vtl.model.*;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.script.ScriptEngine;

/**
 * The <code>InMemoryProcessingEngine</code> class is an implementation of a VTL engine that
 * performs all operations in memory.
 */
public class InMemoryProcessingEngine implements ProcessingEngine {

  @Override
  public DatasetExpression executeCalc(
      DatasetExpression expression,
      Map<String, ResolvableExpression> expressions,
      Map<String, Dataset.Role> roles,
      Map<String, String> expressionStrings) {

    // Copy the structure and mutate based on the expressions.
    var newStructure = new DataStructure(expression.getDataStructure());
    for (String columnName : expressions.keySet()) {
      // TODO: refine nullable strategy
      newStructure.put(
          columnName,
          new Dataset.Component(
              columnName, expressions.get(columnName).getType(), roles.get(columnName), true));
    }
    newStructure.reindexKeys();

    return new DatasetExpression(expression) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        var dataset = expression.resolve(context);
        List<DataPoint> result =
            dataset.getDataPoints().stream()
                .map(
                    dataPoint -> {
                      var newDataPoint = new DataPoint(newStructure, dataPoint);
                      for (String columnName : expressions.keySet()) {
                        newDataPoint.set(
                            columnName, expressions.get(columnName).resolve(dataPoint));
                      }
                      return newDataPoint;
                    })
                .collect(toList());
        return InMemoryDataset.ofDataPoints(result, newStructure);
      }

      @Override
      public DataStructure getDataStructure() {
        return newStructure;
      }
    };
  }

  @Override
  public DatasetExpression executeFilter(
      DatasetExpression expression, ResolvableExpression filter, String filterText) {
    return new DatasetExpression(expression) {

      @Override
      public DataStructure getDataStructure() {
        return expression.getDataStructure();
      }

      @Override
      public Dataset resolve(Map<String, Object> context) {
        Dataset resolve = expression.resolve(context);
        List<DataPoint> result =
            resolve.getDataPoints().stream()
                .filter(
                    map -> {
                      var res = filter.resolve(map);
                      if (res == null) return false;
                      return (boolean) res;
                    })
                .collect(toList());
        return InMemoryDataset.ofDataPoints(result, getDataStructure());
      }
    };
  }

  @Override
  public DatasetExpression executeRename(DatasetExpression expression, Map<String, String> fromTo) {
    if (fromTo.isEmpty()) {
      return expression;
    }
    Map<String, Component> components = new LinkedHashMap<>();
    for (Component component : expression.getDataStructure().values()) {
      String name = fromTo.getOrDefault(component.getName(), component.getName());
      components.put(
          name,
          name.equals(component.getName())
              ? component
              : new Component(
                  name, component.getType(), component.getRole(), component.getNullable()));
    }
    DataStructure renamedStructure = new DataStructure(components.values());
    return new DatasetExpression(expression) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        DataStructure sourceStructure = expression.getDataStructure();
        List<DataPoint> result =
            expression.resolve(context).getDataPoints().stream()
                .map(
                    dataPoint -> {
                      var newDataPoint = new DataPoint(renamedStructure);
                      for (Component component : sourceStructure.values()) {
                        String from = component.getName();
                        String to = fromTo.getOrDefault(from, from);
                        if (renamedStructure.containsKey(to)) {
                          newDataPoint.set(to, dataPoint.get(from));
                        }
                      }
                      return newDataPoint;
                    })
                .collect(toList());
        return InMemoryDataset.ofDataPoints(result, getDataStructure());
      }

      @Override
      public DataStructure getDataStructure() {
        return renamedStructure;
      }
    };
  }

  @Override
  public DatasetExpression executeProject(DatasetExpression expression, List<String> columnNames) {
    DataStructure source = expression.getDataStructure();
    var structure =
        columnNames.stream().map(source::get).filter(Objects::nonNull).collect(toList());
    var newStructure = new DataStructure(structure);

    return new DatasetExpression(expression) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        var columnNames = getColumnNames();
        List<DataPoint> result =
            expression.resolve(context).getDataPoints().stream()
                .map(
                    data -> {
                      var projectedDataPoint = new DataPoint(newStructure);
                      for (String column : columnNames) {
                        projectedDataPoint.set(column, data.get(column));
                      }
                      return projectedDataPoint;
                    })
                .collect(toList());
        return InMemoryDataset.ofDataPoints(result, getDataStructure());
      }

      @Override
      public DataStructure getDataStructure() {
        return newStructure;
      }
    };
  }

  @Override
  public DatasetExpression executeUnion(
      List<DatasetExpression> datasets, List<String> dedupeOnColumns) {
    return new DatasetExpression(datasets.get(0)) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        List<DataPoint> data =
            datasets.stream()
                .flatMap(ds -> ds.resolve(context).getDataPoints().stream())
                .collect(toList());
        if (!dedupeOnColumns.isEmpty()) {
          Set<List<Object>> seen = new LinkedHashSet<>();
          data =
              data.stream()
                  .filter(
                      point -> {
                        List<Object> key =
                            dedupeOnColumns.stream().map(point::get).collect(toList());
                        return seen.add(key);
                      })
                  .collect(toList());
        }
        return InMemoryDataset.ofDataPoints(data, getDataStructure());
      }

      @Override
      public DataStructure getDataStructure() {
        return datasets.get(0).getDataStructure();
      }
    };
  }

  @Override
  public DatasetExpression executeAggr(
      DatasetExpression expression,
      List<String> groupBy,
      Map<String, AggregationExpression> collectorMap) {
    DataStructure inputStructure = expression.getDataStructure();
    DataStructure outputStructure =
        AggregationOutputStructure.mechanical(inputStructure, groupBy, collectorMap);
    DataStructure collectorStructure = collectorOnlyStructure(outputStructure, collectorMap);
    var keyExtractor = new KeyExtractor(groupBy);

    return new DatasetExpression(expression) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        List<DataPoint> data = expression.resolve(Map.of()).getDataPoints();
        MapCollector collector = new MapCollector(collectorStructure, collectorMap);
        List<DataPoint> collect =
            data.stream()
                .collect(Collectors.groupingBy(keyExtractor, collector))
                .entrySet()
                .stream()
                .map(
                    e -> {
                      DataPoint aggregated = e.getValue();
                      DataPoint resultPoint = new DataPoint(outputStructure);
                      for (String key : groupBy) {
                        resultPoint.set(key, e.getKey().get(key));
                      }
                      for (String column : collectorMap.keySet()) {
                        resultPoint.set(column, aggregated.get(column));
                      }
                      return resultPoint;
                    })
                .collect(toList());

        return InMemoryDataset.ofDataPoints(collect, outputStructure);
      }

      @Override
      public DataStructure getDataStructure() {
        return outputStructure;
      }
    };
  }

  private static DataStructure collectorOnlyStructure(
      DataStructure outputStructure, Map<String, AggregationExpression> collectorMap) {
    List<Component> components =
        collectorMap.keySet().stream().map(outputStructure::get).filter(Objects::nonNull).toList();
    return new DataStructure(components);
  }

  @Override
  public DatasetExpression executeSimpleAnalytic(
      DatasetExpression dataset,
      String targetColumnName,
      Analytics.Function function,
      String columnName,
      List<String> partitionBy,
      Map<String, Analytics.Order> orderBy,
      Analytics.WindowSpec window) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatasetExpression executeLeadOrLagAn(
      DatasetExpression dataset,
      String targetColumnName,
      Analytics.Function function,
      String columnName,
      int offset,
      List<String> partitionBy,
      Map<String, Analytics.Order> orderBy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatasetExpression executeRatioToReportAn(
      DatasetExpression dataset,
      String targetColumnName,
      Analytics.Function function,
      String columnName,
      List<String> partitionBy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatasetExpression executeRankAn(
      DatasetExpression dataset,
      String targetColumnName,
      Analytics.Function function,
      List<String> partitionBy,
      Map<String, Analytics.Order> orderBy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatasetExpression executeLeftJoin(
      Map<String, DatasetExpression> datasets, List<Component> components) {
    var iterator = datasets.values().iterator();
    var leftMost = iterator.next();
    while (iterator.hasNext()) {
      leftMost = handleLeftJoin(components, leftMost, iterator.next());
    }
    return leftMost;
  }

  @Override
  public DatasetExpression executeInnerJoin(
      Map<String, DatasetExpression> datasets, List<Component> components) {
    var iterator = datasets.values().iterator();
    var leftMost = iterator.next();
    while (iterator.hasNext()) {
      leftMost = handleInnerJoin(components, leftMost, iterator.next());
    }
    return leftMost;
  }

  @Override
  public DatasetExpression executeCrossJoin(
      Map<String, DatasetExpression> datasets, List<Component> identifiers) {
    var iterator = datasets.values().iterator();
    var leftMost = iterator.next();
    while (iterator.hasNext()) {
      leftMost = handleCrossJoin(identifiers, leftMost, iterator.next());
    }
    return leftMost;
  }

  @Override
  public DatasetExpression executeFullJoin(
      Map<String, DatasetExpression> datasets, List<Component> identifiers) {
    var iterator = datasets.values().iterator();
    var leftMost = iterator.next();
    while (iterator.hasNext()) {
      leftMost = handleFullJoin(identifiers, leftMost, iterator.next());
    }
    return leftMost;
  }

  private DatasetExpression handleInnerJoin(
      List<Component> identifiers, DatasetExpression left, DatasetExpression right) {
    var structure = InMemoryJoinExecutor.commonStructure(identifiers, left, right);
    var joinKeys = InMemoryJoinExecutor.joinKeyColumnNames(identifiers);
    var leftColumns = left.getColumnNames();

    return new DatasetExpression(left) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        var leftStructure = left.getDataStructure();
        var rightStructure = right.getDataStructure();
        List<DataPoint> result =
            InMemoryJoinExecutor.innerJoin(
                structure,
                leftStructure,
                rightStructure,
                leftColumns,
                joinKeys,
                left.resolve(context).getDataPoints(),
                right.resolve(context).getDataPoints());
        return InMemoryDataset.ofDataPoints(result, structure);
      }

      @Override
      public DataStructure getDataStructure() {
        return structure;
      }
    };
  }

  private DatasetExpression handleFullJoin(
      List<Component> identifiers, DatasetExpression left, DatasetExpression right) {
    List<String> dedupeOn = InMemoryJoinExecutor.joinKeyColumnNames(identifiers);
    return executeUnion(
        List.of(handleLeftJoin(identifiers, left, right), handleLeftJoin(identifiers, right, left)),
        dedupeOn);
  }

  private DatasetExpression handleLeftJoin(
      List<Component> identifiers, DatasetExpression left, DatasetExpression right) {
    var structure = InMemoryJoinExecutor.commonStructure(identifiers, left, right);
    var joinKeys = InMemoryJoinExecutor.joinKeyColumnNames(identifiers);
    var leftColumns = left.getColumnNames();

    return new DatasetExpression(left) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        var leftStructure = left.getDataStructure();
        var rightStructure = right.getDataStructure();
        List<DataPoint> result =
            InMemoryJoinExecutor.leftJoin(
                structure,
                leftStructure,
                rightStructure,
                leftColumns,
                joinKeys,
                left.resolve(context).getDataPoints(),
                right.resolve(context).getDataPoints());
        return InMemoryDataset.ofDataPoints(result, structure);
      }

      @Override
      public DataStructure getDataStructure() {
        return structure;
      }
    };
  }

  private DatasetExpression handleCrossJoin(
      List<Component> identifiers, DatasetExpression left, DatasetExpression right) {
    var structure = InMemoryJoinExecutor.commonStructure(identifiers, left, right);
    var leftColumns = left.getColumnNames();

    return new DatasetExpression(left) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        var rightStructure = right.getDataStructure();
        List<DataPoint> result =
            InMemoryJoinExecutor.crossJoin(
                structure,
                rightStructure,
                leftColumns,
                left.resolve(context).getDataPoints(),
                right.resolve(context).getDataPoints());
        return InMemoryDataset.ofDataPoints(result, structure);
      }

      @Override
      public DataStructure getDataStructure() {
        return structure;
      }
    };
  }

  @Override
  public DatasetExpression executePivot(
      DatasetExpression dataset, String idName, String meName, Positioned pos) {
    throw new UnsupportedOperationException();
  }

  /**
   * The <code>Factory</code> class is an implementation of a VTL engine factory that returns
   * in-memory engines.
   */
  public static class Factory implements ProcessingEngineFactory {

    @Override
    public String getName() {
      return "memory";
    }

    @Override
    public ProcessingEngine getProcessingEngine(ScriptEngine engine) {
      return new InMemoryProcessingEngine();
    }
  }
}
